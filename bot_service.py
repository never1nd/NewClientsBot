#!/usr/bin/env python3
"""
NewClientsBot: Telegram notifications for new leads.

Modes:
- mysql: poll website MySQL directly (recommended for InfinityFree)
- feed: poll website HTTP endpoint
- auto: mysql if DB env is present, otherwise feed
"""

from __future__ import annotations

import hashlib
import json
import os
import signal
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
from urllib.request import Request, urlopen

try:
    import pymysql  # type: ignore
except Exception:  # noqa: BLE001
    pymysql = None  # type: ignore


def load_dotenv_file(path: str) -> None:
    if not os.path.isfile(path):
        return

    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if not key or key in os.environ:
                    continue
                value = value.strip().strip('"').strip("'")
                os.environ[key] = value
    except Exception as exc:  # noqa: BLE001
        print(f"[env] failed to load .env: {exc}", flush=True)


def env_int(name: str, default: int, minimum: int | None = None, maximum: int | None = None) -> int:
    raw = os.getenv(name, "").strip()
    if raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    if minimum is not None and value < minimum:
        value = minimum
    if maximum is not None and value > maximum:
        value = maximum
    return value


BASE_DIR = os.path.dirname(__file__)
load_dotenv_file(os.path.join(BASE_DIR, ".env"))

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_ADMIN_IDS_RAW = os.getenv("BOT_ADMIN_IDS", "").strip()
BOT_DB_PATH = os.getenv("BOT_DB_PATH", os.path.join(BASE_DIR, "bot_state.sqlite"))
BOT_POLL_TIMEOUT = env_int("BOT_POLL_TIMEOUT", 25, minimum=5, maximum=60)

LEADS_SOURCE = os.getenv("LEADS_SOURCE", "auto").strip().lower() or "auto"

LEADS_FEED_URL = os.getenv("LEADS_FEED_URL", "").strip()
LEADS_FEED_SECRET = os.getenv("LEADS_FEED_SECRET", "").strip()
LEADS_POLL_INTERVAL = env_int("LEADS_POLL_INTERVAL", 10, minimum=1, maximum=300)
LEADS_BATCH_SIZE = env_int("LEADS_BATCH_SIZE", 20, minimum=1, maximum=100)
LEADS_HTTP_TIMEOUT = env_int("LEADS_HTTP_TIMEOUT", 20, minimum=3, maximum=120)

LEADS_DB_HOST = os.getenv("LEADS_DB_HOST", "").strip()
LEADS_DB_PORT = env_int("LEADS_DB_PORT", 3306, minimum=1, maximum=65535)
LEADS_DB_NAME = os.getenv("LEADS_DB_NAME", "").strip()
LEADS_DB_USER = os.getenv("LEADS_DB_USER", "").strip()
LEADS_DB_PASS = os.getenv("LEADS_DB_PASS", "").strip()
LEADS_DB_CHARSET = os.getenv("LEADS_DB_CHARSET", "utf8mb4").strip() or "utf8mb4"
LEADS_DB_CONNECT_TIMEOUT = env_int("LEADS_DB_CONNECT_TIMEOUT", 8, minimum=3, maximum=60)

HAS_DB_CONFIG = all([LEADS_DB_HOST, LEADS_DB_NAME, LEADS_DB_USER, LEADS_DB_PASS])

if LEADS_SOURCE not in {"auto", "mysql", "feed"}:
    LEADS_SOURCE = "auto"

if LEADS_SOURCE == "auto":
    LEADS_SOURCE = "mysql" if HAS_DB_CONFIG else "feed"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")

if LEADS_SOURCE == "mysql":
    if not HAS_DB_CONFIG:
        raise RuntimeError("MySQL mode selected but LEADS_DB_* variables are incomplete")
    if pymysql is None:
        raise RuntimeError("pymysql is required for MySQL mode (add to requirements.txt)")
else:
    if not LEADS_FEED_URL:
        raise RuntimeError("LEADS_FEED_URL is required in feed mode")
    if not LEADS_FEED_SECRET:
        raise RuntimeError("LEADS_FEED_SECRET is required in feed mode")


def parse_admin_ids(raw: str) -> set[int]:
    ids: set[int] = set()
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            ids.add(int(chunk))
        except ValueError:
            continue
    return ids


ADMIN_IDS = parse_admin_ids(BOT_ADMIN_IDS_RAW)
TELEGRAM_API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
STOP_EVENT = threading.Event()


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class Storage:
    def __init__(self, path: str) -> None:
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.lock = threading.Lock()
        self._init_db()

    def _init_db(self) -> None:
        with self.lock:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS subscribers (
                    chat_id INTEGER PRIMARY KEY,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    added_at TEXT NOT NULL,
                    added_by INTEGER,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS seen_leads (
                    fingerprint TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    meta_key TEXT PRIMARY KEY,
                    meta_value TEXT NOT NULL
                )
                """
            )
            self.conn.commit()

    def set_subscriber(self, chat_id: int, enabled: bool, added_by: int | None) -> None:
        now = now_iso()
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO subscribers (chat_id, enabled, added_at, added_by, updated_at)
                VALUES (:chat_id, :enabled, :added_at, :added_by, :updated_at)
                ON CONFLICT(chat_id) DO UPDATE SET
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at,
                    added_by = COALESCE(subscribers.added_by, excluded.added_by)
                """,
                {
                    "chat_id": chat_id,
                    "enabled": 1 if enabled else 0,
                    "added_at": now,
                    "added_by": added_by,
                    "updated_at": now,
                },
            )
            self.conn.commit()

    def disable_subscriber(self, chat_id: int) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE subscribers SET enabled = 0, updated_at = :updated_at WHERE chat_id = :chat_id",
                {"chat_id": chat_id, "updated_at": now_iso()},
            )
            self.conn.commit()

    def list_subscribers(self, enabled_only: bool = False) -> list[tuple[int, int]]:
        query = "SELECT chat_id, enabled FROM subscribers"
        if enabled_only:
            query += " WHERE enabled = 1"
        query += " ORDER BY chat_id"
        with self.lock:
            rows = self.conn.execute(query).fetchall()
        return [(int(row[0]), int(row[1])) for row in rows]

    def mark_seen(self, fingerprint: str) -> bool:
        with self.lock:
            try:
                self.conn.execute(
                    "INSERT INTO seen_leads (fingerprint, created_at) VALUES (:fingerprint, :created_at)",
                    {"fingerprint": fingerprint, "created_at": now_iso()},
                )
                self.conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def _get_meta_int(self, key: str, default: int = 0) -> int:
        with self.lock:
            row = self.conn.execute(
                "SELECT meta_value FROM meta WHERE meta_key = :meta_key LIMIT 1",
                {"meta_key": key},
            ).fetchone()
        if row is None:
            return default
        try:
            return int(row[0])
        except (TypeError, ValueError):
            return default

    def _set_meta_int(self, key: str, value: int) -> None:
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO meta (meta_key, meta_value)
                VALUES (:meta_key, :meta_value)
                ON CONFLICT(meta_key) DO UPDATE SET meta_value = excluded.meta_value
                """,
                {"meta_key": key, "meta_value": str(value)},
            )
            self.conn.commit()

    def get_update_offset(self) -> int:
        return self._get_meta_int("update_offset", 0)

    def set_update_offset(self, offset: int) -> None:
        self._set_meta_int("update_offset", offset)

    def get_last_lead_id(self) -> int:
        return self._get_meta_int("last_lead_id", 0)

    def set_last_lead_id(self, last_id: int) -> None:
        self._set_meta_int("last_lead_id", last_id)


STORAGE = Storage(BOT_DB_PATH)


class MySqlLeadSource:
    def __init__(self) -> None:
        self.conn: Any = None

    def _connect(self) -> None:
        self.conn = pymysql.connect(  # type: ignore[operator]
            host=LEADS_DB_HOST,
            port=LEADS_DB_PORT,
            user=LEADS_DB_USER,
            password=LEADS_DB_PASS,
            database=LEADS_DB_NAME,
            charset=LEADS_DB_CHARSET,
            connect_timeout=LEADS_DB_CONNECT_TIMEOUT,
            read_timeout=LEADS_HTTP_TIMEOUT,
            write_timeout=LEADS_HTTP_TIMEOUT,
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,  # type: ignore[union-attr]
        )

    def _ensure_conn(self) -> None:
        if self.conn is None:
            self._connect()
            return
        try:
            self.conn.ping(reconnect=True)
        except Exception:
            self.close()
            self._connect()

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None

    def fetch(self, after_id: int, limit: int) -> tuple[list[dict[str, Any]], int]:
        self._ensure_conn()

        query = (
            "SELECT id, name, contact, message, source_page, consent, ip, created_at "
            "FROM leads WHERE id > %s ORDER BY id ASC LIMIT %s"
        )

        max_id = after_id
        leads: list[dict[str, Any]] = []

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (after_id, limit))
                rows = cursor.fetchall()
        except Exception:
            self.close()
            raise

        for row in rows:
            lead_id = int(row.get("id") or 0)
            if lead_id > max_id:
                max_id = lead_id

            created = row.get("created_at")
            if isinstance(created, datetime):
                created_at = created.strftime("%Y-%m-%d %H:%M:%S")
            else:
                created_at = str(created or "")

            leads.append(
                {
                    "id": lead_id,
                    "name": str(row.get("name") or "")[:120],
                    "contact": str(row.get("contact") or "")[:180],
                    "message": str(row.get("message") or "")[:1200],
                    "source_page": str(row.get("source_page") or "")[:160],
                    "created_at": created_at,
                    "site_host": "",
                    "ip": str(row.get("ip") or "")[:64],
                }
            )

        return leads, max_id


MYSQL_SOURCE = MySqlLeadSource() if LEADS_SOURCE == "mysql" else None


def is_admin(chat_id: int) -> bool:
    return chat_id in ADMIN_IDS


def telegram_request(method: str, payload: dict[str, Any] | None = None, timeout: int = 35) -> dict[str, Any]:
    url = f"{TELEGRAM_API_BASE}/{method}"
    data = None
    if payload is not None:
        data = urlencode(payload).encode("utf-8")

    request = Request(url=url, data=data, method="POST" if data is not None else "GET")
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
        parsed = json.loads(body)
        if isinstance(parsed, dict):
            return parsed
        return {"ok": False, "description": "Telegram API returned non-object JSON"}
    except HTTPError as exc:
        try:
            err = exc.read().decode("utf-8", errors="replace")
        except Exception:
            err = str(exc)
        return {"ok": False, "description": f"HTTPError {exc.code}: {err}"}
    except URLError as exc:
        return {"ok": False, "description": f"URLError: {exc}"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "description": f"Unexpected error: {exc}"}


def send_message(chat_id: int, text: str) -> dict[str, Any]:
    return telegram_request(
        "sendMessage",
        {
            "chat_id": str(chat_id),
            "text": text,
            "disable_web_page_preview": "true",
        },
    )


def command_help() -> str:
    return (
        "Команды:\n"
        "/start - помощь\n"
        "/myid - показать ваш Telegram ID\n"
        "/on - включить уведомления для себя\n"
        "/off - отключить уведомления для себя\n"
        "/add <id> - (админ) подключить пользователя\n"
        "/remove <id> - (админ) отключить пользователя\n"
        "/list - (админ) список подписчиков"
    )


def parse_target_id(parts: list[str]) -> int | None:
    if len(parts) < 2:
        return None
    try:
        return int(parts[1].strip())
    except ValueError:
        return None


def handle_command(chat_id: int, text: str) -> None:
    parts = text.strip().split(maxsplit=1)
    if not parts:
        return

    cmd = parts[0].split("@", 1)[0].lower()

    if cmd in {"/start", "/help"}:
        send_message(chat_id, command_help())
        return

    if cmd == "/myid":
        send_message(chat_id, f"Ваш Telegram ID: {chat_id}")
        return

    if cmd == "/on":
        STORAGE.set_subscriber(chat_id, True, chat_id)
        send_message(chat_id, "Уведомления включены для этого ID.")
        return

    if cmd == "/off":
        STORAGE.set_subscriber(chat_id, False, chat_id)
        send_message(chat_id, "Уведомления отключены для этого ID.")
        return

    if cmd == "/add":
        if not is_admin(chat_id):
            send_message(chat_id, "Команда доступна только администратору.")
            return
        target = parse_target_id(text.strip().split())
        if target is None:
            send_message(chat_id, "Использование: /add <telegram_id>")
            return
        STORAGE.set_subscriber(target, True, chat_id)
        send_message(chat_id, f"ID {target} подключен к уведомлениям.")
        return

    if cmd == "/remove":
        if not is_admin(chat_id):
            send_message(chat_id, "Команда доступна только администратору.")
            return
        target = parse_target_id(text.strip().split())
        if target is None:
            send_message(chat_id, "Использование: /remove <telegram_id>")
            return
        STORAGE.set_subscriber(target, False, chat_id)
        send_message(chat_id, f"ID {target} отключен от уведомлений.")
        return

    if cmd == "/list":
        if not is_admin(chat_id):
            send_message(chat_id, "Команда доступна только администратору.")
            return
        rows = STORAGE.list_subscribers(enabled_only=False)
        if not rows:
            send_message(chat_id, "Подписчиков пока нет.")
            return
        lines = ["Подписчики:"]
        for row_chat_id, enabled in rows:
            lines.append(f"- {row_chat_id} ({'ON' if enabled else 'OFF'})")
        send_message(chat_id, "\n".join(lines))
        return


def lead_fingerprint(lead: dict[str, Any]) -> str:
    if lead.get("id") not in (None, ""):
        return f"id:{lead['id']}"

    raw = "|".join(
        [
            str(lead.get("name", "")),
            str(lead.get("contact", "")),
            str(lead.get("message", "")),
            str(lead.get("created_at", "")),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()


def normalize_lead(payload: dict[str, Any]) -> dict[str, Any]:
    def read(key: str, max_len: int) -> str:
        value = str(payload.get(key, "")).strip()
        return value[:max_len]

    lead_id: int | None
    try:
        lead_id = int(payload.get("id"))
    except Exception:  # noqa: BLE001
        lead_id = None

    return {
        "id": lead_id,
        "name": read("name", 120),
        "contact": read("contact", 180),
        "message": read("message", 1200),
        "source_page": read("source_page", 160),
        "created_at": read("created_at", 64) or now_iso(),
        "site_host": read("site_host", 120),
        "ip": read("ip", 64),
    }


def format_lead_message(lead: dict[str, Any]) -> str:
    parts = [
        "Новая заявка",
        f"ID: {lead.get('id') if lead.get('id') is not None else '-'}",
        f"Имя: {lead.get('name') or '-'}",
        f"Контакт: {lead.get('contact') or '-'}",
        f"Комментарий: {lead.get('message') or '-'}",
        f"Источник: {lead.get('source_page') or '-'}",
        f"Сайт: {lead.get('site_host') or '-'}",
        f"IP: {lead.get('ip') or '-'}",
        f"Время: {lead.get('created_at') or '-'}",
    ]
    return "\n".join(parts)


def notify_subscribers(lead: dict[str, Any]) -> dict[str, Any]:
    fingerprint = lead_fingerprint(lead)
    if not STORAGE.mark_seen(fingerprint):
        return {"ok": True, "duplicate": True, "sent": 0, "total": 0}

    rows = STORAGE.list_subscribers(enabled_only=True)
    if not rows:
        return {"ok": True, "duplicate": False, "sent": 0, "total": 0}

    text = format_lead_message(lead)
    sent = 0
    failed = 0

    for chat_id, _ in rows:
        response = send_message(chat_id, text)
        if response.get("ok"):
            sent += 1
            continue

        failed += 1
        description = str(response.get("description", "")).lower()
        if "blocked" in description or "chat not found" in description:
            STORAGE.disable_subscriber(chat_id)

    return {"ok": True, "duplicate": False, "sent": sent, "failed": failed, "total": len(rows)}


def process_update(update: dict[str, Any]) -> None:
    message = update.get("message")
    if not isinstance(message, dict):
        return

    chat = message.get("chat")
    if not isinstance(chat, dict):
        return

    chat_id = chat.get("id")
    text = message.get("text")
    if not isinstance(chat_id, int) or not isinstance(text, str):
        return

    if text.startswith("/"):
        handle_command(chat_id, text)


def poll_updates_forever() -> None:
    offset = STORAGE.get_update_offset()

    while not STOP_EVENT.is_set():
        response = telegram_request(
            "getUpdates",
            {
                "timeout": str(BOT_POLL_TIMEOUT),
                "offset": str(offset),
                "allowed_updates": json.dumps(["message"]),
            },
            timeout=max(BOT_POLL_TIMEOUT + 10, 30),
        )

        if not response.get("ok"):
            print(f"[tg] getUpdates error: {response.get('description', 'unknown')}", flush=True)
            time.sleep(3)
            continue

        updates = response.get("result", [])
        if not isinstance(updates, list):
            time.sleep(1)
            continue

        for update in updates:
            if not isinstance(update, dict):
                continue

            update_id = update.get("update_id")
            if isinstance(update_id, int):
                offset = update_id + 1
                STORAGE.set_update_offset(offset)

            process_update(update)


def build_feed_url(after_id: int, limit: int) -> str:
    parsed = urlparse(LEADS_FEED_URL)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["after_id"] = str(max(0, after_id))
    query["limit"] = str(max(1, limit))
    new_query = urlencode(query)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def fetch_leads_from_feed(after_id: int, limit: int) -> tuple[list[dict[str, Any]], int]:
    url = build_feed_url(after_id, limit)
    request = Request(url=url, method="GET", headers={"X-Bot-Secret": LEADS_FEED_SECRET})

    with urlopen(request, timeout=LEADS_HTTP_TIMEOUT) as response:
        raw = response.read().decode("utf-8", errors="replace")

    payload = json.loads(raw)
    if not isinstance(payload, dict) or not payload.get("ok"):
        raise RuntimeError(f"Feed returned non-ok response: {str(payload)[:300]}")

    leads_raw = payload.get("leads", [])
    if not isinstance(leads_raw, list):
        leads_raw = []

    leads: list[dict[str, Any]] = []
    max_id = after_id

    for item in leads_raw:
        if not isinstance(item, dict):
            continue

        lead = normalize_lead(item)
        leads.append(lead)

        lead_id = lead.get("id")
        if isinstance(lead_id, int) and lead_id > max_id:
            max_id = lead_id

    payload_max_id = payload.get("max_id")
    if isinstance(payload_max_id, int) and payload_max_id > max_id:
        max_id = payload_max_id

    return leads, max_id


def fetch_leads(after_id: int, limit: int) -> tuple[list[dict[str, Any]], int]:
    if LEADS_SOURCE == "mysql":
        assert MYSQL_SOURCE is not None
        leads, max_id = MYSQL_SOURCE.fetch(after_id, limit)
        return [normalize_lead(lead) for lead in leads], max_id

    return fetch_leads_from_feed(after_id, limit)


def poll_leads_forever() -> None:
    after_id = STORAGE.get_last_lead_id()

    while not STOP_EVENT.is_set():
        try:
            leads, max_id = fetch_leads(after_id=after_id, limit=LEADS_BATCH_SIZE)

            if leads:
                print(f"[leads] fetched {len(leads)} new lead(s), after_id={after_id} -> max_id={max_id}", flush=True)

            for lead in leads:
                result = notify_subscribers(lead)
                if not result.get("duplicate"):
                    print(
                        f"[notify] lead_id={lead.get('id')} sent={result.get('sent', 0)}/{result.get('total', 0)} failed={result.get('failed', 0)}",
                        flush=True,
                    )

            if max_id > after_id:
                after_id = max_id
                STORAGE.set_last_lead_id(after_id)

            if len(leads) >= LEADS_BATCH_SIZE:
                continue

            STOP_EVENT.wait(LEADS_POLL_INTERVAL)
        except Exception as exc:  # noqa: BLE001
            print(f"[leads] error: {exc}", flush=True)
            STOP_EVENT.wait(max(LEADS_POLL_INTERVAL, 3))


def bootstrap_admin_subscribers() -> None:
    if not ADMIN_IDS:
        return
    for admin_id in ADMIN_IDS:
        STORAGE.set_subscriber(admin_id, True, admin_id)


def shutdown(signum: int, frame: Any) -> None:  # noqa: ARG001
    STOP_EVENT.set()
    if MYSQL_SOURCE is not None:
        MYSQL_SOURCE.close()


def main() -> None:
    print("NewClientsBot starting...", flush=True)
    print(f"Source mode: {LEADS_SOURCE}", flush=True)
    print(f"Admins: {sorted(ADMIN_IDS) if ADMIN_IDS else 'not set'}", flush=True)

    if LEADS_SOURCE == "feed":
        print(f"Feed URL: {LEADS_FEED_URL}", flush=True)
    else:
        print(f"DB host: {LEADS_DB_HOST}:{LEADS_DB_PORT}", flush=True)
        print(f"DB name: {LEADS_DB_NAME}", flush=True)
        print(f"DB user: {LEADS_DB_USER}", flush=True)

    bootstrap_admin_subscribers()
    print(f"Enabled subscribers: {len(STORAGE.list_subscribers(enabled_only=True))}", flush=True)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    leads_thread = threading.Thread(target=poll_leads_forever, daemon=True)
    leads_thread.start()

    poll_updates_forever()


if __name__ == "__main__":
    main()
