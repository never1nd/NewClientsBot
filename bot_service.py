#!/usr/bin/env python3
"""
NewClientsBot: Telegram notifications for new leads.

Mode: pull
- Bot polls Telegram updates (long polling) for commands.
- Bot polls website feed endpoint for new leads.
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


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_ADMIN_IDS_RAW = os.getenv("BOT_ADMIN_IDS", "").strip()
BOT_DB_PATH = os.getenv("BOT_DB_PATH", os.path.join(os.path.dirname(__file__), "bot_state.sqlite"))
BOT_POLL_TIMEOUT = int(os.getenv("BOT_POLL_TIMEOUT", "25"))

LEADS_FEED_URL = os.getenv("LEADS_FEED_URL", "").strip()
LEADS_FEED_SECRET = os.getenv("LEADS_FEED_SECRET", "").strip()
LEADS_POLL_INTERVAL = max(1, int(os.getenv("LEADS_POLL_INTERVAL", "10")))
LEADS_BATCH_SIZE = max(1, min(100, int(os.getenv("LEADS_BATCH_SIZE", "20"))))
LEADS_HTTP_TIMEOUT = max(3, int(os.getenv("LEADS_HTTP_TIMEOUT", "20")))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")

if not LEADS_FEED_URL:
    raise RuntimeError("LEADS_FEED_URL is required")

if not LEADS_FEED_SECRET:
    raise RuntimeError("LEADS_FEED_SECRET is required")


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


def fetch_leads(after_id: int, limit: int) -> tuple[list[dict[str, Any]], int]:
    url = build_feed_url(after_id, limit)
    request = Request(url=url, method="GET", headers={"X-Bot-Secret": LEADS_FEED_SECRET})

    with urlopen(request, timeout=LEADS_HTTP_TIMEOUT) as response:
        raw = response.read().decode("utf-8", errors="replace")

    payload = json.loads(raw)
    if not isinstance(payload, dict) or not payload.get("ok"):
        raise RuntimeError("Feed returned non-ok response")

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


def poll_leads_forever() -> None:
    after_id = STORAGE.get_last_lead_id()

    while not STOP_EVENT.is_set():
        try:
            leads, max_id = fetch_leads(after_id=after_id, limit=LEADS_BATCH_SIZE)

            for lead in leads:
                notify_subscribers(lead)

            if max_id > after_id:
                after_id = max_id
                STORAGE.set_last_lead_id(after_id)

            if len(leads) >= LEADS_BATCH_SIZE:
                continue

            STOP_EVENT.wait(LEADS_POLL_INTERVAL)
        except Exception:  # noqa: BLE001
            STOP_EVENT.wait(max(LEADS_POLL_INTERVAL, 3))


def shutdown(signum: int, frame: Any) -> None:  # noqa: ARG001
    STOP_EVENT.set()


def main() -> None:
    print("NewClientsBot starting...")
    print(f"Admins: {sorted(ADMIN_IDS) if ADMIN_IDS else 'not set'}")
    print(f"Feed URL: {LEADS_FEED_URL}")

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    leads_thread = threading.Thread(target=poll_leads_forever, daemon=True)
    leads_thread.start()

    poll_updates_forever()


if __name__ == "__main__":
    main()
