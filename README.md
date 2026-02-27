# NewClientsBot

Python bot for lead notifications from the website.

## What it does
- Polls Telegram for commands (no webhook needed).
- Polls site feed endpoint for new leads.
- Sends notifications to subscribed Telegram IDs.

## Required environment variables
- `BOT_TOKEN` - Telegram bot token.
- `LEADS_FEED_URL` - website endpoint URL (for example `https://imagedirect.ct.ws/bot-leads-feed.php`).
- `LEADS_FEED_SECRET` - shared secret, must match `BOT_FEED_SECRET` on website.

## Optional environment variables
- `BOT_ADMIN_IDS` - comma-separated Telegram IDs for `/add`, `/remove`, `/list`.
- `BOT_DB_PATH` - sqlite path, default `./bot_state.sqlite`.
- `BOT_POLL_TIMEOUT` - Telegram getUpdates timeout, default `25`.
- `LEADS_POLL_INTERVAL` - seconds between feed polls, default `10`.
- `LEADS_BATCH_SIZE` - leads per request, default `20`.
- `LEADS_HTTP_TIMEOUT` - HTTP timeout for feed request, default `20`.

## Run
```bash
python bot_service.py
```

## Telegram commands
- `/start` or `/help` - help
- `/myid` - shows your Telegram ID
- `/on` - enable notifications for yourself
- `/off` - disable notifications for yourself
- `/add <id>` - admin only, enable for another user
- `/remove <id>` - admin only, disable another user
- `/list` - admin only, list all subscribers

## Website integration (PHP)
1. Upload `bot-leads-feed.php` to website root.
2. In `includes/bootstrap.php` set `BOT_FEED_SECRET`.
3. Set same value as `LEADS_FEED_SECRET` in bot env.

## Security note
The token shared in chat should be treated as compromised. Rotate it in BotFather after setup.
