# NumPlus Telegram Bot Client

Standalone Telegram bot client that reads OTP messages from NumPlus API and forwards new messages to Telegram groups.

## Features
- Polls API every 30 seconds.
- Supports multiple API accounts.
- Stores account API tokens in `token_cache.json`.
- Refreshes account tokens automatically every 2 hours (or on failure).
- Supports multiple Telegram groups.
- Stores sent messages in a daily file (`daily_messages/messages_YYYY-MM-DD.json`).
- Automatically deletes previous days and keeps current day file only.
- Formats message with:
  - quoted header (service short + country code + flag + number)
  - message body as code block
  - copy-code button

## Files
- `bot.py`: main runner
- `cli.py`: account/group/store management
- `accounts.json`: API accounts list
- `groups.json`: Telegram groups list
- `platforms.json`: service names, shortcuts, emojis, custom emoji ids
- `country_codes.json`: country metadata
- `daily_messages/messages_YYYY-MM-DD.json`: sent message state for current day
- `token_cache.json`: cached account tokens with expiry metadata
- `.env`: runtime config

## Setup
```bash
cd telegram_bot_client
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Environment
Set values in `.env`:

```env
API_BASE_URL=http://127.0.0.1:8000
API_START_DATE=2025-01-01
API_SESSION_TOKEN=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
BOT_LIMIT=30
USE_CUSTOM_EMOJI=0
```

Notes:
- If `accounts.json` has valid accounts, `API_SESSION_TOKEN` can stay empty.
- Bot asks for missing values at runtime.
- Bot always asks for start date on each run.
- Cached account tokens expire after 2 hours and are renewed automatically.

## Run
Run continuous mode:
```bash
python bot.py
```

Run one cycle only:
```bash
python bot.py --once
```

## CLI
Add account:
```bash
python cli.py add-account --name acc1 --email you@example.com --password YOUR_PASSWORD
```

Add group:
```bash
python cli.py add-group --name main --chat-id -1001234567890
```

List config:
```bash
python cli.py list-accounts
python cli.py list-groups
```

Clear stored sent messages:
```bash
python cli.py clear-store
python cli.py clear-store --start-date 2025-01-01
```

Set custom emoji id for a service:
```bash
python cli.py set-platform-emoji-id --key whatsapp --emoji-id 5472096095280572227
```
