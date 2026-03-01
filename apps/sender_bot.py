import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import os
import re
import sys
import time
from datetime import date
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import requests
from dotenv import load_dotenv
from app.paths import (
    ACCOUNTS_FILE,
    BASE_DIR,
    COUNTRY_FILE,
    DAILY_STORE_DIR,
    GROUPS_FILE,
    LOGS_DIR,
    PLATFORMS_FILE,
    RUNTIME_CONFIG_FILE,
    STORE_FILE,
    TOKEN_CACHE_FILE,
)
from app.storage import (
    delete_daily_store,
    get_daily_store,
    list_daily_store_days,
    load_json as db_load_json,
    save_json as db_save_json,
    set_daily_store,
)

TOKEN_TTL_SECONDS = 2 * 60 * 60
TOKEN_REFRESH_SKEW_SECONDS = 5 * 60
PLACEHOLDER_VALUES = {
    "https://your-api-domain.example.com",
    "123456789:EXAMPLE_BOT_TOKEN",
    "-1001234567890",
    "YOUR_PASSWORD",
}

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
logger = logging.getLogger("numplus-bot")
LOG_THROTTLE_SECONDS = 120
DEFAULT_POLL_INTERVAL_SECONDS = 30
DEFAULT_GROUP_SEND_INTERVAL_SECONDS = 0.2
DEFAULT_FETCH_TIMEOUT_SECONDS = 90
_LAST_LOG_AT: dict[str, int] = {}

DEFAULT_COUNTRIES: list[dict[str, str]] = [
    {"dial_code": "20", "name_ar": "مصر", "name_en": "Egypt", "iso2": "EG", "emoji": "🇪🇬", "emoji_id": ""},
    {"dial_code": "225", "name_ar": "ساحل العاج", "name_en": "Cote d'Ivoire", "iso2": "CI", "emoji": "🇨🇮", "emoji_id": ""},
    {"dial_code": "971", "name_ar": "الإمارات", "name_en": "United Arab Emirates", "iso2": "AE", "emoji": "🇦🇪", "emoji_id": ""},
    {"dial_code": "966", "name_ar": "السعودية", "name_en": "Saudi Arabia", "iso2": "SA", "emoji": "🇸🇦", "emoji_id": ""},
    {"dial_code": "965", "name_ar": "الكويت", "name_en": "Kuwait", "iso2": "KW", "emoji": "🇰🇼", "emoji_id": ""},
    {"dial_code": "968", "name_ar": "عُمان", "name_en": "Oman", "iso2": "OM", "emoji": "🇴🇲", "emoji_id": ""},
    {"dial_code": "974", "name_ar": "قطر", "name_en": "Qatar", "iso2": "QA", "emoji": "🇶🇦", "emoji_id": ""},
    {"dial_code": "973", "name_ar": "البحرين", "name_en": "Bahrain", "iso2": "BH", "emoji": "🇧🇭", "emoji_id": ""},
    {"dial_code": "962", "name_ar": "الأردن", "name_en": "Jordan", "iso2": "JO", "emoji": "🇯🇴", "emoji_id": ""},
    {"dial_code": "961", "name_ar": "لبنان", "name_en": "Lebanon", "iso2": "LB", "emoji": "🇱🇧", "emoji_id": ""},
    {"dial_code": "212", "name_ar": "المغرب", "name_en": "Morocco", "iso2": "MA", "emoji": "🇲🇦", "emoji_id": ""},
    {"dial_code": "213", "name_ar": "الجزائر", "name_en": "Algeria", "iso2": "DZ", "emoji": "🇩🇿", "emoji_id": ""},
    {"dial_code": "216", "name_ar": "تونس", "name_en": "Tunisia", "iso2": "TN", "emoji": "🇹🇳", "emoji_id": ""},
    {"dial_code": "218", "name_ar": "ليبيا", "name_en": "Libya", "iso2": "LY", "emoji": "🇱🇾", "emoji_id": ""},
    {"dial_code": "229", "name_ar": "بنين", "name_en": "Benin", "iso2": "BJ", "emoji": "🇧🇯", "emoji_id": ""},
    {"dial_code": "261", "name_ar": "مدغشقر", "name_en": "Madagascar", "iso2": "MG", "emoji": "🇲🇬", "emoji_id": ""},
    {"dial_code": "254", "name_ar": "كينيا", "name_en": "Kenya", "iso2": "KE", "emoji": "🇰🇪", "emoji_id": ""},
    {"dial_code": "234", "name_ar": "نيجيريا", "name_en": "Nigeria", "iso2": "NG", "emoji": "🇳🇬", "emoji_id": ""},
    {"dial_code": "1", "name_ar": "الولايات المتحدة", "name_en": "United States", "iso2": "US", "emoji": "🇺🇸", "emoji_id": ""},
    {"dial_code": "44", "name_ar": "المملكة المتحدة", "name_en": "United Kingdom", "iso2": "GB", "emoji": "🇬🇧", "emoji_id": ""},
    {"dial_code": "33", "name_ar": "فرنسا", "name_en": "France", "iso2": "FR", "emoji": "🇫🇷", "emoji_id": ""},
    {"dial_code": "49", "name_ar": "ألمانيا", "name_en": "Germany", "iso2": "DE", "emoji": "🇩🇪", "emoji_id": ""},
    {"dial_code": "39", "name_ar": "إيطاليا", "name_en": "Italy", "iso2": "IT", "emoji": "🇮🇹", "emoji_id": ""},
    {"dial_code": "34", "name_ar": "إسبانيا", "name_en": "Spain", "iso2": "ES", "emoji": "🇪🇸", "emoji_id": ""},
    {"dial_code": "90", "name_ar": "تركيا", "name_en": "Turkey", "iso2": "TR", "emoji": "🇹🇷", "emoji_id": ""},
    {"dial_code": "7", "name_ar": "روسيا", "name_en": "Russia", "iso2": "RU", "emoji": "🇷🇺", "emoji_id": ""},
    {"dial_code": "86", "name_ar": "الصين", "name_en": "China", "iso2": "CN", "emoji": "🇨🇳", "emoji_id": ""},
    {"dial_code": "91", "name_ar": "الهند", "name_en": "India", "iso2": "IN", "emoji": "🇮🇳", "emoji_id": ""},
    {"dial_code": "92", "name_ar": "باكستان", "name_en": "Pakistan", "iso2": "PK", "emoji": "🇵🇰", "emoji_id": ""},
    {"dial_code": "62", "name_ar": "إندونيسيا", "name_en": "Indonesia", "iso2": "ID", "emoji": "🇮🇩", "emoji_id": ""},
    {"dial_code": "63", "name_ar": "الفلبين", "name_en": "Philippines", "iso2": "PH", "emoji": "🇵🇭", "emoji_id": ""},
    {"dial_code": "84", "name_ar": "فيتنام", "name_en": "Vietnam", "iso2": "VN", "emoji": "🇻🇳", "emoji_id": ""},
    {"dial_code": "66", "name_ar": "تايلاند", "name_en": "Thailand", "iso2": "TH", "emoji": "🇹🇭", "emoji_id": ""},
    {"dial_code": "60", "name_ar": "ماليزيا", "name_en": "Malaysia", "iso2": "MY", "emoji": "🇲🇾", "emoji_id": ""},
    {"dial_code": "65", "name_ar": "سنغافورة", "name_en": "Singapore", "iso2": "SG", "emoji": "🇸🇬", "emoji_id": ""},
    {"dial_code": "81", "name_ar": "اليابان", "name_en": "Japan", "iso2": "JP", "emoji": "🇯🇵", "emoji_id": ""},
    {"dial_code": "82", "name_ar": "كوريا الجنوبية", "name_en": "South Korea", "iso2": "KR", "emoji": "🇰🇷", "emoji_id": ""},
    {"dial_code": "61", "name_ar": "أستراليا", "name_en": "Australia", "iso2": "AU", "emoji": "🇦🇺", "emoji_id": ""},
    {"dial_code": "55", "name_ar": "البرازيل", "name_en": "Brazil", "iso2": "BR", "emoji": "🇧🇷", "emoji_id": ""},
    {"dial_code": "52", "name_ar": "المكسيك", "name_en": "Mexico", "iso2": "MX", "emoji": "🇲🇽", "emoji_id": ""},
    {"dial_code": "27", "name_ar": "جنوب أفريقيا", "name_en": "South Africa", "iso2": "ZA", "emoji": "🇿🇦", "emoji_id": ""},
]


class ColorFormatter(logging.Formatter):
    RESET = "\033[0m"
    COLORS = {
        logging.DEBUG: "\033[36m",
        logging.INFO: "\033[32m",
        logging.WARNING: "\033[33m",
        logging.ERROR: "\033[31m",
        logging.CRITICAL: "\033[35m",
    }

    def format(self, record: logging.LogRecord) -> str:
        original = record.levelname
        color = self.COLORS.get(record.levelno, "")
        try:
            if color and sys.stdout.isatty():
                record.levelname = f"{color}{original}{self.RESET}"
            return super().format(record)
        finally:
            record.levelname = original


def setup_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level)
    logger.propagate = False

    # Reset handlers to avoid duplicate logs when script is reloaded.
    logger.handlers.clear()

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(ColorFormatter(LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(console)

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    file_handler = TimedRotatingFileHandler(
        filename=str(LOGS_DIR / "bot.log"),
        when="midnight",
        interval=1,
        backupCount=14,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(file_handler)


def ask(prompt: str, default: str | None = None) -> str:
    if default is None:
        return input(f"{prompt}: ").strip()
    value = input(f"{prompt} [{default}]: ").strip()
    return value or default


def ask_missing(prompt: str, current: str) -> str:
    if is_real_value(current):
        return current.strip()
    return ask(prompt)


def is_real_value(value: str | None) -> bool:
    v = str(value or "").strip()
    if not v:
        return False
    if v in PLACEHOLDER_VALUES:
        return False
    low = v.lower()
    if "example" in low or "your-api-domain" in low or "your_password" in low:
        return False
    return True


def digits_only(text: str) -> str:
    return "".join(ch for ch in (text or "") if ch.isdigit())


def load_json_list(path: Path) -> list[dict]:
    data = db_load_json(path, [])
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def load_countries() -> list[dict[str, str]]:
    rows_raw = load_json_list(COUNTRY_FILE)
    rows: list[dict[str, str]] = []
    seen_dials: set[str] = set()
    for row in rows_raw:
        dial = digits_only(str(row.get("dial_code", "")).strip())
        if not dial:
            continue
        if dial in seen_dials:
            continue
        seen_dials.add(dial)
        rows.append(
            {
                "dial_code": dial,
                "name_ar": str(row.get("name_ar", "")).strip(),
                "name_en": str(row.get("name_en", "")).strip(),
                "iso2": str(row.get("iso2", "")).strip().upper(),
                "emoji": str(row.get("emoji", "")).strip(),
                "emoji_id": str(row.get("emoji_id", "")).strip(),
            }
        )
    for default_row in DEFAULT_COUNTRIES:
        dial = digits_only(str(default_row.get("dial_code", "")).strip())
        if not dial or dial in seen_dials:
            continue
        seen_dials.add(dial)
        rows.append(dict(default_row))
    if not rows:
        rows = [dict(x) for x in DEFAULT_COUNTRIES]
    rows.sort(key=lambda x: len(str(x.get("dial_code", ""))), reverse=True)
    return rows


def load_platforms() -> dict[str, str]:
    rows = load_json_list(PLATFORMS_FILE)
    out: dict[str, str] = {}
    for r in rows:
        key = normalize_service_key(str(r.get("key", "")))
        short = str(r.get("short", "")).strip()
        if key and short:
            out[key] = short
    # Safety fallback when platforms store is missing.
    if not out:
        out = {
            "whatsapp": "WA",
            "telegram": "TG",
            "facebook": "FB",
            "instagram": "IG",
            "twitter": "X",
            "tiktok": "TT",
        }
    return out


def load_accounts() -> list[dict[str, str]]:
    rows = load_json_list(ACCOUNTS_FILE)
    # Backward compatible loader: supports JSON object {"accounts":[...]}
    # and simple line format: "email password".
    if not rows and ACCOUNTS_FILE.exists():
        try:
            raw = ACCOUNTS_FILE.read_text(encoding="utf-8").strip()
            if raw.startswith("{"):
                obj = json.loads(raw)
                maybe_rows = obj.get("accounts") if isinstance(obj, dict) else None
                if isinstance(maybe_rows, list):
                    rows = [x for x in maybe_rows if isinstance(x, dict)]
            elif raw:
                parsed_rows: list[dict[str, str]] = []
                for idx, line in enumerate(raw.splitlines(), start=1):
                    v = line.strip()
                    if not v or v.startswith("#"):
                        continue
                    parts = v.split()
                    if len(parts) >= 2:
                        email = parts[0].strip()
                        password = " ".join(parts[1:]).strip()
                        parsed_rows.append(
                            {
                                "name": f"account_{idx}",
                                "email": email,
                                "password": password,
                                "enabled": True,
                            }
                        )
                rows = parsed_rows
        except Exception:
            rows = []
    out: list[dict[str, str]] = []
    for r in rows:
        enabled = bool(r.get("enabled", True))
        email = str(r.get("email", "")).strip()
        password = str(r.get("password", "")).strip()
        name = str(r.get("name", email)).strip() or email
        if enabled and email and password:
            out.append({"name": name, "email": email, "password": password})
    return out


def load_groups() -> list[dict[str, str]]:
    raw = db_load_json(GROUPS_FILE, [])
    rows: list[dict] = []
    if isinstance(raw, list):
        rows = [x for x in raw if isinstance(x, dict)]
    elif isinstance(raw, dict):
        # Backward compatibility: {"groups":[...]}
        maybe = raw.get("groups")
        if isinstance(maybe, list):
            rows = [x for x in maybe if isinstance(x, dict)]
    out: list[dict[str, str]] = []
    for r in rows:
        enabled = bool(r.get("enabled", True))
        chat_id = str(r.get("chat_id") or r.get("id") or "").strip()
        name = str(r.get("name", chat_id)).strip() or chat_id
        # Skip placeholder/demo group ids so .env fallback can be used.
        if enabled and is_real_value(chat_id):
            out.append({"name": name, "chat_id": chat_id})
    return out


def detect_country(number: str, countries: list[dict[str, str]]) -> dict[str, str]:
    num = digits_only(number)
    if num.startswith("00"):
        num = num[2:]
    for row in countries:
        dial = str(row.get("dial_code", ""))
        if dial and num.startswith(dial):
            return row
    return {"name_ar": "غير معروف", "name_en": "Unknown", "iso2": "UN", "dial_code": ""}


def iso_to_flag(iso2: str) -> str:
    code = (iso2 or "").upper()
    if len(code) != 2 or not code.isalpha():
        return "🏳️"
    base = 127397
    return chr(base + ord(code[0])) + chr(base + ord(code[1]))


def service_short(service_name: str, platforms: dict[str, str]) -> str:
    key = normalize_service_key(service_name)
    if key in platforms:
        return str(platforms[key]).upper()
    # Better fallback for common services
    if "whatsapp" in key or key == "wa":
        return "WA"
    if "telegram" in key or key == "tg":
        return "TG"
    if "facebook" in key or key == "fb":
        return "FB"
    return ((service_name or "")[:2] or "NA").upper()


def service_emoji_id(service_name: str, platform_rows: list[dict]) -> str:
    key = normalize_service_key(service_name)
    for row in platform_rows:
        if normalize_service_key(str(row.get("key", ""))) == key:
            return str(row.get("emoji_id", "")).strip()
    return ""


def service_emoji_alt(service_name: str, platform_rows: list[dict]) -> str:
    key = normalize_service_key(service_name)
    for row in platform_rows:
        if normalize_service_key(str(row.get("key", ""))) == key:
            alt = str(row.get("emoji", "")).strip()
            if alt:
                return alt
    return "✨"


def normalize_service_key(value: str) -> str:
    s = str(value or "").strip().lower()
    # remove separators and punctuation so "Whats App", "whats-app", etc. match.
    return re.sub(r"[^a-z0-9]+", "", s)


def extract_code(message: str) -> str:
    text = message or ""
    # Prefer patterns like 123-456 then fallback to plain 4-8 digits.
    m = re.search(r"\b\d{2,4}-\d{2,4}\b", text)
    if m:
        return m.group(0)
    m2 = re.search(r"\b\d{4,8}\b", text)
    if m2:
        return m2.group(0)
    return ""


def mask_number_middle(value: str, hidden_digits: int = 2) -> str:
    raw = str(value or "").strip()
    if not raw:
        return raw
    chars = list(raw)
    digit_positions = [i for i, ch in enumerate(chars) if ch.isdigit()]
    if len(digit_positions) <= (hidden_digits + 2):
        return raw
    mid_start = (len(digit_positions) - hidden_digits) // 2
    target_positions = digit_positions[mid_start : mid_start + hidden_digits]
    for pos in target_positions:
        chars[pos] = "•"
    return "".join(chars)


def build_message(item: dict, countries: list[dict[str, str]], platforms: dict[str, str], platform_rows: list[dict]) -> str:
    raw_number = str(item.get("number", ""))
    number_digits = digits_only(raw_number)
    number_with_plus = f"+{number_digits}" if number_digits else raw_number
    number_display = mask_number_middle(number_with_plus, hidden_digits=2)
    service_name = str(item.get("service_name", "Unknown"))
    short = service_short(service_name, platforms)
    semoji_id = service_emoji_id(service_name, platform_rows)
    semoji_alt = service_emoji_alt(service_name, platform_rows)
    use_custom_emoji = os.getenv("USE_CUSTOM_EMOJI", "0").strip() == "1"
    country = detect_country(raw_number, countries)
    iso2 = str(country.get("iso2") or "UN").upper()
    flag = iso_to_flag(iso2)
    cemoji_id = str(country.get("emoji_id", "")).strip()
    cemoji_alt = str(country.get("emoji", "")).strip() or flag
    message_text = str(item.get("message", "")).strip()
    escaped_head = _md_escape(f"{short} {iso2} {number_display}")
    escaped_msg = _md_code_escape(message_text)
    custom_service = f"![{semoji_alt}](tg://emoji?id={semoji_id})" if (use_custom_emoji and semoji_id) else semoji_alt
    custom_country = f"![{cemoji_alt}](tg://emoji?id={cemoji_id})" if (use_custom_emoji and cemoji_id) else cemoji_alt
    return f"> {custom_service} {custom_country} *{escaped_head}*\n```\n{escaped_msg}\n```"


def _md_escape(text: str) -> str:
    # MarkdownV2 special chars
    out = re.sub(r"([_\\*\\[\\]\\(\\)~`>#+\\-=|{}.!])", r"\\\1", text or "")
    return out.replace("+", r"\+")


def _md_code_escape(text: str) -> str:
    t = text or ""
    # Keep code block valid.
    t = t.replace("```", "'''")
    return t


def send_telegram_message(bot_token: str, chat_id: str, text: str, copy_value: str) -> dict:
    api = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "reply_markup": {
            "inline_keyboard": [
                [{"text": f"{copy_value}", "style": "success", "copy_text": {"text": copy_value}}],
            ]
        },
        "disable_web_page_preview": True,
    }
    r = requests.post(api, json=payload, timeout=30)
    data = r.json()
    if data.get("ok"):
        return data

    # Fallback if copy_text is unsupported in the current Bot API/client environment.
    payload["reply_markup"] = {
        "inline_keyboard": [
            [{"text": f"{copy_value}", "style": "success", "url": f"https://t.me/share/url?url={copy_value}"}],
        ]
    }
    r2 = requests.post(api, json=payload, timeout=30)
    return r2.json()


def edit_telegram_message(bot_token: str, chat_id: str, message_id: int, text: str, copy_value: str) -> dict:
    api = f"https://api.telegram.org/bot{bot_token}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "reply_markup": {
            "inline_keyboard": [
                [{"text": f"{copy_value}", "style": "success", "copy_text": {"text": copy_value}}],
            ]
        },
        "disable_web_page_preview": True,
    }
    r = requests.post(api, json=payload, timeout=30)
    data = r.json()
    if data.get("ok"):
        return data
    desc = str(data.get("description", "")).lower()
    if "message is not modified" in desc:
        # Treat "not modified" as success to avoid sending duplicate messages.
        return {"ok": True, "result": {"message_id": message_id}, "not_modified": True}

    payload["reply_markup"] = {
        "inline_keyboard": [
            [{"text": f"{copy_value}", "style": "success", "url": f"https://t.me/share/url?url={copy_value}"}],
        ]
    }
    r2 = requests.post(api, json=payload, timeout=30)
    data2 = r2.json()
    if not data2.get("ok"):
        desc2 = str(data2.get("description", "")).lower()
        if "message is not modified" in desc2:
            return {"ok": True, "result": {"message_id": message_id}, "not_modified": True}
    return data2


def _telegram_retry_after_seconds(resp: dict) -> int:
    if not isinstance(resp, dict):
        return 0
    try:
        params = resp.get("parameters") or {}
        return int(params.get("retry_after") or 0)
    except Exception:
        return 0


def _today_key() -> str:
    return date.today().isoformat()


def _daily_store_path(day_key: str) -> Path:
    return DAILY_STORE_DIR / f"messages_{day_key}.json"


def cleanup_old_daily_files(current_day_key: str) -> None:
    for day_key in list_daily_store_days():
        if day_key != current_day_key:
            delete_daily_store(day_key)
    # Keep legacy files clean in case old process created them.
    DAILY_STORE_DIR.mkdir(parents=True, exist_ok=True)
    keep_path = _daily_store_path(current_day_key).resolve()
    for p in DAILY_STORE_DIR.glob("messages_*.json"):
        try:
            if p.resolve() != keep_path:
                p.unlink(missing_ok=True)
        except Exception:
            continue


def load_daily_store(day_key: str) -> dict:
    data = get_daily_store(day_key, {})
    if isinstance(data, dict) and isinstance(data.get("seen_keys"), list) and isinstance(data.get("sent"), list):
        data["day"] = day_key
        if not isinstance(data.get("latest_by_thread"), dict):
            data["latest_by_thread"] = {}
        if not isinstance(data.get("delivered_by_msg"), dict):
            data["delivered_by_msg"] = {}
        return data
    return {"day": day_key, "seen_keys": [], "sent": [], "latest_by_thread": {}, "delivered_by_msg": {}}


def save_daily_store(day_key: str, store: dict) -> None:
    set_daily_store(day_key, store)


def load_token_cache() -> dict:
    data = db_load_json(TOKEN_CACHE_FILE, {"accounts": {}})
    if isinstance(data, dict) and isinstance(data.get("accounts"), dict):
        return data
    return {"accounts": {}}


def load_runtime_config() -> dict:
    data = db_load_json(RUNTIME_CONFIG_FILE, {"fetch_codes_enabled": True})
    if isinstance(data, dict):
        if "fetch_codes_enabled" not in data:
            data["fetch_codes_enabled"] = True
        return data
    return {"fetch_codes_enabled": True}


def runtime_start_date(default_value: str) -> str:
    cfg = load_runtime_config()
    value = str(cfg.get("messages_start_date", "")).strip()
    if value:
        return normalize_start_date(value)
    return normalize_start_date(default_value)


def runtime_api_base(default_value: str) -> str:
    cfg = load_runtime_config()
    value = str(cfg.get("api_base_url", "")).strip().rstrip("/")
    return value or str(default_value or "").strip().rstrip("/")


def runtime_api_session_token(default_value: str) -> str:
    cfg = load_runtime_config()
    value = str(cfg.get("api_session_token", "")).strip()
    return value or str(default_value or "").strip()


def runtime_api_key(default_value: str) -> str:
    cfg = load_runtime_config()
    value = str(cfg.get("api_key", "")).strip()
    return value or str(default_value or "").strip()


def runtime_bot_limit(default_value: int) -> int:
    cfg = load_runtime_config()
    raw = str(cfg.get("bot_limit", default_value)).strip()
    try:
        n = int(raw)
    except Exception:
        n = int(default_value)
    if n <= 0:
        return 0
    return max(1, min(10000, n))


def runtime_poll_interval(default_value: int) -> int:
    cfg = load_runtime_config()
    try:
        n = int(str(cfg.get("poll_interval_seconds", default_value)).strip())
    except Exception:
        n = int(default_value)
    return max(5, min(300, n))


def runtime_messages_update_marker() -> str:
    cfg = load_runtime_config()
    return str(cfg.get("messages_update_requested_at", "")).strip()


def is_fetch_codes_enabled() -> bool:
    cfg = load_runtime_config()
    return bool(cfg.get("fetch_codes_enabled", True))


def save_token_cache(cache: dict) -> None:
    db_save_json(TOKEN_CACHE_FILE, cache)


def cache_get_valid_token(cache: dict, account_name: str) -> str | None:
    row = (cache.get("accounts") or {}).get(account_name)
    if not isinstance(row, dict):
        return None
    token = str(row.get("token", "")).strip()
    expires_at = int(row.get("expires_at", 0) or 0)
    if not token or expires_at <= int(time.time()) + TOKEN_REFRESH_SKEW_SECONDS:
        return None
    return token


def cache_set_token(cache: dict, account_name: str, token: str) -> None:
    now = int(time.time())
    cache.setdefault("accounts", {})[account_name] = {
        "token": token,
        "obtained_at": now,
        "expires_at": now + TOKEN_TTL_SECONDS,
    }


def get_or_refresh_account_token(
    api_base: str,
    api_key: str,
    account: dict[str, str],
    account_tokens: dict[str, str],
    token_cache: dict,
) -> str | None:
    name = account["name"]
    mem_tok = account_tokens.get(name)
    if mem_tok and cache_get_valid_token(token_cache, name):
        return mem_tok

    cached_tok = cache_get_valid_token(token_cache, name)
    if cached_tok:
        account_tokens[name] = cached_tok
        return cached_tok

    new_tok = api_login(api_base, api_key, account["email"], account["password"])
    if not new_tok:
        return None
    account_tokens[name] = new_tok
    cache_set_token(token_cache, name, new_tok)
    save_token_cache(token_cache)
    return new_tok


def msg_key(item: dict) -> str:
    number = str(item.get("number", ""))
    service_name = str(item.get("service_name", ""))
    message = str(item.get("message", ""))
    rng = str(item.get("range", ""))
    # Include stable identifiers/timestamps when available so repeated messages
    # with same content are not dropped by dedup logic.
    for k in (
        "id",
        "message_id",
        "sms_id",
        "code_id",
        "created_at",
        "received_at",
        "timestamp",
        "date",
        "time",
    ):
        v = str(item.get(k, "")).strip()
        if v:
            return f"{number}|{service_name}|{rng}|{message}|{k}={v}"
    return f"{number}|{service_name}|{rng}|{message}"


def thread_key(item: dict) -> str:
    number = str(item.get("number", ""))
    service_name = str(item.get("service_name", ""))
    rng = str(item.get("range", ""))
    return f"{number}|{service_name}|{rng}"


def normalize_start_date(raw: str) -> str:
    v = (raw or "").strip()
    parts = v.split("-")
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        y, m, d = parts
        if len(y) == 4:
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    return date.today().isoformat()


def _extract_login_token(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""

    candidates: list[object] = [payload]
    for key in ("data", "result"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            candidates.append(nested)

    token_keys = ("token", "access_token", "session_token", "api_token", "jwt")
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        for key in token_keys:
            value = str(candidate.get(key, "")).strip()
            if value:
                return value
    return ""


def _extract_login_error(payload: object) -> str:
    if isinstance(payload, dict):
        for key in ("message", "error", "detail", "errors"):
            value = payload.get(key)
            if value:
                return str(value)
    return ""


def _short_text(value: object, max_len: int = 220) -> str:
    text = str(value or "").strip()
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


def _should_log(key: str, throttle_seconds: int = LOG_THROTTLE_SECONDS) -> bool:
    now = int(time.time())
    last = int(_LAST_LOG_AT.get(key, 0))
    if now - last < throttle_seconds:
        return False
    _LAST_LOG_AT[key] = now
    return True


def _classify_request_error(exc: Exception) -> str:
    txt = str(exc)
    low = txt.lower()
    if "name or service not known" in low or "failed to resolve" in low or "nameresolutionerror" in low:
        return "dns_error"
    if "timed out" in low or "timeout" in low:
        return "timeout"
    if "connection refused" in low:
        return "connection_refused"
    return "network_error"


def _api_headers(api_key: str) -> dict[str, str]:
    key = str(api_key or "").strip()
    if not key:
        return {}
    return {"X-API-Key": key}


def check_api_health(api_base: str) -> bool:
    url = f"{api_base}/api/v1/health"
    try:
        r = requests.get(url, timeout=20)
    except requests.RequestException as exc:
        reason = _classify_request_error(exc)
        logger.error("api health failed | endpoint=%s | reason=%s | error=%s", url, reason, _short_text(exc))
        return False

    body_snippet = ""
    try:
        payload = r.json()
        body_snippet = _short_text(payload)
    except ValueError:
        body_snippet = _short_text(r.text)

    if r.status_code != 200:
        logger.warning(
            "api health responded non-200 | endpoint=%s | status=%s | body=%s",
            url,
            r.status_code,
            body_snippet,
        )
        return False

    logger.info("api health ok | endpoint=%s | body=%s", url, body_snippet)
    return True


def api_login(api_base: str, api_key: str, email: str, password: str) -> str | None:
    url = f"{api_base}/api/v1/auth/login"
    try:
        r = requests.post(url, json={"email": email, "password": password}, headers=_api_headers(api_key), timeout=90)
    except requests.RequestException as exc:
        reason = _classify_request_error(exc)
        key = f"login_req_{email}_{reason}"
        if _should_log(key):
            logger.error(
                "login request failed | account=%s | reason=%s | endpoint=%s | error=%s",
                email,
                reason,
                url,
                _short_text(exc),
            )
        return None

    try:
        payload: object = r.json()
    except ValueError:
        payload = None

    if r.status_code != 200:
        err = _extract_login_error(payload) or (r.text or "").strip()
        logger.warning(
            "login failed | account=%s | endpoint=%s | status=%s | error=%s",
            email,
            url,
            r.status_code,
            _short_text(err or "no error message"),
        )
        return None

    token = _extract_login_token(payload if isinstance(payload, dict) else {})
    if token:
        return token

    logger.warning("login response missing token | account=%s | endpoint=%s", email, url)
    return None


def fetch_messages(api_base: str, api_key: str, api_token: str, start_date: str, limit: int) -> list[dict]:
    endpoint = f"{api_base}/api/v1/biring/code"
    try:
        fetch_timeout = int(str(os.getenv("API_FETCH_TIMEOUT_SEC", str(DEFAULT_FETCH_TIMEOUT_SECONDS))).strip() or str(DEFAULT_FETCH_TIMEOUT_SECONDS))
    except Exception:
        fetch_timeout = DEFAULT_FETCH_TIMEOUT_SECONDS
    fetch_timeout = max(15, min(300, fetch_timeout))
    try:
        r = requests.post(
            endpoint,
            json={"token": api_token, "start_date": start_date},
            headers=_api_headers(api_key),
            timeout=fetch_timeout,
        )
    except requests.RequestException as exc:
        reason = _classify_request_error(exc)
        raise RuntimeError(f"request failed ({reason}): {exc}") from exc
    try:
        j = r.json()
    except ValueError as exc:
        raise RuntimeError(f"invalid json response | status={r.status_code} | body={_short_text(r.text)}") from exc
    if r.status_code != 200:
        raise RuntimeError(str(j))
    rows = ((j.get("data") or {}).get("messages") or [])
    if limit <= 0:
        return rows
    return rows[:limit]


def run_loop(
    start_date: str,
    api_base: str,
    api_key: str,
    api_token: str,
    tg_token: str,
    target_groups: list[dict[str, str]],
    limit: int,
    once: bool,
) -> None:
    current_api_base = runtime_api_base(api_base)
    current_api_key = runtime_api_key(api_key)
    current_api_token = runtime_api_session_token(api_token)
    current_start_date = runtime_start_date(start_date)
    current_limit = runtime_bot_limit(limit)
    current_poll_interval = runtime_poll_interval(DEFAULT_POLL_INTERVAL_SECONDS)

    countries = load_countries()
    platform_rows = load_json_list(PLATFORMS_FILE)
    platforms = load_platforms()
    active_day = _today_key()
    cleanup_old_daily_files(active_day)
    day_store = load_daily_store(active_day)
    seen_keys = set(day_store.get("seen_keys", []))
    latest_by_thread = day_store.get("latest_by_thread", {})
    if not isinstance(latest_by_thread, dict):
        latest_by_thread = {}
        day_store["latest_by_thread"] = latest_by_thread
    delivered_by_msg = day_store.get("delivered_by_msg", {})
    if not isinstance(delivered_by_msg, dict):
        delivered_by_msg = {}
        day_store["delivered_by_msg"] = delivered_by_msg

    accounts = load_accounts()
    token_cache = load_token_cache()
    account_tokens: dict[str, str] = {}
    current_target_groups: list[dict[str, str]] = list(target_groups)
    update_marker = runtime_messages_update_marker()
    group_min_interval = float(os.getenv("TG_GROUP_MIN_INTERVAL_SEC", str(DEFAULT_GROUP_SEND_INTERVAL_SECONDS)).strip() or DEFAULT_GROUP_SEND_INTERVAL_SECONDS)
    last_group_send_at: dict[str, float] = {}
    invalid_groups: set[str] = set()
    for acc in accounts:
        tok = get_or_refresh_account_token(current_api_base, current_api_key, acc, account_tokens, token_cache)
        if tok:
            logger.info("account ready | account=%s", acc["name"])
        else:
            logger.warning("account login failed | account=%s", acc["name"])

    logger.info(
        "started polling | interval=%ss | start_date=%s | limit=%s",
        current_poll_interval,
        current_start_date,
        current_limit,
    )
    logger.info("press Ctrl+C to stop")

    while True:
        latest_marker = runtime_messages_update_marker()
        if latest_marker and latest_marker != update_marker:
            update_marker = latest_marker
            current_api_base = runtime_api_base(current_api_base)
            current_api_key = runtime_api_key(current_api_key)
            current_api_token = runtime_api_session_token(current_api_token)
            current_start_date = runtime_start_date(current_start_date)
            current_limit = runtime_bot_limit(current_limit)
            current_poll_interval = runtime_poll_interval(current_poll_interval)
            countries = load_countries()
            platform_rows = load_json_list(PLATFORMS_FILE)
            platforms = load_platforms()
            accounts = load_accounts()
            current_target_groups = load_groups()
            invalid_groups.clear()
            token_cache = load_token_cache()
            account_tokens = {}
            # Reload persisted message state immediately after runtime updates
            # (e.g. when admin clears saved messages) without waiting for restart/day-rotation.
            day_store = load_daily_store(active_day)
            seen_keys = set(day_store.get("seen_keys", []))
            latest_by_thread = day_store.get("latest_by_thread", {})
            if not isinstance(latest_by_thread, dict):
                latest_by_thread = {}
                day_store["latest_by_thread"] = latest_by_thread
            delivered_by_msg = day_store.get("delivered_by_msg", {})
            if not isinstance(delivered_by_msg, dict):
                delivered_by_msg = {}
                day_store["delivered_by_msg"] = delivered_by_msg
            logger.info(
                "runtime refresh requested | marker=%s | api_base=%s | start_date=%s | limit=%s | groups=%s",
                latest_marker,
                current_api_base,
                current_start_date,
                current_limit,
                len(current_target_groups),
            )

        if not is_fetch_codes_enabled():
            if _should_log("fetch_paused", throttle_seconds=120):
                logger.info("fetch codes is paused by runtime config")
            if once:
                return
            time.sleep(current_poll_interval)
            continue

        if not current_target_groups:
            if _should_log("no_groups_configured", throttle_seconds=120):
                logger.warning("no groups configured; skipping send cycle")
            if once:
                return
            time.sleep(current_poll_interval)
            continue

        now_day = _today_key()
        if now_day != active_day:
            active_day = now_day
            cleanup_old_daily_files(active_day)
            day_store = load_daily_store(active_day)
            seen_keys = set(day_store.get("seen_keys", []))
            latest_by_thread = day_store.get("latest_by_thread", {})
            if not isinstance(latest_by_thread, dict):
                latest_by_thread = {}
                day_store["latest_by_thread"] = latest_by_thread
            delivered_by_msg = day_store.get("delivered_by_msg", {})
            if not isinstance(delivered_by_msg, dict):
                delivered_by_msg = {}
                day_store["delivered_by_msg"] = delivered_by_msg
            logger.info("rotated daily store | day=%s", active_day)

        all_rows: list[dict] = []
        account_jobs: list[tuple[str, dict[str, str], str]] = []
        for acc in accounts:
            name = acc["name"]
            tok = get_or_refresh_account_token(current_api_base, current_api_key, acc, account_tokens, token_cache)
            if tok:
                account_jobs.append((name, acc, tok))

        total_jobs = len(account_jobs) + (1 if current_api_token else 0)
        if total_jobs:
            max_workers = max(1, min(16, total_jobs))
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures: dict = {}
                if current_api_token:
                    fut = pool.submit(fetch_messages, current_api_base, current_api_key, current_api_token, current_start_date, current_limit)
                    futures[fut] = ("api_token", None, None)
                for name, acc, tok in account_jobs:
                    fut = pool.submit(fetch_messages, current_api_base, current_api_key, tok, current_start_date, current_limit)
                    futures[fut] = ("account", name, acc)

                for fut in as_completed(futures):
                    src, name, acc = futures[fut]
                    try:
                        all_rows.extend(fut.result())
                        continue
                    except Exception as exc:
                        if src == "api_token":
                            logger.warning("api token fetch failed | error=%s", _short_text(exc))
                            continue
                        if not acc or not name:
                            continue
                        # Retry once with fresh login token when account token is stale.
                        new_tok = api_login(current_api_base, current_api_key, acc["email"], acc["password"])
                        if not new_tok:
                            logger.warning("account fetch failed | account=%s | error=%s", name, _short_text(exc))
                            continue
                        account_tokens[name] = new_tok
                        cache_set_token(token_cache, name, new_tok)
                        save_token_cache(token_cache)
                        try:
                            all_rows.extend(fetch_messages(current_api_base, current_api_key, new_tok, current_start_date, current_limit))
                        except Exception as retry_exc:
                            logger.warning(
                                "account fetch retry failed | account=%s | error=%s",
                                name,
                                _short_text(retry_exc),
                            )

        uniq: dict[str, dict] = {}
        for row in all_rows:
            uniq[msg_key(row)] = row
        rows = list(uniq.values()) if current_limit <= 0 else list(uniq.values())[:current_limit]
        dispatch_tasks: list[tuple[dict, list[dict[str, str]], bool]] = []
        for item in rows:
            mkey = msg_key(item)
            delivered_raw = delivered_by_msg.get(mkey, [])
            delivered_set = set(str(x) for x in delivered_raw) if isinstance(delivered_raw, list) else set()
            missing_groups = [
                grp
                for grp in current_target_groups
                if grp["chat_id"] not in delivered_set and grp["chat_id"] not in invalid_groups
            ]
            if missing_groups:
                dispatch_tasks.append((item, missing_groups, mkey not in seen_keys))

        if not dispatch_tasks:
            if _should_log("no_new_messages", throttle_seconds=300):
                logger.info("no new messages")
            if once:
                return
            time.sleep(current_poll_interval)
            continue

        new_count = sum(1 for _item, _targets, is_new in dispatch_tasks if is_new)
        retry_count = len(dispatch_tasks) - new_count
        logger.info("messages to deliver | total=%s | new=%s | retry=%s", len(dispatch_tasks), new_count, retry_count)
        for idx, (item, task_groups, _is_new) in enumerate(dispatch_tasks, start=1):
            number = str(item.get("number", ""))
            message_text = str(item.get("message", ""))
            code = extract_code(message_text) or number
            text = build_message(item, countries, platforms, platform_rows)
            mkey = msg_key(item)
            delivered_raw = delivered_by_msg.get(mkey, [])
            delivered_set = set(str(x) for x in delivered_raw) if isinstance(delivered_raw, list) else set()
            tkey = thread_key(item)
            prev_map = latest_by_thread.get(tkey, {})
            if not isinstance(prev_map, dict):
                prev_map = {}

            any_sent = False
            sent_info: list[dict[str, str | int | None]] = []
            next_map: dict[str, int] = {}
            for grp in task_groups:
                gid = grp["chat_id"]
                gname = grp["name"]
                if gid in invalid_groups:
                    continue
                prev_msg_id_raw = prev_map.get(gid)
                j: dict = {}
                action = "send"
                if isinstance(prev_msg_id_raw, int):
                    try:
                        j = edit_telegram_message(tg_token, gid, prev_msg_id_raw, text, code)
                        action = "edit"
                    except Exception as exc:
                        logger.warning("edit failed | idx=%s | group=%s | error=%s", idx, gname, _short_text(exc))
                        j = {}

                if not j or not j.get("ok"):
                    # Rate-limit only real send operations per group.
                    last_ts = last_group_send_at.get(gid, 0.0)
                    now_ts = time.monotonic()
                    wait = group_min_interval - (now_ts - last_ts)
                    if wait > 0:
                        time.sleep(wait)
                    try:
                        j = send_telegram_message(tg_token, gid, text, code)
                        action = "send"
                    except Exception as exc:
                        logger.error("send failed | idx=%s | group=%s | error=%s", idx, gname, _short_text(exc))
                        continue
                    if not j.get("ok"):
                        retry_after = _telegram_retry_after_seconds(j)
                        if retry_after > 0:
                            time.sleep(max(1, retry_after + 1))
                            try:
                                j = send_telegram_message(tg_token, gid, text, code)
                                action = "send"
                            except Exception as exc:
                                logger.error("send failed after retry | idx=%s | group=%s | error=%s", idx, gname, _short_text(exc))
                                continue
                        if not j.get("ok"):
                            desc = str(j.get("description") or "").lower()
                            if "chat not found" in desc:
                                invalid_groups.add(gid)
                                logger.error("group disabled (chat not found) | group=%s | chat_id=%s", gname, gid)
                            else:
                                logger.error("send failed | idx=%s | group=%s | response=%s", idx, gname, _short_text(j))
                            continue

                any_sent = True
                result_row = j.get("result") or {}
                msg_id = result_row.get("message_id") or prev_msg_id_raw
                if isinstance(msg_id, int):
                    next_map[gid] = msg_id
                sent_info.append({"group": gname, "chat_id": gid, "message_id": msg_id})
                logger.info("%s ok | idx=%s | group=%s | message_id=%s | code=%s", action, idx, gname, msg_id, code)
                last_group_send_at[gid] = time.monotonic()
                delivered_set.add(gid)

            if any_sent or delivered_set:
                seen_keys.add(mkey)
                merged_map = dict(prev_map)
                merged_map.update(next_map)
                latest_by_thread[tkey] = merged_map
                delivered_by_msg[mkey] = sorted(delivered_set)
                day_store["sent"].append(
                    {
                        "number": number,
                        "code": code,
                        "service_name": item.get("service_name"),
                        "range": item.get("range"),
                        "message": item.get("message"),
                        "revenue": item.get("revenue"),
                        "groups": sent_info,
                        "thread_key": tkey,
                        "sent_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
                day_store["seen_keys"] = list(seen_keys)
                day_store["latest_by_thread"] = latest_by_thread
                day_store["delivered_by_msg"] = delivered_by_msg
                save_daily_store(active_day, day_store)

        if once:
            return
        time.sleep(current_poll_interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="NumPlus Telegram Bot Client")
    parser.add_argument("--once", action="store_true", help="Run one polling cycle then exit")
    parser.add_argument("--no-input", action="store_true", help="Run without interactive prompts using .env/config files")
    args = parser.parse_args()

    load_dotenv(BASE_DIR / ".env")
    setup_logging()
    default_api = runtime_api_base(os.getenv("API_BASE_URL", "").strip())
    default_api_key = runtime_api_key(os.getenv("API_KEY", "").strip())
    default_start = runtime_start_date(os.getenv("API_START_DATE", "2025-01-01").strip())
    default_api_token = runtime_api_session_token(os.getenv("API_SESSION_TOKEN", "").strip())
    default_tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    default_limit = str(runtime_bot_limit(int(str(os.getenv("BOT_LIMIT", "30") or "30").strip() or "30")))

    print("=== NumPlus Telegram Bot Client ===")
    if args.no_input:
        api_base = (default_api if is_real_value(default_api) else "http://127.0.0.1:8000").rstrip("/")
        api_key = default_api_key.strip()
        tg_token = default_tg_token.strip()
        target_groups = load_groups()
        accounts = load_accounts()
        api_token = default_api_token if is_real_value(default_api_token) else ""
        start_date_raw = default_start or date.today().isoformat()
        start_date = normalize_start_date(start_date_raw)
        try:
            limit = int(default_limit or "30")
        except Exception:
            limit = 30
    else:
        api_base = runtime_api_base(default_api if is_real_value(default_api) else "http://127.0.0.1:8000").rstrip("/")
        api_key = runtime_api_key(default_api_key).strip()
        tg_token = ask_missing("Telegram bot token", default_tg_token)
        target_groups = load_groups()

        accounts = load_accounts()
        api_token = default_api_token if is_real_value(default_api_token) else ""
        start_date = normalize_start_date(default_start or date.today().isoformat())
        try:
            limit = int(default_limit or "30")
        except Exception:
            limit = 30

    if not tg_token:
        logger.error("telegram bot token missing")
        return
    if not target_groups:
        logger.warning("no target groups configured at startup; sender will stay idle until groups are added")

    check_api_health(api_base)

    try:
        run_loop(start_date, api_base, api_key, api_token, tg_token, target_groups, limit, args.once)
    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
