import argparse
import json
import os
import re
import time
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
COUNTRY_FILE = BASE_DIR / "country_codes.json"
PLATFORMS_FILE = BASE_DIR / "platforms.json"
ACCOUNTS_FILE = BASE_DIR / "accounts.json"
GROUPS_FILE = BASE_DIR / "groups.json"
STORE_FILE = BASE_DIR / "sent_codes_store.json"


def ask(prompt: str, default: str | None = None) -> str:
    if default is None:
        return input(f"{prompt}: ").strip()
    value = input(f"{prompt} [{default}]: ").strip()
    return value or default


def ask_missing(prompt: str, current: str) -> str:
    if current.strip():
        return current.strip()
    return ask(prompt)


def digits_only(text: str) -> str:
    return "".join(ch for ch in (text or "") if ch.isdigit())


def load_json_list(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
    except Exception:
        pass
    return []


def load_countries() -> list[dict[str, str]]:
    rows = [x for x in load_json_list(COUNTRY_FILE) if x.get("dial_code")]
    rows.sort(key=lambda x: len(str(x.get("dial_code", ""))), reverse=True)
    return rows


def load_platforms() -> dict[str, str]:
    rows = load_json_list(PLATFORMS_FILE)
    out: dict[str, str] = {}
    for r in rows:
        key = str(r.get("key", "")).strip().lower()
        short = str(r.get("short", "")).strip()
        if key and short:
            out[key] = short
    return out


def load_accounts() -> list[dict[str, str]]:
    rows = load_json_list(ACCOUNTS_FILE)
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
    rows = load_json_list(GROUPS_FILE)
    out: list[dict[str, str]] = []
    for r in rows:
        enabled = bool(r.get("enabled", True))
        chat_id = str(r.get("chat_id", "")).strip()
        name = str(r.get("name", chat_id)).strip() or chat_id
        if enabled and chat_id:
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
    key = (service_name or "").strip().lower()
    if key in platforms:
        return str(platforms[key]).upper()
    return (service_name[:2] or "NA").upper()


def service_emoji_id(service_name: str, platform_rows: list[dict]) -> str:
    key = (service_name or "").strip().lower()
    for row in platform_rows:
        if str(row.get("key", "")).strip().lower() == key:
            return str(row.get("emoji_id", "")).strip()
    return ""


def service_emoji_alt(service_name: str, platform_rows: list[dict]) -> str:
    key = (service_name or "").strip().lower()
    for row in platform_rows:
        if str(row.get("key", "")).strip().lower() == key:
            alt = str(row.get("emoji", "")).strip()
            if alt:
                return alt
    return "✨"


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


def build_message(item: dict, countries: list[dict[str, str]], platforms: dict[str, str], platform_rows: list[dict]) -> str:
    raw_number = str(item.get("number", ""))
    number_digits = digits_only(raw_number)
    number_with_plus = f"+{number_digits}" if number_digits else raw_number
    service_name = str(item.get("service_name", "Unknown"))
    short = service_short(service_name, platforms)
    semoji_id = service_emoji_id(service_name, platform_rows)
    semoji_alt = service_emoji_alt(service_name, platform_rows)
    use_custom_emoji = os.getenv("USE_CUSTOM_EMOJI", "0").strip() == "1"
    country = detect_country(raw_number, countries)
    iso2 = country.get("iso2", "UN")
    flag = iso_to_flag(iso2)
    message_text = str(item.get("message", "")).strip()
    escaped_head = _md_escape(f"{short} {iso2} {flag} {number_with_plus}")
    escaped_msg = _md_code_escape(message_text)
    custom = f"![{semoji_alt}](tg://emoji?id={semoji_id}) " if (use_custom_emoji and semoji_id) else f"{semoji_alt} "
    return f"> {custom}*{escaped_head}*\n```\n{escaped_msg}\n```"


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


def load_store() -> dict:
    if not STORE_FILE.exists():
        return {"by_start_date": {}}
    try:
        data = json.loads(STORE_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("by_start_date"), dict):
            return data
    except Exception:
        pass
    return {"by_start_date": {}}


def save_store(store: dict) -> None:
    STORE_FILE.write_text(json.dumps(store, ensure_ascii=False, indent=2), encoding="utf-8")


def msg_key(item: dict) -> str:
    number = str(item.get("number", ""))
    service_name = str(item.get("service_name", ""))
    message = str(item.get("message", ""))
    rng = str(item.get("range", ""))
    return f"{number}|{service_name}|{rng}|{message}"


def normalize_start_date(raw: str) -> str:
    v = (raw or "").strip()
    parts = v.split("-")
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        y, m, d = parts
        if len(y) == 4:
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    return date.today().isoformat()


def api_login(api_base: str, email: str, password: str) -> str | None:
    try:
        r = requests.post(
            f"{api_base}/api/v1/auth/login",
            json={"email": email, "password": password},
            timeout=90,
        )
        j = r.json()
        if r.status_code == 200:
            return (j.get("data") or {}).get("token")
    except Exception:
        return None
    return None


def fetch_messages(api_base: str, api_token: str, start_date: str, limit: int) -> list[dict]:
    r = requests.post(
        f"{api_base}/api/v1/biring/code",
        json={"token": api_token, "start_date": start_date},
        timeout=600,
    )
    j = r.json()
    if r.status_code != 200:
        raise RuntimeError(str(j))
    return ((j.get("data") or {}).get("messages") or [])[:limit]


def run_loop(start_date: str, api_base: str, api_token: str, tg_token: str, target_groups: list[dict[str, str]], limit: int, once: bool) -> None:
    countries = load_countries()
    platform_rows = load_json_list(PLATFORMS_FILE)
    platforms = load_platforms()
    store = load_store()
    bucket = store["by_start_date"].setdefault(start_date, {"seen_keys": [], "sent": []})
    seen_keys = set(bucket.get("seen_keys", []))

    accounts = load_accounts()
    account_tokens: dict[str, str] = {}
    for acc in accounts:
        tok = api_login(api_base, acc["email"], acc["password"])
        if tok:
            account_tokens[acc["name"]] = tok
            print(f"account ready: {acc['name']}")
        else:
            print(f"account login failed: {acc['name']}")

    print(f"\nStarted polling every 30 seconds | start_date={start_date} | limit={limit}")
    print("Press Ctrl+C to stop.\n")

    while True:
        all_rows: list[dict] = []

        if api_token:
            try:
                all_rows.extend(fetch_messages(api_base, api_token, start_date, limit))
            except Exception as exc:
                print(f"API token fetch failed: {exc}")

        for acc in accounts:
            name = acc["name"]
            tok = account_tokens.get(name)
            if not tok:
                tok = api_login(api_base, acc["email"], acc["password"])
                if tok:
                    account_tokens[name] = tok
            if not tok:
                continue
            try:
                all_rows.extend(fetch_messages(api_base, tok, start_date, limit))
            except Exception:
                new_tok = api_login(api_base, acc["email"], acc["password"])
                if not new_tok:
                    continue
                account_tokens[name] = new_tok
                try:
                    all_rows.extend(fetch_messages(api_base, new_tok, start_date, limit))
                except Exception:
                    continue

        uniq: dict[str, dict] = {}
        for row in all_rows:
            uniq[msg_key(row)] = row
        rows = list(uniq.values())[:limit]
        new_rows = [x for x in rows if msg_key(x) not in seen_keys]

        if not new_rows:
            print(f"[{time.strftime('%H:%M:%S')}] no new messages")
            if once:
                return
            time.sleep(30)
            continue

        print(f"[{time.strftime('%H:%M:%S')}] new messages: {len(new_rows)}")
        for idx, item in enumerate(new_rows, start=1):
            number = str(item.get("number", ""))
            message_text = str(item.get("message", ""))
            code = extract_code(message_text) or number
            text = build_message(item, countries, platforms, platform_rows)

            any_sent = False
            sent_info: list[dict[str, str | int | None]] = []
            for grp in target_groups:
                gid = grp["chat_id"]
                gname = grp["name"]
                try:
                    j = send_telegram_message(tg_token, gid, text, code)
                except Exception as exc:
                    print(f"[{idx}] send failed ({gname}): {exc}")
                    continue
                if not j.get("ok"):
                    print(f"[{idx}] send failed ({gname}): {j}")
                    continue
                any_sent = True
                msg_id = (j.get("result") or {}).get("message_id")
                sent_info.append({"group": gname, "chat_id": gid, "message_id": msg_id})
                print(f"[{idx}] sent -> {gname} | message_id={msg_id} | code={code}")

            if any_sent:
                mkey = msg_key(item)
                seen_keys.add(mkey)
                bucket["sent"].append(
                    {
                        "number": number,
                        "code": code,
                        "service_name": item.get("service_name"),
                        "range": item.get("range"),
                        "message": item.get("message"),
                        "revenue": item.get("revenue"),
                        "groups": sent_info,
                        "sent_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
                bucket["seen_keys"] = list(seen_keys)
                save_store(store)

        if once:
            return
        time.sleep(30)


def main() -> None:
    parser = argparse.ArgumentParser(description="NumPlus Telegram Bot Client")
    parser.add_argument("--once", action="store_true", help="Run one polling cycle then exit")
    args = parser.parse_args()

    load_dotenv(BASE_DIR / ".env")

    default_api = os.getenv("API_BASE_URL", "http://127.0.0.1:8000").strip()
    default_start = os.getenv("API_START_DATE", "2025-01-01").strip()
    default_api_token = os.getenv("API_SESSION_TOKEN", "").strip()
    default_tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    default_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    default_limit = os.getenv("BOT_LIMIT", "30").strip()

    print("=== NumPlus Telegram Bot Client ===")
    api_base = ask_missing("API domain", default_api).rstrip("/")
    tg_token = ask_missing("Telegram bot token", default_tg_token)

    groups = load_groups()
    if groups:
        target_groups = groups
    else:
        chat_id = ask_missing("Telegram group/chat id", default_chat_id)
        target_groups = [{"name": "default_group", "chat_id": chat_id}]

    # Ask only if token missing and no usable accounts file.
    accounts = load_accounts()
    api_token = default_api_token
    if not api_token and not accounts:
        api_token = ask("API session token (missing and no accounts found)")

    # Always ask start date every run.
    start_date_raw = ask("Start date YYYY-MM-DD", default_start or date.today().isoformat())
    start_date = normalize_start_date(start_date_raw)
    if start_date != start_date_raw:
        print(f"Normalized/invalid date input. Using: {start_date}")

    limit_raw = ask("Messages limit", default_limit or "30")
    try:
        limit = max(1, min(100, int(limit_raw)))
    except Exception:
        limit = 30

    try:
        run_loop(start_date, api_base, api_token, tg_token, target_groups, limit, args.once)
    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
