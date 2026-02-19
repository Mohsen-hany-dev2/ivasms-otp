import argparse
import json
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
ACCOUNTS_FILE = BASE_DIR / "accounts.json"
GROUPS_FILE = BASE_DIR / "groups.json"
STORE_FILE = BASE_DIR / "sent_codes_store.json"
PLATFORMS_FILE = BASE_DIR / "platforms.json"


def load_json(path: Path, fallback):
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def save_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def add_account(name: str, email: str, password: str, enabled: bool) -> None:
    rows = load_json(ACCOUNTS_FILE, [])
    rows = [x for x in rows if not (x.get("email") == email)]
    rows.append({"name": name, "email": email, "password": password, "enabled": enabled})
    save_json(ACCOUNTS_FILE, rows)
    print(f"added account: {email}")


def add_group(name: str, chat_id: str, enabled: bool) -> None:
    rows = load_json(GROUPS_FILE, [])
    rows = [x for x in rows if not (str(x.get("chat_id")) == str(chat_id))]
    rows.append({"name": name, "chat_id": str(chat_id), "enabled": enabled})
    save_json(GROUPS_FILE, rows)
    print(f"added group: {chat_id}")


def clear_store(start_date: str | None) -> None:
    data = load_json(STORE_FILE, {"by_start_date": {}})
    if start_date:
        data.setdefault("by_start_date", {}).pop(start_date, None)
        save_json(STORE_FILE, data)
        print(f"cleared store for start_date={start_date}")
    else:
        save_json(STORE_FILE, {"by_start_date": {}})
        print("cleared all stored messages")


def list_accounts() -> None:
    rows = load_json(ACCOUNTS_FILE, [])
    print(json.dumps(rows, ensure_ascii=False, indent=2))


def list_groups() -> None:
    rows = load_json(GROUPS_FILE, [])
    print(json.dumps(rows, ensure_ascii=False, indent=2))


def set_platform_emoji_id(key: str, emoji_id: str) -> None:
    rows = load_json(PLATFORMS_FILE, [])
    updated = False
    for row in rows:
        if str(row.get("key", "")).strip().lower() == key.strip().lower():
            row["emoji_id"] = emoji_id.strip()
            updated = True
            break
    if not updated:
        rows.append(
            {
                "key": key.strip().lower(),
                "name_ar": key,
                "name_en": key,
                "short": key[:2].upper(),
                "emoji": "",
                "emoji_id": emoji_id.strip(),
            }
        )
    save_json(PLATFORMS_FILE, rows)
    print(f"set emoji_id for platform '{key}'")


def main() -> None:
    p = argparse.ArgumentParser(description="Manage bot accounts/groups/store")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_add_acc = sub.add_parser("add-account")
    p_add_acc.add_argument("--name", required=True)
    p_add_acc.add_argument("--email", required=True)
    p_add_acc.add_argument("--password", required=True)
    p_add_acc.add_argument("--disabled", action="store_true")

    p_add_grp = sub.add_parser("add-group")
    p_add_grp.add_argument("--name", required=True)
    p_add_grp.add_argument("--chat-id", required=True)
    p_add_grp.add_argument("--disabled", action="store_true")

    p_clear = sub.add_parser("clear-store")
    p_clear.add_argument("--start-date")

    sub.add_parser("list-accounts")
    sub.add_parser("list-groups")

    p_set_emoji = sub.add_parser("set-platform-emoji-id")
    p_set_emoji.add_argument("--key", required=True)
    p_set_emoji.add_argument("--emoji-id", required=True)

    args = p.parse_args()

    if args.cmd == "add-account":
        add_account(args.name, args.email, args.password, enabled=not args.disabled)
    elif args.cmd == "add-group":
        add_group(args.name, args.chat_id, enabled=not args.disabled)
    elif args.cmd == "clear-store":
        clear_store(args.start_date)
    elif args.cmd == "list-accounts":
        list_accounts()
    elif args.cmd == "list-groups":
        list_groups()
    elif args.cmd == "set-platform-emoji-id":
        set_platform_emoji_id(args.key, args.emoji_id)


if __name__ == "__main__":
    main()
