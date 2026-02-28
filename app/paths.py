import os
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]


def _resolve_namespace() -> str:
    raw = str(os.getenv("IVASMS_DATA_NAMESPACE", "")).strip().lower()
    if not raw:
        return "main"
    cleaned = re.sub(r"[^a-z0-9_-]+", "-", raw).strip("-_")
    return cleaned or "main"


NAMESPACE = _resolve_namespace()

if NAMESPACE == "main":
    DATA_DIR = BASE_DIR / "data"
    DAILY_STORE_DIR = BASE_DIR / "daily_messages"
    EXPORT_DIR = BASE_DIR / "exports"
    LOGS_DIR = BASE_DIR / "logs"
else:
    DATA_DIR = BASE_DIR / "data" / NAMESPACE
    DAILY_STORE_DIR = BASE_DIR / "daily_messages" / NAMESPACE
    EXPORT_DIR = BASE_DIR / "exports" / NAMESPACE
    LOGS_DIR = BASE_DIR / "logs" / NAMESPACE

DB_FILE = DATA_DIR / "storage.db"

COUNTRY_FILE = BASE_DIR / "country_codes.json"
PLATFORMS_FILE = BASE_DIR / "platforms.json"
ACCOUNTS_FILE = BASE_DIR / "accounts.json"
GROUPS_FILE = BASE_DIR / "groups.json"
STORE_FILE = BASE_DIR / "sent_codes_store.json"
TOKEN_CACHE_FILE = BASE_DIR / "token_cache.json"
RUNTIME_CONFIG_FILE = BASE_DIR / "runtime_config.json"
RANGES_STORE_FILE = BASE_DIR / "ranges_store.json"

def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DAILY_STORE_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
