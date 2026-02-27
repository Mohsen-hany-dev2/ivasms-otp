import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from app.paths import (
    ACCOUNTS_FILE,
    BASE_DIR,
    COUNTRY_FILE,
    EXPORT_DIR,
    GROUPS_FILE,
    PLATFORMS_FILE,
    RANGES_STORE_FILE,
    RUNTIME_CONFIG_FILE,
)
from app.storage import clear_daily_store, get_daily_store, list_daily_store_days
from app.storage import load_json as db_load_json, save_json as db_save_json


MAIN_TITLE = "༺═══⇓ لوحة التحكم ⇓═══༻"
TRAFFIC_TITLE = "༺═════⇓ الترافيك ⇓═════༻"
NUMBERS_TITLE = "༺═════⇓ الأرقام ⇓═════༻"
ACCOUNTS_TITLE = "༺═════⇓ حساباتي ⇓═════༻"
GROUPS_TITLE = "༺═════⇓ الجروبات ⇓═════༻"
STATS_TITLE = "༺═════⇓ الاحصائيات ⇓═════༻"
MESSAGES_TITLE = "༺═════⇓ الرسائل ⇓═════༻"

TOKEN_KEYS = ("token", "access_token", "session_token", "api_token", "jwt")
DEFAULT_ADMIN_IDS = {7011309417}


class PanelBot:
    def __init__(self) -> None:
        load_dotenv(BASE_DIR / ".env")
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.api_base = os.getenv("API_BASE_URL", "").strip().rstrip("/")
        self.api_session_token = os.getenv("API_SESSION_TOKEN", "").strip()
        self.poll_interval = 2
        self.user_state: dict[int, dict[str, Any]] = {}
        self.last_update_id = 0
        self.admin_ids = self._load_admin_ids()
        self.executor = ThreadPoolExecutor(max_workers=6)
        self.cache_ttl_seconds = 45
        self.platforms_cache: dict[str, Any] = {"at": 0.0, "data": []}
        self.traffic_cache: dict[str, dict[str, Any]] = {}
        self.user_traffic_cache: dict[int, dict[str, list[dict[str, str]]]] = {}
        self.user_numbers_cache: dict[int, dict[str, list[dict[str, str]]]] = {}
        self.user_numbers_view_account: dict[int, str | None] = {}
        self.user_view_rev: dict[int, int] = {}
        self.user_lang: dict[int, str] = {}

        if not self.bot_token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is missing in .env")
        if not self.api_base:
            raise RuntimeError("API_BASE_URL is missing in .env")

        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        self.ensure_runtime_config()
        self.refresh_runtime_settings()

    def _load_admin_ids(self) -> set[int]:
        raw = os.getenv("PANEL_ADMIN_IDS", "").strip()
        if not raw:
            return set(DEFAULT_ADMIN_IDS)
        out: set[int] = set()
        for chunk in re.split(r"[,\s]+", raw):
            value = chunk.strip()
            if not value:
                continue
            if value.isdigit():
                out.add(int(value))
        if not out:
            return set(DEFAULT_ADMIN_IDS)
        return out

    def _env_admin_ids(self) -> set[int]:
        raw = os.getenv("PANEL_ADMIN_IDS", "").strip()
        out: set[int] = set()
        for chunk in re.split(r"[,\s]+", raw):
            value = chunk.strip()
            if value.isdigit():
                out.add(int(value))
        return out

    def is_admin(self, user_id: int) -> bool:
        return int(user_id or 0) in self.admin_ids

    def is_primary_admin(self, user_id: int) -> bool:
        # Main owner/admin account only.
        return int(user_id or 0) == min(DEFAULT_ADMIN_IDS)

    def bump_view_rev(self, user_id: int) -> int:
        uid = int(user_id)
        self.user_view_rev[uid] = int(self.user_view_rev.get(uid, 0)) + 1
        return self.user_view_rev[uid]

    def is_view_current(self, user_id: int, view_rev: int | None) -> bool:
        if view_rev is None:
            return True
        return int(self.user_view_rev.get(int(user_id), 0)) == int(view_rev)

    # -------------------------- files/json --------------------------
    def load_json(self, path: Path, fallback: Any) -> Any:
        return db_load_json(path, fallback)

    def save_json(self, path: Path, data: Any) -> None:
        db_save_json(path, data)

    def _is_valid_day(self, value: str) -> bool:
        v = str(value or "").strip()
        parts = v.split("-")
        if len(parts) != 3 or any(not p.isdigit() for p in parts):
            return False
        y, m, d = parts
        if len(y) != 4:
            return False
        try:
            date(int(y), int(m), int(d))
        except ValueError:
            return False
        return True

    def ensure_runtime_config(self) -> None:
        data = self.load_json(RUNTIME_CONFIG_FILE, {})
        if not isinstance(data, dict):
            data = {}
        if "fetch_codes_enabled" not in data:
            data["fetch_codes_enabled"] = True
        if "messages_start_date" not in data:
            data["messages_start_date"] = os.getenv("API_START_DATE", "2025-01-01").strip() or "2025-01-01"
        if "messages_start_date_auto_today" not in data:
            data["messages_start_date_auto_today"] = False
        if "start_date_prompt_pending" not in data:
            data["start_date_prompt_pending"] = not self._is_valid_day(str(data.get("messages_start_date", "")).strip())
        if "api_base_url" not in data:
            data["api_base_url"] = os.getenv("API_BASE_URL", "").strip().rstrip("/")
        if "api_session_token" not in data:
            data["api_session_token"] = os.getenv("API_SESSION_TOKEN", "").strip()
        if "bot_limit" not in data:
            data["bot_limit"] = int(str(os.getenv("BOT_LIMIT", "30") or "30").strip() or "30")
        if not isinstance(data.get("panel_admin_ids"), list):
            env_admins = sorted(self._env_admin_ids() or DEFAULT_ADMIN_IDS)
            data["panel_admin_ids"] = env_admins
        if not isinstance(data.get("language_overrides"), dict):
            data["language_overrides"] = {}
        if not isinstance(data.get("managed_bots"), list):
            data["managed_bots"] = []
        self.save_json(RUNTIME_CONFIG_FILE, data)

    def _load_runtime_cfg(self) -> dict[str, Any]:
        cfg = self.load_json(RUNTIME_CONFIG_FILE, {})
        return cfg if isinstance(cfg, dict) else {}

    def _save_runtime_cfg(self, cfg: dict[str, Any]) -> None:
        now = self._now_marker()
        cfg["updated_at"] = now
        cfg["bot_restart_requested_at"] = now
        self.save_json(RUNTIME_CONFIG_FILE, cfg)

    def request_bot_restart(self) -> None:
        cfg = self.load_json(RUNTIME_CONFIG_FILE, {})
        if not isinstance(cfg, dict):
            cfg = {}
        now = self._now_marker()
        cfg["updated_at"] = now
        cfg["bot_restart_requested_at"] = now
        self.save_json(RUNTIME_CONFIG_FILE, cfg)

    def _now_marker(self) -> str:
        # Use microseconds so repeated actions in the same second still trigger.
        return datetime.now().isoformat(sep=" ", timespec="microseconds")

    def refresh_runtime_settings(self) -> None:
        cfg = self._load_runtime_cfg()
        api_base = str(cfg.get("api_base_url", "")).strip().rstrip("/")
        if api_base:
            self.api_base = api_base
        session_tok = str(cfg.get("api_session_token", "")).strip()
        if session_tok:
            self.api_session_token = session_tok
        admins: set[int] = set(DEFAULT_ADMIN_IDS) | self._env_admin_ids()
        runtime_admins = cfg.get("panel_admin_ids")
        if isinstance(runtime_admins, list):
            for x in runtime_admins:
                s = str(x).strip()
                if s.isdigit():
                    admins.add(int(s))
        self.admin_ids = admins or set(DEFAULT_ADMIN_IDS)

    def get_runtime_api_base(self) -> str:
        cfg = self._load_runtime_cfg()
        value = str(cfg.get("api_base_url", "")).strip().rstrip("/")
        return value or self.api_base

    def set_runtime_api_base(self, value: str) -> bool:
        v = str(value or "").strip().rstrip("/")
        if not (v.startswith("http://") or v.startswith("https://")):
            return False
        cfg = self._load_runtime_cfg()
        cfg["api_base_url"] = v
        self._save_runtime_cfg(cfg)
        self.refresh_runtime_settings()
        return True

    def get_runtime_bot_limit(self) -> int:
        cfg = self._load_runtime_cfg()
        try:
            n = int(str(cfg.get("bot_limit", "30")).strip())
        except Exception:
            n = 30
        if n <= 0:
            return 0
        return max(1, min(10000, n))

    def set_runtime_bot_limit(self, value: str) -> bool:
        try:
            n = int(str(value or "").strip())
        except Exception:
            return False
        if n <= 0:
            n = 0
        else:
            n = max(1, min(10000, n))
        cfg = self._load_runtime_cfg()
        cfg["bot_limit"] = n
        self._save_runtime_cfg(cfg)
        return True

    def get_runtime_admin_ids(self) -> list[int]:
        return sorted(self.admin_ids)

    def add_runtime_admin(self, value: str) -> bool:
        v = str(value or "").strip()
        if not v.isdigit():
            return False
        aid = int(v)
        cfg = self._load_runtime_cfg()
        admins = cfg.get("panel_admin_ids")
        if not isinstance(admins, list):
            admins = []
        out: list[int] = []
        seen: set[int] = set()
        for x in admins + [aid]:
            sx = str(x).strip()
            if not sx.isdigit():
                continue
            xi = int(sx)
            if xi in seen:
                continue
            seen.add(xi)
            out.append(xi)
        cfg["panel_admin_ids"] = out
        self._save_runtime_cfg(cfg)
        self.refresh_runtime_settings()
        return True

    def remove_runtime_admin(self, value: str) -> bool:
        v = str(value or "").strip()
        if not v.isdigit():
            return False
        aid = int(v)
        cfg = self._load_runtime_cfg()
        admins = cfg.get("panel_admin_ids")
        if not isinstance(admins, list):
            admins = []
        out: list[int] = []
        removed = False
        for x in admins:
            sx = str(x).strip()
            if not sx.isdigit():
                continue
            xi = int(sx)
            if xi == aid:
                removed = True
                continue
            out.append(xi)
        if not removed:
            return False
        cfg["panel_admin_ids"] = out
        self._save_runtime_cfg(cfg)
        self.refresh_runtime_settings()
        return True

    def get_user_lang_override(self, user_id: int) -> str:
        cfg = self.load_json(RUNTIME_CONFIG_FILE, {})
        if not isinstance(cfg, dict):
            return ""
        overrides = cfg.get("language_overrides")
        if not isinstance(overrides, dict):
            return ""
        lang = str(overrides.get(str(int(user_id)), "")).strip().lower()
        return lang if lang in {"ar", "en"} else ""

    def set_user_lang_override(self, user_id: int, lang: str) -> None:
        value = str(lang or "").strip().lower()
        if value not in {"ar", "en"}:
            return
        cfg = self.load_json(RUNTIME_CONFIG_FILE, {})
        if not isinstance(cfg, dict):
            cfg = {}
        overrides = cfg.get("language_overrides")
        if not isinstance(overrides, dict):
            overrides = {}
        overrides[str(int(user_id))] = value
        cfg["language_overrides"] = overrides
        cfg["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_json(RUNTIME_CONFIG_FILE, cfg)

    def get_runtime_start_date(self) -> str:
        cfg = self.load_json(RUNTIME_CONFIG_FILE, {})
        raw = str(cfg.get("messages_start_date", "")).strip() if isinstance(cfg, dict) else ""
        if isinstance(cfg, dict) and bool(cfg.get("messages_start_date_auto_today", False)):
            today = date.today().strftime("%Y-%m-%d")
            if raw != today:
                cfg["messages_start_date"] = today
                cfg["updated_at"] = self._now_marker()
                self.save_json(RUNTIME_CONFIG_FILE, cfg)
                return today
        if self._is_valid_day(raw):
            return raw
        env_default = os.getenv("API_START_DATE", "2025-01-01").strip() or "2025-01-01"
        return env_default

    def is_start_date_prompt_pending(self) -> bool:
        cfg = self.load_json(RUNTIME_CONFIG_FILE, {})
        if isinstance(cfg, dict) and "start_date_prompt_pending" in cfg:
            return bool(cfg.get("start_date_prompt_pending", False))
        # Backward compatibility: if no valid start date, keep prompt pending.
        return not self._is_valid_day(self.get_runtime_start_date())

    def set_start_date_prompt_pending(self, pending: bool) -> None:
        cfg = self.load_json(RUNTIME_CONFIG_FILE, {})
        if not isinstance(cfg, dict):
            cfg = {}
        cfg["start_date_prompt_pending"] = bool(pending)
        cfg["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_json(RUNTIME_CONFIG_FILE, cfg)

    def set_runtime_start_date(self, day_value: str) -> bool:
        value = str(day_value or "").strip()
        if not self._is_valid_day(value):
            return False
        cfg = self.load_json(RUNTIME_CONFIG_FILE, {})
        if not isinstance(cfg, dict):
            cfg = {}
        cfg["messages_start_date"] = value
        cfg["messages_start_date_auto_today"] = value == date.today().strftime("%Y-%m-%d")
        cfg["start_date_prompt_pending"] = False
        self._save_runtime_cfg(cfg)
        return True

    def load_managed_bots(self) -> list[dict[str, Any]]:
        cfg = self._load_runtime_cfg()
        rows = cfg.get("managed_bots")
        if not isinstance(rows, list):
            return []
        out: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            tok = str(row.get("token", "")).strip()
            if not tok:
                continue
            storage = str(row.get("storage", "private")).strip().lower()
            if storage not in {"private", "shared"}:
                storage = "private"
            out.append(
                {
                    "token": tok,
                    "storage": storage,
                    "created_by": str(row.get("created_by", "")).strip(),
                    "created_at": str(row.get("created_at", "")).strip(),
                }
            )
        return out

    def save_managed_bots(self, rows: list[dict[str, Any]]) -> None:
        cfg = self._load_runtime_cfg()
        cfg["managed_bots"] = rows
        self._save_runtime_cfg(cfg)

    def upsert_managed_bot(self, token: str, storage: str, created_by: int) -> None:
        tok = str(token or "").strip()
        if not tok:
            return
        storage_value = str(storage or "private").strip().lower()
        if storage_value not in {"private", "shared"}:
            storage_value = "private"
        rows = self.load_managed_bots()
        out: list[dict[str, Any]] = []
        found = False
        for row in rows:
            if str(row.get("token", "")).strip() == tok:
                row["storage"] = storage_value
                row["created_by"] = str(created_by)
                row["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                found = True
            out.append(row)
        if not found:
            out.append(
                {
                    "token": tok,
                    "storage": storage_value,
                    "created_by": str(created_by),
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
        self.save_managed_bots(out)

    def delete_managed_bot(self, token: str) -> bool:
        tok = str(token or "").strip()
        if not tok:
            return False
        rows = self.load_managed_bots()
        out = [row for row in rows if str(row.get("token", "")).strip() != tok]
        if len(out) == len(rows):
            return False
        self.save_managed_bots(out)
        return True

    def request_messages_refresh(self) -> None:
        cfg = self.load_json(RUNTIME_CONFIG_FILE, {})
        if not isinstance(cfg, dict):
            cfg = {}
        now = self._now_marker()
        cfg["messages_update_requested_at"] = now
        cfg["updated_at"] = now
        self.save_json(RUNTIME_CONFIG_FILE, cfg)

    def mark_runtime_change(self) -> None:
        # Unified marker update for any runtime-affecting change.
        self.request_bot_restart()
        self.request_messages_refresh()

    def fetch_codes_enabled(self) -> bool:
        data = self.load_json(RUNTIME_CONFIG_FILE, {"fetch_codes_enabled": True})
        return bool(data.get("fetch_codes_enabled", True))

    def set_fetch_codes_enabled(self, enabled: bool) -> None:
        data = self.load_json(RUNTIME_CONFIG_FILE, {})
        if not isinstance(data, dict):
            data = {}
        data["fetch_codes_enabled"] = bool(enabled)
        self._save_runtime_cfg(data)

    def load_accounts(self) -> list[dict[str, Any]]:
        rows = self.load_json(ACCOUNTS_FILE, [])
        if not isinstance(rows, list):
            return []
        out: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or row.get("email") or "account").strip()
            email = str(row.get("email") or "").strip()
            password = str(row.get("password") or "").strip()
            enabled = bool(row.get("enabled", True))
            if email and password:
                out.append({"name": name, "email": email, "password": password, "enabled": enabled})
        return out

    def save_accounts(self, rows: list[dict[str, Any]]) -> None:
        self.save_json(ACCOUNTS_FILE, rows)
        self.request_bot_restart()

    def load_groups(self) -> list[dict[str, Any]]:
        rows = self.load_json(GROUPS_FILE, [])
        if not isinstance(rows, list):
            return []
        out: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or row.get("chat_id") or "group").strip()
            chat_id = str(row.get("chat_id") or "").strip()
            enabled = bool(row.get("enabled", True))
            if chat_id:
                out.append({"name": name, "chat_id": chat_id, "enabled": enabled})
        return out

    def save_groups(self, rows: list[dict[str, Any]]) -> None:
        self.save_json(GROUPS_FILE, rows)
        self.request_bot_restart()

    def load_services(self) -> list[dict[str, str]]:
        rows = self.load_json(PLATFORMS_FILE, [])
        if not isinstance(rows, list):
            return []
        out: list[dict[str, str]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = str(row.get("key", "")).strip()
            short = str(row.get("short", "")).strip()
            emoji = str(row.get("emoji", "")).strip()
            emoji_id = str(row.get("emoji_id", "")).strip()
            if key:
                out.append({"key": key, "short": short, "emoji": emoji, "emoji_id": emoji_id})
        return out

    def save_services(self, rows: list[dict[str, str]]) -> None:
        self.save_json(PLATFORMS_FILE, rows)
        self.mark_runtime_change()

    def load_countries_store(self) -> list[dict[str, str]]:
        rows = self.load_json(COUNTRY_FILE, [])
        if not isinstance(rows, list):
            return []
        out: list[dict[str, str]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            dial = str(row.get("dial_code", "")).strip()
            if not dial:
                continue
            out.append(
                {
                    "dial_code": dial,
                    "name_ar": str(row.get("name_ar", "")).strip(),
                    "name_en": str(row.get("name_en", "")).strip(),
                    "iso2": str(row.get("iso2", "")).strip().upper(),
                    "emoji": str(row.get("emoji", "")).strip(),
                    "emoji_id": str(row.get("emoji_id", "")).strip(),
                }
            )
        return out

    def save_countries_store(self, rows: list[dict[str, str]]) -> None:
        self.save_json(COUNTRY_FILE, rows)
        self.mark_runtime_change()

    def normalize_group_target(self, raw: str) -> str:
        value = str(raw or "").strip()
        if not value:
            return ""
        if value.startswith("-100") and value[1:].isdigit():
            return value
        if value.startswith("@"):
            return value
        if "t.me/" in value:
            part = value.split("t.me/", 1)[1].strip().strip("/")
            part = part.split("?", 1)[0].strip("/")
            if part:
                if part.startswith("+") or part.startswith("joinchat/") or part.startswith("c/"):
                    return value
                return f"@{part.lstrip('@')}"
        if value.isdigit():
            return value
        return value

    def load_ranges_store(self) -> dict[str, Any]:
        data = self.load_json(RANGES_STORE_FILE, {})
        if not isinstance(data, dict):
            data = {}
        if not isinstance(data.get("ranges"), dict):
            data["ranges"] = {}
        if not isinstance(data.get("meta"), dict):
            data["meta"] = {}
        return data

    def save_ranges_store(self, store: dict[str, Any]) -> None:
        store["meta"] = {
            **(store.get("meta") if isinstance(store.get("meta"), dict) else {}),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.save_json(RANGES_STORE_FILE, store)

    def range_limit_total(self) -> int:
        raw = os.getenv("RANGE_MAX_TOTAL", "1000").strip()
        try:
            value = int(raw)
        except Exception:
            value = 1000
        return max(50, value)

    # -------------------------- telegram api --------------------------
    def tg_api(self, method: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"https://api.telegram.org/bot{self.bot_token}/{method}"
        try:
            r = requests.post(url, json=payload or {}, timeout=40)
            return r.json()
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def _md_escape(self, text: str) -> str:
        out = text or ""
        for ch in ("\\", "*", "_", "`", "["):
            out = out.replace(ch, f"\\{ch}")
        return out

    def _html_escape(self, text: str) -> str:
        out = str(text or "")
        out = out.replace("&", "&amp;")
        out = out.replace("<", "&lt;")
        out = out.replace(">", "&gt;")
        return out

    def _format_text(self, text: str) -> str:
        lines: list[str] = []
        for raw in (text or "").splitlines():
            line = raw.rstrip()
            if not line:
                lines.append("")
                continue
            if line.startswith("__BLOCK__ "):
                title = line.replace("__BLOCK__ ", "", 1)
                lines.append(f"<pre>{self._html_escape(title)}</pre>")
            else:
                lines.append(f"<blockquote><b>{self._html_escape(line)}</b></blockquote>")
        return "\n".join(lines)

    def _pad_text_for_keyboard(self, text: str, keyboard: list[list[dict[str, Any]]] | None) -> str:
        return text or ""

    def send_text(self, chat_id: int | str, text: str, keyboard: list[list[dict[str, Any]]] | None = None) -> None:
        padded_text = self._pad_text_for_keyboard(text, keyboard)
        formatted = self._format_text(padded_text)
        body: dict[str, Any] = {
            "chat_id": chat_id,
            "text": formatted,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if keyboard is not None:
            body["reply_markup"] = {"inline_keyboard": keyboard}
        res = self.tg_api("sendMessage", body)
        if res.get("ok"):
            return

        # Fallback for parse/style incompatibilities.
        body_fallback: dict[str, Any] = {
            "chat_id": chat_id,
            "text": formatted,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if keyboard is not None:
            body_fallback["reply_markup"] = {"inline_keyboard": self._sanitize_keyboard(keyboard)}
        fallback_res = self.tg_api("sendMessage", body_fallback)
        if fallback_res.get("ok"):
            return

        plain_body: dict[str, Any] = {
            "chat_id": chat_id,
            "text": padded_text.replace("__SPACER__", " "),
            "disable_web_page_preview": True,
        }
        if keyboard is not None:
            plain_body["reply_markup"] = {"inline_keyboard": self._sanitize_keyboard(keyboard)}
        self.tg_api("sendMessage", plain_body)

    def edit_text(self, chat_id: int | str, message_id: int, text: str, keyboard: list[list[dict[str, Any]]] | None = None) -> None:
        padded_text = self._pad_text_for_keyboard(text, keyboard)
        formatted = self._format_text(padded_text)
        body: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": formatted,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if keyboard is not None:
            body["reply_markup"] = {"inline_keyboard": keyboard}
        result = self.tg_api("editMessageText", body)
        if not result.get("ok"):
            # Retry edit without parse mode and with sanitized keyboard.
            body_fallback: dict[str, Any] = {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": formatted,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            if keyboard is not None:
                body_fallback["reply_markup"] = {"inline_keyboard": self._sanitize_keyboard(keyboard)}
            retry = self.tg_api("editMessageText", body_fallback)
            if not retry.get("ok"):
                plain_body: dict[str, Any] = {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "text": padded_text.replace("__SPACER__", " "),
                    "disable_web_page_preview": True,
                }
                if keyboard is not None:
                    plain_body["reply_markup"] = {"inline_keyboard": self._sanitize_keyboard(keyboard)}
                retry_plain = self.tg_api("editMessageText", plain_body)
                if not retry_plain.get("ok"):
                    self.send_text(chat_id, text, keyboard)

    def answer_callback(self, callback_id: str, text: str = "") -> None:
        payload = {"callback_query_id": callback_id}
        if text:
            payload["text"] = text[:180]
            payload["show_alert"] = False
        self.tg_api("answerCallbackQuery", payload)

    def send_document(self, chat_id: int | str, file_path: Path, caption: str = "") -> None:
        url = f"https://api.telegram.org/bot{self.bot_token}/sendDocument"
        try:
            with file_path.open("rb") as f:
                data = {"chat_id": str(chat_id), "caption": caption}
                requests.post(url, data=data, files={"document": f}, timeout=60)
        except Exception:
            self.send_text(chat_id, "فشل إرسال الملف.")

    def get_file_content(self, file_id: str) -> str:
        res = self.tg_api("getFile", {"file_id": file_id})
        if not res.get("ok"):
            return ""
        file_path = ((res.get("result") or {}).get("file_path") or "").strip()
        if not file_path:
            return ""
        url = f"https://api.telegram.org/file/bot{self.bot_token}/{file_path}"
        try:
            r = requests.get(url, timeout=40)
            if r.status_code != 200:
                return ""
            return r.text
        except Exception:
            return ""

    # -------------------------- generic helpers --------------------------
    def _extract_token(self, payload: Any) -> str:
        if not isinstance(payload, dict):
            return ""
        candidates: list[dict[str, Any]] = [payload]
        for key in ("data", "result"):
            nested = payload.get(key)
            if isinstance(nested, dict):
                candidates.append(nested)
        for candidate in candidates:
            for key in TOKEN_KEYS:
                tok = str(candidate.get(key, "")).strip()
                if tok:
                    return tok
        return ""

    def api_post(self, path: str, body: dict[str, Any], timeout: int = 60) -> tuple[bool, Any, str]:
        url = f"{self.api_base}{path}"
        try:
            r = requests.post(url, json=body, timeout=timeout)
        except requests.RequestException as exc:
            return False, None, str(exc)

        try:
            payload: Any = r.json()
        except ValueError:
            payload = {"raw": r.text}

        if r.status_code != 200:
            if isinstance(payload, dict):
                msg = str(payload.get("message") or payload.get("error") or payload.get("detail") or payload).strip()
            else:
                msg = str(payload)
            return False, payload, f"status={r.status_code} {msg}"
        if isinstance(payload, dict):
            status_val = payload.get("status")
            if isinstance(status_val, bool) and not status_val:
                msg = str(payload.get("message") or payload.get("error") or payload.get("detail") or payload).strip()
                return False, payload, msg or "status=false"
            if isinstance(status_val, str) and status_val.strip().lower() in {"error", "failed", "fail"}:
                msg = str(payload.get("message") or payload.get("error") or payload.get("detail") or payload).strip()
                return False, payload, msg or f"status={status_val}"
        return True, payload, ""

    def api_login(self, email: str, password: str) -> tuple[str | None, str]:
        ok, payload, err = self.api_post("/api/v1/auth/login", {"email": email, "password": password}, timeout=60)
        if not ok:
            return None, err
        token = self._extract_token(payload)
        if token:
            return token, ""
        return None, "login succeeded without token"

    def active_accounts(self) -> list[dict[str, Any]]:
        return [x for x in self.load_accounts() if bool(x.get("enabled", True))]

    def resolve_targets(self) -> list[tuple[str, str]]:
        targets: list[tuple[str, str]] = []
        for acc in self.active_accounts():
            token, _err = self.api_login(acc["email"], acc["password"])
            if token:
                targets.append((acc["name"], token))
        if targets:
            return targets
        if self.api_session_token:
            return [("session", self.api_session_token)]
        return []

    def extract_list_payload(self, payload: Any) -> list[Any]:
        data = payload
        if isinstance(payload, dict):
            for key in ("data", "result"):
                if key in payload:
                    data = payload[key]
                    break
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("items", "rows", "numbers", "applications", "apps", "services"):
                v = data.get(key)
                if isinstance(v, list):
                    return v
        return []

    # -------------------------- keyboards --------------------------
    def _pattern_rows(
        self,
        buttons: list[dict[str, Any]],
        back_callback: str | None = None,
        back_text: str = "رجوع",
    ) -> list[list[dict[str, Any]]]:
        # توزيع ثابت: 1 ثم 2 ثم 2 ثم 2 ثم 1
        pattern = [1, 2, 2, 2, 1]
        rows: list[list[dict[str, Any]]] = []
        idx = 0
        step = 0
        total = len(buttons)
        while idx < total:
            take = pattern[step % len(pattern)]
            rows.append(buttons[idx : idx + take])
            idx += take
            step += 1
        if back_callback:
            rows.append([self._btn(back_text, callback_data=back_callback, style="primary")])
        return rows

    def _q(self, title: str) -> str:
        return f"__BLOCK__ {title}"

    def _show_loading(self, chat_id: int | str, message_id: int, title: str, message: str, back_callback: str, user_id: int | None = None) -> None:
        back_label = "رجوع" if user_id is None else self._tr(user_id, "رجوع", "Back")
        self.edit_text(
            chat_id,
            message_id,
            self._q(title) + f"\n{message}",
            [[self._btn(back_label, callback_data=back_callback, style="primary")]],
        )

    def _run_async(self, fn, *args, **kwargs) -> None:
        self.executor.submit(fn, *args, **kwargs)

    def _traffic_key(self, app_name: str) -> str:
        return (app_name or "").strip().lower()

    def _set_user_lang(self, user_id: int, language_code: str | None) -> None:
        override = self.get_user_lang_override(user_id)
        if override:
            self.user_lang[int(user_id)] = override
            return
        code = str(language_code or "").strip().lower()
        if not code:
            code = "ar"
        self.user_lang[int(user_id)] = code

    def _is_ar(self, user_id: int) -> bool:
        code = self.user_lang.get(int(user_id), "")
        return code.startswith("ar")

    def _tr(self, user_id: int, ar_text: str, en_text: str) -> str:
        return ar_text if self._is_ar(user_id) else en_text

    def _title_main(self, user_id: int) -> str:
        return self._q(self._tr(user_id, f"🧭 {MAIN_TITLE}", "🧭 ༺═════⇓ Control Panel ⇓═════༻"))

    def _title_traffic(self, user_id: int) -> str:
        return self._q(self._tr(user_id, f"📊 {TRAFFIC_TITLE}", "📊 ༺═════⇓ Traffic ⇓═════༻"))

    def _title_numbers(self, user_id: int) -> str:
        return self._q(self._tr(user_id, f"📱 {NUMBERS_TITLE}", "📱 ༺═════⇓ Numbers ⇓═════༻"))

    def _title_accounts(self, user_id: int) -> str:
        return self._q(self._tr(user_id, f"👤 {ACCOUNTS_TITLE}", "👤 ༺═════⇓ Accounts ⇓═════༻"))

    def _title_groups(self, user_id: int) -> str:
        return self._q(self._tr(user_id, f"👥 {GROUPS_TITLE}", "👥 ༺═════⇓ Groups ⇓═════༻"))

    def _title_stats(self, user_id: int) -> str:
        return self._q(self._tr(user_id, f"📈 {STATS_TITLE}", "📈 ༺═════⇓ Statistics ⇓═════༻"))

    def _title_messages(self, user_id: int) -> str:
        return self._q(self._tr(user_id, f"💬 {MESSAGES_TITLE}", "💬 ༺═════⇓ Messages ⇓═════༻"))

    def _set_user_traffic_rows(self, user_id: int, app_name: str, rows: list[dict[str, str]]) -> None:
        key = self._traffic_key(app_name)
        self.user_traffic_cache.setdefault(int(user_id), {})[key] = rows

    def _get_user_traffic_rows(self, user_id: int, app_name: str) -> list[dict[str, str]] | None:
        key = self._traffic_key(app_name)
        rows = self.user_traffic_cache.get(int(user_id), {}).get(key)
        if isinstance(rows, list):
            return rows
        return None

    def _set_user_numbers_rows(self, user_id: int, rows: list[dict[str, str]], scope: str = "all") -> None:
        key = str(scope or "all").strip().lower() or "all"
        self.user_numbers_cache.setdefault(int(user_id), {})[key] = rows

    def _get_user_numbers_rows(self, user_id: int, scope: str = "all") -> list[dict[str, str]] | None:
        key = str(scope or "all").strip().lower() or "all"
        rows = self.user_numbers_cache.get(int(user_id), {}).get(key)
        if isinstance(rows, list):
            return rows
        return None

    def _btn(
        self,
        text: str,
        *,
        callback_data: str | None = None,
        url: str | None = None,
        copy_text: str | None = None,
        style: str | None = None,
    ) -> dict[str, Any]:
        btn: dict[str, Any] = {"text": text}
        if url:
            btn["url"] = url
        elif callback_data is not None:
            btn["callback_data"] = callback_data
        if copy_text is not None:
            btn["copy_text"] = {"text": copy_text}
        if "url" not in btn and "callback_data" not in btn and "copy_text" not in btn:
            btn["callback_data"] = "noop"
        if style in {"danger", "success", "primary"}:
            btn["style"] = style
        return btn

    def _sanitize_keyboard(self, keyboard: list[list[dict[str, Any]]]) -> list[list[dict[str, Any]]]:
        allowed = {"text", "callback_data", "url", "copy_text"}
        out: list[list[dict[str, Any]]] = []
        for row in keyboard:
            new_row: list[dict[str, Any]] = []
            for btn in row:
                if not isinstance(btn, dict):
                    continue
                clean = {k: v for k, v in btn.items() if k in allowed}
                if "text" not in clean:
                    continue
                # copy_text fallback safety
                if "copy_text" in clean and not isinstance(clean.get("copy_text"), dict):
                    clean.pop("copy_text", None)
                if "callback_data" not in clean and "url" not in clean and "copy_text" not in clean:
                    clean["callback_data"] = "noop"
                new_row.append(clean)
            if new_row:
                out.append(new_row)
        return out

    def kb_numbers_scope(self, user_id: int) -> list[list[dict[str, Any]]]:
        buttons = [
            self._btn(self._tr(user_id, "📋 عرض جميع الأرقام", "📋 Show All Numbers"), callback_data="numbers_show_all", style="primary"),
            self._btn(self._tr(user_id, "🎯 عرض مخصص (حسب الحساب)", "🎯 Custom View (by account)"), callback_data="numbers_show_custom", style="primary"),
        ]
        return self._pattern_rows(buttons, back_callback="numbers_menu", back_text=self._tr(user_id, "رجوع", "Back"))

    def _active_account_names(self) -> list[str]:
        names: list[str] = []
        for row in self.active_accounts():
            name = str(row.get("name") or "").strip()
            if name:
                names.append(name)
        return names

    def _account_name_by_pick(self, pick: str) -> str:
        names = self._active_account_names()
        try:
            idx = int(str(pick).strip()) - 1
        except Exception:
            return ""
        if idx < 0 or idx >= len(names):
            return ""
        return names[idx]

    def kb_account_picker(
        self,
        user_id: int,
        callback_prefix: str,
        *,
        back_callback: str,
    ) -> list[list[dict[str, Any]]]:
        names = self._active_account_names()
        if not names:
            return [[self._btn(self._tr(user_id, "لا يوجد حسابات مفعلة.", "No active accounts."), callback_data="noop", style="danger")], [self._btn(self._tr(user_id, "رجوع", "Back"), callback_data=back_callback, style="primary")]]
        buttons = [
            self._btn(f"👤 {name}", callback_data=f"{callback_prefix}:{idx}", style="primary")
            for idx, name in enumerate(names, start=1)
        ]
        return self._pattern_rows(buttons, back_callback=back_callback, back_text=self._tr(user_id, "رجوع", "Back"))

    def kb_numbers_request_mode(self, user_id: int) -> list[list[dict[str, Any]]]:
        buttons = [
            self._btn(self._tr(user_id, "🎯 تحكم عادي (حساب واحد)", "🎯 Normal Mode (Single Account)"), callback_data="numbers_req_mode_normal", style="primary"),
            self._btn(self._tr(user_id, "🧩 تحكم متعدد (عدة حسابات)", "🧩 Multi Mode (Multi Accounts)"), callback_data="numbers_req_mode_multi", style="success"),
        ]
        return self._pattern_rows(buttons, back_callback="numbers_menu", back_text=self._tr(user_id, "رجوع", "Back"))

    def kb_numbers_req_multi_accounts(self, user_id: int, selected: set[str] | None = None) -> list[list[dict[str, Any]]]:
        selected = selected or set()
        names = self._active_account_names()
        buttons: list[dict[str, Any]] = []
        for idx, name in enumerate(names, start=1):
            checked = "✅" if name in selected else "☑️"
            buttons.append(self._btn(f"{checked} {name}", callback_data=f"numbers_req_multi_toggle:{idx}", style="primary"))
        buttons.append(self._btn(self._tr(user_id, "✅ تم", "✅ Done"), callback_data="numbers_req_multi_done", style="success"))
        return self._pattern_rows(buttons, back_callback="numbers_request", back_text=self._tr(user_id, "رجوع", "Back"))

    def _range_remaining(self, range_name: str, account_name: str | None = None, account_names: list[str] | None = None) -> int:
        store = self.load_ranges_store()
        entry = self.range_entry(store, range_name)
        requested = int(entry.get("requested_total", 0) or 0)
        if account_names:
            requested = 0
            accounts = entry.get("accounts") if isinstance(entry.get("accounts"), dict) else {}
            names_lc = {str(x).strip().lower() for x in account_names if str(x).strip()}
            for name, row in accounts.items():
                if not isinstance(row, dict):
                    continue
                if str(name).strip().lower() in names_lc:
                    requested += int(row.get("requested_total", 0) or 0)
            return max(0, self.range_limit_total() * max(1, len(names_lc)) - requested)
        if account_name:
            accounts = entry.get("accounts") if isinstance(entry.get("accounts"), dict) else {}
            acc_row = accounts.get(account_name) if isinstance(accounts.get(account_name), dict) else {}
            requested = int(acc_row.get("requested_total", 0) or 0)
        return max(0, self.range_limit_total() - requested)

    def kb_main(self, user_id: int) -> list[list[dict[str, Any]]]:
        enabled = self.fetch_codes_enabled()
        toggle_label = (
            self._tr(user_id, "زر جلب الاكواد : مفعل 🟢", "Fetch Codes: ON 🟢")
            if enabled
            else self._tr(user_id, "زر جلب الاكواد : مغلق 🔴", "Fetch Codes: OFF 🔴")
        )
        buttons = [
            self._btn(toggle_label, callback_data="toggle_fetch", style="success" if enabled else "danger"),
            self._btn(self._tr(user_id, "⚙️ المتغيرات", "⚙️ Variables"), callback_data="vars_menu", style="primary"),
            self._btn(self._tr(user_id, "💬 الرسائل", "💬 Messages"), callback_data="messages_menu", style="primary"),
            self._btn(self._tr(user_id, "🌐 اللغة", "🌐 Language"), callback_data="lang_menu", style="primary"),
            self._btn(self._tr(user_id, "📊 الترافيك", "📊 Traffic"), callback_data="traffic_menu", style="primary"),
            self._btn(self._tr(user_id, "🧩 المنصات المتاحة", "🧩 Platforms"), callback_data="show_platforms", style="primary"),
            self._btn(self._tr(user_id, "📱 ارقام", "📱 Numbers"), callback_data="numbers_menu", style="primary"),
            self._btn(self._tr(user_id, "💰 رصيدي", "💰 Balances"), callback_data="balances", style="success"),
            self._btn(self._tr(user_id, "📈 الاحصائيات", "📈 Statistics"), callback_data="stats", style="primary"),
            self._btn(self._tr(user_id, "👥 الجروبات", "👥 Groups"), callback_data="groups_menu", style="primary"),
            self._btn(self._tr(user_id, "👤 حساباتي", "👤 Accounts"), callback_data="accounts_menu", style="primary"),
            self._btn(self._tr(user_id, "🆘 المساعدة", "🆘 Help"), url="https://t.me/XET_F", style="primary"),
        ]
        return self._pattern_rows(buttons)

    def kb_messages_menu(self, user_id: int) -> list[list[dict[str, Any]]]:
        buttons = [
            self._btn(self._tr(user_id, "📄 عرض الرسائل", "📄 Show Messages"), callback_data="messages_show", style="primary"),
            self._btn(self._tr(user_id, "🗑️ حذف الرسائل", "🗑️ Delete Messages"), callback_data="messages_delete_confirm", style="danger"),
        ]
        return self._pattern_rows(buttons, back_callback="main_menu", back_text=self._tr(user_id, "رجوع", "Back"))

    def kb_vars_menu(self, user_id: int) -> list[list[dict[str, Any]]]:
        buttons = [
            self._btn(self._tr(user_id, "🌐 API URL", "🌐 API URL"), callback_data="var_set_api_url", style="primary"),
            self._btn(self._tr(user_id, "🗓️ Start Date", "🗓️ Start Date"), callback_data="var_set_start_date", style="primary"),
            self._btn(self._tr(user_id, "📦 BOT LIMIT", "📦 BOT LIMIT"), callback_data="var_set_bot_limit", style="primary"),
            self._btn(self._tr(user_id, "👮 إدارة الأدمن", "👮 Admin Management"), callback_data="var_admins_menu", style="primary"),
            self._btn(self._tr(user_id, "📢 إعدادات النشر", "📢 Publish Settings"), callback_data="publish_settings_menu", style="primary"),
            self._btn(self._tr(user_id, "🔁 إعادة تشغيل البوت", "🔁 Restart Bot"), callback_data="var_restart", style="danger"),
        ]
        if self.is_primary_admin(user_id):
            buttons.insert(4, self._btn(self._tr(user_id, "🤖 إدارة البوتات", "🤖 Bots Management"), callback_data="var_bots_menu", style="primary"))
        return self._pattern_rows(buttons, back_callback="main_menu", back_text=self._tr(user_id, "رجوع", "Back"))

    def kb_admins_menu(self, user_id: int) -> list[list[dict[str, Any]]]:
        buttons = [
            self._btn(self._tr(user_id, "➕ إضافة أدمن", "➕ Add Admin"), callback_data="var_admin_add", style="success"),
            self._btn(self._tr(user_id, "🗑️ حذف أدمن", "🗑️ Delete Admin"), callback_data="var_admin_delete_menu", style="danger"),
            self._btn(self._tr(user_id, "📄 عرض الأدمن", "📄 Show Admins"), callback_data="var_admin_list", style="primary"),
        ]
        return self._pattern_rows(buttons, back_callback="vars_menu", back_text=self._tr(user_id, "رجوع", "Back"))

    def kb_bots_mgmt_menu(self, user_id: int) -> list[list[dict[str, Any]]]:
        buttons = [
            self._btn(self._tr(user_id, "➕ إضافة بوت", "➕ Add Bot"), callback_data="var_bot_add", style="success"),
            self._btn(self._tr(user_id, "🗑️ حذف بوت", "🗑️ Delete Bot"), callback_data="var_bot_delete_menu", style="danger"),
            self._btn(self._tr(user_id, "📄 عرض البوتات", "📄 Show Bots"), callback_data="var_bot_list", style="primary"),
        ]
        return self._pattern_rows(buttons, back_callback="vars_menu", back_text=self._tr(user_id, "رجوع", "Back"))

    def kb_publish_settings_menu(self, user_id: int) -> list[list[dict[str, Any]]]:
        buttons = [
            self._btn(self._tr(user_id, "🧩 الخدمات", "🧩 Services"), callback_data="publish_services_menu", style="primary"),
            self._btn(self._tr(user_id, "🌍 البلدان", "🌍 Countries"), callback_data="publish_countries_menu", style="primary"),
        ]
        return self._pattern_rows(buttons, back_callback="vars_menu", back_text=self._tr(user_id, "رجوع", "Back"))

    def kb_publish_services_menu(self, user_id: int) -> list[list[dict[str, Any]]]:
        buttons = [
            self._btn(self._tr(user_id, "📄 عرض", "📄 Show"), callback_data="publish_services_show", style="primary"),
            self._btn(self._tr(user_id, "➕ إضافة", "➕ Add"), callback_data="publish_services_add", style="success"),
            self._btn(self._tr(user_id, "✏️ تعديل", "✏️ Edit"), callback_data="publish_services_edit_menu", style="primary"),
            self._btn(self._tr(user_id, "🗑️ حذف", "🗑️ Delete"), callback_data="publish_services_delete_menu", style="danger"),
        ]
        return self._pattern_rows(buttons, back_callback="publish_settings_menu", back_text=self._tr(user_id, "رجوع", "Back"))

    def kb_publish_countries_menu(self, user_id: int) -> list[list[dict[str, Any]]]:
        buttons = [
            self._btn(self._tr(user_id, "📄 عرض", "📄 Show"), callback_data="publish_countries_show", style="primary"),
            self._btn(self._tr(user_id, "➕ إضافة", "➕ Add"), callback_data="publish_countries_add", style="success"),
            self._btn(self._tr(user_id, "✏️ تعديل", "✏️ Edit"), callback_data="publish_countries_edit_menu", style="primary"),
            self._btn(self._tr(user_id, "🗑️ حذف", "🗑️ Delete"), callback_data="publish_countries_delete_menu", style="danger"),
        ]
        return self._pattern_rows(buttons, back_callback="publish_settings_menu", back_text=self._tr(user_id, "رجوع", "Back"))

    def kb_back_main(self, user_id: int) -> list[list[dict[str, Any]]]:
        return [[self._btn(self._tr(user_id, "رجوع", "Back"), callback_data="main_menu", style="primary")]]

    def kb_numbers_menu(self, user_id: int) -> list[list[dict[str, Any]]]:
        buttons = [
            self._btn(self._tr(user_id, "📋 عرض الارقام", "📋 Show Numbers"), callback_data="numbers_show", style="primary"),
            self._btn(self._tr(user_id, "📤 تصدير الارقام", "📤 Export Numbers"), callback_data="numbers_export_menu", style="primary"),
            self._btn(self._tr(user_id, "🛒 طلب ارقام", "🛒 Request Numbers"), callback_data="numbers_request", style="success"),
            self._btn(self._tr(user_id, "🗑️ حذف ارقام", "🗑️ Delete Numbers"), callback_data="numbers_delete", style="danger"),
        ]
        return self._pattern_rows(buttons, back_callback="main_menu")

    def kb_accounts_menu(self, user_id: int) -> list[list[dict[str, Any]]]:
        buttons = [
            self._btn(self._tr(user_id, "➕ إضافة حساب", "➕ Add Account"), callback_data="acc_add", style="success"),
            self._btn(self._tr(user_id, "🗑️ حذف حساب", "🗑️ Delete Account"), callback_data="acc_delete_menu", style="danger"),
            self._btn(self._tr(user_id, "✏️ تعديل حساب", "✏️ Edit Account"), callback_data="acc_edit_menu", style="primary"),
            self._btn(self._tr(user_id, "📄 عرض الحسابات", "📄 List Accounts"), callback_data="acc_list", style="primary"),
        ]
        return self._pattern_rows(buttons, back_callback="main_menu")

    def kb_groups_menu(self, user_id: int) -> list[list[dict[str, Any]]]:
        buttons = [
            self._btn(self._tr(user_id, "➕ إضافة جروب", "➕ Add Group"), callback_data="grp_add", style="success"),
            self._btn(self._tr(user_id, "🗑️ حذف جروب", "🗑️ Delete Group"), callback_data="grp_delete_menu", style="danger"),
            self._btn(self._tr(user_id, "📄 عرض الجروبات", "📄 List Groups"), callback_data="grp_list", style="primary"),
        ]
        return self._pattern_rows(buttons, back_callback="main_menu")

    def kb_export_menu(self, user_id: int) -> list[list[dict[str, Any]]]:
        buttons = [
            self._btn(self._tr(user_id, "📦 تصدير شامل", "📦 Full Export"), callback_data="exp_full", style="primary"),
            self._btn(self._tr(user_id, "🏷️ تصدير حسب الرينج", "🏷️ Export by Range"), callback_data="exp_by_range", style="primary"),
            self._btn(self._tr(user_id, "🌍 تصدير حسب الدولة", "🌍 Export by Country"), callback_data="exp_by_country", style="primary"),
        ]
        return self._pattern_rows(buttons, back_callback="numbers_menu", back_text=self._tr(user_id, "رجوع", "Back"))

    def kb_export_formats(self, prefix: str, user_id: int) -> list[list[dict[str, Any]]]:
        buttons = [
            self._btn("TXT", callback_data=f"{prefix}:txt", style="primary"),
            self._btn("CSV", callback_data=f"{prefix}:csv", style="primary"),
            self._btn("JSON", callback_data=f"{prefix}:json", style="primary"),
        ]
        return self._pattern_rows(buttons, back_callback="numbers_export_menu", back_text=self._tr(user_id, "رجوع", "Back"))

    def _export_field_label(self, user_id: int, field: str) -> str:
        if field == "number":
            return self._tr(user_id, "الأرقام", "numbers")
        if field == "range":
            return self._tr(user_id, "اسم الرينج", "range name")
        if field == "id":
            return self._tr(user_id, "الايدي", "id")
        return field

    def kb_export_fields(self, scope: str, user_id: int, selected: set[str] | None = None) -> list[list[dict[str, Any]]]:
        selected = selected or set()

        def label(base: str, checked: bool) -> str:
            return f"{'✅' if checked else '☑️'} {base}"

        buttons = [
            self._btn(
                label(self._tr(user_id, "📞 الأرقام", "📞 Numbers"), "number" in selected),
                callback_data=f"exp_field_toggle:{scope}:number",
                style="primary",
            ),
            self._btn(
                label(self._tr(user_id, "🏷️ اسم الرينج", "🏷️ Range Name"), "range" in selected),
                callback_data=f"exp_field_toggle:{scope}:range",
                style="primary",
            ),
            self._btn(
                label(self._tr(user_id, "🆔 الايدي", "🆔 ID"), "id" in selected),
                callback_data=f"exp_field_toggle:{scope}:id",
                style="primary",
            ),
            self._btn(self._tr(user_id, "✅ تم", "✅ Done"), callback_data=f"exp_field_done:{scope}", style="success"),
        ]
        return self._pattern_rows(buttons, back_callback="numbers_export_menu", back_text=self._tr(user_id, "رجوع", "Back"))

    # -------------------------- data fetch --------------------------
    def fetch_platforms(self, refresh: bool = False) -> list[str]:
        now = time.time()
        if not refresh:
            cached = self.platforms_cache.get("data")
            cached_at = float(self.platforms_cache.get("at") or 0.0)
            if isinstance(cached, list) and cached and (now - cached_at) <= self.cache_ttl_seconds:
                return [str(x) for x in cached]

        targets = self.resolve_targets()
        names: set[str] = set()
        for _name, token in targets:
            ok, payload, _err = self.api_post("/api/v1/applications/available", {"token": token}, timeout=90)
            if not ok:
                continue
            rows = self.extract_list_payload(payload)
            for row in rows:
                if isinstance(row, dict):
                    label = str(row.get("name") or row.get("app_name") or row.get("key") or row.get("service_name") or "").strip()
                else:
                    label = str(row).strip()
                if label:
                    names.add(label)
        out = sorted(names, key=lambda x: x.lower())
        self.platforms_cache = {"at": now, "data": out}
        return out

    def fetch_traffic(self, app_name: str, refresh: bool = False) -> list[dict[str, str]]:
        key = self._traffic_key(app_name)
        now = time.time()
        if not refresh:
            cached_row = self.traffic_cache.get(key)
            if isinstance(cached_row, dict):
                cached_at = float(cached_row.get("at") or 0.0)
                cached_data = cached_row.get("data")
                if isinstance(cached_data, list) and (now - cached_at) <= self.cache_ttl_seconds:
                    return [x for x in cached_data if isinstance(x, dict)]

        targets = self.resolve_targets()
        merged: dict[str, dict[str, Any]] = {}

        for _name, token in targets:
            ok, payload, _err = self.api_post(
                "/api/v1/traffic/services",
                {"token": token, "app_name": app_name},
                timeout=120,
            )
            if not ok:
                continue
            rows = self.extract_list_payload(payload)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                range_name = str(row.get("range") or row.get("range_name") or row.get("termination") or "UNKNOWN").strip() or "UNKNOWN"
                cnt_raw = row.get("count") or row.get("total") or row.get("messages") or 0
                try:
                    cnt = int(str(cnt_raw).strip())
                except Exception:
                    cnt = 0
                last = str(row.get("last_message_time") or row.get("updated_at") or row.get("last") or "-").strip() or "-"

                bucket = merged.setdefault(range_name, {"range": range_name, "count": 0, "last": "-"})
                bucket["count"] += cnt
                if bucket["last"] == "-" and last != "-":
                    bucket["last"] = last

        out = list(merged.values())
        out.sort(key=lambda x: int(x.get("count") or 0), reverse=True)
        final_rows = [{"range": str(x["range"]), "count": str(x["count"]), "last": str(x["last"])} for x in out]
        self.traffic_cache[key] = {"at": now, "data": final_rows}
        return final_rows

    def fetch_numbers(self, account_name: str | None = None) -> list[dict[str, str]]:
        targets = self.resolve_targets()
        if account_name:
            target_name = str(account_name).strip().lower()
            targets = [row for row in targets if str(row[0]).strip().lower() == target_name]
        merged: list[dict[str, str]] = []
        seen: set[str] = set()

        for account_name, token in targets:
            ok, payload, _err = self.api_post("/api/v1/numbers/announce", {"token": token}, timeout=120)
            if not ok:
                continue
            rows = self.extract_list_payload(payload)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                number = str(row.get("number") or row.get("phone") or row.get("msisdn") or row.get("mobile") or "").strip()
                range_name = str(row.get("range") or row.get("range_name") or row.get("termination") or "UNKNOWN").strip() or "UNKNOWN"
                id_value = str(row.get("id") or row.get("number_id") or row.get("uid") or number).strip()
                key = f"{account_name}|{number}|{range_name}|{id_value}"
                if key in seen:
                    continue
                seen.add(key)
                merged.append({"number": number, "range": range_name, "id": id_value, "account": account_name})

        return merged

    def fetch_balances(self) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        for acc in self.active_accounts():
            token, err = self.api_login(acc["email"], acc["password"])
            if not token:
                out.append({"name": acc["name"], "email": acc["email"], "balance": "login failed", "err": err})
                continue
            ok, payload, req_err = self.api_post("/api/v1/balance", {"token": token}, timeout=60)
            if not ok:
                out.append({"name": acc["name"], "email": acc["email"], "balance": "error", "err": req_err})
                continue

            balance: str = "-"
            if isinstance(payload, (int, float, str)):
                balance = str(payload)
            elif isinstance(payload, dict):
                probe = payload
                for key in ("data", "result"):
                    if isinstance(probe.get(key), dict):
                        probe = probe[key]
                for key in ("balance", "wallet", "credit", "amount"):
                    if key in probe:
                        balance = str(probe.get(key))
                        break
            out.append({"name": acc["name"], "email": acc["email"], "balance": balance, "err": ""})
        return out

    # -------------------------- range request / delete --------------------------
    def range_entry(self, store: dict[str, Any], range_name: str) -> dict[str, Any]:
        ranges = store.setdefault("ranges", {})
        if range_name not in ranges or not isinstance(ranges.get(range_name), dict):
            ranges[range_name] = {
                "requested_total": 0,
                "last_requested_at": "",
                "available_numbers_count": 0,
                "last_numbers_sync_at": "",
                "sample_numbers": [],
                "accounts": {},
            }
        return ranges[range_name]

    def request_numbers_for_range(
        self,
        user_id: int,
        range_name: str,
        count: int,
        account_name: str | None = None,
        account_names: list[str] | None = None,
    ) -> str:
        range_name = str(range_name or "").strip()
        if not range_name:
            return self._tr(user_id, "اسم الرينج مطلوب.", "Range name is required.")
        if count < 50 or count > 1000 or (count % 50 != 0):
            return self._tr(user_id, "العدد لازم يكون من 50 إلى 1000 ومضاعف 50.", "Count must be 50..1000 and divisible by 50.")

        store = self.load_ranges_store()
        entry = self.range_entry(store, range_name)

        selected_names: list[str] = []
        if account_names:
            selected_names = [str(x).strip() for x in account_names if str(x).strip()]
        elif account_name:
            selected_names = [str(account_name).strip()]

        max_total = self.range_limit_total() * max(1, len(selected_names))
        already = int(entry.get("requested_total", 0) or 0)
        if selected_names:
            accounts_map = entry.get("accounts") if isinstance(entry.get("accounts"), dict) else {}
            already = 0
            names_lc = {x.lower() for x in selected_names}
            for n, acc_row in accounts_map.items():
                if not isinstance(acc_row, dict):
                    continue
                if str(n).strip().lower() in names_lc:
                    already += int(acc_row.get("requested_total", 0) or 0)
        remaining = max_total - already
        if remaining <= 0:
            return self._tr(user_id, f"الرينج {range_name} وصل الحد الأقصى ({max_total}).", f"Range {range_name} reached max limit ({max_total}).")
        if remaining < 50:
            return self._tr(user_id, f"المتبقي للرينج {range_name} هو {remaining} وأقل طلب 50.", f"Remaining for range {range_name} is {remaining}; minimum request is 50.")
        if count > remaining:
            allowed = remaining - (remaining % 50)
            if allowed < 50:
                return self._tr(user_id, f"المتبقي {remaining} وأقل طلب 50.", f"Remaining is {remaining}; minimum request is 50.")
            return self._tr(user_id, f"العدد المطلوب {count} أكبر من المتبقي {remaining}. المسموح الآن {allowed}.", f"Requested {count} is greater than remaining {remaining}. Allowed now: {allowed}.")

        targets = self.resolve_targets()
        if selected_names:
            names_lc = {x.lower() for x in selected_names}
            targets = [row for row in targets if str(row[0]).strip().lower() in names_lc]
        if not targets:
            return self._tr(user_id, "الحساب غير متاح أو لا يوجد توكن صالح لتنفيذ الطلب.", "Selected account is not available or has no valid token.")

        calls_needed = count // 50
        calls_by_account: dict[str, int] = {name: 0 for name, _tok in targets}
        for idx in range(calls_needed):
            name, _tok = targets[idx % len(targets)]
            calls_by_account[name] = int(calls_by_account.get(name, 0)) + 1
        summary: list[str] = []
        total_success = 0

        for name, token in targets:
            success_calls = 0
            last_err = ""
            account_calls = int(calls_by_account.get(name, 0))
            if account_calls <= 0:
                continue
            for _idx in range(1, account_calls + 1):
                ok, _payload, req_err = self.api_post("/api/v1/order/range", {"token": token, "range_name": range_name}, timeout=90)
                if ok:
                    success_calls += 1
                else:
                    last_err = req_err

            requested_numbers = success_calls * 50
            if requested_numbers > 0:
                entry["requested_total"] = int(entry.get("requested_total", 0)) + requested_numbers
                entry["last_requested_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                accounts = entry.get("accounts")
                if not isinstance(accounts, dict):
                    accounts = {}
                    entry["accounts"] = accounts
                row = accounts.get(name) if isinstance(accounts.get(name), dict) else {}
                row["requested_total"] = int(row.get("requested_total", 0) or 0) + requested_numbers
                row["last_requested_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                accounts[name] = row
                total_success += requested_numbers

            expected_for_account = account_calls * 50
            if success_calls == account_calls:
                summary.append(self._tr(user_id, f"{name}: تم طلب {requested_numbers}/{expected_for_account}", f"{name}: requested {requested_numbers}/{expected_for_account}"))
            else:
                summary.append(
                    self._tr(user_id, f"{name}: نجاح جزئي {requested_numbers}/{expected_for_account}", f"{name}: partial success {requested_numbers}/{expected_for_account}")
                    + (self._tr(user_id, f" | خطأ: {last_err}", f" | error: {last_err}") if last_err else "")
                )

        self.save_ranges_store(store)
        updated = self.range_entry(store, range_name)
        new_remaining = max_total - int(updated.get("requested_total", 0) or 0)

        return "\n".join(
            [
                self._tr(user_id, f"تم تنفيذ طلب الرينج: {range_name}", f"Range request completed: {range_name}"),
                self._tr(user_id, f"الإجمالي الناجح: {total_success}", f"Total success: {total_success}"),
                *summary,
                self._tr(user_id, f"المتبقي من الحد: {max(0, new_remaining)}", f"Remaining limit: {max(0, new_remaining)}"),
            ]
        )

    def delete_numbers(self, user_id: int, items: list[str]) -> str:
        cleaned = [x.strip() for x in items if str(x).strip()]
        if not cleaned:
            return self._tr(user_id, "لم يتم العثور على أرقام/IDs صالحة للحذف.", "No valid numbers/IDs found to delete.")

        targets = self.resolve_targets()
        if not targets:
            return self._tr(user_id, "لا يوجد حساب/توكن صالح للحذف.", "No valid account/token available for deletion.")

        # Map provided numbers to number IDs when possible.
        current_rows = self.fetch_numbers()
        number_to_id: dict[str, str] = {}
        id_to_account: dict[str, str] = {}
        for row in current_rows:
            number = str(row.get("number", "")).strip()
            rid = str(row.get("id", "")).strip()
            acc = str(row.get("account", "")).strip()
            if number and rid:
                number_to_id[number] = rid
                n_digits = "".join(ch for ch in number if ch.isdigit())
                if n_digits:
                    number_to_id[n_digits] = rid
                    number_to_id[f"+{n_digits}"] = rid
                if acc:
                    id_to_account[rid] = acc

        ids: list[str] = []
        unresolved: list[str] = []
        for item in cleaned:
            raw = str(item).strip()
            raw_digits = "".join(ch for ch in raw if ch.isdigit())
            mapped = number_to_id.get(raw) or (number_to_id.get(raw_digits) if raw_digits else None) or (number_to_id.get(f"+{raw_digits}") if raw_digits else None)
            if mapped:
                ids.append(mapped)
            else:
                # If it's likely already an ID, keep it; otherwise mark unresolved.
                if "-" in raw or raw.isdigit():
                    ids.append(raw)
                else:
                    unresolved.append(raw)

        # unique ids preserving order
        unique_ids: list[str] = []
        seen: set[str] = set()
        for rid in ids:
            if not rid or rid in seen:
                continue
            seen.add(rid)
            unique_ids.append(rid)

        if not unique_ids:
            if unresolved:
                return self._tr(user_id, "لم يتم العثور على IDs للأرقام المدخلة.", "No IDs found for provided numbers.") + "\n" + "\n".join(f"- {x}" for x in unresolved[:25])
            return self._tr(user_id, "لم يتم استخراج IDs صالحة للحذف.", "Could not extract valid IDs for deletion.")

        # Group IDs by owning account so deletion goes to correct account.
        by_account: dict[str, list[str]] = {}
        for rid in unique_ids:
            owner = id_to_account.get(rid, "")
            key = owner if owner else "__ALL__"
            by_account.setdefault(key, []).append(rid)

        total_ok_accounts = 0
        total_removed_ids = 0
        details: list[str] = []
        last_err = ""

        for name, token in targets:
            account_ids = by_account.get(name, []) + by_account.get("__ALL__", [])
            # unique per account
            dedup_ids: list[str] = []
            seen_local: set[str] = set()
            for rid in account_ids:
                if rid in seen_local:
                    continue
                seen_local.add(rid)
                dedup_ids.append(rid)
            if not dedup_ids:
                details.append(f"{name}: skipped")
                continue

            account_removed = 0
            if len(dedup_ids) == 1:
                rid = dedup_ids[0]
                single_ok = False
                single_err = ""
                for body in (
                    {"token": token, "number_id": rid},
                    {"token": token, "id": rid},
                ):
                    ok, _payload, err = self.api_post("/api/v1/numbers/remove", body, timeout=90)
                    if ok:
                        single_ok = True
                        break
                    single_err = err
                if single_ok:
                    account_removed = 1
                else:
                    last_err = f"{name}: {single_err}"
            else:
                bulk_ok = False
                bulk_err = ""
                bulk_payload: Any = None
                for body in (
                    {"token": token, "ids": dedup_ids, "max_workers": min(8, len(dedup_ids))},
                    {"token": token, "number_ids": dedup_ids},
                    {"token": token, "ids": ",".join(dedup_ids)},
                ):
                    ok, payload, err = self.api_post("/api/v1/numbers/remove/bulk", body, timeout=120)
                    if ok:
                        bulk_ok = True
                        bulk_payload = payload
                        break
                    bulk_err = err
                if bulk_ok:
                    removed = len(dedup_ids)
                    if isinstance(bulk_payload, dict):
                        for key in ("removed", "deleted", "success_count", "count"):
                            if key in bulk_payload:
                                try:
                                    removed = int(str(bulk_payload.get(key)).strip())
                                except Exception:
                                    pass
                                break
                    account_removed = max(0, removed)
                else:
                    # Fallback to single remove when bulk fails.
                    for rid in dedup_ids:
                        one_ok = False
                        one_err = ""
                        for body_one in (
                            {"token": token, "number_id": rid},
                            {"token": token, "id": rid},
                        ):
                            ok_one, _payload_one, err_one = self.api_post(
                                "/api/v1/numbers/remove",
                                body_one,
                                timeout=90,
                            )
                            if ok_one:
                                one_ok = True
                                break
                            one_err = err_one
                        if one_ok:
                            account_removed += 1
                        else:
                            last_err = f"{name}: {one_err or bulk_err}"

            if account_removed > 0:
                total_ok_accounts += 1
                total_removed_ids += account_removed
                details.append(f"{name}: removed={account_removed}")
            else:
                details.append(f"{name}: failed")

        unresolved_text = ""
        if unresolved:
            unresolved_text = "\n" + self._tr(user_id, "لم يتم العثور على IDs لهذه القيم:", "No IDs found for these values:") + "\n" + "\n".join(f"- {x}" for x in unresolved[:25])

        if total_removed_ids > 0:
            return self._tr(user_id, "تم الحذف بنجاح.", "Deletion completed successfully.") + "\n" + "\n".join(details) + "\n" + self._tr(user_id, f"إجمالي المحذوف: {total_removed_ids}", f"Total deleted: {total_removed_ids}") + unresolved_text
        return self._tr(user_id, f"فشل حذف الأرقام. آخر خطأ: {last_err or 'unknown'}", f"Failed to delete numbers. Last error: {last_err or 'unknown'}") + unresolved_text

    # -------------------------- rendering --------------------------
    def show_main(self, chat_id: int | str, user_id: int, message_id: int | None = None) -> None:
        text = self._title_main(user_id) + "\n" + self._tr(user_id, "اختر الإجراء المطلوب.", "Choose an action.")
        kb = self.kb_main(user_id)
        if message_id is None:
            self.send_text(chat_id, text, kb)
        else:
            self.edit_text(chat_id, message_id, text, kb)

    def show_platforms(
        self,
        chat_id: int | str,
        user_id: int,
        message_id: int | None = None,
        include_back: bool = True,
        page: int = 1,
        refresh: bool = False,
        view_rev: int | None = None,
    ) -> None:
        if not self.is_view_current(user_id, view_rev):
            return
        apps = self.fetch_platforms(refresh=refresh)
        if not self.is_view_current(user_id, view_rev):
            return
        if not apps:
            text = self._tr(user_id, "لا توجد منصات متاحة الآن.", "No platforms available now.")
            kb = self.kb_back_main(user_id)
        else:
            per_page = 10
            total = len(apps)
            total_pages = max(1, (total + per_page - 1) // per_page)
            page = max(1, min(total_pages, int(page or 1)))
            start = (page - 1) * per_page
            end = start + per_page
            page_apps = apps[start:end]

            text = self._q(self._tr(user_id, "༺═════⇓ المنصات المتاحة ⇓═════༻", "༺═════⇓ Available Platforms ⇓═════༻")) + "\n" + self._tr(user_id, f"الصفحة: {page}/{total_pages}", f"Page: {page}/{total_pages}")
            kb = [[self._btn(self._tr(user_id, "اسم المنصة", "Platform Name"), callback_data="noop", style="primary")]]
            for app in page_apps:
                kb.append([self._btn(app, callback_data=f"traffic_app:{app}", style="primary")])

            nav_row: list[dict[str, Any]] = []
            if page > 1:
                nav_row.append(self._btn(self._tr(user_id, "⬅️ السابق", "⬅️ Prev"), callback_data=f"platforms_nav:{page-1}", style="danger"))
            else:
                nav_row.append(self._btn("—", callback_data="noop", style="primary"))
            nav_row.append(self._btn(f"{page}/{total_pages}", callback_data="noop", style="primary"))
            if page < total_pages:
                nav_row.append(self._btn(self._tr(user_id, "التالي ➡️", "Next ➡️"), callback_data=f"platforms_nav:{page+1}", style="success"))
            else:
                nav_row.append(self._btn("—", callback_data="noop", style="primary"))
            kb.append(nav_row)

            if include_back:
                kb.append([self._btn(self._tr(user_id, "رجوع", "Back"), callback_data="main_menu", style="primary")])

        if message_id is None:
            self.send_text(chat_id, text, kb)
        else:
            if not self.is_view_current(user_id, view_rev):
                return
            self.edit_text(chat_id, message_id, text, kb)

    def _render_traffic_menu(self, chat_id: int | str, message_id: int, user_id: int, page: int = 1, refresh: bool = False, view_rev: int | None = None) -> None:
        if not self.is_view_current(user_id, view_rev):
            return
        apps = self.fetch_platforms(refresh=refresh)
        if not self.is_view_current(user_id, view_rev):
            return
        per_page = 10
        total = len(apps)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page = max(1, min(total_pages, int(page or 1)))
        start = (page - 1) * per_page
        end = start + per_page
        page_apps = apps[start:end]

        kb: list[list[dict[str, Any]]] = [[self._btn(self._tr(user_id, "اسم المنصة", "Platform Name"), callback_data="noop", style="primary")]]
        for app in page_apps:
            kb.append([self._btn(app, callback_data=f"traffic_app:{app}", style="primary")])

        nav_row: list[dict[str, Any]] = []
        if page > 1:
            nav_row.append(self._btn(self._tr(user_id, "⬅️ السابق", "⬅️ Prev"), callback_data=f"traffic_menu_nav:{page-1}", style="danger"))
        else:
            nav_row.append(self._btn("—", callback_data="noop", style="primary"))
        nav_row.append(self._btn(f"{page}/{total_pages}", callback_data="noop", style="primary"))
        if page < total_pages:
            nav_row.append(self._btn(self._tr(user_id, "التالي ➡️", "Next ➡️"), callback_data=f"traffic_menu_nav:{page+1}", style="success"))
        else:
            nav_row.append(self._btn("—", callback_data="noop", style="primary"))
        kb.append(nav_row)
        kb.append([self._btn(self._tr(user_id, "رجوع", "Back"), callback_data="main_menu", style="primary")])

        text = self._title_traffic(user_id) + "\n" + self._tr(user_id, "اختر منصة لعرض الترافيك.", "Choose a platform to view traffic.")
        if not self.is_view_current(user_id, view_rev):
            return
        self.edit_text(chat_id, message_id, text, kb)

    def _render_traffic_rows(self, chat_id: int | str, message_id: int, user_id: int, app_name: str, rows: list[dict[str, str]], page: int = 1, view_rev: int | None = None) -> None:
        if not self.is_view_current(user_id, view_rev):
            return
        per_page = 10
        total = len(rows)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page = max(1, min(total_pages, int(page or 1)))
        start = (page - 1) * per_page
        end = start + per_page
        page_rows = rows[start:end]

        text_lines = [self._title_traffic(user_id), self._tr(user_id, f"الخدمة: {app_name}", f"Service: {app_name}"), self._tr(user_id, f"الصفحة: {page}/{total_pages}", f"Page: {page}/{total_pages}")]

        buttons: list[list[dict[str, Any]]] = [
            [
                self._btn(self._tr(user_id, "اسم الرينج", "Range Name"), callback_data="noop", style="primary"),
                self._btn(self._tr(user_id, "عدد الرسائل", "Messages"), callback_data="noop", style="primary"),
            ]
        ]
        if page_rows:
            for row in page_rows:
                buttons.append(
                    [
                self._btn(f"{row['range']}", copy_text=row["range"], style="success"),
                        self._btn(f"{row['count']}", callback_data="noop", style="primary"),
                    ]
                )
        else:
            buttons.append([self._btn(self._tr(user_id, "لا توجد بيانات ترافيك", "No traffic data"), callback_data="noop", style="danger")])

        nav_row: list[dict[str, Any]] = []
        if page > 1:
            nav_row.append(self._btn(self._tr(user_id, "⬅️ السابق", "⬅️ Prev"), callback_data=f"traffic_nav:{app_name}:{page-1}", style="danger"))
        else:
            nav_row.append(self._btn("—", callback_data="noop", style="primary"))
        nav_row.append(self._btn(f"{page}/{total_pages}", callback_data="noop", style="primary"))
        if page < total_pages:
            nav_row.append(self._btn(self._tr(user_id, "التالي ➡️", "Next ➡️"), callback_data=f"traffic_nav:{app_name}:{page+1}", style="success"))
        else:
            nav_row.append(self._btn("—", callback_data="noop", style="primary"))
        buttons.append(nav_row)

        buttons.append([self._btn(self._tr(user_id, "رجوع", "Back"), callback_data="traffic_menu", style="primary")])
        if not self.is_view_current(user_id, view_rev):
            return
        self.edit_text(chat_id, message_id, "\n".join(text_lines), buttons)

    def show_traffic_for_app(
        self,
        chat_id: int | str,
        message_id: int,
        user_id: int,
        app_name: str,
        page: int = 1,
        refresh: bool = False,
        view_rev: int | None = None,
    ) -> None:
        if not self.is_view_current(user_id, view_rev):
            return
        if refresh:
            rows = self.fetch_traffic(app_name, refresh=True)
            self._set_user_traffic_rows(user_id, app_name, rows)
        else:
            rows = self._get_user_traffic_rows(user_id, app_name) or []
            if not rows:
                rows = self.fetch_traffic(app_name, refresh=True)
                self._set_user_traffic_rows(user_id, app_name, rows)
        if not self.is_view_current(user_id, view_rev):
            return
        self._render_traffic_rows(chat_id, message_id, user_id, app_name, rows, page, view_rev)

    def _mask_id(self, value: str) -> str:
        v = str(value or "").strip()
        if len(v) <= 6:
            return v
        return f"{v[:3]}...{v[-3:]}"

    def show_numbers(
        self,
        chat_id: int | str,
        message_id: int,
        user_id: int,
        page: int = 1,
        refresh: bool = False,
        account_name: str | None = None,
        view_rev: int | None = None,
    ) -> None:
        if not self.is_view_current(user_id, view_rev):
            return
        scope_key = "all" if not account_name else f"acc:{str(account_name).strip().lower()}"
        if refresh:
            rows = self.fetch_numbers(account_name=account_name)
            self._set_user_numbers_rows(user_id, rows, scope=scope_key)
        else:
            rows = self._get_user_numbers_rows(user_id, scope=scope_key) or []
            if not rows:
                rows = self.fetch_numbers(account_name=account_name)
                self._set_user_numbers_rows(user_id, rows, scope=scope_key)
        if not self.is_view_current(user_id, view_rev):
            return

        per_page = 10
        total = len(rows)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page = max(1, min(total_pages, int(page or 1)))
        start = (page - 1) * per_page
        end = start + per_page
        page_rows = rows[start:end]

        lines = [
            self._title_numbers(user_id),
            self._tr(
                user_id,
                "👤 الحساب: جميع الحسابات" if not account_name else f"👤 الحساب: {account_name}",
                "👤 Account: All Accounts" if not account_name else f"👤 Account: {account_name}",
            ),
            self._tr(user_id, "💡 توضيح: اضغط على الرقم أو الـID للنسخ.", "💡 Hint: tap number or ID to copy."),
            self._tr(user_id, f"📄 الصفحة: {page}/{total_pages}", f"📄 Page: {page}/{total_pages}"),
        ]
        kb: list[list[dict[str, Any]]] = [
            [
                self._btn(self._tr(user_id, "الايدي", "ID"), callback_data="noop", style="primary"),
                self._btn(self._tr(user_id, "📞 الرقم", "📞 Number"), callback_data="noop", style="primary"),
            ]
        ]

        if page_rows:
            for row in page_rows:
                rid = str(row.get("id", "")).strip()
                number = str(row.get("number", "")).strip()
                kb.append(
                    [
                        self._btn(rid or "-", copy_text=rid or "-", style="success"),
                        self._btn(number or "-", copy_text=number or "-", style="success"),
                    ]
                )
        else:
            kb.append([self._btn(self._tr(user_id, "لا توجد أرقام الآن.", "No numbers now."), callback_data="noop", style="danger")])

        nav_row: list[dict[str, Any]] = []
        if page > 1:
            nav_row.append(self._btn(self._tr(user_id, "⬅️ السابق", "⬅️ Prev"), callback_data=f"numbers_nav:{page-1}", style="danger"))
        else:
            nav_row.append(self._btn("—", callback_data="noop", style="primary"))
        nav_row.append(self._btn(f"{page}/{total_pages}", callback_data="noop", style="primary"))
        if page < total_pages:
            nav_row.append(self._btn(self._tr(user_id, "التالي ➡️", "Next ➡️"), callback_data=f"numbers_nav:{page+1}", style="success"))
        else:
            nav_row.append(self._btn("—", callback_data="noop", style="primary"))
        kb.append(nav_row)
        kb.append([self._btn(self._tr(user_id, "رجوع", "Back"), callback_data="numbers_menu", style="primary")])

        if not self.is_view_current(user_id, view_rev):
            return
        self.edit_text(chat_id, message_id, "\n".join(lines), kb)

    def show_balances(self, chat_id: int | str, message_id: int, user_id: int, view_rev: int | None = None) -> None:
        if not self.is_view_current(user_id, view_rev):
            return
        rows = self.fetch_balances()
        text = self._q(self._tr(user_id, "༺═════⇓ رصيدي ⇓═════༻", "༺═════⇓ Balances ⇓═════༻"))
        kb: list[list[dict[str, Any]]] = []

        if not rows:
            kb.append([self._btn(self._tr(user_id, "لا يوجد حسابات", "No accounts"), callback_data="noop", style="danger")])
        else:
            kb.append(
                [
                    self._btn(self._tr(user_id, "اسم الحساب", "Account"), callback_data="noop", style="primary"),
                    self._btn(self._tr(user_id, "الرصيد", "Balance"), callback_data="noop", style="primary"),
                ]
            )
            for row in rows:
                label_name = row.get("name", "-")
                label_balance = row.get("balance", "-")
                kb.append(
                    [
                        self._btn(f"👤 {label_name}", callback_data="noop", style="primary"),
                        self._btn(f"💰 {label_balance}", callback_data="noop", style="success"),
                    ]
                )

        kb.append([self._btn(self._tr(user_id, "رجوع", "Back"), callback_data="main_menu", style="primary")])
        if not self.is_view_current(user_id, view_rev):
            return
        self.edit_text(chat_id, message_id, text, kb)

    def show_stats(self, chat_id: int | str, message_id: int, user_id: int, view_rev: int | None = None) -> None:
        if not self.is_view_current(user_id, view_rev):
            return
        day_keys = sorted(list_daily_store_days())
        if not day_keys:
            text = self._title_stats(user_id) + "\n" + self._tr(user_id, "لا توجد بيانات إحصائيات حتى الآن.", "No statistics data yet.")
            if not self.is_view_current(user_id, view_rev):
                return
            self.edit_text(chat_id, message_id, text, self.kb_back_main(user_id))
            return

        total_messages = 0
        total_deliveries = 0
        unique_numbers: set[str] = set()
        by_service: dict[str, int] = defaultdict(int)
        by_group: dict[str, int] = defaultdict(int)

        for day_key in day_keys:
            day_payload = get_daily_store(day_key, {})
            if not isinstance(day_payload, dict):
                continue
            sent_rows = day_payload.get("sent")
            if not isinstance(sent_rows, list):
                continue
            for row in sent_rows:
                if not isinstance(row, dict):
                    continue
                total_messages += 1
                number = str(row.get("number") or "").strip()
                if number:
                    unique_numbers.add(number)
                service = str(row.get("service_name") or "unknown").strip() or "unknown"
                by_service[service] += 1
                groups = row.get("groups")
                if isinstance(groups, list):
                    for g in groups:
                        if not isinstance(g, dict):
                            continue
                        gname = str(g.get("group") or g.get("chat_id") or "unknown").strip() or "unknown"
                        by_group[gname] += 1
                        total_deliveries += 1

        top_service = "-"
        top_group = "-"
        if by_service:
            top_service = sorted(by_service.items(), key=lambda kv: kv[1], reverse=True)[0][0]
        if by_group:
            top_group = sorted(by_group.items(), key=lambda kv: kv[1], reverse=True)[0][0]

        day_from = day_keys[0]
        day_to = day_keys[-1]
        period = day_from if day_from == day_to else f"{day_from} -> {day_to}"

        lines = [
            self._title_stats(user_id),
            self._tr(user_id, f"🗓️ الفترة: {period}", f"🗓️ Period: {period}"),
            self._tr(user_id, f"📆 عدد الأيام: {len(day_keys)}", f"📆 Days: {len(day_keys)}"),
            self._tr(user_id, f"✉️ إجمالي الرسائل: {total_messages}", f"✉️ Total messages: {total_messages}"),
            self._tr(user_id, f"📬 إجمالي مرات الوصول: {total_deliveries}", f"📬 Total deliveries: {total_deliveries}"),
            self._tr(user_id, f"🔢 الأرقام الفريدة: {len(unique_numbers)}", f"🔢 Unique numbers: {len(unique_numbers)}"),
            self._tr(user_id, f"🏆 أعلى خدمة: {top_service}", f"🏆 Top service: {top_service}"),
            self._tr(user_id, f"👥 أعلى جروب: {top_group}", f"👥 Top group: {top_group}"),
        ]
        if not self.is_view_current(user_id, view_rev):
            return
        self.edit_text(chat_id, message_id, "\n".join(lines), self.kb_back_main(user_id))

    def show_saved_messages(self, chat_id: int | str, message_id: int, user_id: int) -> None:
        day_keys = sorted(list_daily_store_days())
        if not day_keys:
            text = self._title_messages(user_id) + "\n" + self._tr(
                user_id,
                "لا توجد رسائل محفوظة حتى الآن.",
                "No saved messages yet.",
            )
            self.edit_text(chat_id, message_id, text, self.kb_messages_menu(user_id))
            return

        total_messages = 0
        all_rows: list[dict[str, Any]] = []
        for day_key in day_keys:
            day_payload = get_daily_store(day_key, {})
            if not isinstance(day_payload, dict):
                continue
            sent_rows = day_payload.get("sent")
            if not isinstance(sent_rows, list):
                continue
            for row in sent_rows:
                if isinstance(row, dict):
                    all_rows.append(row)
            total_messages += len(sent_rows)

        lines = [
            self._title_messages(user_id),
            self._tr(user_id, f"📆 الأيام: {len(day_keys)}", f"📆 Days: {len(day_keys)}"),
            self._tr(user_id, f"✉️ إجمالي الرسائل: {total_messages}", f"✉️ Total messages: {total_messages}"),
        ]

        preview = all_rows[-10:] if all_rows else []
        if preview:
            lines.append(self._tr(user_id, "🧾 آخر 10 رسائل:", "🧾 Last 10 messages:"))
            for row in preview:
                sent_at = str(row.get("sent_at") or "-")
                service = str(row.get("service_name") or "-")
                rng = str(row.get("range") or "-")
                code = str(row.get("code") or "-")
                lines.append(f"{sent_at} | {service} | {rng} | {code}")
        else:
            lines.append(self._tr(user_id, "لا توجد بيانات تفصيلية.", "No detailed rows."))

        self.edit_text(chat_id, message_id, "\n".join(lines), self.kb_messages_menu(user_id))

    def show_variables(self, chat_id: int | str, message_id: int, user_id: int) -> None:
        api_url = self.get_runtime_api_base()
        start_date = self.get_runtime_start_date()
        limit = self.get_runtime_bot_limit()
        admins = ", ".join(str(x) for x in self.get_runtime_admin_ids())
        text = "\n".join(
            [
                self._q(self._tr(user_id, "༺═════⇓ المتغيرات ⇓═════༻", "༺═════⇓ Variables ⇓═════༻")),
                self._tr(user_id, f"🌐 API URL: {api_url or '-'}", f"🌐 API URL: {api_url or '-'}"),
                self._tr(user_id, f"🗓️ Start Date: {start_date}", f"🗓️ Start Date: {start_date}"),
                self._tr(user_id, f"📦 BOT LIMIT: {limit}", f"📦 BOT LIMIT: {limit}"),
                self._tr(user_id, f"👮 Admin IDs: {admins}", f"👮 Admin IDs: {admins}"),
            ]
        )
        self.edit_text(chat_id, message_id, text, self.kb_vars_menu(user_id))

    def show_accounts(self, chat_id: int | str, message_id: int, user_id: int) -> None:
        rows = self.load_accounts()
        if not rows:
            text = self._title_accounts(user_id) + "\n" + self._tr(user_id, "لا توجد حسابات.", "No accounts.")
        else:
            lines = [self._title_accounts(user_id)]
            for idx, row in enumerate(rows, start=1):
                state = self._tr(user_id, "مفعل", "Enabled") if bool(row.get("enabled", True)) else self._tr(user_id, "مغلق", "Disabled")
                lines.append(f"{idx}. {row.get('name')} | {row.get('email')} | {state}")
            text = "\n".join(lines)
        self.edit_text(chat_id, message_id, text, self.kb_accounts_menu(user_id))

    def show_groups(self, chat_id: int | str, message_id: int, user_id: int) -> None:
        rows = self.load_groups()
        if not rows:
            text = self._title_groups(user_id) + "\n" + self._tr(user_id, "لا توجد جروبات.", "No groups.")
        else:
            lines = [self._title_groups(user_id)]
            for idx, row in enumerate(rows, start=1):
                state = self._tr(user_id, "مفعل", "Enabled") if bool(row.get("enabled", True)) else self._tr(user_id, "مغلق", "Disabled")
                lines.append(f"{idx}. {row.get('name')} | {row.get('chat_id')} | {state}")
            text = "\n".join(lines)
        self.edit_text(chat_id, message_id, text, self.kb_groups_menu(user_id))

    # -------------------------- export --------------------------
    def detect_country_code(self, number: str) -> str:
        digits = "".join(ch for ch in (number or "") if ch.isdigit())
        if digits.startswith("20"):
            return "EG"
        if digits.startswith("229"):
            return "BJ"
        if digits.startswith("1"):
            return "US"
        return "UNK"

    def export_numbers(
        self,
        chat_id: int | str,
        user_id: int,
        rows: list[dict[str, str]],
        fmt: str,
        tag: str,
        fields: list[str] | None = None,
    ) -> None:
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        allowed = ["number", "range", "id"]
        normalized = [f for f in (fields or []) if f in allowed]
        if not normalized:
            normalized = allowed
        suffix = "_".join(normalized)
        stem = f"numbers_{tag}_{suffix}_{now}"
        header_by_field = {"number": self._tr(user_id, "الرقم", "number"), "range": self._tr(user_id, "الرينج", "range"), "id": self._tr(user_id, "الايدي", "id")}

        if fmt == "json":
            path = EXPORT_DIR / f"{stem}.json"
            payload = [{k: str(r.get(k, "")).strip() for k in normalized} for r in rows]
            self.save_json(path, payload)
        elif fmt == "csv":
            path = EXPORT_DIR / f"{stem}.csv"
            lines = [",".join(normalized)]
            for r in rows:
                lines.append(",".join(str(r.get(k, "")).replace(",", " ").replace("\n", " ").strip() for k in normalized))
            path.write_text("\n".join(lines), encoding="utf-8")
        else:
            path = EXPORT_DIR / f"{stem}.txt"
            lines = [" | ".join(header_by_field[k] for k in normalized)]
            for r in rows:
                lines.append(" | ".join(str(r.get(k, "")).strip() for k in normalized))
            path.write_text("\n".join(lines), encoding="utf-8")

        selected = ", ".join(self._export_field_label(user_id, f) for f in normalized)
        try:
            self.send_document(chat_id, path, caption=self._tr(user_id, f"تم التصدير: {path.name}\nالمحتوى: {selected}", f"Exported: {path.name}\nField: {selected}"))
        finally:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass

    def _export_full_numbers(self, chat_id: int | str, user_id: int, fmt: str, fields: list[str] | None = None) -> None:
        rows = self.fetch_numbers()
        if not rows:
            self.send_text(chat_id, self._tr(user_id, "لا توجد أرقام للتصدير.", "No numbers to export."))
            return
        self.export_numbers(chat_id, user_id, rows, fmt, "full", fields)

    def _export_by_range(self, chat_id: int | str, user_id: int, fmt: str, range_name: str, fields: list[str] | None = None) -> None:
        rows = [r for r in self.fetch_numbers() if str(r.get("range", "")).strip().lower() == range_name.strip().lower()]
        if rows:
            self.export_numbers(chat_id, user_id, rows, fmt, f"range_{range_name.replace(' ', '_')}", fields)
        else:
            self.send_text(chat_id, self._tr(user_id, "لا توجد أرقام لهذا الرينج.", "No numbers for this range."))

    def _export_by_country(self, chat_id: int | str, user_id: int, fmt: str, country: str, fields: list[str] | None = None) -> None:
        rows = [r for r in self.fetch_numbers() if self.detect_country_code(str(r.get("number", ""))) == country]
        if rows:
            self.export_numbers(chat_id, user_id, rows, fmt, f"country_{country}", fields)
        else:
            self.send_text(chat_id, self._tr(user_id, "لا توجد أرقام لهذه الدولة.", "No numbers for this country."))

    def _render_export_menu(self, chat_id: int | str, message_id: int, user_id: int) -> None:
        text = self._q(self._tr(user_id, "༺═════⇓ تصدير الارقام ⇓═════༻", "༺═════⇓ Export Numbers ⇓═════༻")) + "\n" + self._tr(user_id, "اختر نوع التصدير.", "Choose export type.")
        self.edit_text(chat_id, message_id, text, self.kb_export_menu(user_id))

    def _process_range_request(
        self,
        chat_id: int | str,
        user_id: int,
        range_name: str,
        count: int,
        account_name: str | None = None,
        account_names: list[str] | None = None,
    ) -> None:
        result = self.request_numbers_for_range(user_id, range_name, count, account_name, account_names)
        self.send_text(chat_id, result)
        self.show_main(chat_id, user_id)

    def _process_delete_request(self, chat_id: int | str, user_id: int, items: list[str]) -> None:
        result = self.delete_numbers(user_id, items)
        self.send_text(chat_id, result)
        self.show_main(chat_id, user_id)

    # -------------------------- state machine --------------------------
    def set_state(self, user_id: int, mode: str, data: dict[str, Any] | None = None) -> None:
        self.user_state[user_id] = {"mode": mode, "data": data or {}}

    def clear_state(self, user_id: int) -> None:
        self.user_state.pop(user_id, None)

    def get_state(self, user_id: int) -> dict[str, Any] | None:
        return self.user_state.get(user_id)

    # -------------------------- callbacks --------------------------
    def handle_callback(self, q: dict[str, Any]) -> None:
        callback_id = str(q.get("id") or "")
        data = str(q.get("data") or "")
        msg = q.get("message") or {}
        chat = msg.get("chat") or {}
        chat_type = str(chat.get("type") or "")
        chat_id = ((msg.get("chat") or {}).get("id") or 0)
        message_id = int(msg.get("message_id") or 0)
        from_user = q.get("from") or {}
        user_id = int(from_user.get("id") or 0)
        self._set_user_lang(user_id, from_user.get("language_code"))
        self.refresh_runtime_settings()

        # Control panel is private-only; ignore callbacks from groups/channels.
        if chat_type and chat_type != "private":
            self.answer_callback(callback_id)
            return

        if not self.is_admin(user_id):
            self.answer_callback(callback_id)
            return

        self.answer_callback(callback_id)

        if data == "noop":
            return
        if data == "main_menu":
            self.clear_state(user_id)
            self.bump_view_rev(user_id)
            self.show_main(chat_id, user_id, message_id)
            return
        if data == "toggle_fetch":
            enabled = self.fetch_codes_enabled()
            self.set_fetch_codes_enabled(not enabled)
            self.mark_runtime_change()
            self.show_main(chat_id, user_id, message_id)
            return

        if data in {"set_start_date", "var_set_start_date"}:
            current = self.get_runtime_start_date()
            self.set_state(user_id, "wait_var_start_date")
            self.send_text(
                chat_id,
                self._tr(
                    user_id,
                    f"اكتب وقت البداية بصيغة YYYY-MM-DD\nالحالي: {current}",
                    f"Send start date in YYYY-MM-DD format\nCurrent: {current}",
                ),
            )
            return

        if data == "vars_menu":
            self.show_variables(chat_id, message_id, user_id)
            return

        if data == "var_admins_menu":
            self.edit_text(
                chat_id,
                message_id,
                self._q(self._tr(user_id, "༺═════⇓ إدارة الأدمن ⇓═════༻", "༺═════⇓ Admin Management ⇓═════༻"))
                + "\n"
                + self._tr(user_id, "اختر العملية.", "Choose an action."),
                self.kb_admins_menu(user_id),
            )
            return

        if data == "var_set_api_url":
            current = self.get_runtime_api_base()
            self.set_state(user_id, "wait_var_api_url")
            self.send_text(
                chat_id,
                self._tr(
                    user_id,
                    f"اكتب API URL\nالحالي: {current}",
                    f"Send API URL\nCurrent: {current}",
                ),
            )
            return

        if data == "var_set_bot_limit":
            current = self.get_runtime_bot_limit()
            self.set_state(user_id, "wait_var_bot_limit")
            self.send_text(
                chat_id,
                self._tr(
                    user_id,
                    f"اكتب BOT LIMIT (0 = بدون حد)\nالحالي: {current}",
                    f"Send BOT LIMIT (0 = unlimited)\nCurrent: {current}",
                ),
            )
            return

        if data in {"var_add_admin", "var_admin_add"}:
            self.set_state(user_id, "wait_var_add_admin")
            self.send_text(
                chat_id,
                self._tr(user_id, "اكتب Telegram User ID للأدمن الجديد.", "Send Telegram User ID for new admin."),
            )
            return

        if data == "var_admin_list":
            admins_txt = ", ".join(str(x) for x in self.get_runtime_admin_ids()) or "-"
            self.edit_text(
                chat_id,
                message_id,
                self._q(self._tr(user_id, "༺═════⇓ إدارة الأدمن ⇓═════༻", "༺═════⇓ Admin Management ⇓═════༻"))
                + "\n"
                + self._tr(user_id, f"الأدمن الحاليين:\n{admins_txt}", f"Current admins:\n{admins_txt}"),
                self.kb_admins_menu(user_id),
            )
            return

        if data == "var_admin_delete_menu":
            admin_ids = self.get_runtime_admin_ids()
            buttons = [self._btn(f"🗑️ {aid}", callback_data=f"var_admin_del:{aid}", style="danger") for aid in admin_ids]
            kb = self._pattern_rows(buttons, back_callback="var_admins_menu", back_text=self._tr(user_id, "رجوع", "Back"))
            self.edit_text(
                chat_id,
                message_id,
                self._q(self._tr(user_id, "༺═════⇓ إدارة الأدمن ⇓═════༻", "༺═════⇓ Admin Management ⇓═════༻"))
                + "\n"
                + self._tr(user_id, "اختر الأدمن المراد حذفه.", "Choose admin to delete."),
                kb,
            )
            return

        if data.startswith("var_admin_del:"):
            aid = data.split(":", 1)[1].strip()
            if not self.remove_runtime_admin(aid):
                self.answer_callback(callback_id, self._tr(user_id, "تعذر حذف الأدمن.", "Could not delete admin."))
            else:
                self.mark_runtime_change()
                self.answer_callback(callback_id, self._tr(user_id, "تم حذف الأدمن.", "Admin deleted."))
            admin_ids = self.get_runtime_admin_ids()
            admins_txt = ", ".join(str(x) for x in admin_ids) or "-"
            self.edit_text(
                chat_id,
                message_id,
                self._q(self._tr(user_id, "༺═════⇓ إدارة الأدمن ⇓═════༻", "༺═════⇓ Admin Management ⇓═════༻"))
                + "\n"
                + self._tr(user_id, f"الأدمن الحاليين:\n{admins_txt}", f"Current admins:\n{admins_txt}"),
                self.kb_admins_menu(user_id),
            )
            return

        if data == "var_bots_menu":
            if not self.is_primary_admin(user_id):
                self.answer_callback(callback_id, self._tr(user_id, "غير متاح.", "Not allowed."))
                return
            self.edit_text(
                chat_id,
                message_id,
                self._q(self._tr(user_id, "༺═════⇓ إدارة البوتات ⇓═════༻", "༺═════⇓ Bots Management ⇓═════༻"))
                + "\n"
                + self._tr(user_id, "اختر العملية.", "Choose an action."),
                self.kb_bots_mgmt_menu(user_id),
            )
            return

        if data == "var_bot_add":
            if not self.is_primary_admin(user_id):
                self.answer_callback(callback_id, self._tr(user_id, "غير متاح.", "Not allowed."))
                return
            self.set_state(user_id, "wait_new_bot_token")
            self.send_text(chat_id, self._tr(user_id, "ارسل توكن البوت.", "Send bot token."))
            return

        if data.startswith("var_bot_store:"):
            if not self.is_primary_admin(user_id):
                self.answer_callback(callback_id, self._tr(user_id, "غير متاح.", "Not allowed."))
                return
            storage = data.split(":", 1)[1].strip().lower()
            st = self.get_state(user_id) or {}
            if st.get("mode") != "wait_new_bot_storage":
                self.send_text(chat_id, self._tr(user_id, "ابدأ من إدارة البوتات.", "Start from bots management first."))
                return
            token = str((st.get("data") or {}).get("token") or "").strip()
            if not token:
                self.send_text(chat_id, self._tr(user_id, "التوكن غير صالح.", "Invalid token."))
                return
            self.upsert_managed_bot(token, storage, user_id)
            self.clear_state(user_id)
            self.mark_runtime_change()
            self.send_text(chat_id, self._tr(user_id, "تم حفظ البوت.", "Bot saved."), self.kb_bots_mgmt_menu(user_id))
            return

        if data == "var_bot_list":
            if not self.is_primary_admin(user_id):
                self.answer_callback(callback_id, self._tr(user_id, "غير متاح.", "Not allowed."))
                return
            rows = self.load_managed_bots()
            lines = [self._q(self._tr(user_id, "༺═════⇓ إدارة البوتات ⇓═════༻", "༺═════⇓ Bots Management ⇓═════༻"))]
            if not rows:
                lines.append(self._tr(user_id, "لا توجد بوتات إضافية.", "No extra bots."))
            else:
                for i, row in enumerate(rows, start=1):
                    tok = str(row.get("token", ""))
                    masked = f"{tok[:12]}...{tok[-6:]}" if len(tok) > 20 else tok
                    lines.append(self._tr(user_id, f"{i}. {masked} | التخزين: {row.get('storage')}", f"{i}. {masked} | storage: {row.get('storage')}"))
            self.edit_text(chat_id, message_id, "\n".join(lines), self.kb_bots_mgmt_menu(user_id))
            return

        if data == "var_bot_delete_menu":
            if not self.is_primary_admin(user_id):
                self.answer_callback(callback_id, self._tr(user_id, "غير متاح.", "Not allowed."))
                return
            rows = self.load_managed_bots()
            buttons: list[dict[str, Any]] = []
            for idx, row in enumerate(rows, start=1):
                tok = str(row.get("token", ""))
                masked = f"{tok[:10]}...{tok[-5:]}" if len(tok) > 16 else tok
                buttons.append(self._btn(f"🗑️ {masked}", callback_data=f"var_bot_del:{idx}", style="danger"))
            kb = self._pattern_rows(buttons, back_callback="var_bots_menu", back_text=self._tr(user_id, "رجوع", "Back"))
            self.edit_text(chat_id, message_id, self._tr(user_id, "اختر البوت المراد حذفه.", "Choose bot to delete."), kb)
            return

        if data.startswith("var_bot_del:"):
            if not self.is_primary_admin(user_id):
                self.answer_callback(callback_id, self._tr(user_id, "غير متاح.", "Not allowed."))
                return
            try:
                idx = int(data.split(":", 1)[1]) - 1
            except Exception:
                idx = -1
            rows = self.load_managed_bots()
            if idx < 0 or idx >= len(rows):
                self.answer_callback(callback_id, self._tr(user_id, "اختيار غير صالح.", "Invalid selection."))
                return
            tok = str(rows[idx].get("token", "")).strip()
            if self.delete_managed_bot(tok):
                self.mark_runtime_change()
            self.answer_callback(callback_id, self._tr(user_id, "تم حذف البوت.", "Bot deleted."))
            self.edit_text(chat_id, message_id, self._tr(user_id, "تم التحديث.", "Updated."), self.kb_bots_mgmt_menu(user_id))
            return

        if data == "publish_settings_menu":
            self.edit_text(
                chat_id,
                message_id,
                self._q(self._tr(user_id, "༺═════⇓ إعدادات النشر ⇓═════༻", "༺═════⇓ Publish Settings ⇓═════༻"))
                + "\n"
                + self._tr(user_id, "اختر القسم.", "Choose section."),
                self.kb_publish_settings_menu(user_id),
            )
            return

        if data == "publish_services_menu":
            self.edit_text(chat_id, message_id, self._tr(user_id, "إعدادات الخدمات", "Services settings"), self.kb_publish_services_menu(user_id))
            return

        if data == "publish_countries_menu":
            self.edit_text(chat_id, message_id, self._tr(user_id, "إعدادات البلدان", "Countries settings"), self.kb_publish_countries_menu(user_id))
            return

        if data == "publish_services_show":
            rows = self.load_services()
            lines = [self._q(self._tr(user_id, "༺═════⇓ الخدمات ⇓═════༻", "༺═════⇓ Services ⇓═════༻"))]
            if not rows:
                lines.append(self._tr(user_id, "لا توجد خدمات.", "No services."))
            else:
                for i, row in enumerate(rows, start=1):
                    lines.append(f"{i}. {row.get('key')} | {row.get('short')} | {row.get('emoji') or '-'} | {row.get('emoji_id') or '-'}")
            self.edit_text(chat_id, message_id, "\n".join(lines), self.kb_publish_services_menu(user_id))
            return

        if data == "publish_countries_show":
            rows = self.load_countries_store()
            lines = [self._q(self._tr(user_id, "༺═════⇓ البلدان ⇓═════༻", "༺═════⇓ Countries ⇓═════༻"))]
            if not rows:
                lines.append(self._tr(user_id, "لا توجد بلدان.", "No countries."))
            else:
                for i, row in enumerate(rows, start=1):
                    lines.append(f"{i}. +{row.get('dial_code')} | {row.get('iso2')} | {row.get('name_ar') or row.get('name_en')} | {row.get('emoji') or '-'} | {row.get('emoji_id') or '-'}")
            self.edit_text(chat_id, message_id, "\n".join(lines), self.kb_publish_countries_menu(user_id))
            return

        if data == "publish_services_add":
            self.set_state(user_id, "wait_publish_service_add")
            self.send_text(chat_id, self._tr(user_id, "ارسل: key,short,emoji(optional),emoji_id(optional)", "Send: key,short,emoji(optional),emoji_id(optional)"))
            return

        if data == "publish_countries_add":
            self.set_state(user_id, "wait_publish_country_add")
            self.send_text(chat_id, self._tr(user_id, "ارسل: dial_code,iso2,name_ar,name_en,emoji(optional),emoji_id(optional)", "Send: dial_code,iso2,name_ar,name_en,emoji(optional),emoji_id(optional)"))
            return

        if data == "publish_services_delete_menu":
            rows = self.load_services()
            buttons = [self._btn(f"🗑️ {row.get('key')}", callback_data=f"publish_service_del:{idx}", style="danger") for idx, row in enumerate(rows, start=1)]
            kb = self._pattern_rows(buttons, back_callback="publish_services_menu", back_text=self._tr(user_id, "رجوع", "Back"))
            self.edit_text(chat_id, message_id, self._tr(user_id, "اختر خدمة للحذف.", "Choose service to delete."), kb)
            return

        if data.startswith("publish_service_del:"):
            try:
                idx = int(data.split(":", 1)[1]) - 1
            except Exception:
                idx = -1
            rows = self.load_services()
            if 0 <= idx < len(rows):
                rows.pop(idx)
                self.save_services(rows)
            self.edit_text(chat_id, message_id, self._tr(user_id, "تم حذف الخدمة.", "Service deleted."), self.kb_publish_services_menu(user_id))
            return

        if data == "publish_countries_delete_menu":
            rows = self.load_countries_store()
            buttons = [self._btn(f"🗑️ +{row.get('dial_code')}", callback_data=f"publish_country_del:{idx}", style="danger") for idx, row in enumerate(rows, start=1)]
            kb = self._pattern_rows(buttons, back_callback="publish_countries_menu", back_text=self._tr(user_id, "رجوع", "Back"))
            self.edit_text(chat_id, message_id, self._tr(user_id, "اختر بلد للحذف.", "Choose country to delete."), kb)
            return

        if data.startswith("publish_country_del:"):
            try:
                idx = int(data.split(":", 1)[1]) - 1
            except Exception:
                idx = -1
            rows = self.load_countries_store()
            if 0 <= idx < len(rows):
                rows.pop(idx)
                self.save_countries_store(rows)
            self.edit_text(chat_id, message_id, self._tr(user_id, "تم حذف البلد.", "Country deleted."), self.kb_publish_countries_menu(user_id))
            return

        if data == "publish_services_edit_menu":
            rows = self.load_services()
            buttons = [self._btn(f"✏️ {row.get('key')}", callback_data=f"publish_service_edit:{idx}", style="primary") for idx, row in enumerate(rows, start=1)]
            kb = self._pattern_rows(buttons, back_callback="publish_services_menu", back_text=self._tr(user_id, "رجوع", "Back"))
            self.edit_text(chat_id, message_id, self._tr(user_id, "اختر خدمة للتعديل.", "Choose service to edit."), kb)
            return

        if data.startswith("publish_service_edit:"):
            try:
                idx = int(data.split(":", 1)[1]) - 1
            except Exception:
                idx = -1
            self.set_state(user_id, "wait_publish_service_edit_field", {"index": idx})
            kb = self._pattern_rows(
                [
                    self._btn(self._tr(user_id, "الاسم", "Name"), callback_data="publish_service_field:key", style="primary"),
                    self._btn(self._tr(user_id, "الايموجي", "Emoji"), callback_data="publish_service_field:emoji", style="primary"),
                ],
                back_callback="publish_services_menu",
                back_text=self._tr(user_id, "رجوع", "Back"),
            )
            self.edit_text(chat_id, message_id, self._tr(user_id, "اختر نوع التعديل.", "Choose edit field."), kb)
            return

        if data.startswith("publish_service_field:"):
            field = data.split(":", 1)[1].strip().lower()
            st = self.get_state(user_id) or {}
            if st.get("mode") != "wait_publish_service_edit_field":
                return
            payload = st.get("data") or {}
            payload["field"] = field
            self.set_state(user_id, "wait_publish_service_edit_value", payload)
            self.send_text(chat_id, self._tr(user_id, "ارسل القيمة الجديدة (للايموجي يمكنك: emoji,emoji_id).", "Send new value (for emoji you can send: emoji,emoji_id)."))
            return

        if data == "publish_countries_edit_menu":
            rows = self.load_countries_store()
            buttons = [self._btn(f"✏️ +{row.get('dial_code')}", callback_data=f"publish_country_edit:{idx}", style="primary") for idx, row in enumerate(rows, start=1)]
            kb = self._pattern_rows(buttons, back_callback="publish_countries_menu", back_text=self._tr(user_id, "رجوع", "Back"))
            self.edit_text(chat_id, message_id, self._tr(user_id, "اختر بلد للتعديل.", "Choose country to edit."), kb)
            return

        if data.startswith("publish_country_edit:"):
            try:
                idx = int(data.split(":", 1)[1]) - 1
            except Exception:
                idx = -1
            self.set_state(user_id, "wait_publish_country_edit_field", {"index": idx})
            kb = self._pattern_rows(
                [
                    self._btn(self._tr(user_id, "الاسم", "Name"), callback_data="publish_country_field:name", style="primary"),
                    self._btn(self._tr(user_id, "الاختصار", "Short"), callback_data="publish_country_field:iso2", style="primary"),
                    self._btn(self._tr(user_id, "الايموجي", "Emoji"), callback_data="publish_country_field:emoji", style="primary"),
                ],
                back_callback="publish_countries_menu",
                back_text=self._tr(user_id, "رجوع", "Back"),
            )
            self.edit_text(chat_id, message_id, self._tr(user_id, "اختر نوع التعديل.", "Choose edit field."), kb)
            return

        if data.startswith("publish_country_field:"):
            field = data.split(":", 1)[1].strip().lower()
            st = self.get_state(user_id) or {}
            if st.get("mode") != "wait_publish_country_edit_field":
                return
            payload = st.get("data") or {}
            payload["field"] = field
            self.set_state(user_id, "wait_publish_country_edit_value", payload)
            self.send_text(chat_id, self._tr(user_id, "ارسل القيمة الجديدة (للاسم: name_ar,name_en | للايموجي: emoji,emoji_id).", "Send new value (for name: name_ar,name_en | for emoji: emoji,emoji_id)."))
            return

        if data in {"var_reload", "var_restart"}:
            self.mark_runtime_change()
            self.answer_callback(callback_id, self._tr(user_id, "تم طلب إعادة تشغيل البوت.", "Bot restart requested."))
            self.show_variables(chat_id, message_id, user_id)
            return

        if data == "lang_menu":
            text = self._title_main(user_id) + "\n" + self._tr(user_id, "اختر اللغة", "Choose language")
            kb = self._pattern_rows(
                [
                    self._btn("العربية", callback_data="set_lang:ar", style="primary"),
                    self._btn("English", callback_data="set_lang:en", style="primary"),
                ],
                back_callback="main_menu",
                back_text=self._tr(user_id, "رجوع", "Back"),
            )
            self.edit_text(chat_id, message_id, text, kb)
            return

        if data.startswith("set_lang:"):
            lang = data.split(":", 1)[1].strip().lower()
            if lang not in {"ar", "en"}:
                return
            self.set_user_lang_override(user_id, lang)
            self.user_lang[user_id] = lang
            self.answer_callback(callback_id, "تم" if lang == "ar" else "Done")
            self.show_main(chat_id, user_id, message_id)
            return

        if data == "messages_menu":
            self.edit_text(
                chat_id,
                message_id,
                self._title_messages(user_id) + "\n" + self._tr(user_id, "اختر العملية.", "Choose an action."),
                self.kb_messages_menu(user_id),
            )
            return

        if data == "messages_show":
            self._show_loading(
                chat_id,
                message_id,
                self._tr(user_id, MESSAGES_TITLE, "༺═════⇓ Messages ⇓═════༻"),
                self._tr(user_id, "⏳ جاري تحميل الرسائل...", "⏳ Loading messages..."),
                "messages_menu",
                user_id,
            )
            self._run_async(self.show_saved_messages, chat_id, message_id, user_id)
            return

        if data == "messages_delete_confirm":
            clear_daily_store()
            self.answer_callback(callback_id, self._tr(user_id, "تم حذف الرسائل المحفوظة.", "Saved messages deleted."))
            self.edit_text(
                chat_id,
                message_id,
                self._title_messages(user_id) + "\n" + self._tr(user_id, "تم حذف كل الرسائل المحفوظة.", "All saved messages were deleted."),
                self.kb_messages_menu(user_id),
            )
            return

        if data == "refresh_data":
            self.platforms_cache = {"at": 0.0, "data": []}
            self.traffic_cache = {}
            self.user_traffic_cache.pop(user_id, None)
            self.user_numbers_cache.pop(user_id, None)
            self.request_messages_refresh()
            self.answer_callback(callback_id, self._tr(user_id, "تم التحديث.", "Refreshed."))
            self.edit_text(
                chat_id,
                message_id,
                self._title_messages(user_id) + "\n" + self._tr(user_id, "تم تحديث الكاش الداخلي.", "Internal cache refreshed."),
                self.kb_messages_menu(user_id),
            )
            return

        if data == "traffic_menu":
            rev = self.bump_view_rev(user_id)
            self._show_loading(chat_id, message_id, TRAFFIC_TITLE, self._tr(user_id, "⏳ جاري تحميل المنصات...", "⏳ Loading platforms..."), "main_menu", user_id)
            self._run_async(self._render_traffic_menu, chat_id, message_id, user_id, 1, True, rev)
            return

        if data.startswith("traffic_menu_nav:"):
            try:
                page = int(data.split(":", 1)[1])
            except Exception:
                page = 1
            rev = self.user_view_rev.get(user_id, 0)
            self._show_loading(chat_id, message_id, TRAFFIC_TITLE, self._tr(user_id, "⏳ جاري تحميل المنصات...", "⏳ Loading platforms..."), "main_menu", user_id)
            self._run_async(self._render_traffic_menu, chat_id, message_id, user_id, page, False, rev)
            return

        if data.startswith("traffic_app:"):
            app = data.split(":", 1)[1]
            rev = self.user_view_rev.get(user_id, 0)
            self._show_loading(chat_id, message_id, TRAFFIC_TITLE, self._tr(user_id, f"🧩 الخدمة: {app}\n⏳ جاري تحميل الترافيك...", f"🧩 Service: {app}\n⏳ Loading traffic..."), "traffic_menu", user_id)
            self._run_async(self.show_traffic_for_app, chat_id, message_id, user_id, app, 1, True, rev)
            return

        if data.startswith("traffic_nav:"):
            parts = data.split(":", 2)
            if len(parts) < 3:
                return
            app = parts[1]
            try:
                page = int(parts[2])
            except Exception:
                page = 1
            rev = self.user_view_rev.get(user_id, 0)
            self._show_loading(chat_id, message_id, TRAFFIC_TITLE, self._tr(user_id, f"🧩 الخدمة: {app}\n⏳ جاري تحميل الصفحة...", f"🧩 Service: {app}\n⏳ Loading page..."), "traffic_menu", user_id)
            self._run_async(self.show_traffic_for_app, chat_id, message_id, user_id, app, page, False, rev)
            return

        if data == "show_platforms":
            rev = self.bump_view_rev(user_id)
            self._show_loading(chat_id, message_id, TRAFFIC_TITLE, self._tr(user_id, "⏳ جاري تحميل المنصات...", "⏳ Loading platforms..."), "main_menu", user_id)
            self._run_async(self.show_platforms, chat_id, user_id, message_id, True, 1, True, rev)
            return

        if data.startswith("platforms_nav:"):
            try:
                page = int(data.split(":", 1)[1])
            except Exception:
                page = 1
            rev = self.user_view_rev.get(user_id, 0)
            self._show_loading(chat_id, message_id, TRAFFIC_TITLE, self._tr(user_id, "⏳ جاري تحميل المنصات...", "⏳ Loading platforms..."), "main_menu", user_id)
            self._run_async(self.show_platforms, chat_id, user_id, message_id, True, page, False, rev)
            return

        if data == "numbers_menu":
            self.bump_view_rev(user_id)
            self.edit_text(
                chat_id,
                message_id,
                self._title_numbers(user_id) + "\n" + self._tr(user_id, "اختر العملية.", "Choose an action."),
                self.kb_numbers_menu(user_id),
            )
            return

        if data == "numbers_show":
            text = self._title_numbers(user_id) + "\n" + self._tr(user_id, "اختر نوع العرض.", "Choose display type.")
            self.edit_text(chat_id, message_id, text, self.kb_numbers_scope(user_id))
            return

        if data == "numbers_show_all":
            self.user_numbers_view_account[user_id] = None
            rev = self.bump_view_rev(user_id)
            self._show_loading(chat_id, message_id, NUMBERS_TITLE, self._tr(user_id, "⏳ جاري تحميل الأرقام...", "⏳ Loading numbers..."), "numbers_menu", user_id)
            self._run_async(self.show_numbers, chat_id, message_id, user_id, 1, True, None, rev)
            return

        if data == "numbers_show_custom":
            self.edit_text(
                chat_id,
                message_id,
                self._title_numbers(user_id) + "\n" + self._tr(user_id, "اختر الحساب لعرض الأرقام.", "Choose account to show numbers."),
                self.kb_account_picker(user_id, "numbers_show_acc", back_callback="numbers_show"),
            )
            return

        if data.startswith("numbers_show_acc:"):
            account_name = self._account_name_by_pick(data.split(":", 1)[1])
            if not account_name:
                self.answer_callback(callback_id, self._tr(user_id, "اختيار حساب غير صالح.", "Invalid account selection."))
                return
            self.user_numbers_view_account[user_id] = account_name
            rev = self.bump_view_rev(user_id)
            self._show_loading(chat_id, message_id, NUMBERS_TITLE, self._tr(user_id, "⏳ جاري تحميل الأرقام...", "⏳ Loading numbers..."), "numbers_show", user_id)
            self._run_async(self.show_numbers, chat_id, message_id, user_id, 1, True, account_name, rev)
            return

        if data.startswith("numbers_nav:"):
            try:
                page = int(data.split(":", 1)[1])
            except Exception:
                page = 1
            rev = self.user_view_rev.get(user_id, 0)
            self._show_loading(chat_id, message_id, NUMBERS_TITLE, self._tr(user_id, "⏳ جاري تحميل الصفحة...", "⏳ Loading page..."), "numbers_menu", user_id)
            account_name = self.user_numbers_view_account.get(user_id)
            self._run_async(self.show_numbers, chat_id, message_id, user_id, page, False, account_name, rev)
            return

        if data == "numbers_export_menu":
            text = self._q(self._tr(user_id, "༺═════⇓ تصدير الارقام ⇓═════༻", "༺═════⇓ Export Numbers ⇓═════༻")) + "\n" + self._tr(user_id, "اختر نوع التصدير.", "Choose export type.")
            self.edit_text(chat_id, message_id, text, self.kb_export_menu(user_id))
            return

        if data == "exp_full":
            self.set_state(user_id, "wait_export_full_fields", {"fields": []})
            self.edit_text(
                chat_id,
                message_id,
                self._tr(user_id, "اختر المحتوى المطلوب ثم اضغط تم:", "Select fields then press Done:"),
                self.kb_export_fields("full", user_id, set()),
            )
            return

        if data.startswith("exp_field_toggle:"):
            parts = data.split(":", 2)
            if len(parts) != 3:
                return
            scope = parts[1].strip().lower()
            field = parts[2].strip().lower()
            if field not in {"number", "range", "id"}:
                return
            mode_expected = {
                "full": "wait_export_full_fields",
                "range": "wait_export_range_field",
                "country": "wait_export_country_field",
            }.get(scope, "")
            st = self.get_state(user_id) or {}
            if st.get("mode") != mode_expected:
                self.send_text(chat_id, self._tr(user_id, "ابدأ من قائمة التصدير أولًا.", "Start from export menu first."))
                return
            data_st = st.get("data") or {}
            selected = set(str(x) for x in (data_st.get("fields") or []) if str(x) in {"number", "range", "id"})
            if field in selected:
                selected.remove(field)
            else:
                selected.add(field)
            data_st["fields"] = sorted(selected)
            self.set_state(user_id, mode_expected, data_st)
            self.edit_text(
                chat_id,
                message_id,
                self._tr(user_id, "اختر المحتوى المطلوب ثم اضغط تم:", "Select fields then press Done:"),
                self.kb_export_fields(scope, user_id, selected),
            )
            return

        if data.startswith("exp_field_done:"):
            scope = data.split(":", 1)[1].strip().lower()
            mode_expected = {
                "full": "wait_export_full_fields",
                "range": "wait_export_range_field",
                "country": "wait_export_country_field",
            }.get(scope, "")
            st = self.get_state(user_id) or {}
            if st.get("mode") != mode_expected:
                self.send_text(chat_id, self._tr(user_id, "ابدأ من قائمة التصدير أولًا.", "Start from export menu first."))
                return
            data_st = st.get("data") or {}
            fields = [str(x) for x in (data_st.get("fields") or []) if str(x) in {"number", "range", "id"}]
            if not fields:
                self.send_text(chat_id, self._tr(user_id, "لازم تختار عنصر واحد على الأقل.", "Pick at least one field."))
                return
            selected_label = ", ".join(self._export_field_label(user_id, x) for x in fields)
            if scope == "full":
                self.set_state(user_id, "wait_export_full_format", {"fields": fields})
                self.edit_text(chat_id, message_id, self._tr(user_id, f"تم اختيار: {selected_label}\nاختر صيغة التصدير:", f"Selected: {selected_label}\nChoose export format:"), self.kb_export_formats("expfull", user_id))
                return
            if scope == "range":
                range_name = str(data_st.get("range") or "").strip()
                self.set_state(user_id, "wait_export_range_format", {"range": range_name, "fields": fields})
                self.edit_text(chat_id, message_id, self._tr(user_id, f"تم اختيار: {selected_label}\nاختر صيغة التصدير للرينج: {range_name}", f"Selected: {selected_label}\nChoose format for range: {range_name}"), self.kb_export_formats("exprange", user_id))
                return
            if scope == "country":
                country = str(data_st.get("country") or "").upper()
                self.set_state(user_id, "wait_export_country_format", {"country": country, "fields": fields})
                self.edit_text(chat_id, message_id, self._tr(user_id, f"تم اختيار: {selected_label}\nاختر صيغة التصدير للدولة: {country}", f"Selected: {selected_label}\nChoose format for country: {country}"), self.kb_export_formats("expcountry", user_id))
                return

        if data.startswith("expfull:"):
            fmt = data.split(":", 1)[1]
            self._show_loading(chat_id, message_id, self._tr(user_id, "༺═════⇓ تصدير الارقام ⇓═════༻", "༺═════⇓ Export Numbers ⇓═════༻"), self._tr(user_id, "⏳ جاري تجهيز التصدير...", "⏳ Preparing export..."), "numbers_export_menu", user_id)
            state = self.get_state(user_id) or {}
            fields: list[str] = []
            if state.get("mode") == "wait_export_full_format":
                fields = [str(x) for x in ((state.get("data") or {}).get("fields") or [])]
            self.clear_state(user_id)
            self._run_async(self._export_full_numbers, chat_id, user_id, fmt, fields)
            return

        if data == "exp_by_range":
            self.set_state(user_id, "wait_export_range")
            self.send_text(chat_id, self._tr(user_id, "اكتب اسم الرينج للتصدير المخصص:", "Type range name for custom export:"))
            return

        if data == "exp_by_country":
            self.set_state(user_id, "wait_export_country")
            self.send_text(chat_id, self._tr(user_id, "اكتب كود الدولة (مثال: EG أو BJ أو US):", "Type country code (example: EG, BJ, US):"))
            return

        if data == "numbers_request":
            self.edit_text(
                chat_id,
                message_id,
                self._title_numbers(user_id) + "\n" + self._tr(user_id, "اختر نوع التحكم قبل اختيار الحساب.", "Choose request mode before selecting accounts."),
                self.kb_numbers_request_mode(user_id),
            )
            return

        if data == "numbers_req_mode_normal":
            self.edit_text(
                chat_id,
                message_id,
                self._title_numbers(user_id) + "\n" + self._tr(user_id, "اختر الحساب لتنفيذ طلب الأرقام.", "Choose account to request numbers."),
                self.kb_account_picker(user_id, "numbers_req_acc", back_callback="numbers_request"),
            )
            return

        if data == "numbers_req_mode_multi":
            self.set_state(user_id, "wait_req_multi_accounts", {"selected": []})
            self.edit_text(
                chat_id,
                message_id,
                self._title_numbers(user_id)
                + "\n"
                + self._tr(
                    user_id,
                    "اختر الحسابات ثم اضغط تم.\nكل حساب يضيف 1000 حد للرينج.",
                    "Choose accounts then press Done.\nEach account adds 1000 range limit.",
                ),
                self.kb_numbers_req_multi_accounts(user_id, set()),
            )
            return

        if data.startswith("numbers_req_multi_toggle:"):
            st = self.get_state(user_id) or {}
            if st.get("mode") != "wait_req_multi_accounts":
                self.send_text(chat_id, self._tr(user_id, "ابدأ من طلب الأرقام أولًا.", "Start from request numbers first."))
                return
            payload = st.get("data") or {}
            selected = set(str(x) for x in (payload.get("selected") or []))
            account_name = self._account_name_by_pick(data.split(":", 1)[1])
            if not account_name:
                return
            if account_name in selected:
                selected.remove(account_name)
            else:
                selected.add(account_name)
            payload["selected"] = sorted(selected)
            self.set_state(user_id, "wait_req_multi_accounts", payload)
            self.edit_text(
                chat_id,
                message_id,
                self._title_numbers(user_id)
                + "\n"
                + self._tr(
                    user_id,
                    f"الحسابات المختارة: {len(selected)}",
                    f"Selected accounts: {len(selected)}",
                ),
                self.kb_numbers_req_multi_accounts(user_id, selected),
            )
            return

        if data == "numbers_req_multi_done":
            st = self.get_state(user_id) or {}
            if st.get("mode") != "wait_req_multi_accounts":
                self.send_text(chat_id, self._tr(user_id, "ابدأ من طلب الأرقام أولًا.", "Start from request numbers first."))
                return
            payload = st.get("data") or {}
            selected = [str(x) for x in (payload.get("selected") or []) if str(x).strip()]
            if not selected:
                self.send_text(chat_id, self._tr(user_id, "اختر حساب واحد على الأقل.", "Select at least one account."))
                return
            store = self.load_ranges_store()
            hint_rows: list[str] = []
            ranges = store.get("ranges") if isinstance(store.get("ranges"), dict) else {}
            for rname, entry in list(ranges.items())[:10]:
                if not isinstance(entry, dict):
                    continue
                rem = self._range_remaining(str(rname), account_names=selected)
                hint_rows.append(self._tr(user_id, f"- {rname} | المتبقي: {rem}", f"- {rname} | remaining: {rem}"))
            hint = "\n".join(hint_rows) if hint_rows else self._tr(user_id, "لا توجد رينجات محفوظة بعد.", "No saved ranges yet.")
            self.set_state(user_id, "wait_range_name", {"accounts": selected, "mode": "multi"})
            self.send_text(
                chat_id,
                self._tr(
                    user_id,
                    f"تم اختيار {len(selected)} حساب.\nاكتب اسم الرينج للطلب:\n\n",
                    f"Selected {len(selected)} accounts.\nType range name to request:\n\n",
                )
                + hint,
            )
            return

        if data.startswith("numbers_req_acc:"):
            account_name = self._account_name_by_pick(data.split(":", 1)[1])
            if not account_name:
                self.answer_callback(callback_id, self._tr(user_id, "اختيار حساب غير صالح.", "Invalid account selection."))
                return
            store = self.load_ranges_store()
            hint_rows: list[str] = []
            ranges = store.get("ranges") if isinstance(store.get("ranges"), dict) else {}
            for rname, entry in list(ranges.items())[:10]:
                if not isinstance(entry, dict):
                    continue
                rem = self._range_remaining(str(rname), account_name)
                hint_rows.append(self._tr(user_id, f"- {rname} | المتبقي: {rem}", f"- {rname} | remaining: {rem}"))
            hint = "\n".join(hint_rows) if hint_rows else self._tr(user_id, "لا توجد رينجات محفوظة بعد.", "No saved ranges yet.")
            self.set_state(user_id, "wait_range_name", {"account": account_name, "mode": "normal"})
            self.send_text(
                chat_id,
                self._tr(user_id, f"الحساب المختار: {account_name}\nاكتب اسم الرينج للطلب:\n\n", f"Selected account: {account_name}\nType range name to request:\n\n")
                + hint,
            )
            return

        if data == "numbers_delete":
            self.set_state(user_id, "wait_delete_numbers")
            self.send_text(chat_id, self._tr(user_id, "اكتب IDs/أرقام (كل عنصر في سطر) أو ارسل ملف txt/csv/json.", "Type IDs/numbers (one per line) or send txt/csv/json file."))
            return

        if data == "balances":
            rev = self.bump_view_rev(user_id)
            self._show_loading(chat_id, message_id, self._tr(user_id, "༺═════⇓ رصيدي ⇓═════༻", "༺═════⇓ Balances ⇓═════༻"), self._tr(user_id, "⏳ جاري تحميل الرصيد...", "⏳ Loading balances..."), "main_menu", user_id)
            self._run_async(self.show_balances, chat_id, message_id, user_id, rev)
            return

        if data == "stats":
            rev = self.bump_view_rev(user_id)
            self._show_loading(
                chat_id,
                message_id,
                self._tr(user_id, STATS_TITLE, "༺═════⇓ Statistics ⇓═════༻"),
                self._tr(user_id, "⏳ جاري تحميل الاحصائيات...", "⏳ Loading statistics..."),
                "main_menu",
                user_id,
            )
            self._run_async(self.show_stats, chat_id, message_id, user_id, rev)
            return

        if data == "groups_menu":
            self.edit_text(
                chat_id,
                message_id,
                self._title_groups(user_id) + "\n" + self._tr(user_id, "اختر العملية.", "Choose an action."),
                self.kb_groups_menu(user_id),
            )
            return

        if data == "grp_list":
            self.show_groups(chat_id, message_id, user_id)
            return

        if data == "grp_add":
            self.set_state(user_id, "wait_add_group")
            self.send_text(
                chat_id,
                self._tr(
                    user_id,
                    "ارسل الجروب بهذا الشكل:\nname,target\ntarget يقبل: -100... أو @username أو رابط t.me/username",
                    "Send group in this format:\nname,target\ntarget accepts: -100... or @username or t.me/username link",
                ),
            )
            return

        if data == "grp_delete_menu":
            rows = self.load_groups()
            buttons = [
                self._btn(self._tr(user_id, f"حذف {row.get('name')} | {row.get('chat_id')}", f"Delete {row.get('name')} | {row.get('chat_id')}"), callback_data=f"grp_del:{idx-1}", style="danger")
                for idx, row in enumerate(rows, start=1)
            ]
            kb = self._pattern_rows(buttons, back_callback="groups_menu")
            self.edit_text(chat_id, message_id, self._tr(user_id, "اختر الجروب المراد حذفه:", "Choose group to delete:"), kb)
            return

        if data.startswith("grp_del:"):
            idx = int(data.split(":", 1)[1])
            rows = self.load_groups()
            if idx < 0 or idx >= len(rows):
                self.send_text(chat_id, self._tr(user_id, "الجروب غير موجود.", "Group not found."))
                return
            removed = rows.pop(idx)
            self.save_groups(rows)
            self.send_text(chat_id, self._tr(user_id, f"تم حذف الجروب: {removed.get('chat_id')}", f"Group deleted: {removed.get('chat_id')}"))
            self.show_main(chat_id, user_id)
            return

        if data == "accounts_menu":
            self.edit_text(
                chat_id,
                message_id,
                self._title_accounts(user_id) + "\n" + self._tr(user_id, "اختر العملية.", "Choose an action."),
                self.kb_accounts_menu(user_id),
            )
            return

        if data == "acc_list":
            self.show_accounts(chat_id, message_id, user_id)
            return

        if data == "acc_add":
            self.set_state(user_id, "wait_add_account")
            self.send_text(
                chat_id,
                self._tr(
                    user_id,
                    "ارسل الحساب/الحسابات بهذا الشكل:\nname,email,password\n- يمكن ارسال أكثر من سطر\n- أو ارسل ملف txt/csv/json بنفس التنسيق",
                    "Send account(s) in this format:\nname,email,password\n- You can send multiple lines\n- Or send txt/csv/json file with same format",
                ),
            )
            return

        if data == "acc_delete_menu":
            rows = self.load_accounts()
            buttons = [
                self._btn(self._tr(user_id, f"حذف {row.get('name')} | {row.get('email')}", f"Delete {row.get('name')} | {row.get('email')}"), callback_data=f"acc_del:{idx-1}", style="danger")
                for idx, row in enumerate(rows, start=1)
            ]
            kb = self._pattern_rows(buttons, back_callback="accounts_menu")
            self.edit_text(chat_id, message_id, self._tr(user_id, "اختر الحساب المراد حذفه:", "Choose account to delete:"), kb)
            return

        if data.startswith("acc_del:"):
            idx = int(data.split(":", 1)[1])
            rows = self.load_accounts()
            if idx < 0 or idx >= len(rows):
                self.send_text(chat_id, self._tr(user_id, "الحساب غير موجود.", "Account not found."))
                return
            removed = rows.pop(idx)
            self.save_accounts(rows)
            self.send_text(chat_id, self._tr(user_id, f"تم حذف الحساب: {removed.get('email')}", f"Account deleted: {removed.get('email')}"))
            self.show_main(chat_id, user_id)
            return

        if data == "acc_edit_menu":
            rows = self.load_accounts()
            buttons = [
                self._btn(self._tr(user_id, f"تعديل {row.get('name')} | {row.get('email')}", f"Edit {row.get('name')} | {row.get('email')}"), callback_data=f"acc_edit:{idx-1}", style="primary")
                for idx, row in enumerate(rows, start=1)
            ]
            kb = self._pattern_rows(buttons, back_callback="accounts_menu")
            self.edit_text(chat_id, message_id, self._tr(user_id, "اختر الحساب المراد تعديله:", "Choose account to edit:"), kb)
            return

        if data.startswith("acc_edit:"):
            idx = int(data.split(":", 1)[1])
            rows = self.load_accounts()
            if idx < 0 or idx >= len(rows):
                self.send_text(chat_id, self._tr(user_id, "الحساب غير موجود.", "Account not found."))
                return
            row = rows[idx]
            self.set_state(user_id, "wait_edit_account", {"index": idx})
            self.send_text(
                chat_id,
                self._tr(
                    user_id,
                    "ارسل التعديل بهذا الشكل:\nname,email,password,enabled\nenabled = 1 أو 0\n\n"
                    f"الحالي: {row.get('name')},{row.get('email')},***,{1 if row.get('enabled', True) else 0}",
                    "Send update in this format:\nname,email,password,enabled\nenabled = 1 or 0\n\n"
                    f"Current: {row.get('name')},{row.get('email')},***,{1 if row.get('enabled', True) else 0}",
                ),
            )
            return

    # -------------------------- text / documents --------------------------
    def parse_delete_items(self, raw: str) -> list[str]:
        if not raw:
            return []
        lines = []
        for chunk in re.split(r"[\n,;\s]+", raw):
            item = chunk.strip().strip('"').strip("'")
            if item:
                lines.append(item)
        # unique preserving order
        out: list[str] = []
        seen: set[str] = set()
        for item in lines:
            if item in seen:
                continue
            seen.add(item)
            out.append(item)
        return out

    def parse_accounts_items(self, raw: str) -> tuple[list[dict[str, Any]], list[str]]:
        if not raw:
            return [], []
        lines = [ln.strip() for ln in str(raw).splitlines() if ln.strip()]
        parsed: list[dict[str, Any]] = []
        bad: list[str] = []
        for line in lines:
            if line.startswith("#"):
                continue
            parts = [x.strip() for x in line.split(",")]
            if len(parts) < 3:
                bad.append(line)
                continue
            name = parts[0]
            email = parts[1]
            password = ",".join(parts[2:]).strip()
            if not name:
                name = email
            if not email or not password:
                bad.append(line)
                continue
            parsed.append({"name": name, "email": email, "password": password, "enabled": True})
        # Deduplicate by email, keep last row.
        by_email: dict[str, dict[str, Any]] = {}
        for row in parsed:
            by_email[str(row.get("email", "")).strip().lower()] = row
        return list(by_email.values()), bad

    def handle_text_message(self, msg: dict[str, Any]) -> None:
        chat = msg.get("chat") or {}
        chat_type = str(chat.get("type") or "")
        from_user = msg.get("from") or {}
        chat_id = int(chat.get("id") or 0)
        user_id = int(from_user.get("id") or 0)
        self._set_user_lang(user_id, from_user.get("language_code"))
        self.refresh_runtime_settings()
        text = str(msg.get("text") or "").strip()
        state = self.get_state(user_id)

        # Never reply inside groups/channels.
        if chat_type and chat_type != "private":
            return

        if not self.is_admin(user_id):
            return

        if text in ("/start", "start", "menu", "/menu"):
            self.clear_state(user_id)
            if self.is_start_date_prompt_pending():
                current = self.get_runtime_start_date()
                self.set_state(user_id, "wait_start_date")
                self.send_text(
                    chat_id,
                    self._tr(
                        user_id,
                        f"اكتب وقت البداية لجلب الرسائل بصيغة YYYY-MM-DD\nالحالي: {current}",
                        f"Send message start date in YYYY-MM-DD format\nCurrent: {current}",
                    ),
                )
            else:
                self.show_main(chat_id, user_id)
            return

        if text.lower() in ("/lang", "lang", "language", "اللغة"):
            text_msg = self._title_main(user_id) + "\n" + self._tr(user_id, "اختر اللغة", "Choose language")
            kb = self._pattern_rows(
                [
                    self._btn("العربية", callback_data="set_lang:ar", style="primary"),
                    self._btn("English", callback_data="set_lang:en", style="primary"),
                ],
                back_callback="main_menu",
                back_text=self._tr(user_id, "رجوع", "Back"),
            )
            self.send_text(chat_id, text_msg, kb)
            return

        if not state:
            # If state was lost but admin sent a valid start date directly, accept it.
            if self._is_valid_day(text) and self.set_runtime_start_date(text):
                self.send_text(
                    chat_id,
                    self._tr(
                        user_id,
                        f"تم حفظ وقت البداية: {text}",
                        f"Start date saved: {text}",
                    ),
                )
                self.show_main(chat_id, user_id)
                return
            self.send_text(chat_id, self._tr(user_id, "اكتب /start لفتح لوحة التحكم.", "Send /start to open control panel."))
            return

        mode = state.get("mode")
        data = state.get("data") or {}

        if mode == "wait_start_date":
            if not self.set_runtime_start_date(text):
                self.send_text(chat_id, self._tr(user_id, "صيغة غير صحيحة. اكتب YYYY-MM-DD", "Invalid format. Use YYYY-MM-DD"))
                return
            self.clear_state(user_id)
            self.send_text(
                chat_id,
                self._tr(
                    user_id,
                    f"تم حفظ وقت البداية: {text}\nسيتم استخدامه عند تشغيل جلب الرسائل.",
                    f"Start date saved: {text}\nIt will be used by the fetch bot.",
                ),
            )
            self.show_main(chat_id, user_id)
            return

        if mode == "wait_var_start_date":
            if not self.set_runtime_start_date(text):
                self.send_text(chat_id, self._tr(user_id, "صيغة غير صحيحة. اكتب YYYY-MM-DD", "Invalid format. Use YYYY-MM-DD"))
                return
            self.clear_state(user_id)
            self.mark_runtime_change()
            self.send_text(chat_id, self._tr(user_id, f"تم حفظ Start Date: {text}", f"Start Date saved: {text}"))
            self.show_main(chat_id, user_id)
            return

        if mode == "wait_var_api_url":
            if not self.set_runtime_api_base(text):
                self.send_text(chat_id, self._tr(user_id, "صيغة URL غير صحيحة. اكتب http://... أو https://...", "Invalid URL. Use http://... or https://..."))
                return
            self.clear_state(user_id)
            self.mark_runtime_change()
            self.send_text(chat_id, self._tr(user_id, f"تم حفظ API URL: {self.get_runtime_api_base()}", f"API URL saved: {self.get_runtime_api_base()}"))
            self.show_main(chat_id, user_id)
            return

        if mode == "wait_var_bot_limit":
            if not self.set_runtime_bot_limit(text):
                self.send_text(chat_id, self._tr(user_id, "القيمة غير صحيحة. اكتب 0 (بدون حد) أو رقمًا صحيحًا.", "Invalid value. Send 0 (unlimited) or a valid number."))
                return
            self.clear_state(user_id)
            self.mark_runtime_change()
            self.send_text(chat_id, self._tr(user_id, f"تم حفظ BOT LIMIT: {self.get_runtime_bot_limit()}", f"BOT LIMIT saved: {self.get_runtime_bot_limit()}"))
            self.show_main(chat_id, user_id)
            return

        if mode == "wait_var_add_admin":
            if not self.add_runtime_admin(text):
                self.send_text(chat_id, self._tr(user_id, "ID غير صحيح.", "Invalid ID."))
                return
            self.clear_state(user_id)
            self.mark_runtime_change()
            admins_txt = ", ".join(str(x) for x in self.get_runtime_admin_ids()) or "-"
            self.send_text(
                chat_id,
                self._tr(
                    user_id,
                    f"تمت إضافة الأدمن بنجاح.\nالأدمن الحاليين:\n{admins_txt}",
                    f"Admin added successfully.\nCurrent admins:\n{admins_txt}",
                ),
                self.kb_admins_menu(user_id),
            )
            return

        if mode == "wait_new_bot_token":
            if not self.is_primary_admin(user_id):
                self.clear_state(user_id)
                self.send_text(chat_id, self._tr(user_id, "غير متاح.", "Not allowed."))
                return
            token = str(text or "").strip()
            if ":" not in token or len(token) < 20:
                self.send_text(chat_id, self._tr(user_id, "توكن غير صالح.", "Invalid token."))
                return
            self.set_state(user_id, "wait_new_bot_storage", {"token": token})
            kb = self._pattern_rows(
                [
                    self._btn(self._tr(user_id, "🗂️ تخزين خاص", "🗂️ Private Storage"), callback_data="var_bot_store:private", style="primary"),
                    self._btn(self._tr(user_id, "🔗 تخزين مشترك مع البوت الأساسي", "🔗 Shared With Main Bot"), callback_data="var_bot_store:shared", style="success"),
                ],
                back_callback="var_bots_menu",
                back_text=self._tr(user_id, "رجوع", "Back"),
            )
            self.send_text(chat_id, self._tr(user_id, "اختر نوع التخزين.", "Choose storage mode."), kb)
            return

        if mode == "wait_publish_service_add":
            parts = [x.strip() for x in text.split(",")]
            if len(parts) < 2:
                self.send_text(chat_id, self._tr(user_id, "صيغة غير صحيحة.", "Invalid format."))
                return
            key = parts[0]
            short = parts[1]
            emoji = parts[2] if len(parts) > 2 else ""
            emoji_id = parts[3] if len(parts) > 3 else ""
            rows = self.load_services()
            rows = [r for r in rows if str(r.get("key", "")).strip().lower() != key.lower()]
            rows.append({"key": key, "short": short, "emoji": emoji, "emoji_id": emoji_id})
            self.save_services(rows)
            self.clear_state(user_id)
            self.send_text(chat_id, self._tr(user_id, "تمت إضافة الخدمة.", "Service added."), self.kb_publish_services_menu(user_id))
            return

        if mode == "wait_publish_country_add":
            parts = [x.strip() for x in text.split(",")]
            if len(parts) < 4:
                self.send_text(chat_id, self._tr(user_id, "صيغة غير صحيحة.", "Invalid format."))
                return
            dial = "".join(ch for ch in parts[0] if ch.isdigit())
            iso2 = parts[1].upper()
            name_ar = parts[2]
            name_en = parts[3]
            emoji = parts[4] if len(parts) > 4 else ""
            emoji_id = parts[5] if len(parts) > 5 else ""
            rows = self.load_countries_store()
            rows = [r for r in rows if str(r.get("dial_code", "")).strip() != dial]
            rows.append({"dial_code": dial, "iso2": iso2, "name_ar": name_ar, "name_en": name_en, "emoji": emoji, "emoji_id": emoji_id})
            self.save_countries_store(rows)
            self.clear_state(user_id)
            self.send_text(chat_id, self._tr(user_id, "تمت إضافة البلد.", "Country added."), self.kb_publish_countries_menu(user_id))
            return

        if mode == "wait_publish_service_edit_value":
            idx = int(data.get("index", -1))
            field = str(data.get("field", "")).strip().lower()
            rows = self.load_services()
            if idx < 0 or idx >= len(rows):
                self.clear_state(user_id)
                self.send_text(chat_id, self._tr(user_id, "العنصر غير موجود.", "Item no longer exists."))
                return
            if field == "key":
                rows[idx]["key"] = text.strip()
            elif field == "emoji":
                parts = [x.strip() for x in text.split(",", 1)]
                rows[idx]["emoji"] = parts[0] if parts else ""
                rows[idx]["emoji_id"] = parts[1] if len(parts) > 1 else ""
            else:
                self.send_text(chat_id, self._tr(user_id, "نوع تعديل غير صالح.", "Invalid edit field."))
                return
            self.save_services(rows)
            self.clear_state(user_id)
            self.send_text(chat_id, self._tr(user_id, "تم تعديل الخدمة.", "Service updated."), self.kb_publish_services_menu(user_id))
            return

        if mode == "wait_publish_country_edit_value":
            idx = int(data.get("index", -1))
            field = str(data.get("field", "")).strip().lower()
            rows = self.load_countries_store()
            if idx < 0 or idx >= len(rows):
                self.clear_state(user_id)
                self.send_text(chat_id, self._tr(user_id, "العنصر غير موجود.", "Item no longer exists."))
                return
            if field == "name":
                parts = [x.strip() for x in text.split(",", 1)]
                rows[idx]["name_ar"] = parts[0] if parts else rows[idx].get("name_ar", "")
                rows[idx]["name_en"] = parts[1] if len(parts) > 1 else rows[idx].get("name_en", "")
            elif field == "iso2":
                rows[idx]["iso2"] = text.strip().upper()
            elif field == "emoji":
                parts = [x.strip() for x in text.split(",", 1)]
                rows[idx]["emoji"] = parts[0] if parts else ""
                rows[idx]["emoji_id"] = parts[1] if len(parts) > 1 else ""
            else:
                self.send_text(chat_id, self._tr(user_id, "نوع تعديل غير صالح.", "Invalid edit field."))
                return
            self.save_countries_store(rows)
            self.clear_state(user_id)
            self.send_text(chat_id, self._tr(user_id, "تم تعديل البلد.", "Country updated."), self.kb_publish_countries_menu(user_id))
            return

        if mode == "wait_add_account":
            rows_in, bad = self.parse_accounts_items(text)
            if not rows_in:
                self.send_text(chat_id, self._tr(user_id, "الصيغة غير صحيحة. المطلوب لكل سطر: name,email,password", "Invalid format. Expected per line: name,email,password"))
                return
            rows = self.load_accounts()
            old_by_email = {str(x.get("email", "")).strip().lower(): x for x in rows}
            added = 0
            updated = 0
            for row in rows_in:
                email_key = str(row.get("email", "")).strip().lower()
                if email_key in old_by_email:
                    updated += 1
                else:
                    added += 1
                old_by_email[email_key] = row
            rows = list(old_by_email.values())
            self.save_accounts(rows)
            self.clear_state(user_id)
            bad_note = ""
            if bad:
                bad_note = "\n" + self._tr(user_id, f"أسطر غير صالحة: {len(bad)}", f"Invalid lines: {len(bad)}")
            self.send_text(
                chat_id,
                self._tr(
                    user_id,
                    f"تم حفظ الحسابات.\nمضاف: {added}\nمحدّث: {updated}{bad_note}",
                    f"Accounts saved.\nAdded: {added}\nUpdated: {updated}{bad_note}",
                ),
            )
            self.show_main(chat_id, user_id)
            return

        if mode == "wait_add_group":
            parts = [x.strip() for x in text.split(",", 1)]
            if len(parts) == 1:
                name = parts[0]
                target = parts[0]
            else:
                name, target = parts[0], parts[1]
            chat_id_value = self.normalize_group_target(target)
            if not chat_id_value:
                self.send_text(chat_id, self._tr(user_id, "صيغة الجروب غير صحيحة.", "Invalid group format."))
                return
            rows = self.load_groups()
            rows = [x for x in rows if str(x.get("chat_id", "")).strip() != chat_id_value]
            rows.append({"name": name or chat_id_value, "chat_id": chat_id_value, "enabled": True})
            self.save_groups(rows)
            self.clear_state(user_id)
            self.send_text(chat_id, self._tr(user_id, f"تمت إضافة الجروب: {chat_id_value}", f"Group added: {chat_id_value}"))
            self.show_main(chat_id, user_id)
            return

        if mode == "wait_edit_account":
            idx = int(data.get("index", -1))
            rows = self.load_accounts()
            if idx < 0 or idx >= len(rows):
                self.clear_state(user_id)
                self.send_text(chat_id, self._tr(user_id, "الحساب لم يعد موجودًا.", "Account no longer exists."))
                return
            parts = [x.strip() for x in text.split(",")]
            if len(parts) < 4:
                self.send_text(chat_id, self._tr(user_id, "الصيغة غير صحيحة. المطلوب: name,email,password,enabled", "Invalid format. Expected: name,email,password,enabled"))
                return
            name = parts[0]
            email = parts[1]
            enabled = parts[-1] in ("1", "true", "True", "yes", "y")
            password = ",".join(parts[2:-1]).strip()
            rows[idx] = {"name": name, "email": email, "password": password, "enabled": enabled}
            self.save_accounts(rows)
            self.clear_state(user_id)
            self.send_text(chat_id, self._tr(user_id, f"تم تعديل الحساب: {email}", f"Account updated: {email}"))
            self.show_main(chat_id, user_id)
            return

        if mode == "wait_export_range":
            range_name = text
            self.set_state(user_id, "wait_export_range_field", {"range": range_name, "fields": []})
            self.send_text(
                chat_id,
                self._tr(user_id, f"اختر المحتوى المراد تصديره للرينج: {range_name}\nثم اضغط تم.", f"Choose export fields for range: {range_name}\nThen press Done."),
                self.kb_export_fields("range", user_id, set()),
            )
            return

        if mode == "wait_export_country":
            code = text.upper()
            self.set_state(user_id, "wait_export_country_field", {"country": code, "fields": []})
            self.send_text(
                chat_id,
                self._tr(user_id, f"اختر المحتوى المراد تصديره للدولة: {code}\nثم اضغط تم.", f"Choose export fields for country: {code}\nThen press Done."),
                self.kb_export_fields("country", user_id, set()),
            )
            return

        if mode == "wait_range_name":
            range_name = text
            account_name = str(data.get("account") or "").strip()
            account_names = [str(x) for x in (data.get("accounts") or []) if str(x).strip()]
            remain = self._range_remaining(range_name, account_name or None, account_names or None)
            self.set_state(
                user_id,
                "wait_range_count",
                {"range": range_name, "remaining": remain, "account": account_name, "accounts": account_names, "mode": data.get("mode")},
            )
            selected_text = account_name if account_name else ", ".join(account_names)
            self.send_text(
                chat_id,
                self._tr(
                    user_id,
                    f"الحسابات: {selected_text}\nاكتب العدد المطلوب (مضاعف 50).\nالمتبقي للرينج {range_name}: {remain}",
                    f"Accounts: {selected_text}\nType requested count (multiple of 50).\nRemaining for range {range_name}: {remain}",
                ),
            )
            return

        if mode == "wait_range_count":
            range_name = str(data.get("range") or "")
            account_name = str(data.get("account") or "").strip()
            account_names = [str(x) for x in (data.get("accounts") or []) if str(x).strip()]
            if not text.isdigit():
                self.send_text(chat_id, self._tr(user_id, "العدد لازم يكون رقم صحيح.", "Count must be a valid integer."))
                return
            count = int(text)
            self.clear_state(user_id)
            self.send_text(chat_id, self._tr(user_id, "جاري تنفيذ طلب الأرقام...", "Processing number request..."))
            self._run_async(self._process_range_request, chat_id, user_id, range_name, count, account_name or None, account_names or None)
            return

        if mode == "wait_delete_numbers":
            items = self.parse_delete_items(text)
            self.clear_state(user_id)
            self.send_text(chat_id, self._tr(user_id, "جاري حذف الأرقام...", "Deleting numbers..."))
            self._run_async(self._process_delete_request, chat_id, user_id, items)
            return

        self.send_text(chat_id, self._tr(user_id, "غير مفهوم. اكتب /start للعودة للقائمة.", "Not understood. Send /start to return to menu."))

    def handle_document_message(self, msg: dict[str, Any]) -> None:
        chat = msg.get("chat") or {}
        chat_type = str(chat.get("type") or "")
        from_user = msg.get("from") or {}
        chat_id = int(chat.get("id") or 0)
        user_id = int(from_user.get("id") or 0)
        self._set_user_lang(user_id, from_user.get("language_code"))
        self.refresh_runtime_settings()
        if chat_type and chat_type != "private":
            return
        if not self.is_admin(user_id):
            return
        state = self.get_state(user_id)
        mode = str((state or {}).get("mode") or "")
        if mode not in {"wait_delete_numbers", "wait_add_account"}:
            self.send_text(chat_id, self._tr(user_id, "الملف تم استلامه، لكن لا يوجد عملية قيد الانتظار.", "File received, but there is no pending operation."))
            return

        doc = msg.get("document") or {}
        file_id = str(doc.get("file_id") or "")
        if not file_id:
            self.send_text(chat_id, self._tr(user_id, "تعذر قراءة الملف.", "Could not read the file."))
            return

        content = self.get_file_content(file_id)
        if mode == "wait_delete_numbers":
            items = self.parse_delete_items(content)
            self.clear_state(user_id)
            self.send_text(chat_id, self._tr(user_id, "جاري حذف الأرقام من الملف...", "Deleting numbers from file..."))
            self._run_async(self._process_delete_request, chat_id, user_id, items)
            return

        rows_in, bad = self.parse_accounts_items(content)
        if not rows_in:
            self.send_text(chat_id, self._tr(user_id, "الملف لا يحتوي حسابات صالحة.", "The file contains no valid accounts."))
            return
        rows = self.load_accounts()
        old_by_email = {str(x.get("email", "")).strip().lower(): x for x in rows}
        added = 0
        updated = 0
        for row in rows_in:
            email_key = str(row.get("email", "")).strip().lower()
            if email_key in old_by_email:
                updated += 1
            else:
                added += 1
            old_by_email[email_key] = row
        self.save_accounts(list(old_by_email.values()))
        self.clear_state(user_id)
        bad_note = ""
        if bad:
            bad_note = "\n" + self._tr(user_id, f"أسطر غير صالحة: {len(bad)}", f"Invalid lines: {len(bad)}")
        self.send_text(
            chat_id,
            self._tr(
                user_id,
                f"تمت إضافة الحسابات من الملف.\nمضاف: {added}\nمحدّث: {updated}{bad_note}",
                f"Accounts imported from file.\nAdded: {added}\nUpdated: {updated}{bad_note}",
            ),
        )
        self.show_main(chat_id, user_id)

    # -------------------------- update router --------------------------
    def process_update(self, update: dict[str, Any]) -> None:
        callback_query = update.get("callback_query")
        if isinstance(callback_query, dict):
            data = str(callback_query.get("data") or "")
            user = callback_query.get("from") or {}
            user_id = int(user.get("id") or 0)
            self._set_user_lang(user_id, user.get("language_code"))

            if data.startswith("exprange:"):
                fmt = data.split(":", 1)[1]
                state = self.get_state(user_id)
                if state and state.get("mode") == "wait_export_range_format":
                    range_name = str((state.get("data") or {}).get("range") or "")
                    fields = [str(x) for x in ((state.get("data") or {}).get("fields") or [])]
                    chat_id = ((callback_query.get("message") or {}).get("chat") or {}).get("id")
                    msg = callback_query.get("message") or {}
                    mid = int(msg.get("message_id") or 0)
                    if mid:
                        self._show_loading(
                            chat_id,
                            mid,
                            self._tr(user_id, "༺═════⇓ تصدير الارقام ⇓═════༻", "༺═════⇓ Export Numbers ⇓═════༻"),
                            self._tr(user_id, "جاري تجهيز التصدير...", "Preparing export..."),
                            "numbers_export_menu",
                            user_id,
                        )
                    self._run_async(self._export_by_range, chat_id, user_id, fmt, range_name, fields)
                    self.clear_state(user_id)
                self.answer_callback(str(callback_query.get("id") or ""))
                return

            if data.startswith("expcountry:"):
                fmt = data.split(":", 1)[1]
                state = self.get_state(user_id)
                if state and state.get("mode") == "wait_export_country_format":
                    country = str((state.get("data") or {}).get("country") or "").upper()
                    fields = [str(x) for x in ((state.get("data") or {}).get("fields") or [])]
                    chat_id = ((callback_query.get("message") or {}).get("chat") or {}).get("id")
                    msg = callback_query.get("message") or {}
                    mid = int(msg.get("message_id") or 0)
                    if mid:
                        self._show_loading(
                            chat_id,
                            mid,
                            self._tr(user_id, "༺═════⇓ تصدير الارقام ⇓═════༻", "༺═════⇓ Export Numbers ⇓═════༻"),
                            self._tr(user_id, "جاري تجهيز التصدير...", "Preparing export..."),
                            "numbers_export_menu",
                            user_id,
                        )
                    self._run_async(self._export_by_country, chat_id, user_id, fmt, country, fields)
                    self.clear_state(user_id)
                self.answer_callback(str(callback_query.get("id") or ""))
                return

            self.handle_callback(callback_query)
            return

        message = update.get("message")
        if isinstance(message, dict):
            if message.get("text"):
                self.handle_text_message(message)
                return
            if message.get("document"):
                self.handle_document_message(message)
                return

    # -------------------------- polling --------------------------
    def run(self) -> None:
        print("Panel bot started. Press Ctrl+C to stop.")
        while True:
            payload = {"timeout": 25, "offset": self.last_update_id + 1}
            res = self.tg_api("getUpdates", payload)
            if not res.get("ok"):
                time.sleep(self.poll_interval)
                continue
            updates = res.get("result") or []
            if not isinstance(updates, list):
                time.sleep(self.poll_interval)
                continue

            for upd in updates:
                try:
                    upd_id = int(upd.get("update_id") or 0)
                    if upd_id > self.last_update_id:
                        self.last_update_id = upd_id
                    self.process_update(upd)
                except Exception as exc:
                    print(f"update processing error: {exc}")


def main() -> None:
    bot = PanelBot()
    bot.run()


if __name__ == "__main__":
    main()
