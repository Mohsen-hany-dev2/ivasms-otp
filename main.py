import fcntl
import os
import subprocess
import sys
import time
from pathlib import Path

from app.paths import RUNTIME_CONFIG_FILE
from app.storage import load_json as db_load_json


BASE_DIR = Path(__file__).resolve().parent
LOCK_FILE = BASE_DIR / "logs" / "main.lock"
PRIMARY_ADMIN_ID = 7011309417


def start_process(script_name: str, *extra_args: str, env_overrides: dict[str, str] | None = None) -> subprocess.Popen:
    env = os.environ.copy()
    for k, v in (env_overrides or {}).items():
        env[str(k)] = str(v)
    return subprocess.Popen(
        [sys.executable, str(BASE_DIR / script_name), *extra_args],
        cwd=str(BASE_DIR),
        env=env,
    )


def acquire_main_lock():
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    fh = LOCK_FILE.open("w", encoding="utf-8")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("Another main.py instance is already running. Exiting.")
        raise SystemExit(0)
    fh.write(str(time.time()))
    fh.flush()
    return fh


def get_restart_marker() -> str:
    cfg = db_load_json(RUNTIME_CONFIG_FILE, {})
    if not isinstance(cfg, dict):
        return ""
    return str(cfg.get("bot_restart_requested_at", "")).strip()


def build_specs() -> dict[str, tuple[str, tuple[str, ...], dict[str, str]]]:
    specs: dict[str, tuple[str, tuple[str, ...], dict[str, str]]] = {
        "sender": ("bot.py", ("--no-input",), {}),
        "panel": ("panel_bot.py", (), {}),
    }
    cfg = db_load_json(RUNTIME_CONFIG_FILE, {})
    if not isinstance(cfg, dict):
        return specs
    rows = cfg.get("managed_bots")
    if not isinstance(rows, list):
        return specs

    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        if not bool(row.get("enabled", True)):
            continue
        token = str(row.get("bot_token", "") or row.get("token", "")).strip()
        if not token or ":" not in token:
            continue
        storage_mode = str(row.get("storage_mode", "") or row.get("storage", "private")).strip().lower()
        if storage_mode not in {"private", "shared"}:
            storage_mode = "private"
        bot_id = str(row.get("id", "")).strip() or str(idx)
        env_overrides = {"TELEGRAM_BOT_TOKEN": token}
        if storage_mode == "private":
            env_overrides["IVASMS_DATA_NAMESPACE"] = f"bot_{bot_id}"

        # Managed bot admins: primary owner + creator + bot-specific admins.
        admin_ids: set[int] = {PRIMARY_ADMIN_ID}
        created_by = str(row.get("created_by", "")).strip()
        if created_by.isdigit():
            admin_ids.add(int(created_by))
        for x in (row.get("admin_ids") or []):
            s = str(x).strip()
            if s.isdigit():
                admin_ids.add(int(s))
        env_overrides["PANEL_ADMIN_IDS"] = ",".join(str(x) for x in sorted(admin_ids))

        specs[f"managed_sender_{bot_id}"] = (
            "bot.py",
            ("--no-input",),
            env_overrides.copy(),
        )
        specs[f"managed_panel_{bot_id}"] = (
            "panel_bot.py",
            (),
            env_overrides.copy(),
        )
    return specs


def specs_fingerprint(specs: dict[str, tuple[str, tuple[str, ...], dict[str, str]]]) -> tuple:
    items: list[tuple[str, str, tuple[str, ...], tuple[tuple[str, str], ...]]] = []
    for name, (script_name, extra_args, env_overrides) in specs.items():
        env_items = tuple(sorted((str(k), str(v)) for k, v in (env_overrides or {}).items()))
        items.append((name, script_name, tuple(extra_args), env_items))
    return tuple(sorted(items))


def stop_process(proc: subprocess.Popen, timeout_seconds: float = 8.0) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    deadline = time.time() + timeout_seconds
    while time.time() < deadline and proc.poll() is None:
        time.sleep(0.2)
    if proc.poll() is None:
        proc.kill()


def main() -> int:
    _lock_handle = acquire_main_lock()
    print("Starting sender/panel processes...")
    specs = build_specs()
    procs = {
        name: start_process(script_name, *extra_args, env_overrides=env_overrides)
        for name, (script_name, extra_args, env_overrides) in specs.items()
    }
    last_specs_fp = specs_fingerprint(specs)
    last_restart_marker = get_restart_marker()

    try:
        while True:
            marker = get_restart_marker()
            latest_specs = build_specs()
            latest_specs_fp = specs_fingerprint(latest_specs)
            if (marker and marker != last_restart_marker) or latest_specs_fp != last_specs_fp:
                reason = f"marker={marker}" if (marker and marker != last_restart_marker) else "managed bots changed"
                print(f"Restart requested ({reason}). Restarting processes...")
                for p in procs.values():
                    stop_process(p)
                specs = latest_specs
                procs = {
                    name: start_process(script_name, *extra_args, env_overrides=env_overrides)
                    for name, (script_name, extra_args, env_overrides) in specs.items()
                }
                last_specs_fp = latest_specs_fp
                last_restart_marker = marker
                time.sleep(1)
                continue

            for name, p in list(procs.items()):
                rc = p.poll()
                if rc is not None:
                    print(f"Process exited (name={name}, pid={p.pid}, code={rc}). Restarting process...")
                    script_name, extra_args, env_overrides = specs[name]
                    procs[name] = start_process(script_name, *extra_args, env_overrides=env_overrides)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping all processes...")
        for p in procs.values():
            stop_process(p)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
