import fcntl
import signal
import subprocess
import sys
import time
from pathlib import Path

from app.paths import RUNTIME_CONFIG_FILE
from app.storage import load_json as db_load_json


BASE_DIR = Path(__file__).resolve().parent
LOCK_FILE = BASE_DIR / "logs" / "main.lock"


def start_process(script_name: str, *extra_args: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, str(BASE_DIR / script_name), *extra_args],
        cwd=str(BASE_DIR),
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
    print("Starting sender bot (bot.py) and control panel bot (panel_bot.py)...")
    specs = {
        "sender": ("bot.py", ("--no-input",)),
        "panel": ("panel_bot.py", ()),
    }
    procs = {
        "sender": start_process("bot.py", "--no-input"),
        "panel": start_process("panel_bot.py"),
    }
    last_restart_marker = get_restart_marker()

    try:
        while True:
            marker = get_restart_marker()
            if marker and marker != last_restart_marker:
                print(f"Restart requested by runtime config marker={marker}. Restarting bots...")
                for p in procs.values():
                    stop_process(p)
                procs = {
                    "sender": start_process("bot.py", "--no-input"),
                    "panel": start_process("panel_bot.py"),
                }
                last_restart_marker = marker
                time.sleep(1)
                continue

            for name, p in list(procs.items()):
                rc = p.poll()
                if rc is not None:
                    print(f"Process exited (name={name}, pid={p.pid}, code={rc}). Restarting process...")
                    script_name, extra = specs[name]
                    procs[name] = start_process(script_name, *extra)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping all processes...")
        for p in procs.values():
            stop_process(p)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
