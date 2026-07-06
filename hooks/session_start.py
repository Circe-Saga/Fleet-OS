#!/usr/bin/env python3
"""SessionStart hook — inject the agent's fleet brief as context + stamp the session marker.

Fail-open on EVERY path (brief F3): a broken hook must never brick a worker session.
stdout of a SessionStart hook is added to the agent's context, so the agent cannot forget
to read its brief — it never even has to ask for it.
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

FLEET_HOME = Path(__file__).resolve().parents[1]


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        payload = {}
    slug = os.environ.get("FLEET_SLUG")
    if not slug:
        return 0  # not a fleet-managed session — stay silent
    cwd = Path(payload.get("cwd") or os.getcwd())
    try:  # stamp session start for stop_guard's "checkpoint since" query
        marker = cwd / ".claude" / ".fleet_session_start"
        marker.parent.mkdir(exist_ok=True)
        marker.write_text(datetime.now(timezone.utc).isoformat(timespec="seconds"))
    except Exception:
        pass
    try:
        env = {**os.environ}
        r = subprocess.run([sys.executable, str(FLEET_HOME / "fleet.py"), "brief", "--as", slug],
                           capture_output=True, text=True, timeout=20, env=env)
        if r.returncode == 0 and r.stdout.strip():
            print(f"=== FLEET BRIEF for {slug} (auto-injected; source of truth: fleet.db) ===")
            print(r.stdout)
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
