#!/usr/bin/env python3
"""Stop hook — refuse to end a turn without a fleet checkpoint (C1: enforced-or-absent).

Blocks AT MOST ONCE per stop (honors stop_hook_active), and fails open on every error path:
degraded means v1 honor-system behavior, never a bricked session. It DEMANDS a checkpoint;
it never fabricates one.
"""
import json
import os
import sqlite3
import sys
from pathlib import Path

FLEET_HOME = Path(__file__).resolve().parents[1]


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0
    if payload.get("stop_hook_active"):   # we already blocked once this stop — let it through
        return 0
    slug = os.environ.get("FLEET_SLUG")
    if not slug:
        return 0
    try:
        cwd = Path(payload.get("cwd") or os.getcwd())
        marker = cwd / ".claude" / ".fleet_session_start"
        since = marker.read_text().strip() if marker.exists() else None
        db = os.environ.get("FLEET_DB", str(FLEET_HOME / "fleet.db"))
        con = sqlite3.connect(db, timeout=3.0)
        try:
            if since:
                row = con.execute("SELECT COUNT(*) FROM updates WHERE slug=? AND ts>=?",
                                  (slug, since)).fetchone()
            else:
                row = con.execute("SELECT COUNT(*) FROM updates WHERE slug=?", (slug,)).fetchone()
        finally:
            con.close()
        if row and row[0] > 0:
            return 0  # checkpoint exists — stop allowed
    except Exception:
        return 0  # fail-open
    print(json.dumps({
        "decision": "block",
        "reason": (
            f"No fleet checkpoint this session. Before stopping, journal your hand-back so the "
            f"Conductor can route/unblock without re-reading your transcript:\n"
            f"  python3 {FLEET_HOME}/fleet.py checkpoint --as {slug} \\\n"
            f"    --did \"<what you worked on>\" --next \"<what remains>\" \\\n"
            f"    [--found \"<discovery>\"] [--files \"<paths>\"] [--blockers \"<waiting on>\"] \\\n"
            f"    [--review \"<question>\" --ask]   # --ask raises it as a human decision\n"
            f"Then stop again. (This guard blocks only once.)")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
