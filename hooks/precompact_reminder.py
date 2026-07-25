#!/usr/bin/env python3
"""PreCompact hook — loud reminder to checkpoint before compaction (cannot block; fail-open).

fleet.db (your checkpoints) + your task row ARE your durable memory across compaction; the
SessionStart hook re-grounds you from them afterwards.
"""
import json
import os
import sys
from pathlib import Path

FLEET_HOME = Path(__file__).resolve().parents[1]


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        payload = {}
    slug = os.environ.get("FLEET_SLUG")
    if not slug:
        return 0
    try:
        cwd = Path(payload.get("cwd") or os.getcwd())
        marker = cwd / ".claude" / ".fleet_session_start"
        since = marker.read_text().strip() if marker.exists() else None
        # Route through fleet's dual-backend connection (FLEET_DB may be a postgres:// DSN).
        # Any failure below falls through to the fail-open except.
        if str(FLEET_HOME) not in sys.path:
            sys.path.insert(0, str(FLEET_HOME))
        import fleet
        con = fleet.connect()
        try:
            q = ("SELECT COUNT(*) FROM updates WHERE slug=? AND ts>=?" if since
                 else "SELECT COUNT(*) FROM updates WHERE slug=?")
            row = con.execute(q, (slug, since) if since else (slug,)).fetchone()
        finally:
            con.close()
        if not row or row[0] == 0:
            print(f"⚠ COMPACTING WITHOUT A CHECKPOINT — run this first or your progress is "
                  f"unrecoverable by the Conductor:\n  python3 {FLEET_HOME}/fleet.py checkpoint "
                  f"--as {slug} --did … --next …")
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
