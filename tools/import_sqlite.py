#!/usr/bin/env python3
"""One-shot importer: an existing single-host fleet.db (SQLite) → the shared Postgres.

Adopting a machine that has been running Fleet-OS on a local SQLite fleet.db into a shared
Postgres backend means moving its rows across *with their ids and history intact*: task #7 must
still be task #7, every event/message/decision keeps its number, and the `host` column (added by
the multihost migration) is stamped so the shared DB knows which machine these rows came from.

    python tools/import_sqlite.py --source /path/to/fleet.db.copy --host <machine-name> \
        --target postgres://user@host/fleet

  --source   a WAL-checkpointed COPY of the machine's fleet.db. Checkpoint first so all committed
             rows live in the main db file, not a side -wal:
                 sqlite3 fleet.db 'PRAGMA wal_checkpoint(TRUNCATE);'
             then copy the file and point --source at the copy (never the live db).
  --host     value written into the host column of every host-scoped row (workstreams/runs/leases)
             that does not already carry one. This is the machine identity in the shared DB.
  --target   destination DSN. Defaults to $FLEET_DB. The intended target is the shared Postgres
             (postgres://…); a SQLite path also works and is what the tests exercise.

IMPORTANT — the target must be a FRESH fleet database. Ids are copied verbatim, so a second
fleet.db (whose ids also start at 1) or any organically-written row would collide. Run this import
BEFORE any new rows are written to the target. The tool refuses if the target already contains
fleet rows or a prior import marker, so a re-run is a clean no-op rather than a double import.

What it does, atomically (one transaction; a failure rolls the whole thing back):
  1. refuses if the target already holds an import marker OR any fleet rows (not fresh);
  2. copies every application table, preserving primary-key ids;
  3. backfills host on workstreams/runs/leases from --host (existing non-null host is kept);
  4. advances each table's identity sequence past the largest imported id (Postgres; SQLite's
     sqlite_sequence advances itself on an explicit-id insert);
  5. records an import marker in `events` so a re-run detects the prior import and refuses.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

# fleet.py lives at the repo root (this file is tools/import_sqlite.py). Import it to reuse the
# exact schema/migrations, the dual-backend connection wrapper, and the write-transaction helper —
# so the target schema can never drift from what fleet.py itself creates. fleet reads FLEET_DB at
# import time, so the target DSN must be in the environment BEFORE `import fleet` (done in main()).
REPO_ROOT = Path(__file__).resolve().parent.parent

# No foreign keys are declared between these tables, so insertion order is free. schema_migrations
# is owned by the target's own connect()/run_migrations() and is intentionally NOT copied.
IMPORT_TABLES = [
    "workstreams", "read_state", "decisions", "messages",
    "updates", "tasks", "runs", "events", "leases",
]
# Host-scoped tables: their rows belong to one machine, so a legacy (pre-multihost) row with a null
# host is stamped with --host on the way in.
HOST_TABLES = {"workstreams", "runs", "leases"}
# Tables whose id is an AUTOINCREMENT/identity PK. Under Postgres the backing sequence must be
# advanced past the largest imported id or the next organic insert would collide; under SQLite the
# sqlite_sequence counter advances itself when an explicit id is inserted, so this is a no-op.
IDENTITY_TABLES = ["decisions", "messages", "updates", "tasks", "runs", "events", "leases"]

IMPORT_ACTOR = "importer"
IMPORT_ACTION = "sqlite-import"


def _sqlite_has_table(src: sqlite3.Connection, table: str) -> bool:
    return src.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _sqlite_columns(src: sqlite3.Connection, table: str) -> list[str]:
    return [r["name"] for r in src.execute(f"PRAGMA table_info({table})").fetchall()]


def _copy_table(fleet, con, src: sqlite3.Connection, table: str, host: str) -> int:
    """Copy one table source→target, preserving ids. Only columns present in BOTH schemas are
    copied (so an older source missing a v3 column, or vice versa, is tolerated). Returns rowcount."""
    if not _sqlite_has_table(src, table):
        return 0
    target_cols = fleet._table_columns(con, table)
    cols = [c for c in _sqlite_columns(src, table) if c in target_cols]
    if not cols:
        return 0
    # Ensure host is written for host-scoped tables even when the source predates the multihost
    # migration and has no host column of its own.
    host_backfill = table in HOST_TABLES and "host" in target_cols
    if host_backfill and "host" not in cols:
        cols.append("host")
    insert = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})"
    n = 0
    for row in src.execute(f"SELECT * FROM {table}").fetchall():
        keys = set(row.keys())
        vals = []
        for c in cols:
            if c == "host" and host_backfill:
                cur = row["host"] if "host" in keys else None
                vals.append(cur if cur not in (None, "") else host)  # keep an existing host
            else:
                vals.append(row[c])
        con.execute(insert, vals)
        n += 1
    return n


def _advance_sequences(fleet, con) -> None:
    """Postgres only: push each identity sequence past the largest imported id. setval's third arg
    (is_called) is false when the table is empty, so nextval then returns 1 rather than 2."""
    if not fleet.IS_PG:
        return
    for tbl in IDENTITY_TABLES:
        con.execute(
            f"SELECT setval(pg_get_serial_sequence('{tbl}','id'), "
            f"GREATEST((SELECT COALESCE(MAX(id),0) FROM {tbl}), 1), "
            f"(SELECT MAX(id) FROM {tbl}) IS NOT NULL)")


def _prior_import(con):
    return con.execute(
        "SELECT ts, detail FROM events WHERE actor=? AND action=? ORDER BY id LIMIT 1",
        (IMPORT_ACTOR, IMPORT_ACTION)).fetchone()


def _nonempty_tables(con) -> list[tuple[str, int]]:
    out = []
    for t in IMPORT_TABLES:
        n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        if n:
            out.append((t, n))
    return out


def run_import(source: str, host: str, target: str) -> int:
    src_path = Path(source)
    if not src_path.is_file():
        print(f"ERROR: --source not found: {source}", file=sys.stderr)
        return 2

    os.environ["FLEET_DB"] = target
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    import fleet  # noqa: E402  (must follow the FLEET_DB assignment above)

    con = fleet.connect()  # runs migrations → target has the full, current schema
    try:
        prior = _prior_import(con)
        if prior:
            print(f"REFUSED: target already imported at {prior['ts']} "
                  f"({prior['detail']}). Re-import into a shared DB would collide ids.",
                  file=sys.stderr)
            return 3
        nonempty = _nonempty_tables(con)
        if nonempty:
            desc = ", ".join(f"{t}={n}" for t, n in nonempty)
            print(f"REFUSED: target is not a fresh fleet database ({desc}). Import must run "
                  f"BEFORE any rows are written, or ids will collide.", file=sys.stderr)
            return 3

        # Read-only, so we can never mutate the source; uri= lets us pass mode=ro.
        src = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)
        src.row_factory = sqlite3.Row
        try:
            counts: dict[str, int] = {}
            with fleet.write_txn(con):
                for t in IMPORT_TABLES:
                    counts[t] = _copy_table(fleet, con, src, t, host)
                _advance_sequences(fleet, con)
                detail = json.dumps({"source": src_path.name, "host": host, "rows": counts},
                                    sort_keys=True)
                con.execute(
                    "INSERT INTO events (actor,commander,action,subject,detail,ts) "
                    "VALUES (?,?,?,?,?,?)",
                    (IMPORT_ACTOR, fleet._this_commander(), IMPORT_ACTION,
                     str(src_path), detail, fleet.now_iso()))
        finally:
            src.close()
    finally:
        con.close()

    total = sum(counts.values())
    print(f"Imported {total} rows from {src_path.name} into {target} (host={host}).")
    for t in IMPORT_TABLES:
        if counts[t]:
            print(f"  {t}: {counts[t]}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="import_sqlite.py",
        description="One-shot import of a single-host fleet.db (SQLite) into the shared Postgres, "
                    "preserving ids and history. The target MUST be a FRESH fleet database — run "
                    "this BEFORE any new rows are written to it, or copied ids will collide.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Checkpoint the source first: sqlite3 fleet.db 'PRAGMA wal_checkpoint(TRUNCATE);' "
               "then import a COPY of the file, never the live db.")
    ap.add_argument("--source", required=True,
                    help="path to a WAL-checkpointed COPY of the fleet.db to import")
    ap.add_argument("--host", required=True,
                    help="machine identity to backfill into host-scoped rows (workstreams/runs/leases)")
    ap.add_argument("--target", default=os.environ.get("FLEET_DB"),
                    help="destination DSN (default: $FLEET_DB); the shared postgres://… backend")
    a = ap.parse_args(argv)
    if not a.target:
        print("ERROR: no target — pass --target or set FLEET_DB", file=sys.stderr)
        return 2
    return run_import(a.source, a.host, a.target)


if __name__ == "__main__":
    sys.exit(main())
