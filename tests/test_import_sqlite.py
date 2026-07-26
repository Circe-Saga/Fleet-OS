"""Tests for tools/import_sqlite.py — the one-shot SQLite→shared-Postgres importer.

Reuses FleetCase from the main suite, so every test runs against SQLite (default) and against a
local Postgres when FLEET_PG_TEST is set. The *target* is FleetCase's per-test DB (self.env's
FLEET_DB, reset clean each test); the *source* is a separate SQLite fleet.db populated via the real
CLI so its rows carry genuine ids and history.
"""
from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

from test_fleet import FleetCase, FLEET, PY

IMPORTER = FLEET.parent / "tools" / "import_sqlite.py"


class TestImportSqlite(FleetCase):
    def _make_source(self) -> Path:
        """A standalone SQLite fleet.db with realistic ids/history: two tasks, a workstream, a
        message (all via the CLI), plus a run and a lease raw-inserted with a NULL host to prove
        the --host backfill. WAL-checkpointed at the end, as the tool's usage requires."""
        src = Path(self.tmp.name) / "source-fleet.db"
        senv = {"FLEET_DB": str(src)}
        self.fleet("task", "add", "--objective", "alpha", "--kind", "implement",
                   "--done", "x", env_extra=senv)                                     # task id 1
        self.fleet("task", "add", "--objective", "bravo", "--kind", "review",
                   "--done", "y", env_extra=senv)                                     # task id 2
        self.fleet("set", "--slug", "lane-a", env_extra=senv)                         # host NULL
        self.fleet("msg", "--from", "lane-a", "--to", "all", "--tier", "FYI",
                   "--body", "hello", env_extra=senv)                                 # message id 1
        con = sqlite3.connect(src)
        con.execute("INSERT INTO runs (agent_slug,product,status,started_ts) VALUES (?,?,?,?)",
                    ("lane-a", "claude", "finished", "2026-01-01T00:00:00+00:00"))
        con.execute("INSERT INTO leases (resource,holder_slug,acquired_ts,ttl_seconds,released_ts) "
                    "VALUES (?,?,?,?,?)",
                    ("ios-sim", "lane-a", "2026-01-01T00:00:00+00:00", 3600,
                     "2026-01-01T01:00:00+00:00"))
        con.commit()
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        con.close()
        return src

    def _import(self, src: Path, host: str = "machineX"):
        return subprocess.run(
            [PY, str(IMPORTER), "--source", str(src), "--host", host,
             "--target", self.env["FLEET_DB"]],
            capture_output=True, text=True, env=self.env, timeout=120)

    def test_import_preserves_ids_backfills_host_and_advances_sequences(self):
        src = self._make_source()
        r = self._import(src)
        self.assertEqual(r.returncode, 0, f"stdout:{r.stdout}\nstderr:{r.stderr}")

        # ids preserved verbatim
        tasks = self.q("SELECT id, objective FROM tasks ORDER BY id")
        self.assertEqual([(t["id"], t["objective"]) for t in tasks], [(1, "alpha"), (2, "bravo")])
        self.assertEqual(self.q("SELECT COUNT(*) AS c FROM messages")[0]["c"], 1)

        # host backfilled from --host on every host-scoped table (source rows were NULL)
        self.assertEqual(self.q("SELECT host FROM workstreams WHERE slug='lane-a'")[0]["host"],
                         "machineX")
        self.assertEqual(self.q("SELECT host FROM runs")[0]["host"], "machineX")
        self.assertEqual(self.q("SELECT host FROM leases")[0]["host"], "machineX")

        # sequences advanced past max id: the next organic task is #3, not a colliding #1
        r2 = self.fleet("task", "add", "--objective", "charlie", "--kind", "doc", "--done", "z")
        new_id = int(r2.stdout.split("#")[1].split(" ")[0])
        self.assertEqual(new_id, 3)

    def test_reimport_is_refused(self):
        src = self._make_source()
        self.assertEqual(self._import(src).returncode, 0)
        r = self._import(src)
        self.assertEqual(r.returncode, 3)
        self.assertIn("already imported", r.stderr)
        # the second run imported nothing — still exactly the two original tasks
        self.assertEqual(self.q("SELECT COUNT(*) AS c FROM tasks")[0]["c"], 2)

    def test_nonfresh_target_is_refused(self):
        # organic rows (not a prior import marker) must also block the import — ids would collide
        self.add_task(objective="pre-existing on the target")
        src = self._make_source()
        r = self._import(src)
        self.assertEqual(r.returncode, 3)
        self.assertIn("not a fresh", r.stderr)

    def test_missing_source_errors(self):
        r = self._import(Path(self.tmp.name) / "nope.db")
        self.assertEqual(r.returncode, 2)
        self.assertIn("not found", r.stderr)


if __name__ == "__main__":
    import unittest
    unittest.main()
