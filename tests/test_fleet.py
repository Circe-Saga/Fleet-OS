"""Fleet-OS test suite — drives the real CLI as subprocesses against a tmp DB.

Covers the acceptance criteria in briefs F0–F5 (schema idempotency + v1 upgrade, gated task
transitions, supersedes, decision expiry, digest, concurrency, spawn/harvest with a fake
launcher, dry-run, hooks fail-open/block-once, waiting-queue rendering, review policy).
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def _iso(offset_seconds: int = 0) -> str:
    return (datetime.now(timezone.utc)
            + timedelta(seconds=offset_seconds)).isoformat(timespec="seconds")

HERE = Path(__file__).resolve().parent
FLEET = HERE.parent / "fleet.py"
HOOKS = HERE.parent / "hooks"
PY = sys.executable

# Dual-backend: the suite runs against SQLite (default) OR a local Postgres when FLEET_PG_TEST is
# set to a postgres:// DSN. Under Postgres each test gets a clean database by resetting the public
# schema in setUp; the raw-SQL test helpers (q/raw_exec) route to psycopg with `?`→`%s`.
PG_DSN = os.environ.get("FLEET_PG_TEST") or None
USE_PG = bool(PG_DSN)
if USE_PG:
    import psycopg
    from psycopg.rows import dict_row


def _pg(sql: str) -> str:
    """Translate the SQLite placeholders the tests use (`?` only) for psycopg."""
    return sql.replace("%", "%%").replace("?", "%s")


class FleetCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.db = root / "fleet.db"
        self.workspace = root / "workspace"
        self.workspace.mkdir()
        self.runs = root / "runs"
        self.waiting = root / "WAITING_ON_YOU.md"
        if USE_PG:
            self._pg_reset()
        db_setting = PG_DSN if USE_PG else str(self.db)
        # Hermetic env: drop any ambient FLEET_* (a fleet worker running the suite exports
        # FLEET_SLUG/FLEET_TASK/FLEET_DB, which would otherwise leak into the CLI + hook
        # subprocesses). Everything the suite needs is set explicitly below.
        base_env = {k: v for k, v in os.environ.items() if not k.startswith("FLEET_")}
        self.env = {
            **base_env,
            "FLEET_DB": db_setting,
            "FLEET_WORKSPACE": str(self.workspace),
            "FLEET_RUNS_DIR": str(self.runs),
            "FLEET_WORKTREES": str(self.workspace / ".worktrees"),
            "FLEET_WAITING_OUT": str(self.waiting),
            "FLEET_PROD_REPOS": "acme-api,acme-editor",
            "FLEET_LAUNCHER_CLAUDE": f"{PY} {HERE / 'fake_claude.py'}",
        }

    def tearDown(self):
        self.tmp.cleanup()

    def fleet(self, *args, expect=0, env_extra=None):
        env = {**self.env, **(env_extra or {})}
        r = subprocess.run([PY, str(FLEET), *args], capture_output=True, text=True, env=env,
                           timeout=120)
        if expect is not None:
            self.assertEqual(r.returncode, expect,
                             f"fleet {' '.join(args)}\nstdout: {r.stdout}\nstderr: {r.stderr}")
        return r

    def _pg_reset(self):
        """Give each test a clean database by dropping and recreating the public schema."""
        with psycopg.connect(PG_DSN, autocommit=True) as con:
            con.execute("DROP SCHEMA IF EXISTS public CASCADE")
            con.execute("CREATE SCHEMA public")

    def q(self, sql, params=()):
        if USE_PG:
            with psycopg.connect(PG_DSN, autocommit=True, row_factory=dict_row) as con:
                return [dict(r) for r in con.execute(_pg(sql), params).fetchall()]
        con = sqlite3.connect(self.db)
        con.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in con.execute(sql, params).fetchall()]
        finally:
            con.close()

    def raw_exec(self, sql, params=()):
        """Write directly to the DB (bypassing the CLI) — for setting up fixtures like a stale
        'running' run row. Routes to whichever backend is under test."""
        if USE_PG:
            with psycopg.connect(PG_DSN, autocommit=True) as con:
                con.execute(_pg(sql), params)
            return
        con = sqlite3.connect(self.db)
        try:
            con.execute(sql, params)
            con.commit()
        finally:
            con.close()   # close even when the statement raises (e.g. a unique-violation probe),
            #               else the lingering connection holds the DB lock for the next subprocess

    def add_task(self, objective="do the thing", kind="implement", **kw):
        args = ["task", "add", "--objective", objective, "--kind", kind,
                "--done", kw.pop("done", "tests green")]
        for k, v in kw.items():
            args += [f"--{k.replace('_', '-')}", str(v)]
        r = self.fleet(*args)
        return int(r.stdout.split("#")[1].split(" ")[0])

    def register_worktree(self, slug):
        """Real dir + agent row: verify refuses to run anywhere but a registered worktree."""
        wt = self.workspace / ".worktrees" / f"{slug}--wt"
        wt.mkdir(parents=True, exist_ok=True)
        self.fleet("set", "--slug", slug, "--worktree", str(wt))
        return wt


class TestSchema(FleetCase):
    def test_idempotent_connect(self):
        self.fleet("status")
        self.fleet("status")  # second connect re-runs SCHEMA + migrations without error

    @unittest.skipIf(USE_PG, "legacy fleet.db in-place upgrade is a SQLite-only adoption path")
    def test_v1_db_upgrades_in_place(self):
        con = sqlite3.connect(self.db)  # minimal v1-shaped tables (no v2 columns)
        con.executescript("""
          CREATE TABLE workstreams (slug TEXT PRIMARY KEY, owner TEXT, layer TEXT,
            state TEXT NOT NULL DEFAULT 'ACTIVE', status_line TEXT, next TEXT, blockers TEXT,
            branch TEXT, kestral_id TEXT, updated_ts TEXT NOT NULL);
          CREATE TABLE decisions (id INTEGER PRIMARY KEY AUTOINCREMENT, stream TEXT,
            question TEXT NOT NULL, detail TEXT, raised_ts TEXT NOT NULL, resolved_ts TEXT,
            resolution TEXT, kestral_id TEXT);
          CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, from_stream TEXT NOT NULL,
            to_addr TEXT NOT NULL, tier TEXT NOT NULL, body TEXT NOT NULL, ref TEXT,
            ts TEXT NOT NULL, resolved_ts TEXT);
        """)
        con.execute("INSERT INTO workstreams (slug, updated_ts) VALUES ('legacy', '2026-01-01')")
        con.commit()
        con.close()
        self.fleet("status")  # triggers migrations
        cols = {r["name"] for r in self.q("SELECT name FROM pragma_table_info('decisions')")}
        self.assertIn("default_recommendation", cols)
        self.assertIn("expires_ts", cols)
        cols = {r["name"] for r in self.q("SELECT name FROM pragma_table_info('workstreams')")}
        self.assertIn("worktree", cols)
        self.assertEqual(self.q("SELECT slug FROM workstreams")[0]["slug"], "legacy")


class TestTaskGates(FleetCase):
    def test_worker_cannot_dispose(self):
        tid = self.add_task()
        r = self.fleet("task", "set-state", "--id", str(tid), "--state", "DONE",
                       "--actor", "lane-worker", expect=2)
        self.assertIn("workers propose", r.stderr)

    def test_conductor_needs_verified_evidence(self):
        tid = self.add_task(owner="lane-w")
        self.register_worktree("lane-w")
        self.fleet("task", "set-state", "--id", str(tid), "--state", "IN_PROGRESS",
                   "--actor", "conductor")
        r = self.fleet("task", "set-state", "--id", str(tid), "--state", "IN_REVIEW",
                       "--actor", "conductor", expect=2)
        self.assertIn("evidence not verified", r.stderr)
        self.fleet("task", "evidence", "--id", str(tid), "--test-cmd", "true")
        self.fleet("verify", "--task", str(tid))                       # exit 0 → verified
        self.fleet("task", "set-state", "--id", str(tid), "--state", "IN_REVIEW",
                   "--actor", "conductor")                             # now granted

    def test_failed_verify_bounces(self):
        tid = self.add_task(owner="lane-w")
        self.register_worktree("lane-w")
        self.fleet("task", "evidence", "--id", str(tid), "--test-cmd", "false")
        r = self.fleet("verify", "--task", str(tid), expect=1)
        self.assertIn("GATE BOUNCE", r.stdout)
        ev = json.loads(self.q("SELECT evidence FROM tasks WHERE id=?", (tid,))[0]["evidence"])
        self.assertNotIn("verified_by_conductor", ev)
        actions = [e["action"] for e in self.q("SELECT action FROM events")]
        self.assertIn("gate-bounce", actions)

    def test_new_claim_invalidates_verification(self):
        tid = self.add_task(owner="lane-w")
        self.register_worktree("lane-w")
        self.fleet("task", "evidence", "--id", str(tid), "--test-cmd", "true")
        self.fleet("verify", "--task", str(tid))
        self.fleet("task", "evidence", "--id", str(tid), "--pr", "#99")  # new claim
        ev = json.loads(self.q("SELECT evidence FROM tasks WHERE id=?", (tid,))[0]["evidence"])
        self.assertNotIn("verified_by_conductor", ev)

    def test_open_review_blocks_done(self):
        tid = self.add_task(owner="lane-w")
        self.register_worktree("lane-w")
        self.fleet("task", "evidence", "--id", str(tid), "--test-cmd", "true")
        self.fleet("verify", "--task", str(tid))
        rid = self.add_task(objective="review it", kind="review", depends=str(tid))
        r = self.fleet("task", "set-state", "--id", str(tid), "--state", "DONE",
                       "--actor", "conductor", expect=2)
        self.assertIn("review task", r.stderr)
        self.fleet("task", "set-state", "--id", str(rid), "--state", "PARKED",
                   "--actor", "conductor")  # ungated state; clears the blocker
        self.fleet("task", "set-state", "--id", str(tid), "--state", "DONE",
                   "--actor", "conductor")

    def test_unmet_dependency_blocks_start(self):
        t1 = self.add_task(objective="upstream")
        t2 = self.add_task(objective="downstream", depends=str(t1))
        r = self.fleet("task", "set-state", "--id", str(t2), "--state", "IN_PROGRESS",
                       "--actor", "conductor", expect=2)
        self.assertIn("unmet dependencies", r.stderr)

    def test_territory_overlap_refused(self):
        t1 = self.add_task(objective="a", repo="acme-api", territory="app/services/")
        t2 = self.add_task(objective="b", repo="acme-api",
                           territory="app/services/billing_store.py")
        self.fleet("task", "set-state", "--id", str(t1), "--state", "IN_PROGRESS",
                   "--actor", "conductor")
        r = self.fleet("task", "set-state", "--id", str(t2), "--state", "IN_PROGRESS",
                       "--actor", "conductor", expect=2)
        self.assertIn("territory overlaps", r.stderr)
        t3 = self.add_task(objective="c", repo="acme-api", territory="migrations/")
        self.fleet("task", "set-state", "--id", str(t3), "--state", "IN_PROGRESS",
                   "--actor", "conductor")  # disjoint territory in same repo is fine


class TestMessaging(FleetCase):
    def test_supersedes_removes_from_brief(self):
        self.fleet("set", "--slug", "lane-a", "--state", "ACTIVE")
        self.fleet("msg", "--from", "lane-b", "--to", "lane-a", "--tier", "BLOCKER",
                   "--kind", "handoff", "--body", "root cause is the xfade")
        self.fleet("msg", "--from", "lane-b", "--to", "lane-a", "--tier", "FYI",
                   "--kind", "retraction", "--supersedes", "1",
                   "--body", "correction: decode race, not xfade")
        out = self.fleet("brief", "--as", "lane-a").stdout
        self.assertNotIn("xfade\n", out.replace("not xfade", ""))  # superseded claim gone
        self.assertIn("decode race", out)
        self.assertIn("BLOCKED on you (0)", out)  # supersede also resolved the BLOCKER

    def test_concurrent_writes_both_land(self):
        self.fleet("status")  # init db
        errs = []
        def post(i):
            r = subprocess.run([PY, str(FLEET), "msg", "--from", f"w{i}", "--to", "all",
                                "--body", f"m{i}"], capture_output=True, text=True, env=self.env)
            if r.returncode != 0:
                errs.append(r.stderr)
        threads = [threading.Thread(target=post, args=(i,)) for i in range(6)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual(errs, [])
        self.assertEqual(len(self.q("SELECT id FROM messages")), 6)

    def test_task_addressed_msg_reaches_owner_and_blocker_persists(self):
        # msg to 'task:N' must reach N's owner even though to_addr matches neither
        # all/slug/layer — this is the addressing gap the owner-side task_addrs fix closes.
        self.fleet("set", "--slug", "lane-a", "--state", "ACTIVE")
        tid = self.add_task(owner="lane-a")
        self.fleet("msg", "--from", "conductor", "--to", f"task:{tid}", "--tier", "BLOCKER",
                   "--kind", "request", "--requires-action", "--body", "respawn: clear this first")
        out = self.fleet("brief", "--as", "lane-a").stdout
        self.assertIn("clear this first", out)
        self.assertIn("BLOCKED on you (1)", out)
        # BLOCKER tier is persistent: it must still surface on a second brief, unacked
        out2 = self.fleet("brief", "--as", "lane-a").stdout
        self.assertIn("clear this first", out2)
        self.assertIn("BLOCKED on you (1)", out2)
        self.fleet("ack", "--as", "lane-a", "--id", "1")
        out3 = self.fleet("brief", "--as", "lane-a").stdout
        self.assertNotIn("clear this first", out3)
        self.assertIn("BLOCKED on you (0)", out3)

    def test_gate_request_lands_in_digest(self):
        self.fleet("msg", "--from", "lane-a", "--to", "conductor", "--kind", "request",
                   "--requires-action", "--body", "task #1 ready for IN_REVIEW")
        d = json.loads(self.fleet("digest", "--json").stdout)
        self.assertEqual(len(d["gate_requests"]), 1)
        self.fleet("route", "--msg", "1", "--note", "gated")
        d = json.loads(self.fleet("digest", "--json").stdout)
        self.assertEqual(len(d["unrouted_messages"]), 0)


class TestHierarchy(FleetCase):
    """E-D: tasks.level + parent_id semantics, tree roll-ups (architecture §2.1, decision D-2)."""

    def _add(self, obj, **kw):
        kw.setdefault("kind", "doc")
        return self.add_task(objective=obj, **kw)

    def test_flat_task_default_unchanged(self):
        """A plain `task add` — no --level, no --parent — is a leaf task with no roll-up."""
        tid = self.add_task()
        self.assertEqual(self.q("SELECT level FROM tasks WHERE id=?", (tid,))[0]["level"], "task")
        d = json.loads(self.fleet("digest", "--json").stdout)
        t = next(x for x in d["open_tasks"] if x["id"] == tid)
        self.assertEqual(t["level"], "task")
        self.assertNotIn("rollup", t)

    def test_parent_must_exist(self):
        r = self.fleet("task", "add", "--objective", "x", "--kind", "doc", "--done", "d",
                       "--level", "story", "--parent", "9999", expect=2)
        self.assertIn("no such task", r.stderr)

    def test_level_ordering_enforced(self):
        init = self._add("Initiative", level="initiative")
        epic = self._add("Epic", level="epic", parent=init)  # epic ⊂ initiative — legal
        self.assertEqual(self.q("SELECT parent_id, level FROM tasks WHERE id=?", (epic,))[0]
                         ["parent_id"], init)
        leaf = self.add_task(objective="Leaf")  # a plain task
        # an epic cannot hang off a task (equal-or-deeper parent)
        r = self.fleet("task", "add", "--objective", "bad", "--kind", "doc", "--done", "d",
                       "--level", "epic", "--parent", str(leaf), expect=2)
        self.assertIn("cannot be parented", r.stderr)
        # an initiative is a root — it may not take any parent
        r = self.fleet("task", "add", "--objective", "bad2", "--kind", "doc", "--done", "d",
                       "--level", "initiative", "--parent", str(init), expect=2)
        self.assertIn("cannot be parented", r.stderr)
        # a task may skip levels and sit directly under an initiative (parent strictly shallower)
        self.fleet("task", "add", "--objective", "ok", "--kind", "doc", "--done", "d",
                   "--parent", str(init))

    def test_recursive_cte_rollup_counts_by_state(self):
        """digest --json carries a descendant state tally on every non-task row, three levels deep
        (initiative→epic→story→task), exercising the recursive-CTE walk past a single hop."""
        init = self._add("Init", level="initiative")
        epic = self._add("Epic", level="epic", parent=init)
        story = self._add("Story", level="story", parent=epic)
        t1 = self.add_task(objective="t1", parent=story)
        t2 = self.add_task(objective="t2", parent=story)
        t3 = self.add_task(objective="t3", parent=story)
        # states set directly (a fixture) — the roll-up only reads state, not the gate machinery
        self.raw_exec("UPDATE tasks SET state=? WHERE id=?", ("IN_PROGRESS", t1))
        self.raw_exec("UPDATE tasks SET state=? WHERE id=?", ("DONE", t2))
        # t3 stays QUEUED
        d = json.loads(self.fleet("digest", "--json").stdout)
        tasks = {t["id"]: t for t in d["open_tasks"]}
        # story's direct children: one of each state
        self.assertEqual(tasks[story]["rollup"], {"IN_PROGRESS": 1, "DONE": 1, "QUEUED": 1})
        # epic rolls up story (QUEUED) + the three grandchildren
        self.assertEqual(tasks[epic]["rollup"],
                         {"QUEUED": 2, "IN_PROGRESS": 1, "DONE": 1})
        # initiative rolls up the whole subtree: epic + story + three tasks
        self.assertEqual(tasks[init]["rollup"],
                         {"QUEUED": 3, "IN_PROGRESS": 1, "DONE": 1})
        # a DONE descendant is counted even though it's filtered out of the open-task list
        self.assertNotIn(t2, tasks)
        # leaf rows carry no roll-up
        self.assertNotIn("rollup", tasks[t1])

    def test_task_list_renders_indented_tree(self):
        init = self._add("Init", level="initiative")
        epic = self._add("Epic", level="epic", parent=init)
        leaf = self.add_task(objective="Leaf", parent=epic)
        lines = [ln for ln in self.fleet("task", "list").stdout.splitlines() if ln.strip()]
        idx = {tid: next(i for i, ln in enumerate(lines) if f"#{tid:>3}" in ln)
               for tid in (init, epic, leaf)}
        # parent precedes child, and each rung is indented deeper than the last
        self.assertLess(idx[init], idx[epic])
        self.assertLess(idx[epic], idx[leaf])
        lead = lambda i: len(lines[i]) - len(lines[i].lstrip())
        self.assertLess(lead(idx[init]), lead(idx[epic]))
        self.assertLess(lead(idx[epic]), lead(idx[leaf]))
        # non-task rows show a roll-up tag; the initiative's lone descendant chain is 1 epic
        self.assertIn("[initiative ·", lines[idx[init]])
        self.assertIn("[epic ·", lines[idx[epic]])


class TestDecisions(FleetCase):
    def test_reversible_requires_default(self):
        r = self.fleet("decide", "--question", "flip it?", "--reversible", expect=2)
        self.assertIn("--default", r.stderr)

    def test_expires_requires_reversible(self):
        r = self.fleet("decide", "--question", "flip it?", "--default", "yes",
                       "--expires", "24", expect=2)
        self.assertIn("--reversible", r.stderr)

    def test_expired_reversible_auto_proceeds(self):
        self.fleet("decide", "--stream", "lane-a", "--question", "bump N to 5?",
                   "--default", "yes, cost is capped", "--reversible", "--expires", "0",
                   "--category", "other")
        self.fleet("decide", "--question", "delete prod table?",
                   "--category", "authorization")   # irreversible — must never auto-proceed
        time.sleep(1.1)
        out = self.fleet("resolve-expired").stdout
        self.assertIn("auto-proceeded #1", out)
        rows = self.q("SELECT * FROM decisions ORDER BY id")
        self.assertEqual(rows[0]["auto_resolved"], 1)
        self.assertIn("auto-proceeded on default", rows[0]["resolution"])
        self.assertIsNone(rows[1]["resolved_ts"])
        self.assertIn("auto-resolve", [e["action"] for e in self.q("SELECT action FROM events")])

    def test_resolution_reaches_next_brief(self):
        self.fleet("set", "--slug", "lane-a", "--state", "ACTIVE")
        self.fleet("brief", "--as", "lane-a")  # advance cursor first
        self.fleet("decide", "--stream", "lane-a", "--question", "which judge?")
        self.fleet("resolve", "--id", "1", "--resolution", "use the local Qwen judge")
        out = self.fleet("brief", "--as", "lane-a").stdout
        self.assertIn("ANSWERED", out)
        self.assertIn("local Qwen judge", out)
        self.assertEqual(self.q("SELECT state FROM workstreams WHERE slug='lane-a'")[0]["state"],
                         "ACTIVE")  # released from NEEDS-USER


class TestRunner(FleetCase):
    def scratch_repo(self, name="acme-scratch"):
        repo = self.workspace / name
        repo.mkdir()
        for cmd in (["git", "init", "-b", "main"],
                    ["git", "config", "user.email", "t@t"], ["git", "config", "user.name", "t"]):
            subprocess.run(cmd, cwd=repo, capture_output=True, check=True)
        (repo / "README.md").write_text("scratch\n")
        subprocess.run(["git", "add", "-A"], cwd=repo, capture_output=True, check=True)
        subprocess.run(["git", "commit", "-m", "init"], cwd=repo, capture_output=True, check=True)
        return name

    def test_dry_run_touches_nothing(self):
        repo = self.scratch_repo()
        tid = self.add_task(repo=repo)
        out = self.fleet("spawn", "--task", str(tid), "--agent", "lane-x", "--dry-run").stdout
        self.assertIn("DRY RUN", out)
        self.assertFalse((self.workspace / ".worktrees").exists())
        self.assertEqual(self.q("SELECT id FROM runs"), [])

    def test_spawn_and_harvest(self):
        repo = self.scratch_repo()
        tid = self.add_task(repo=repo, budget="2")
        self.fleet("spawn", "--task", str(tid), "--agent", "lane-x", "--model", "sonnet")
        wt = self.workspace / ".worktrees" / f"lane-x--{repo}"
        self.assertTrue(wt.is_dir(), "worktree created")
        self.assertTrue((wt / ".claude" / "settings.json").exists(), "hooks stamped")
        head = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=wt,
                              capture_output=True, text=True).stdout.strip()
        self.assertEqual(head, f"agent/lane-x/t{tid}")
        deadline = time.time() + 15   # fake launcher exits ~instantly; poll harvest
        while time.time() < deadline:
            out = self.fleet("harvest").stdout
            if "finished" in out:
                break
            time.sleep(0.3)
        self.assertIn("finished", out)
        run = self.q("SELECT * FROM runs")[0]
        self.assertEqual(run["status"], "finished")
        self.assertAlmostEqual(run["cost_usd"], 1.25)
        self.assertEqual(self.q("SELECT session_id FROM workstreams WHERE slug='lane-x'")[0]
                         ["session_id"], "sess-fake-0001")
        self.assertAlmostEqual(self.q("SELECT spent_usd FROM tasks WHERE id=?", (tid,))[0]
                               ["spent_usd"], 1.25)
        out2 = self.fleet("harvest").stdout    # idempotent: nothing left to ingest
        self.assertIn("(no running sessions)", out2)

    def test_spawn_persists_worktree_on_agent_row(self):
        repo = self.scratch_repo()
        tid = self.add_task(repo=repo)
        self.fleet("spawn", "--task", str(tid), "--agent", "lane-x", "--no-launch")
        row = self.q("SELECT repo, worktree FROM workstreams WHERE slug='lane-x'")[0]
        self.assertEqual(row["worktree"],
                         str(self.workspace / ".worktrees" / f"lane-x--{repo}"))
        self.assertEqual(row["repo"], repo)

    def test_spawn_derives_repo_from_territory(self):
        # the common shape in practice: --repo empty, territory 'Repo:paths,…'
        repo = self.scratch_repo()
        tid = self.add_task(territory=f"{repo}:src/**,tests/**")
        self.fleet("spawn", "--task", str(tid), "--agent", "lane-t", "--no-launch")
        wt = self.workspace / ".worktrees" / f"lane-t--{repo}"
        self.assertTrue(wt.is_dir(), "worktree created from territory-derived repo")
        row = self.q("SELECT repo, worktree FROM workstreams WHERE slug='lane-t'")[0]
        self.assertEqual((row["repo"], row["worktree"]), (repo, str(wt)))

    def test_spawn_does_not_clobber_registered_worktree(self):
        repo = self.scratch_repo()
        pinned = self.workspace / "pinned-wt"
        pinned.mkdir()
        self.fleet("set", "--slug", "lane-r", "--worktree", str(pinned))
        tid = self.add_task(repo=repo)
        self.fleet("spawn", "--task", str(tid), "--agent", "lane-r", "--no-launch")
        row = self.q("SELECT worktree FROM workstreams WHERE slug='lane-r'")[0]
        self.assertEqual(row["worktree"], str(pinned))

    def test_spawn_warns_when_no_worktree_resolvable(self):
        tid = self.add_task(territory="docs-only-notes")   # no repo prefix anywhere
        r = self.fleet("spawn", "--task", str(tid), "--agent", "lane-n", "--no-launch")
        self.assertIn("no worktree resolvable", r.stderr)
        row = self.q("SELECT worktree FROM workstreams WHERE slug='lane-n'")[0]
        self.assertIsNone(row["worktree"])

    def test_run_refused_while_running(self):
        self.fleet("set", "--slug", "lane-x", "--state", "ACTIVE", "--session-id", "sess-1")
        self.raw_exec("INSERT INTO runs (agent_slug,product,status,pid,started_ts) "
                      "VALUES ('lane-x','claude','running',999999999,'2026-01-01')")
        r = self.fleet("run", "--agent", "lane-x", "--input", "continue", expect=2)
        self.assertIn("already running", r.stderr)

    def test_harvest_marks_dead_unparseable_failed(self):
        self.fleet("status")  # init the DB schema
        bad = self.runs / "bad.json"
        self.runs.mkdir(parents=True, exist_ok=True)
        bad.write_text("worker exploded, no json here")
        self.raw_exec("INSERT INTO runs (agent_slug,product,status,pid,result_ref,started_ts) "
                      "VALUES ('lane-x','claude','running',999999999,?, '2026-01-01')", (str(bad),))
        # pid 999999999 is dead on any sane box → parse fails → failed
        out = self.fleet("harvest").stdout
        self.assertIn("FAILED", out)
        self.assertEqual(self.q("SELECT status FROM runs")[0]["status"], "failed")


class TestHooks(FleetCase):
    def hook(self, script, payload, env_extra=None):
        env = {**self.env, **(env_extra or {})}
        return subprocess.run([PY, str(HOOKS / script)], input=json.dumps(payload),
                              capture_output=True, text=True, env=env, timeout=30)

    def worktree_with_marker(self, ts="2020-01-01T00:00:00+00:00"):
        wt = Path(self.tmp.name) / "wt"
        (wt / ".claude").mkdir(parents=True)
        (wt / ".claude" / ".fleet_session_start").write_text(ts)
        return wt

    def test_stop_guard_blocks_once_then_allows(self):
        self.fleet("set", "--slug", "lane-a", "--state", "ACTIVE")
        wt = self.worktree_with_marker()
        r = self.hook("stop_guard.py", {"cwd": str(wt)}, {"FLEET_SLUG": "lane-a"})
        self.assertEqual(r.returncode, 0)
        self.assertIn('"block"', r.stdout)
        self.assertIn("fleet.py checkpoint", r.stdout)
        r2 = self.hook("stop_guard.py", {"cwd": str(wt), "stop_hook_active": True},
                       {"FLEET_SLUG": "lane-a"})
        self.assertEqual(r2.stdout.strip(), "")  # block-once: second stop passes

    def test_stop_guard_allows_after_checkpoint(self):
        self.fleet("set", "--slug", "lane-a", "--state", "ACTIVE")
        wt = self.worktree_with_marker()
        self.fleet("checkpoint", "--as", "lane-a", "--did", "ported the fix", "--next", "PR")
        r = self.hook("stop_guard.py", {"cwd": str(wt)}, {"FLEET_SLUG": "lane-a"})
        self.assertEqual(r.stdout.strip(), "")

    def test_hooks_fail_open(self):
        wt = self.worktree_with_marker()
        r = self.hook("stop_guard.py", {"cwd": str(wt)})            # no FLEET_SLUG
        self.assertEqual((r.returncode, r.stdout.strip()), (0, ""))
        r = self.hook("stop_guard.py", {"cwd": str(wt)},
                      {"FLEET_SLUG": "x", "FLEET_DB": "/nonexistent/dir/fleet.db"})
        self.assertEqual((r.returncode, r.stdout.strip()), (0, ""))  # broken DB → fail-open

    def test_session_start_injects_brief_and_marker(self):
        self.fleet("set", "--slug", "lane-a", "--state", "ACTIVE")
        wt = Path(self.tmp.name) / "wt2"
        wt.mkdir()
        r = self.hook("session_start.py", {"cwd": str(wt)}, {"FLEET_SLUG": "lane-a"})
        self.assertEqual(r.returncode, 0)
        self.assertIn("FLEET BRIEF for lane-a", r.stdout)
        self.assertIn("YOUR ROW", r.stdout)
        self.assertTrue((wt / ".claude" / ".fleet_session_start").exists())


class TestHumanSurface(FleetCase):
    def test_waiting_render_order_and_auto_section(self):
        self.fleet("decide", "--question", "ship the font fix?", "--category", "taste",
                   "--default", "ship it")
        self.fleet("decide", "--question", "rotate the prod key?", "--category", "authorization")
        self.fleet("decide", "--question", "bump N?", "--default", "yes", "--reversible",
                   "--expires", "0", "--category", "other")
        time.sleep(1.1)
        self.fleet("resolve-expired")
        self.fleet("render-waiting")
        text = self.waiting.read_text()
        self.assertLess(text.index("[authorization]"), text.index("[taste]"))
        self.assertIn("Auto-proceeded since your last visit (1)", text)
        self.assertIn("waits for you", text)              # irreversible: never auto-proceeds
        self.fleet("render-waiting")                      # auto section shows exactly once
        self.assertNotIn("Auto-proceeded since your last visit", self.waiting.read_text())

    def test_tick_runs_end_to_end(self):
        t1 = self.add_task(objective="upstream")
        self.add_task(objective="downstream", depends=str(t1))
        out = self.fleet("tick").stdout
        self.assertIn("judgment queue", out)
        self.assertIn(f"ready QUEUED tasks [{t1}]", out)
        self.assertTrue(self.waiting.exists())


class TestRolePrompts(FleetCase):
    def role_doc(self):
        p = self.workspace / "ROLES.md"
        p.write_text("# Cabinet\n\n## COMMON CONTEXT (prepend)\nYou are a senior leader.\n\n"
                     "## 1. HEAD OF PRODUCT\nYou own what gets built.\n\n"
                     "## 2. HEAD OF GROWTH\nYou own the funnels.\n")
        return "ROLES.md"  # workspace-relative

    def test_role_rides_system_prompt_on_spawn(self):
        doc = self.role_doc()
        tid = self.add_task(kind="doc", done="memo exists")
        spec = f"{doc}#COMMON CONTEXT,{doc}#HEAD OF PRODUCT"
        self.fleet("set", "--slug", "head-of-product", "--role-prompt", spec)
        r = self.fleet("spawn", "--task", str(tid), "--agent", "head-of-product", "--dry-run")
        self.assertIn("--append-system-prompt", r.stdout)
        self.assertIn("<role prompt,", r.stdout)
        out = self.fleet("starter", "--task", str(tid), "--agent", "head-of-product").stdout
        self.assertIn("ROLE: your standing role prompt", out)

    def test_section_extraction_scopes_correctly(self):
        doc = self.role_doc()
        tid = self.add_task()
        # No masking in starter path — verify via a worker whose role is ONE section
        self.fleet("set", "--slug", "hg", "--role-prompt", f"{doc}#HEAD OF GROWTH")
        # extract via spawn (no repo → no worktree) with real launcher output file
        self.fleet("spawn", "--task", str(tid), "--agent", "hg")
        time.sleep(0.5)
        self.fleet("harvest")
        # the fake launcher received the role via --append-system-prompt; confirm from the
        # recorded spawn command is not possible, so assert on _load_role_text via a second
        # spawn --dry-run length: growth section only (short)
        r = self.fleet("spawn", "--task", str(tid), "--agent", "hg", "--dry-run")
        m = re.search(r"<role prompt, (\d+) chars>", r.stdout)
        self.assertIsNotNone(m)
        self.assertLess(int(m.group(1)), 120)  # one short section, not the whole doc

    def test_missing_role_source_fails_soft(self):
        tid = self.add_task()
        self.fleet("set", "--slug", "ghost", "--role-prompt", "nonexistent/ROLES.md#NOPE")
        # Must not crash the spawn; the role slot still carries a (short) visible missing-note
        # so the worker and Conductor see the gap.
        r = self.fleet("spawn", "--task", str(tid), "--agent", "ghost", "--dry-run")
        m = re.search(r"<role prompt, (\d+) chars>", r.stdout)
        self.assertIsNotNone(m)
        self.assertLess(int(m.group(1)), 100)  # the one-line missing-note, not real content


class TestModels(FleetCase):
    def test_alias_resolves_at_launch(self):
        tid = self.add_task()
        r = self.fleet("spawn", "--task", str(tid), "--agent", "ax", "--model", "fable",
                       "--dry-run")
        self.assertIn("claude-fable-5", r.stdout)
        r = self.fleet("spawn", "--task", str(tid), "--agent", "ax",
                       "--model", "some-future-model", "--dry-run")
        self.assertIn("some-future-model", r.stdout)  # unknown alias passes through

    def test_run_records_model_and_switch_is_logged(self):
        tid = self.add_task()
        self.fleet("spawn", "--task", str(tid), "--agent", "ax", "--model", "sonnet")
        deadline = time.time() + 15
        while time.time() < deadline:
            if "finished" in self.fleet("harvest").stdout:
                break
            time.sleep(0.3)
        self.assertEqual(self.q("SELECT model FROM runs")[0]["model"], "sonnet")
        self.fleet("run", "--agent", "ax", "--input", "continue", "--model", "opus")
        self.assertEqual(self.q("SELECT model FROM workstreams WHERE slug='ax'")[0]["model"],
                         "opus")
        self.assertEqual(self.q("SELECT model FROM runs ORDER BY id DESC")[0]["model"], "opus")
        self.assertIn("model-switch", [e["action"] for e in self.q("SELECT action FROM events")])

    def test_models_view(self):
        self.fleet("set", "--slug", "ax", "--state", "ACTIVE", "--model", "fable")
        out = self.fleet("models").stdout
        self.assertIn("claude-fable-5", out)
        self.assertIn("ACTIVE AGENTS by model", out)
        self.assertIn("fable", out)


class TestKestral(FleetCase):
    def test_pending_and_mark_roundtrip(self):
        self.fleet("set", "--slug", "lane-a", "--state", "ACTIVE")
        tid = self.add_task()
        self.fleet("decide", "--question", "ship?", "--category", "taste", "--default", "yes")
        d = json.loads(self.fleet("kestral-pending").stdout)
        self.assertEqual(len(d["agents"]), 1)
        self.assertEqual(len(d["open_tasks"]), 1)
        self.assertEqual(len(d["open_decisions"]), 1)
        self.assertIn("idempotency", d["mapping"])
        self.assertIsNone(d["open_tasks"][0]["kestral_id"])
        self.fleet("kestral-mark", "--task", str(tid), "--kestral-id", "KT-42")
        self.fleet("kestral-mark", "--workstream", "lane-a", "--kestral-id", "KW-7")
        d = json.loads(self.fleet("kestral-pending").stdout)
        self.assertEqual(d["open_tasks"][0]["kestral_id"], "KT-42")
        self.assertEqual(d["agents"][0]["kestral_id"], "KW-7")
        r = self.fleet("kestral-mark", "--task", "999", "--kestral-id", "X", expect=2)
        self.assertIn("no tasks row", r.stderr)

    def test_mark_requires_exactly_one_target(self):
        self.fleet("status")
        r = self.fleet("kestral-mark", "--kestral-id", "X", expect=2)
        self.assertIn("exactly one", r.stderr)


class TestVerifyRace(FleetCase):
    def test_reclaim_mid_verify_discards_result(self):
        tid = self.add_task(owner="lane-w")
        self.register_worktree("lane-w")
        # claim a slow command that re-claims evidence WHILE verify is running it
        reclaim = (f"{PY} {FLEET} task evidence --id {tid} --test-cmd true >/dev/null 2>&1")
        self.fleet("task", "evidence", "--id", str(tid), "--test-cmd", f"sh -c '{reclaim}'")
        r = self.fleet("verify", "--task", str(tid), expect=1)
        self.assertIn("re-claimed while verify ran", r.stderr)
        ev = json.loads(self.q("SELECT evidence FROM tasks WHERE id=?", (tid,))[0]["evidence"])
        self.assertNotIn("verified_by_conductor", ev)  # stale claim never got stamped
        self.assertIn("gate-stale", [e["action"] for e in self.q("SELECT action FROM events")])


class TestVerifyWorktreeGuard(FleetCase):
    """verify runs in the OWNER's registered worktree, full stop — config gaps exit 2 with a
    remedy instead of silently running from the fleet dir and bouncing valid gates (task #50)."""

    def test_verify_refuses_empty_worktree(self):
        tid = self.add_task(owner="lane-w")
        self.fleet("set", "--slug", "lane-w", "--state", "ACTIVE")   # row exists, worktree empty
        self.fleet("task", "evidence", "--id", str(tid), "--test-cmd", "true")
        r = self.fleet("verify", "--task", str(tid), expect=2)
        self.assertIn("no worktree on record", r.stderr)
        self.assertIn("fleet set --slug lane-w --worktree", r.stderr)   # remedy is actionable
        ev = json.loads(self.q("SELECT evidence FROM tasks WHERE id=?", (tid,))[0]["evidence"])
        self.assertNotIn("verified_by_conductor", ev)
        self.assertNotIn("gate-bounce",           # config error, not a bounce
                         [e["action"] for e in self.q("SELECT action FROM events")])

    def test_verify_refuses_missing_worktree_dir(self):
        tid = self.add_task(owner="lane-w")
        gone = self.workspace / ".worktrees" / "lane-w--gone"        # registered, never created
        self.fleet("set", "--slug", "lane-w", "--worktree", str(gone))
        self.fleet("task", "evidence", "--id", str(tid), "--test-cmd", "true")
        r = self.fleet("verify", "--task", str(tid), expect=2)
        self.assertIn("missing on disk", r.stderr)

    def test_verify_refuses_ownerless_task(self):
        tid = self.add_task()
        self.fleet("task", "evidence", "--id", str(tid), "--test-cmd", "true")
        r = self.fleet("verify", "--task", str(tid), expect=2)
        self.assertIn("OWNER's worktree", r.stderr)


class TestReviewPolicy(FleetCase):
    def test_triggers(self):
        t_prod = self.add_task(repo="acme-api")
        t_lab = self.add_task(repo="acme-evallab", territory="evals/scratch/")
        d = json.loads(self.fleet("review-policy", "--task", str(t_prod)).stdout)
        self.assertTrue(d["tier1_required"])
        d = json.loads(self.fleet("review-policy", "--task", str(t_lab)).stdout)
        self.assertFalse(d["tier1_required"])
        d = json.loads(self.fleet("review-policy", "--task", str(t_lab),
                                  "--diff-lines", "500").stdout)
        self.assertTrue(d["tier1_required"])
        self.assertIn("large-diff:500", d["triggers"])
        self.fleet("task", "evidence", "--id", str(t_lab), "--review-requested")
        d = json.loads(self.fleet("review-policy", "--task", str(t_lab)).stdout)
        self.assertTrue(d["tier1_required"])


class TestLeases(FleetCase):
    def _insert_run(self, slug, pid, status="running"):
        self.fleet("status")   # ensure the schema (incl. runs) exists before the raw INSERT
        self.raw_exec("INSERT INTO runs (agent_slug,product,status,pid,started_ts) "
                      "VALUES (?,'claude',?,?,'2026-01-01')", (slug, status, pid))

    def test_acquire_then_status_shows_holder(self):
        self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-a")
        out = self.fleet("lease", "status", "--resource", "ios-sim").stdout
        self.assertIn("held by lane-a", out)

    def test_acquire_refused_while_live_holder_holds(self):
        self._insert_run("lane-a", os.getpid())   # this test process: a genuinely live pid
        self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-a")
        r = self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-b", expect=2)
        self.assertIn("held by lane-a", r.stderr)
        self.assertIn("Conductor owns visual QA", r.stderr)
        self.assertEqual(self.q("SELECT holder_slug FROM leases WHERE released_ts IS NULL")[0]
                         ["holder_slug"], "lane-a")

    def test_acquire_reaps_stale_holder_whose_run_is_dead(self):
        self._insert_run("lane-a", 999999999)   # dead pid → holder's run is not alive
        self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-a")
        r = self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-b")
        self.assertIn("reaped stale lease", r.stderr)
        rows = self.q("SELECT holder_slug, released_ts FROM leases ORDER BY id")
        self.assertIsNotNone(rows[0]["released_ts"])
        self.assertEqual(rows[1]["holder_slug"], "lane-b")

    def test_refresh_is_idempotent_for_the_same_holder(self):
        self._insert_run("lane-a", os.getpid())
        self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-a")
        r = self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-a")
        self.assertIn("refreshed", r.stdout)
        self.assertEqual(len(self.q("SELECT id FROM leases")), 1)

    def test_release_by_non_holder_refused(self):
        self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-a")
        r = self.fleet("lease", "release", "--resource", "ios-sim", "--as", "lane-b", expect=2)
        self.assertIn("held by lane-a", r.stderr)

    def test_release_frees_the_resource(self):
        self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-a")
        self.fleet("lease", "release", "--resource", "ios-sim", "--as", "lane-a")
        out = self.fleet("lease", "status", "--resource", "ios-sim").stdout
        self.assertIn("free", out)
        self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-b")  # now unblocked

    def test_device_pin_refuses_mismatch_and_allows_match(self):
        env = {"FLEET_LEASE_DEVICES": "ios-sim=iphone-sim-a"}
        r = self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-a",
                       "--device", "some-other-device", expect=2, env_extra=env)
        self.assertIn("pinned to device", r.stderr)
        self.assertEqual(self.q("SELECT id FROM leases"), [])
        r = self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-a",
                       "--device", "iphone-sim-a", env_extra=env)
        self.assertIn("iphone-sim-a", r.stdout)
        # omitting --device falls back to the pin, not an unconstrained lease
        self.fleet("lease", "release", "--resource", "ios-sim", "--as", "lane-a", env_extra=env)
        self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-b", env_extra=env)
        self.assertEqual(self.q("SELECT device FROM leases ORDER BY id DESC LIMIT 1")[0]
                         ["device"], "iphone-sim-a")

    def test_tick_reaps_stale_lease(self):
        self._insert_run("lane-a", 999999999)
        self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-a")
        out = self.fleet("tick").stdout
        self.assertIn("reaped ios-sim", out)
        self.assertIsNotNone(self.q("SELECT released_ts FROM leases")[0]["released_ts"])
        out2 = self.fleet("lease", "status", "--resource", "ios-sim").stdout
        self.assertIn("free", out2)

    def test_status_json(self):
        self._insert_run("lane-a", os.getpid())
        self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-a")
        d = json.loads(self.fleet("lease", "status", "--resource", "ios-sim", "--json").stdout)
        self.assertEqual(d, [{"resource": "ios-sim", "held": True, "holder": "lane-a",
                              "device": None, "acquired_ts": d[0]["acquired_ts"],
                              "stale": False}])

    def test_status_with_no_leases_ever(self):
        out = self.fleet("lease", "status").stdout
        self.assertIn("no leases ever recorded", out)


class TestMultiHost(FleetCase):
    """E-A2: two simulated hosts (FLEET_HOST) against one shared DB. host B's harvest/reap must
    never touch host A's live runs or leases; the lease mutex is DB-enforced; verify defers on a
    remote worktree; resume is host-pinned. (Under SQLite one shared file IS the shared DB.)"""

    def _run(self, slug, host, pid, heartbeat, status="running", result_ref=None):
        self.fleet("status")  # ensure schema (runs.host/heartbeat_ts) exists before the raw INSERT
        self.raw_exec(
            "INSERT INTO runs (agent_slug,product,status,pid,result_ref,host,heartbeat_ts,"
            "started_ts) VALUES (?,'claude',?,?,?,?,?,?)",
            (slug, status, pid, result_ref, host, heartbeat, _iso()))

    def test_harvest_is_host_scoped(self):
        # host A has a live run; host B has a dead run. host B's harvest touches ONLY its own.
        self._run("lane-a", "hostA", 999999998, _iso())
        self._run("lane-b", "hostB", 999999999, _iso())   # dead pid, no result → will fail-harvest
        self.fleet("harvest", env_extra={"FLEET_HOST": "hostB"})
        st = {r["agent_slug"]: r["status"] for r in self.q("SELECT agent_slug, status FROM runs")}
        self.assertEqual(st["lane-a"], "running")   # host A's live run is invisible to host B
        self.assertEqual(st["lane-b"], "failed")    # host B harvested its own dead run

    def test_reap_spares_live_remote_lease_then_reaps_dead_one(self):
        # host A holds ios-sim, guarded by a run on host A with a FRESH heartbeat.
        self._run("lane-a", "hostA", 999999999, _iso())
        self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-a",
                   env_extra={"FLEET_HOST": "hostA"})
        # host B's tick reaper judges the remote holder by heartbeat, not pid → fresh → SPARED.
        self.fleet("tick", env_extra={"FLEET_HOST": "hostB"})
        self.assertIsNone(self.q("SELECT released_ts FROM leases")[0]["released_ts"])
        # heartbeat lapses past the TTL → now host B may reap the abandoned lease.
        self.raw_exec("UPDATE runs SET heartbeat_ts=? WHERE agent_slug='lane-a'", (_iso(-100000),))
        out = self.fleet("tick", env_extra={"FLEET_HOST": "hostB"}).stdout
        self.assertIn("reaped ios-sim", out)
        self.assertIsNotNone(self.q("SELECT released_ts FROM leases")[0]["released_ts"])

    def test_lease_unique_index_rejects_second_active_row(self):
        self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-a")
        # The partial unique index (one unreleased row per resource) is the mutex, enforced by the
        # DB itself — a second active row for the same resource is impossible, backend regardless.
        err = psycopg.errors.UniqueViolation if USE_PG else sqlite3.IntegrityError
        with self.assertRaises(err):
            self.raw_exec("INSERT INTO leases (resource,holder_slug,acquired_ts,ttl_seconds) "
                          "VALUES ('ios-sim','lane-b',?,1800)", (_iso(),))
        # released rows do NOT count — the audit trail (many rows per resource) still works.
        self.fleet("lease", "release", "--resource", "ios-sim", "--as", "lane-a")
        self.raw_exec("INSERT INTO leases (resource,holder_slug,acquired_ts,ttl_seconds) "
                      "VALUES ('ios-sim','lane-b',?,1800)", (_iso(),))

    def test_concurrent_acquire_exactly_one_wins(self):
        self.fleet("status")
        # Give every candidate a locally-live run (host NULL → judged by pid = this process, which
        # IS alive), so no acquire can reap another as "stale" — the only way to win is the mutex.
        for i in range(6):
            self.raw_exec("INSERT INTO runs (agent_slug,product,status,pid,started_ts) "
                          "VALUES (?, 'claude','running',?, ?)", (f"lane-{i}", os.getpid(), _iso()))
        codes = []
        lock = threading.Lock()

        def acq(i):
            r = self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", f"lane-{i}",
                           expect=None)
            with lock:
                codes.append(r.returncode)

        threads = [threading.Thread(target=acq, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(codes.count(0), 1, f"exactly one acquire must win; got {codes}")
        self.assertEqual(len(self.q("SELECT id FROM leases WHERE released_ts IS NULL")), 1)

    def test_concurrent_set_state_guard_runs_inside_txn(self):
        # Two overlapping-territory tasks raced to IN_PROGRESS. The territory guard now reads INSIDE
        # the write transaction, so the two starts serialize and exactly one wins — with the guard
        # outside the txn (the old shape) both would read "no overlap" and both would start.
        t1 = self.add_task(objective="a", repo="acme-api", territory="app/x/")
        t2 = self.add_task(objective="b", repo="acme-api", territory="app/x/")
        codes = []
        lock = threading.Lock()

        def start(tid):
            r = self.fleet("task", "set-state", "--id", str(tid), "--state", "IN_PROGRESS",
                           "--actor", "conductor", expect=None)
            with lock:
                codes.append(r.returncode)

        threads = [threading.Thread(target=start, args=(t1,)),
                   threading.Thread(target=start, args=(t2,))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(codes.count(0), 1, f"exactly one start must win; got {codes}")
        self.assertEqual(len(self.q("SELECT id FROM tasks WHERE state='IN_PROGRESS'")), 1)

    def test_verify_defers_on_remote_worktree(self):
        tid = self.add_task(owner="lane-w")
        wt = self.workspace / ".worktrees" / "lane-w--wt"
        wt.mkdir(parents=True, exist_ok=True)
        # Register the worktree AS host A (binds workstreams.host = hostA).
        self.fleet("set", "--slug", "lane-w", "--worktree", str(wt),
                   env_extra={"FLEET_HOST": "hostA"})
        self.fleet("task", "evidence", "--id", str(tid), "--test-cmd", "true")
        # host B: the worktree is a path on another machine — refuse POLITELY (exit 3), not the
        # exit-2 config hard-error, and name the owning host.
        r = self.fleet("verify", "--task", str(tid), expect=3, env_extra={"FLEET_HOST": "hostB"})
        self.assertIn("owned by host hostA", r.stderr)
        self.assertIn("DEFERRED", r.stderr)
        # host A runs the very same gate green.
        self.fleet("verify", "--task", str(tid), env_extra={"FLEET_HOST": "hostA"})
        ev = json.loads(self.q("SELECT evidence FROM tasks WHERE id=?", (tid,))[0]["evidence"])
        self.assertTrue(ev.get("verified_by_conductor"))

    def test_resume_is_host_pinned(self):
        # A session created on host A resumes only on host A; host B is refused (with --fresh the
        # escape hatch). Registering --session-id binds workstreams.host = the registering host.
        self.fleet("set", "--slug", "lane-w", "--session-id", "sess-123",
                   env_extra={"FLEET_HOST": "hostA"})
        r = self.fleet("run", "--agent", "lane-w", "--input", "hi", expect=2,
                       env_extra={"FLEET_HOST": "hostB"})
        self.assertIn("pinned to host hostA", r.stderr)
        self.assertEqual(self.q("SELECT id FROM runs"), [])  # nothing launched

    def test_events_are_commander_signed(self):
        # The audit log distinguishes commanders even when both act as the "conductor" role.
        self.fleet("lease", "acquire", "--resource", "ios-sim", "--as", "lane-a",
                   env_extra={"FLEET_COMMANDER": "alice@hostA"})
        rows = self.q("SELECT commander FROM events WHERE action='lease-acquire'")
        self.assertEqual(rows[0]["commander"], "alice@hostA")


class _CapturingHandler(BaseHTTPRequestHandler):
    """Local stand-in for the Grok routine webhook. Records the POST; never dials out."""

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n)
        self.server.captured.append({
            "auth": self.headers.get("Authorization"),
            "body": body,
            "path": self.path,
        })
        self.send_response(204)
        self.end_headers()

    def log_message(self, fmt, *args):
        return


_CONTRACT_KEYS = {
    "task_id", "run_id", "objective", "kind", "done_criteria", "dont_touch",
    "context_refs", "budget_usd", "callback_token", "nonce",
}


class TestExternalWorker(FleetCase):
    """Pilot completion checks for fleet dispatch / fleet ingest. The webhook is local."""

    def setUp(self):
        super().setUp()
        self.httpd = None
        ThreadingHTTPServer.allow_reuse_address = True
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _CapturingHandler)
        self.httpd.captured = []
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        port = self.httpd.server_address[1]
        self.bearer = "test-bearer-secret"
        self.env["FLEET_GROK_WEBHOOK_URL"] = f"http://127.0.0.1:{port}/dispatch"
        self.env["FLEET_GROK_WEBHOOK_TOKEN"] = self.bearer

    def tearDown(self):
        if self.httpd is not None:
            self.httpd.shutdown()
            self.httpd.server_close()
        super().tearDown()

    def _investigate(self, objective="Return the first heading of README.md", **kw):
        kw.setdefault("owner", "grok-pilot")
        return self.add_task(objective=objective, kind="investigate", **kw)

    def _dispatch(self, tid):
        before = len(self.httpd.captured)
        r = self.fleet("dispatch", "--task", str(tid))
        self.assertEqual(len(self.httpd.captured), before + 1)
        hit = self.httpd.captured[-1]
        body = json.loads(hit["body"].decode())
        token = body["callback_token"]
        self.assertEqual(r.stdout.count(token), 1)
        self.assertNotIn(self.bearer, r.stdout)
        self.assertNotIn(self.bearer, r.stderr)
        self.assertNotIn(self.env["FLEET_DB"], r.stdout)
        return body["run_id"], token, body, hit

    def _write_result(self, name, obj):
        path = Path(self.tmp.name) / name
        path.write_text(json.dumps(obj))
        return str(path)

    def _result(self, *, status="completed", claim="# Pilot heading", test_cmd="true",
                blockers=None, cost=1.25, ref="ref-1", extra=None):
        obj = {
            "status": status,
            "artifact_links": [],
            "evidence": {"claim": claim, "test_cmd": test_cmd},
            "blockers": blockers or [],
            "cost_usd": cost,
            "external_ref": ref,
        }
        if extra:
            obj.update(extra)
        return obj

    def _snap(self):
        return {
            "runs": self.q("SELECT id, status, cost_usd, external_ref, ended_ts, result_ref "
                           "FROM runs ORDER BY id"),
            "tasks": self.q("SELECT id, state, spent_usd, evidence FROM tasks ORDER BY id"),
            "events": self.q("SELECT action, subject, detail FROM events ORDER BY id"),
            "msgs": self.q("SELECT id, tier, kind, to_addr, body FROM messages ORDER BY id"),
            "updates": self.q("SELECT id, slug, did FROM updates ORDER BY id"),
            "decisions": self.q("SELECT id, question FROM decisions ORDER BY id"),
        }

    def test_dispatch_records_run_event_and_posts_contract_without_secrets(self):
        tid = self._investigate()
        run_id, token, body, hit = self._dispatch(tid)
        self.assertEqual(set(body), _CONTRACT_KEYS)
        self.assertEqual(body["task_id"], tid)
        self.assertEqual(body["run_id"], run_id)
        self.assertEqual(body["kind"], "investigate")
        raw = hit["body"]
        self.assertNotIn(b"FLEET_DB", raw)
        self.assertNotIn(self.bearer.encode(), raw)
        self.assertNotIn(self.env["FLEET_DB"].encode(), raw)
        self.assertNotIn(self.env["FLEET_GROK_WEBHOOK_URL"].encode(), raw)
        self.assertEqual(hit["auth"], f"Bearer {self.bearer}")
        self.assertEqual(hit["path"], "/dispatch")
        row = self.q("SELECT * FROM runs WHERE id=?", (run_id,))[0]
        self.assertEqual(row["status"], "running")
        self.assertEqual(row["await_external"], 1)
        self.assertEqual(row["model"], "grok")
        self.assertEqual(row["product"], "claude")  # CHECK cannot grow; model carries grok
        self.assertIsNone(row["pid"])
        self.assertIsNone(row["external_ref"])
        self.assertEqual(row["callback_token_hash"], hashlib.sha256(token.encode()).hexdigest())
        self.assertNotEqual(row["callback_token_hash"], token)
        self.assertNotIn(token, row["callback_token_hash"])
        events = self.q("SELECT action, detail FROM events WHERE action='dispatch'")
        self.assertEqual(len(events), 1)
        self.assertNotIn(token, events[0]["detail"] or "")
        self.assertNotIn(self.bearer, events[0]["detail"] or "")
        self.assertNotIn(self.env["FLEET_DB"], events[0]["detail"] or "")

    def test_tick_leaves_awaiting_run_running(self):
        tid = self._investigate()
        run_id, _token, _body, _hit = self._dispatch(tid)
        out = self.fleet("tick").stdout
        self.assertIn("awaiting external callback", out)
        row = self.q("SELECT status, ended_ts FROM runs WHERE id=?", (run_id,))[0]
        self.assertEqual(row["status"], "running")
        self.assertIsNone(row["ended_ts"])

    def test_ingest_bad_token_writes_nothing(self):
        tid = self._investigate()
        run_id, _token, _body, _hit = self._dispatch(tid)
        path = self._write_result("bad-token.json", self._result())
        before = self._snap()
        r = self.fleet("ingest", "--run", str(run_id), "--token", "not-the-token",
                       "--file", path, expect=2)
        self.assertIn("REFUSED", r.stderr)
        self.assertEqual(self._snap(), before)

    def test_ingest_completed_books_cost_and_does_not_finish_task(self):
        tid = self._investigate()
        run_id, token, _body, _hit = self._dispatch(tid)
        self.fleet("task", "set-state", "--id", str(tid), "--state", "IN_PROGRESS",
                   "--actor", "conductor")
        self.register_worktree("grok-pilot")
        self.fleet("task", "evidence", "--id", str(tid), "--test-cmd", "true")
        self.fleet("verify", "--task", str(tid))
        ev = json.loads(self.q("SELECT evidence FROM tasks WHERE id=?", (tid,))[0]["evidence"])
        self.assertTrue(ev.get("verified_by_conductor"))
        path = self._write_result("ok.json", self._result(cost=1.5, ref="pilot-ok"))
        self.fleet("ingest", "--run", str(run_id), "--token", token, "--file", path)
        task = self.q("SELECT state, spent_usd, evidence FROM tasks WHERE id=?", (tid,))[0]
        self.assertEqual(task["state"], "IN_PROGRESS")
        self.assertNotEqual(task["state"], "DONE")
        self.assertAlmostEqual(task["spent_usd"], 1.5)
        ev = json.loads(task["evidence"])
        self.assertNotIn("verified_by_conductor", ev)
        self.assertNotIn("verified_ts", ev)
        self.assertEqual(ev["claim"], "# Pilot heading")
        run = self.q("SELECT status, cost_usd, external_ref FROM runs WHERE id=?", (run_id,))[0]
        self.assertEqual(run["status"], "finished")
        self.assertAlmostEqual(run["cost_usd"], 1.5)
        self.assertEqual(run["external_ref"], "pilot-ok")
        self.assertEqual(self.q("SELECT id FROM decisions"), [])

    def test_verify_passes_only_when_claim_matches_readme_heading(self):
        heading = "# Pilot heading"
        tid = self._investigate()
        self.fleet("task", "set-state", "--id", str(tid), "--state", "IN_PROGRESS",
                   "--actor", "conductor")
        wt = self.register_worktree("grok-pilot")
        (wt / "README.md").write_text(heading + "\n\nbody\n")

        def check(expected):
            script = (
                "import pathlib,sys; "
                "got=pathlib.Path('README.md').read_text().splitlines()[0]; "
                f"sys.exit(0 if got=={json.dumps(expected)} else 1)"
            )
            return f"{shlex.quote(sys.executable)} -c {shlex.quote(script)}"

        run_id, token, _body, _hit = self._dispatch(tid)
        wrong = self._write_result("wrong.json", self._result(
            claim="nope", test_cmd=check("nope"), ref="claim-wrong", cost=0.2))
        self.fleet("ingest", "--run", str(run_id), "--token", token, "--file", wrong)
        self.fleet("verify", "--task", str(tid), expect=1)
        ev = json.loads(self.q("SELECT evidence FROM tasks WHERE id=?", (tid,))[0]["evidence"])
        self.assertFalse(ev.get("verified_by_conductor"))
        self.assertEqual(self.q("SELECT state FROM tasks WHERE id=?", (tid,))[0]["state"],
                         "IN_PROGRESS")

        run2, token2, _body2, _hit2 = self._dispatch(tid)
        right = self._write_result("right.json", self._result(
            claim=heading, test_cmd=check(heading), ref="claim-right", cost=0.2))
        self.fleet("ingest", "--run", str(run2), "--token", token2, "--file", right)
        self.fleet("verify", "--task", str(tid))
        ev = json.loads(self.q("SELECT evidence FROM tasks WHERE id=?", (tid,))[0]["evidence"])
        self.assertEqual(ev["claim"], heading)
        self.assertTrue(ev.get("verified_by_conductor"))
        self.assertEqual(self.q("SELECT state FROM tasks WHERE id=?", (tid,))[0]["state"],
                         "IN_PROGRESS")

    def test_ingest_same_external_ref_is_noop_different_ref_refused(self):
        tid = self._investigate()
        run_id, token, _body, _hit = self._dispatch(tid)
        path = self._write_result("once.json", self._result(cost=0.4, ref="same-ref"))
        self.fleet("ingest", "--run", str(run_id), "--token", token, "--file", path)
        spent = self.q("SELECT spent_usd FROM tasks WHERE id=?", (tid,))[0]["spent_usd"]
        events = len(self.q("SELECT id FROM events"))
        again = self.fleet("ingest", "--run", str(run_id), "--token", token, "--file", path)
        self.assertIn("no-op", again.stdout)
        self.assertAlmostEqual(
            self.q("SELECT spent_usd FROM tasks WHERE id=?", (tid,))[0]["spent_usd"], spent)
        self.assertEqual(len(self.q("SELECT id FROM events")), events)
        other = self._write_result("other.json", self._result(cost=9, ref="other-ref"))
        before = self._snap()
        r = self.fleet("ingest", "--run", str(run_id), "--token", token, "--file", other, expect=2)
        self.assertIn("REFUSED", r.stderr)
        self.assertEqual(self._snap(), before)

    def test_blockers_surface_in_digest_not_as_a_decision(self):
        tid = self._investigate()
        self.fleet("task", "set-state", "--id", str(tid), "--state", "IN_PROGRESS",
                   "--actor", "conductor")
        run_id, token, _body, _hit = self._dispatch(tid)
        blockers = [{"summary": "README heading was not in the contract", "needs": "founder"}]
        path = self._write_result("blocked.json", self._result(
            status="blocked", claim="", test_cmd="", blockers=blockers, cost=0.05, ref="blocked-1"))
        self.fleet("ingest", "--run", str(run_id), "--token", token, "--file", path)
        self.assertEqual(self.q("SELECT state FROM tasks WHERE id=?", (tid,))[0]["state"],
                         "IN_PROGRESS")
        ev = json.loads(self.q("SELECT evidence FROM tasks WHERE id=?", (tid,))[0]["evidence"])
        self.assertFalse(ev.get("verified_by_conductor"))
        self.assertEqual(self.q("SELECT id FROM decisions"), [])
        run = self.q("SELECT status FROM runs WHERE id=?", (run_id,))[0]
        self.assertEqual(run["status"], "finished")  # no blocked run status; the message is the signal
        digest = json.loads(self.fleet("digest", "--json").stdout)
        self.assertEqual(digest["open_decisions"], [])
        self.assertTrue(digest["gate_requests"])
        msg = digest["gate_requests"][0]
        self.assertEqual(msg["tier"], "BLOCKER")
        self.assertEqual(msg["kind"], "request")
        self.assertEqual(msg["to_addr"], "conductor")
        self.assertIn("needs founder", msg["body"])
        self.assertIn(msg["id"], [m["id"] for m in digest["unrouted_messages"]])

    def test_dispatch_refuses_non_investigate_repo_and_secret_text(self):
        impl = self.add_task(kind="implement", objective="write code")
        r = self.fleet("dispatch", "--task", str(impl), expect=2)
        self.assertIn("investigate", r.stderr)
        self.assertEqual(self.httpd.captured, [])
        self.assertEqual(self.q("SELECT id FROM runs"), [])

        with_repo = self._investigate(repo="acme-api")
        r = self.fleet("dispatch", "--task", str(with_repo), expect=2)
        self.assertIn("repo", r.stderr)
        self.assertEqual(self.httpd.captured, [])

        leaked = self._investigate(objective="please echo FLEET_DB")
        r = self.fleet("dispatch", "--task", str(leaked), expect=2)
        self.assertIn("FLEET_DB", r.stderr)
        self.assertNotIn("callback_token:", r.stdout)
        self.assertEqual(self.httpd.captured, [])
        self.assertEqual(self.q("SELECT status FROM runs")[0]["status"], "failed")

        self.fleet("dispatch", "--task", str(self._investigate()), expect=2,
                   env_extra={"FLEET_GROK_WEBHOOK_URL": ""})
        # The leaked attempt above is the only run; a missing URL must not add another.
        self.assertEqual(len(self.q("SELECT id FROM runs")), 1)

    def test_stale_signal_after_12h_while_run_stays_open(self):
        tid = self._investigate()
        run_id, _token, _body, _hit = self._dispatch(tid)
        self.fleet("task", "set-state", "--id", str(tid), "--state", "IN_PROGRESS",
                   "--actor", "conductor")
        fresh = json.loads(self.fleet("digest", "--json").stdout)
        self.assertNotIn(tid, [s["task"] for s in fresh["stale_tasks"]])
        old = _iso(-13 * 3600)
        self.raw_exec("UPDATE tasks SET updated_ts=? WHERE id=?", (old, tid))
        self.raw_exec("UPDATE runs SET started_ts=? WHERE id=?", (old, run_id))
        stale = json.loads(self.fleet("digest", "--json").stdout)
        self.assertIn(tid, [s["task"] for s in stale["stale_tasks"]])
        out = self.fleet("tick").stdout
        self.assertIn("STALE", out)
        self.assertIn("awaiting external callback", out)
        row = self.q("SELECT status, ended_ts FROM runs WHERE id=?", (run_id,))[0]
        self.assertEqual(row["status"], "running")
        self.assertIsNone(row["ended_ts"])

    def test_ingest_rejects_unknown_keys_and_bool_cost(self):
        tid = self._investigate()
        run_id, token, _body, _hit = self._dispatch(tid)
        self.fleet("task", "set-state", "--id", str(tid), "--state", "IN_PROGRESS",
                   "--actor", "conductor")
        sneaky = self._write_result("sneaky.json", self._result(extra={"task_state": "DONE"}))
        before = self._snap()
        r = self.fleet("ingest", "--run", str(run_id), "--token", token, "--file", sneaky, expect=2)
        self.assertIn("unknown keys", r.stderr)
        self.assertEqual(self._snap(), before)
        self.assertEqual(self.q("SELECT state FROM tasks WHERE id=?", (tid,))[0]["state"],
                         "IN_PROGRESS")
        bad_cost = self._write_result("bool.json", self._result(cost=True))
        r = self.fleet("ingest", "--run", str(run_id), "--token", token, "--file", bad_cost, expect=2)
        self.assertIn("cost_usd", r.stderr)
        self.assertEqual(self._snap(), before)

    def test_published_example_result_ingests(self):
        example = HERE.parent / "examples" / "grok-pilot-result.json"
        contract = json.loads((HERE.parent / "examples" / "grok-pilot-contract.json").read_text())
        self.assertEqual(set(contract), _CONTRACT_KEYS)
        self.assertNotIn("FLEET_DB", json.dumps(contract))
        tid = self._investigate()
        run_id, token, _body, _hit = self._dispatch(tid)
        self.fleet("ingest", "--run", str(run_id), "--token", token, "--file", str(example))
        ev = json.loads(self.q("SELECT evidence FROM tasks WHERE id=?", (tid,))[0]["evidence"])
        self.assertEqual(ev["claim"], "# Fleet-OS — the Agent OS substrate + worker runtime")
        self.assertAlmostEqual(
            self.q("SELECT spent_usd FROM tasks WHERE id=?", (tid,))[0]["spent_usd"], 0.01)
        self.assertEqual(
            self.q("SELECT external_ref, status FROM runs WHERE id=?", (run_id,))[0]["external_ref"],
            "grok-routine-pilot-1")


if __name__ == "__main__":
    unittest.main()
