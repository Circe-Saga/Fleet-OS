"""Saga-Fleet test suite — drives the real CLI as subprocesses against a tmp DB.

Covers the acceptance criteria in briefs F0–F5 (schema idempotency + v1 upgrade, gated task
transitions, supersedes, decision expiry, digest, concurrency, spawn/harvest with a fake
launcher, dry-run, hooks fail-open/block-once, waiting-queue rendering, review policy).
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
FLEET = HERE.parent / "fleet.py"
HOOKS = HERE.parent / "hooks"
PY = sys.executable


class FleetCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.db = root / "fleet.db"
        self.workspace = root / "workspace"
        self.workspace.mkdir()
        self.runs = root / "runs"
        self.waiting = root / "WAITING_ON_YOU.md"
        self.env = {
            **os.environ,
            "FLEET_DB": str(self.db),
            "FLEET_WORKSPACE": str(self.workspace),
            "FLEET_RUNS_DIR": str(self.runs),
            "FLEET_WORKTREES": str(self.workspace / ".worktrees"),
            "FLEET_WAITING_OUT": str(self.waiting),
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

    def q(self, sql, params=()):
        con = sqlite3.connect(self.db)
        con.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in con.execute(sql, params).fetchall()]
        finally:
            con.close()

    def add_task(self, objective="do the thing", kind="implement", **kw):
        args = ["task", "add", "--objective", objective, "--kind", kind,
                "--done", kw.pop("done", "tests green")]
        for k, v in kw.items():
            args += [f"--{k.replace('_', '-')}", str(v)]
        r = self.fleet(*args)
        return int(r.stdout.split("#")[1].split(" ")[0])


class TestSchema(FleetCase):
    def test_idempotent_connect(self):
        self.fleet("status")
        self.fleet("status")  # second connect re-runs SCHEMA + migrations without error

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
        tid = self.add_task()
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
        tid = self.add_task()
        self.fleet("task", "evidence", "--id", str(tid), "--test-cmd", "false")
        r = self.fleet("verify", "--task", str(tid), expect=1)
        self.assertIn("GATE BOUNCE", r.stdout)
        ev = json.loads(self.q("SELECT evidence FROM tasks WHERE id=?", (tid,))[0]["evidence"])
        self.assertNotIn("verified_by_conductor", ev)
        actions = [e["action"] for e in self.q("SELECT action FROM events")]
        self.assertIn("gate-bounce", actions)

    def test_new_claim_invalidates_verification(self):
        tid = self.add_task()
        self.fleet("task", "evidence", "--id", str(tid), "--test-cmd", "true")
        self.fleet("verify", "--task", str(tid))
        self.fleet("task", "evidence", "--id", str(tid), "--pr", "#99")  # new claim
        ev = json.loads(self.q("SELECT evidence FROM tasks WHERE id=?", (tid,))[0]["evidence"])
        self.assertNotIn("verified_by_conductor", ev)

    def test_open_review_blocks_done(self):
        tid = self.add_task()
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
        t1 = self.add_task(objective="a", repo="Saga-FastAPI", territory="app/services/")
        t2 = self.add_task(objective="b", repo="Saga-FastAPI",
                           territory="app/services/billing_store.py")
        self.fleet("task", "set-state", "--id", str(t1), "--state", "IN_PROGRESS",
                   "--actor", "conductor")
        r = self.fleet("task", "set-state", "--id", str(t2), "--state", "IN_PROGRESS",
                       "--actor", "conductor", expect=2)
        self.assertIn("territory overlaps", r.stderr)
        t3 = self.add_task(objective="c", repo="Saga-FastAPI", territory="migrations/")
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

    def test_gate_request_lands_in_digest(self):
        self.fleet("msg", "--from", "lane-a", "--to", "conductor", "--kind", "request",
                   "--requires-action", "--body", "task #1 ready for IN_REVIEW")
        d = json.loads(self.fleet("digest", "--json").stdout)
        self.assertEqual(len(d["gate_requests"]), 1)
        self.fleet("route", "--msg", "1", "--note", "gated")
        d = json.loads(self.fleet("digest", "--json").stdout)
        self.assertEqual(len(d["unrouted_messages"]), 0)


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
    def scratch_repo(self, name="Saga-Scratch"):
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

    def test_run_refused_while_running(self):
        self.fleet("set", "--slug", "lane-x", "--state", "ACTIVE", "--session-id", "sess-1")
        con = sqlite3.connect(self.db)
        con.execute("INSERT INTO runs (agent_slug,product,status,pid,started_ts) "
                    "VALUES ('lane-x','claude','running',999999999,'2026-01-01')")
        con.commit(); con.close()
        r = self.fleet("run", "--agent", "lane-x", "--input", "continue", expect=2)
        self.assertIn("already running", r.stderr)

    def test_harvest_marks_dead_unparseable_failed(self):
        self.fleet("status")  # init the DB schema
        bad = self.runs / "bad.json"
        self.runs.mkdir(parents=True, exist_ok=True)
        bad.write_text("worker exploded, no json here")
        con = sqlite3.connect(self.db)
        con.execute("INSERT INTO runs (agent_slug,product,status,pid,result_ref,started_ts) "
                    "VALUES ('lane-x','claude','running',999999999,?, '2026-01-01')", (str(bad),))
        con.commit(); con.close()
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


class TestReviewPolicy(FleetCase):
    def test_triggers(self):
        t_prod = self.add_task(repo="Saga-FastAPI")
        t_lab = self.add_task(repo="Saga-EvalLab", territory="evals/scratch/")
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


if __name__ == "__main__":
    unittest.main()
