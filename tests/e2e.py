#!/usr/bin/env python3
"""End-to-end test of the Fleet-OS golden path, in an isolated sandbox.

    python3 tests/e2e.py          # fake mode: free, CI-safe (worker actions simulated)
    python3 tests/e2e.py --live   # real headless Claude Code worker (~$0.30, needs the CLI)

Model access: the live worker runs via Claude Code headless mode (`claude -p`) using your
existing Claude Code auth — NO direct model API and NO Bedrock/AWS credentials are used
anywhere in this test or in the fleet runtime. Fake mode calls no model at all.

Exercises the full loop the unit tests only cover piecewise:
  task add → IN_PROGRESS → spawn (worktree off a scratch repo, hooks stamped)
  → worker edits code + checkpoints (+ claims evidence) → harvest (cost, session)
  → verify (gate-pass in the WORKER's worktree) → review-policy trigger → review task
  → author DONE blocked by open review → review verified+DONE → author IN_REVIEW → DONE
  → reversible decision auto-proceeds via tick → WAITING_ON_YOU renders it
  → retire --rm-worktree → events audit trail complete.

Everything runs against a temp workspace/DB via env overrides; nothing real is touched.
Exit 0 = all assertions passed. Prints a scorecard.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
FLEET = HERE.parent / "fleet.py"
PY = sys.executable

CHECK_CMD = 'python3 -c "import greeting; assert greeting.greeting() == \'hello fleet\'"'

# Dual-backend: set FLEET_PG_TEST to a postgres:// DSN to run the golden path against Postgres
# (its public schema is reset first); otherwise a throwaway SQLite file is used.
PG_DSN = os.environ.get("FLEET_PG_TEST") or None
USE_PG = bool(PG_DSN)
if USE_PG:
    import psycopg
    from psycopg.rows import dict_row


class E2E:
    def __init__(self, live: bool):
        self.live = live
        self.tmp = tempfile.TemporaryDirectory(prefix="fleet-e2e-")
        root = Path(self.tmp.name)
        self.ws = root / "workspace"
        self.ws.mkdir()
        self.waiting = root / "WAITING_ON_YOU.md"
        if USE_PG:
            with psycopg.connect(PG_DSN, autocommit=True) as con:
                con.execute("DROP SCHEMA IF EXISTS public CASCADE")
                con.execute("CREATE SCHEMA public")
        base_env = {k: v for k, v in os.environ.items() if not k.startswith("FLEET_")}
        self.env = {
            **base_env,
            "FLEET_DB": PG_DSN if USE_PG else str(root / "fleet.db"),
            "FLEET_WORKSPACE": str(self.ws),
            "FLEET_WORKTREES": str(self.ws / ".worktrees"),
            "FLEET_RUNS_DIR": str(root / "runs"),
            "FLEET_WAITING_OUT": str(self.waiting),
        }
        if not live:
            self.env["FLEET_LAUNCHER_CLAUDE"] = f"{PY} {HERE / 'fake_claude.py'}"
        self.passed, self.failed = 0, 0

    # ── plumbing ────────────────────────────────────────────────────────────
    def fleet(self, *args, expect=0):
        r = subprocess.run([PY, str(FLEET), *args], capture_output=True, text=True,
                           env=self.env, timeout=120)
        if expect is not None and r.returncode != expect:
            raise AssertionError(f"fleet {' '.join(map(str, args))} → rc {r.returncode}\n"
                                 f"stdout: {r.stdout}\nstderr: {r.stderr}")
        return r

    def q(self, sql, params=()):
        if USE_PG:
            sql = sql.replace("%", "%%").replace("?", "%s")
            with psycopg.connect(PG_DSN, autocommit=True, row_factory=dict_row) as con:
                return [dict(r) for r in con.execute(sql, params).fetchall()]
        con = sqlite3.connect(self.env["FLEET_DB"])
        con.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in con.execute(sql, params).fetchall()]
        finally:
            con.close()

    def check(self, label, cond, detail=""):
        ok = bool(cond)
        self.passed += ok
        self.failed += (not ok)
        print(f"    {'✓' if ok else '✗ FAIL'} {label}" + (f" — {detail}" if detail and not ok else ""))
        return ok

    def stage(self, name):
        print(f"\n── {name} ──")

    # ── scenario ────────────────────────────────────────────────────────────
    def run(self) -> int:
        mode = ("LIVE (real worker via Claude Code headless — your `claude` auth, no direct "
                "API/Bedrock)" if self.live else "FAKE (simulated worker, no model calls)")
        print(f"Fleet-OS e2e — {mode}\nsandbox: {self.tmp.name}")

        self.stage("0 · scratch repo")
        repo = self.ws / "Demo-Repo"
        repo.mkdir()
        for c in (["git", "init", "-b", "main"], ["git", "config", "user.email", "e2e@t"],
                  ["git", "config", "user.name", "e2e"]):
            subprocess.run(c, cwd=repo, capture_output=True, check=True)
        (repo / "greeting.py").write_text('def greeting():\n    return "hello"\n')
        subprocess.run(["git", "add", "-A"], cwd=repo, capture_output=True, check=True)
        subprocess.run(["git", "commit", "-qm", "init"], cwd=repo, capture_output=True, check=True)
        self.check("repo created", (repo / ".git").exists())

        self.stage("1 · conductor: task → IN_PROGRESS")
        self.fleet("task", "add",
                   "--objective", 'Edit greeting.py so greeting() returns "hello fleet" '
                   f"(verify with: {CHECK_CMD}), then claim evidence per your protocol",
                   "--kind", "implement", "--repo", "Demo-Repo", "--budget", "2",
                   "--done", f"`{CHECK_CMD}` exits 0 in the worktree",
                   "--context", f"greeting.py :: the one function to change",
                   "--context", f"{FLEET} :: the fleet CLI — FLEET_DB/FLEET_SLUG/FLEET_TASK "
                                "are preset in your env; use it for checkpoint + evidence")
        self.fleet("task", "set-state", "--id", "1", "--state", "IN_PROGRESS",
                   "--actor", "conductor")
        self.check("task #1 IN_PROGRESS",
                   self.q("SELECT state FROM tasks WHERE id=1")[0]["state"] == "IN_PROGRESS")

        self.stage("2 · spawn worker (isolated worktree, hooks stamped)")
        self.fleet("spawn", "--task", "1", "--agent", "e2e-worker", "--model", "sonnet",
                   "--max-turns", "16")
        wt = self.ws / ".worktrees" / "e2e-worker--Demo-Repo"
        self.check("worktree exists", wt.is_dir())
        self.check("hooks stamped", (wt / ".claude" / "settings.json").exists())
        self.check("agent branch", subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=wt, capture_output=True,
            text=True).stdout.strip() == "agent/e2e-worker/t1")
        self.check("run ledgered as running",
                   self.q("SELECT status FROM runs WHERE id=1")[0]["status"] == "running")

        self.stage("3 · worker works" + (" (real agent — waiting)" if self.live else " (simulated)"))
        if not self.live:
            # Simulate exactly what the live worker does: edit, self-check, claim, checkpoint.
            (wt / "greeting.py").write_text('def greeting():\n    return "hello fleet"\n')
            assert subprocess.run(CHECK_CMD, shell=True, cwd=wt).returncode == 0
            self.fleet("task", "evidence", "--id", "1", "--test-cmd", CHECK_CMD)
            self.fleet("checkpoint", "--as", "e2e-worker", "--did", "greeting returns hello fleet",
                       "--files", "greeting.py", "--next", "awaiting conductor gate")
        deadline = time.time() + (360 if self.live else 20)
        harvested = ""
        while time.time() < deadline:
            out = self.fleet("harvest").stdout
            if "finished" in out or "FAILED" in out:
                harvested = out
                break
            time.sleep(3 if self.live else 0.3)
        self.check("harvest ingested the run", "finished" in harvested, harvested)
        run = self.q("SELECT * FROM runs WHERE id=1")[0]
        self.check("cost recorded", run["cost_usd"] is not None and run["cost_usd"] > 0)
        self.check("session captured for resume",
                   self.q("SELECT session_id FROM workstreams WHERE slug='e2e-worker'")[0]
                   ["session_id"] not in (None, ""))
        self.check("model recorded on run", run["model"] == "sonnet")
        self.check("worker actually changed the code",
                   'hello fleet' in (wt / "greeting.py").read_text())
        self.check("checkpoint journaled",
                   len(self.q("SELECT id FROM updates WHERE slug='e2e-worker'")) >= 1)
        ev = json.loads(self.q("SELECT evidence FROM tasks WHERE id=1")[0]["evidence"])
        if "test_cmd" not in ev:  # live worker may checkpoint without claiming — conductor fills in
            print("    (worker didn't claim evidence; conductor claims from its checkpoint)")
            self.fleet("task", "evidence", "--id", "1", "--test-cmd", CHECK_CMD)

        self.stage("4 · gates: workers propose, conductor verifies + disposes")
        r = self.fleet("task", "set-state", "--id", "1", "--state", "DONE",
                       "--actor", "e2e-worker", expect=2)
        self.check("worker cannot self-dispose", "workers propose" in r.stderr)
        r = self.fleet("task", "set-state", "--id", "1", "--state", "IN_REVIEW",
                       "--actor", "conductor", expect=2)
        self.check("unverified claim refused", "evidence not verified" in r.stderr)
        self.fleet("verify", "--task", "1")
        self.check("verify ran IN THE WORKTREE and passed",
                   json.loads(self.q("SELECT evidence FROM tasks WHERE id=1")[0]["evidence"])
                   .get("verified_by_conductor") is True)

        self.stage("5 · review lane (policy → review task → blocks DONE → clears)")
        pol = json.loads(self.fleet("review-policy", "--task", "1", "--diff-lines", "400").stdout)
        self.check("large-diff triggers tier-1 review", pol["tier1_required"])
        self.fleet("task", "add", "--objective", "review task #1", "--kind", "review",
                   "--depends", "1", "--done", "falsifiable findings or explicit none",
                   "--owner", "e2e-reviewer")
        self.fleet("task", "set-state", "--id", "1", "--state", "IN_REVIEW", "--actor", "conductor")
        r = self.fleet("task", "set-state", "--id", "1", "--state", "DONE",
                       "--actor", "conductor", expect=2)
        self.check("open review blocks DONE", "review task" in r.stderr)
        self.fleet("task", "evidence", "--id", "2", "--test-cmd", CHECK_CMD.replace(
            "greeting.py", "greeting.py"))  # reviewer re-runs the same falsifiable check
        # reviewer's verify runs in author's worktree (no reviewer worktree on record → repo)
        self.fleet("task", "assign", "--id", "2", "--owner", "e2e-worker")
        self.fleet("verify", "--task", "2")
        self.fleet("task", "set-state", "--id", "2", "--state", "DONE", "--actor", "conductor")
        self.fleet("task", "set-state", "--id", "1", "--state", "DONE", "--actor", "conductor")
        self.check("author task DONE after review clears",
                   self.q("SELECT state FROM tasks WHERE id=1")[0]["state"] == "DONE")

        self.stage("6 · decisions + tick + founder queue")
        self.fleet("decide", "--stream", "conductor", "--question", "roll the demo to all repos?",
                   "--default", "yes, behind a flag", "--reversible", "--expires", "0",
                   "--category", "other")
        time.sleep(1.1)
        tick = self.fleet("tick").stdout
        self.check("tick auto-proceeded the expired reversible decision",
                   "auto-proceeded #1" in tick)
        self.check("tick rendered the founder queue", self.waiting.exists())
        self.check("queue shows the auto-proceed (visible, once)",
                   "Auto-proceeded since your last visit" in self.waiting.read_text())
        self.check("judgment queue printed", "judgment queue" in tick)

        self.stage("7 · retire + audit trail")
        self.fleet("retire", "--agent", "e2e-worker", "--rm-worktree")
        self.check("worktree removed", not wt.exists())
        actions = {e["action"] for e in self.q("SELECT action FROM events")}
        need = {"task-add", "task-state", "spawn", "harvest", "gate-pass", "auto-resolve", "retire"}
        self.check("events audit trail complete", need <= actions,
                   f"missing: {need - actions}")

        rep = self.fleet("report", "--days", "1").stdout
        self.check("report counts shipped tasks", "tasks shipped: 2" in rep)

        print(f"\n{'='*60}\nE2E {'PASS' if self.failed == 0 else 'FAIL'} — "
              f"{self.passed} checks passed, {self.failed} failed "
              f"(mode: {'live' if self.live else 'fake'}"
              + (f", worker cost ${run['cost_usd']:.2f}" if run.get("cost_usd") else "") + ")")
        return 0 if self.failed == 0 else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true",
                    help="spawn a REAL headless claude worker (~$0.30) instead of the fake")
    sys.exit(E2E(ap.parse_args().live).run())
