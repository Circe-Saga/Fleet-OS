#!/usr/bin/env python3
"""fleet.py v2 — the Saga Agent OS coordination substrate + worker runtime (workspace-scoped).

Lineage: ported from Saga-EvalLab/evals/scripts/fleet.py (v1, proven in the eval-lab fleet) and
extended per docs/agent-os-design.md. v1 solved STATE (addressed messages, focused-but-complete
briefs, checkpoints, decisions). v2 adds the DRIVER: a task graph, a session ledger, headless
worker spawn/resume/harvest, evidence-verified gates, and the founder queue.

PRINCIPLES (see the design doc):
  C1 enforced-or-absent — protocol obligations are mechanical (hooks, CLI validation, verified
     gates) or they don't exist.
  C2 derive-don't-store — nothing observable from git/GitHub is cached here (no PR/CI columns).
  Authority: workers PROPOSE (msg / checkpoint / decide / evidence claims); the Conductor
     DISPOSES (task states, routing, spawning, auto-resolutions).

Worker surface (unchanged from v1 + evidence claims):
    fleet brief --as <slug>            # session start (hook-injected): what matters to ME
    fleet checkpoint --as <slug> --did … --next … [--review … --ask]
    fleet msg --from <slug> --to <addr> --kind handoff --requires-action --body …
    fleet task evidence --id N --test-cmd "pytest …" --pr "#12"

Conductor surface:
    fleet tick                          # mechanical half of the tick; prints the judgment queue
    fleet digest --json                 # the one-call gather
    fleet spawn --task N | fleet run --agent <slug> --input … | fleet harvest
    fleet verify --task N               # re-run claimed evidence; gate on VERIFIED, not claimed
    fleet task set-state --id N --state IN_REVIEW --actor conductor
    fleet render-waiting                # regenerate the founder queue (WAITING_ON_YOU.md)
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import signal
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

FLEET_HOME = Path(__file__).resolve().parent
WORKSPACE = Path(os.environ.get("FLEET_WORKSPACE", str(FLEET_HOME.parent)))  # …/Circe-Saga
DB_PATH = Path(os.environ.get("FLEET_DB", str(FLEET_HOME / "fleet.db")))
RUNS_DIR = Path(os.environ.get("FLEET_RUNS_DIR", str(FLEET_HOME / "runs")))
WORKTREES = Path(os.environ.get("FLEET_WORKTREES", str(WORKSPACE / ".worktrees")))
WAITING_OUT = Path(os.environ.get("FLEET_WAITING_OUT", str(WORKSPACE / "WAITING_ON_YOU.md")))
STATUS_OUT = Path(os.environ.get("FLEET_STATUS_OUT", str(FLEET_HOME / "STATUS.md")))

AGENT_STATES = ("ACTIVE", "BLOCKED", "NEEDS-USER", "PARKED")
TASK_STATES = ("QUEUED", "IN_PROGRESS", "IN_REVIEW", "BLOCKED", "NEEDS_HUMAN", "DONE", "PARKED")
TASK_KINDS = ("implement", "review", "test", "deploy", "doc", "investigate", "decide")
TIERS = ("FYI", "BLOCKER", "PROTOCOL")
PERSISTENT_TIERS = ("BLOCKER", "PROTOCOL")
MSG_KINDS = ("fyi", "handoff", "request", "contract-change", "review-finding", "retraction")
DECISION_CATEGORIES = ("authorization", "taste", "architecture", "verification", "other")
PRODUCTS = ("claude", "codex")
LIFETIMES = ("territory", "ephemeral")
STALE_DAYS = 3            # agent row staleness (v1)
STALE_TASK_HOURS = 12     # IN_PROGRESS w/o running session + no checkpoint for this long
GATED_STATES = ("IN_REVIEW", "DONE")
CONDUCTOR_ACTORS = ("conductor", "founder")

# Model registry: agents store the ALIAS (so `profiles/<alias>.md` and reports stay stable);
# the launcher resolves it to the product's real model id at invocation time. Unknown names
# pass through untouched — adopting a brand-new model needs no code change, just (optionally)
# a registry entry + a profile file.
MODELS = {
    "haiku":  {"id": "claude-haiku-4-5-20251001", "product": "claude", "tier": "fast"},
    "sonnet": {"id": "claude-sonnet-5",           "product": "claude", "tier": "workhorse"},
    "opus":   {"id": "claude-opus-4-8",           "product": "claude", "tier": "strong"},
    "fable":  {"id": "claude-fable-5",            "product": "claude", "tier": "frontier"},
}


def _resolve_model(name: str | None) -> str | None:
    if not name:
        return None
    return MODELS.get(name, {}).get("id", name)


# Prod-facing repos: tier-1 review triggers (see `review-policy`).
PROD_REPOS = ("Saga-FastAPI", "Saga-AIEditor", "Saga-MediaProcessing", "Saga-CDK",
              "Saga-React-WebApp", "Saga-Core")
SENSITIVE_PATH_HINTS = ("auth", "consent", "privacy", "schema", "contract", "migration",
                        "security", "token", "cred")

SCHEMA = """
CREATE TABLE IF NOT EXISTS workstreams (
  slug         TEXT PRIMARY KEY,
  owner        TEXT,
  layer        TEXT,
  state        TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (state IN ('ACTIVE','BLOCKED','NEEDS-USER','PARKED')),
  status_line  TEXT,
  next         TEXT,
  blockers     TEXT,
  branch       TEXT,
  kestral_id   TEXT,
  updated_ts   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS decisions (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  stream       TEXT,
  question     TEXT NOT NULL,
  detail       TEXT,
  raised_ts    TEXT NOT NULL,
  resolved_ts  TEXT,
  resolution   TEXT,
  kestral_id   TEXT
);
CREATE TABLE IF NOT EXISTS messages (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  from_stream  TEXT NOT NULL,
  to_addr      TEXT NOT NULL,
  tier         TEXT NOT NULL CHECK (tier IN ('FYI','BLOCKER','NEEDS-USER','PROTOCOL')),
  body         TEXT NOT NULL,
  ref          TEXT,
  ts           TEXT NOT NULL,
  resolved_ts  TEXT
);
CREATE TABLE IF NOT EXISTS read_state (
  slug         TEXT PRIMARY KEY,
  last_read_ts TEXT,
  acked_ids    TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS updates (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  slug         TEXT NOT NULL,
  did          TEXT, files TEXT, found TEXT, next TEXT, review TEXT, blockers TEXT,
  state        TEXT, ref TEXT,
  ts           TEXT NOT NULL
);
-- v2: the spine. Tasks flow; agents execute. The state machine lives HERE, not on agents.
CREATE TABLE IF NOT EXISTS tasks (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  objective     TEXT NOT NULL,
  kind          TEXT NOT NULL CHECK (kind IN
                  ('implement','review','test','deploy','doc','investigate','decide')),
  state         TEXT NOT NULL DEFAULT 'QUEUED' CHECK (state IN
                  ('QUEUED','IN_PROGRESS','IN_REVIEW','BLOCKED','NEEDS_HUMAN','DONE','PARKED')),
  owner_slug    TEXT,
  repo          TEXT,
  territory     TEXT,
  branch        TEXT,
  depends_on    TEXT NOT NULL DEFAULT '[]',
  done_criteria TEXT NOT NULL,
  dont_touch    TEXT,
  context_refs  TEXT NOT NULL DEFAULT '[]',
  evidence      TEXT NOT NULL DEFAULT '{}',
  budget_usd    REAL,
  spent_usd     REAL NOT NULL DEFAULT 0,
  parent_id     INTEGER,
  created_ts    TEXT NOT NULL,
  updated_ts    TEXT NOT NULL
);
-- v2: session ledger — one row per headless worker run (cost + liveness record).
CREATE TABLE IF NOT EXISTS runs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  agent_slug    TEXT NOT NULL,
  task_id       INTEGER,
  product       TEXT NOT NULL CHECK (product IN ('claude','codex')),
  model         TEXT,
  session_id    TEXT,
  status        TEXT NOT NULL CHECK (status IN ('running','finished','failed','abandoned')),
  pid           INTEGER,
  cost_usd      REAL,
  num_turns     INTEGER,
  result_ref    TEXT,
  started_ts    TEXT NOT NULL,
  ended_ts      TEXT
);
-- v2: Conductor action log (append-only; the founder's audit surface for Conductor judgment).
CREATE TABLE IF NOT EXISTS events (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  actor   TEXT NOT NULL,
  action  TEXT NOT NULL,
  subject TEXT,
  detail  TEXT,
  ts      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_to ON messages(to_addr, ts);
CREATE INDEX IF NOT EXISTS idx_messages_tier ON messages(tier, resolved_ts);
CREATE INDEX IF NOT EXISTS idx_updates_slug ON updates(slug, ts);
CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
"""

# Idempotent column adds so an existing v1 fleet.db upgrades in place on first connect.
_MIGRATIONS: list[tuple[str, str, str]] = [
    ("workstreams", "product",    "TEXT DEFAULT 'claude'"),
    ("workstreams", "model",      "TEXT"),
    ("workstreams", "repo",       "TEXT"),
    ("workstreams", "worktree",   "TEXT"),
    ("workstreams", "session_id", "TEXT"),
    ("workstreams", "lifetime",   "TEXT DEFAULT 'territory'"),
    # Standing role identity (leadership/any specialized role): comma-separated list of
    # "path[#heading]" specs, resolved relative to WORKSPACE. Injected into the worker's
    # SYSTEM prompt on every spawn/run, so the role persists across sessions and resumes.
    ("workstreams", "role_prompt", "TEXT"),
    ("runs", "model", "TEXT"),   # which model powered the run (alias; the launch-time truth)
    ("messages",  "kind",            "TEXT DEFAULT 'fyi'"),
    ("messages",  "requires_action", "INTEGER DEFAULT 0"),
    ("messages",  "supersedes_id",   "INTEGER"),
    ("messages",  "verified",        "INTEGER DEFAULT 0"),
    ("messages",  "routed_ts",       "TEXT"),
    ("decisions", "default_recommendation", "TEXT"),
    ("decisions", "reversible",   "INTEGER DEFAULT 0"),
    ("decisions", "expires_ts",   "TEXT"),
    ("decisions", "auto_resolved","INTEGER DEFAULT 0"),
    ("decisions", "category",     "TEXT"),
    ("decisions", "shown_ts",     "TEXT"),  # consumed by render-waiting "auto-proceeded" section
    ("decisions", "task_id",      "INTEGER"),
]


# ── time helpers (v1, unchanged) ─────────────────────────────────────────────────────────────
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _parse(ts: str | None) -> datetime | None:
    if not ts:
        return None
    s = ts.strip()
    if len(s) == 10:
        s += "T00:00:00+00:00"
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def ago(ts: str | None) -> str:
    dt = _parse(ts)
    if not dt:
        return "?"
    delta = datetime.now(timezone.utc) - dt
    if delta.days > 0:
        return f"{delta.days}d ago"
    hrs = delta.seconds // 3600
    return f"{hrs}h ago" if hrs else "just now"


def is_stale(ts: str | None) -> bool:
    dt = _parse(ts)
    return bool(dt and (datetime.now(timezone.utc) - dt).days > STALE_DAYS)


# ── connection + concurrency-safe writes (v1 core, + v2 migrations) ─────────────────────────
def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=5.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=5000;")
    con.execute("PRAGMA foreign_keys=ON;")
    con.executescript(SCHEMA)
    existing: dict[str, set[str]] = {}
    for table, col, ddl in _MIGRATIONS:
        if table not in existing:
            existing[table] = {r["name"] for r in con.execute(f"PRAGMA table_info({table})")}
        if col not in existing[table]:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
            existing[table].add(col)
    con.commit()
    return con


@contextmanager
def write_txn(con: sqlite3.Connection):
    con.execute("BEGIN IMMEDIATE;")
    try:
        yield
        con.commit()
    except Exception:
        con.rollback()
        raise


def addressed_to(to_addr: str, slug: str, layer: str | None) -> bool:
    if to_addr == "all" or to_addr == slug:
        return True
    return bool(layer and to_addr == layer)


def _event(con, actor: str, action: str, subject: str | None = None, detail: str | None = None):
    con.execute("INSERT INTO events (actor,action,subject,detail,ts) VALUES (?,?,?,?,?)",
                (actor, action, subject, detail, now_iso()))


def _j(row) -> dict:
    return dict(row) if row is not None else {}


def _loads(s, default):
    try:
        return json.loads(s) if s else default
    except (TypeError, ValueError):
        return default


# ── role prompts (standing identity for any role/team) ──────────────────────────────────────
def _extract_section(md: str, anchor: str) -> str | None:
    """Return the markdown section whose heading contains `anchor` (case-insensitive), up to
    the next heading of the same or higher level. Lets one shared doc (e.g. a leadership
    cabinet file) serve many agents without splitting it into per-role files."""
    lines = md.splitlines()
    a = anchor.lower()
    start = level = None
    for i, ln in enumerate(lines):
        m = re.match(r"(#{1,6})\s+(.*)", ln)
        if m and a in m.group(2).lower():
            start, level = i, len(m.group(1))
            break
    if start is None:
        return None
    out = [lines[start]]
    for ln in lines[start + 1:]:
        m = re.match(r"(#{1,6})\s", ln)
        if m and len(m.group(1)) <= level:
            break
        out.append(ln)
    return "\n".join(out).strip()


def _load_role_text(spec: str | None) -> str | None:
    """Resolve a role_prompt spec: comma-separated "path[#heading]" items, paths relative to
    WORKSPACE unless absolute. Fail-soft: a missing file/section becomes a visible one-line
    note (the worker + Conductor see the gap) rather than a crashed spawn."""
    if not spec:
        return None
    parts = []
    for item in (s.strip() for s in spec.split(",")):
        if not item:
            continue
        path, _, anchor = item.partition("#")
        p = Path(path.strip())
        if not p.is_absolute():
            p = WORKSPACE / p
        try:
            text = p.read_text()
        except OSError:
            parts.append(f"(role prompt source missing: {item})")
            continue
        if anchor.strip():
            text = (_extract_section(text, anchor.strip())
                    or f"(section '{anchor.strip()}' not found in {p.name})")
        parts.append(text.strip())
    return ("\n\n---\n\n".join(parts)) if parts else None


# ── v1 worker commands (ported; msg/decide extended) ─────────────────────────────────────────
def cmd_set(con, a) -> int:
    row = con.execute("SELECT * FROM workstreams WHERE slug=?", (a.slug,)).fetchone()
    cur = dict(row) if row else {
        "slug": a.slug, "owner": None, "layer": None, "state": "ACTIVE", "status_line": None,
        "next": None, "blockers": None, "branch": None, "kestral_id": None, "product": "claude",
        "model": None, "repo": None, "worktree": None, "session_id": None,
        "lifetime": "territory", "role_prompt": None}
    for field, val in (("owner", a.owner), ("layer", a.layer), ("state", a.state),
                       ("status_line", a.status), ("next", a.next), ("blockers", a.blockers),
                       ("branch", a.branch), ("product", a.product), ("model", a.model),
                       ("repo", a.repo), ("worktree", a.worktree), ("session_id", a.session_id),
                       ("lifetime", a.lifetime), ("role_prompt", a.role_prompt)):
        if val is not None:
            cur[field] = val
    if cur["state"] not in AGENT_STATES:
        print(f"ERROR: --state must be one of {AGENT_STATES}", file=sys.stderr)
        return 2
    with write_txn(con):
        con.execute(
            """INSERT INTO workstreams (slug,owner,layer,state,status_line,next,blockers,branch,
                 kestral_id,product,model,repo,worktree,session_id,lifetime,role_prompt,updated_ts)
               VALUES (:slug,:owner,:layer,:state,:status_line,:next,:blockers,:branch,
                 :kestral_id,:product,:model,:repo,:worktree,:session_id,:lifetime,:role_prompt,
                 :updated_ts)
               ON CONFLICT(slug) DO UPDATE SET
                 owner=excluded.owner, layer=excluded.layer, state=excluded.state,
                 status_line=excluded.status_line, next=excluded.next, blockers=excluded.blockers,
                 branch=excluded.branch, product=excluded.product, model=excluded.model,
                 repo=excluded.repo, worktree=excluded.worktree, session_id=excluded.session_id,
                 lifetime=excluded.lifetime, role_prompt=excluded.role_prompt,
                 updated_ts=excluded.updated_ts""",
            {**cur, "updated_ts": now_iso()})
    print(f"✓ updated {a.slug} → {cur['state']}")
    return 0


def cmd_checkpoint(con, a) -> int:
    """Append an immutable journal entry AND refresh the agent snapshot (v1 semantics)."""
    if a.state and a.state not in AGENT_STATES:
        print(f"ERROR: --state must be one of {AGENT_STATES}", file=sys.stderr)
        return 2
    row = con.execute("SELECT * FROM workstreams WHERE slug=?", (a.slug,)).fetchone()
    cur = dict(row) if row else {"slug": a.slug, "state": "ACTIVE", "status_line": None,
                                 "next": None, "blockers": None}
    if a.did is not None:
        cur["status_line"] = a.did
    if a.next is not None:
        cur["next"] = a.next
    if a.blockers is not None:
        cur["blockers"] = a.blockers
    if a.state is not None:
        cur["state"] = a.state
    ts = now_iso()
    raised = None
    with write_txn(con):
        con.execute("""INSERT INTO updates (slug,did,files,found,next,review,blockers,state,ref,ts)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (a.slug, a.did, a.files, a.found, a.next, a.review, a.blockers,
                     cur["state"], a.ref, ts))
        con.execute(
            """INSERT INTO workstreams (slug,state,status_line,next,blockers,updated_ts)
               VALUES (:slug,:state,:status_line,:next,:blockers,:ts)
               ON CONFLICT(slug) DO UPDATE SET
                 state=excluded.state, status_line=excluded.status_line, next=excluded.next,
                 blockers=excluded.blockers, updated_ts=excluded.updated_ts""",
            {**{k: cur.get(k) for k in ("slug", "state", "status_line", "next", "blockers")},
             "ts": ts})
        if a.review and (a.ask or cur["state"] == "NEEDS-USER"):
            c = con.execute("INSERT INTO decisions (stream,question,detail,raised_ts) VALUES (?,?,?,?)",
                            (a.slug, a.review, a.found or a.did, ts))
            con.execute("UPDATE workstreams SET state='NEEDS-USER', updated_ts=? WHERE slug=?",
                        (ts, a.slug))
            raised = c.lastrowid
    print(f"✓ checkpoint logged for {a.slug} @ {ts} (state {cur['state']})"
          + (f"; raised decision #{raised} (waiting on human)" if raised else ""))
    return 0


def cmd_msg(con, a) -> int:
    if a.tier not in TIERS:
        print(f"ERROR: --tier must be one of {TIERS}", file=sys.stderr)
        return 2
    if a.kind not in MSG_KINDS:
        print(f"ERROR: --kind must be one of {MSG_KINDS}", file=sys.stderr)
        return 2
    if a.supersedes:
        old = con.execute("SELECT id FROM messages WHERE id=?", (a.supersedes,)).fetchone()
        if not old:
            print(f"ERROR: --supersedes {a.supersedes}: no such message", file=sys.stderr)
            return 2
    with write_txn(con):
        c = con.execute(
            """INSERT INTO messages (from_stream,to_addr,tier,body,ref,ts,kind,requires_action,
                 supersedes_id,verified) VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (a.from_stream, a.to, a.tier, a.body, a.ref, now_iso(),
             a.kind, 1 if a.requires_action else 0, a.supersedes, 1 if a.verified else 0))
        # A retraction also resolves what it supersedes, so persistent BLOCKERs drop off briefs.
        if a.supersedes:
            con.execute("UPDATE messages SET resolved_ts=? WHERE id=? AND resolved_ts IS NULL",
                        (now_iso(), a.supersedes))
    flags = "".join([" ⚡action" if a.requires_action else "",
                     f" ⤿supersedes #{a.supersedes}" if a.supersedes else ""])
    print(f"✓ msg #{c.lastrowid} [{a.tier}/{a.kind}] {a.from_stream} → {a.to}{flags}")
    return 0


def cmd_decide(con, a) -> int:
    if a.category and a.category not in DECISION_CATEGORIES:
        print(f"ERROR: --category must be one of {DECISION_CATEGORIES}", file=sys.stderr)
        return 2
    if a.reversible and not a.default:
        print("ERROR: --reversible requires --default (what auto-proceeds on expiry?)",
              file=sys.stderr)
        return 2
    expires = None
    if a.expires is not None:
        if not a.reversible:
            print("ERROR: --expires only applies to --reversible decisions "
                  "(irreversible ones wait for a human, forever)", file=sys.stderr)
            return 2
        expires = (datetime.now(timezone.utc) + timedelta(hours=a.expires)).isoformat(timespec="seconds")
    with write_txn(con):
        c = con.execute(
            """INSERT INTO decisions (stream,question,detail,raised_ts,default_recommendation,
                 reversible,expires_ts,category,task_id) VALUES (?,?,?,?,?,?,?,?,?)""",
            (a.stream, a.question, a.detail, now_iso(), a.default,
             1 if a.reversible else 0, expires, a.category, a.task))
        if a.stream:
            con.execute("UPDATE workstreams SET state='NEEDS-USER', updated_ts=? WHERE slug=?",
                        (now_iso(), a.stream))
        if a.task:
            con.execute("UPDATE tasks SET state='NEEDS_HUMAN', updated_ts=? WHERE id=?",
                        (now_iso(), a.task))
    tail = f" (default: {a.default}; auto-proceeds {a.expires}h)" if expires else ""
    print(f"✓ decision #{c.lastrowid} raised (waiting on human){tail}")
    return 0


def _release_stream(con, stream: str | None):
    """Flip NEEDS-USER → ACTIVE once no open decisions remain for the stream (v1 semantics)."""
    if not stream:
        return False
    remaining = con.execute("SELECT COUNT(*) FROM decisions WHERE stream=? AND resolved_ts IS NULL",
                            (stream,)).fetchone()[0]
    st = con.execute("SELECT state FROM workstreams WHERE slug=?", (stream,)).fetchone()
    if remaining == 0 and st and st["state"] == "NEEDS-USER":
        con.execute("UPDATE workstreams SET state='ACTIVE', updated_ts=? WHERE slug=?",
                    (now_iso(), stream))
        return True
    return False


def cmd_resolve(con, a) -> int:
    row = con.execute("SELECT * FROM decisions WHERE id=?", (a.id,)).fetchone()
    if not row:
        print(f"ERROR: no decision #{a.id}", file=sys.stderr)
        return 2
    if not a.resolution:
        print("⚠ no --resolution text: the raising stream gets NO next-action on its brief. "
              "Re-run with --resolution \"<what to do next>\".", file=sys.stderr)
    unstuck = False
    with write_txn(con):
        con.execute("UPDATE decisions SET resolved_ts=?, resolution=? WHERE id=?",
                    (now_iso(), a.resolution, a.id))
        unstuck = _release_stream(con, row["stream"])
        if row["task_id"]:
            con.execute("""UPDATE tasks SET state='IN_PROGRESS', updated_ts=?
                           WHERE id=? AND state='NEEDS_HUMAN'""", (now_iso(), row["task_id"]))
        _event(con, a.actor or "founder", "resolve", f"decision:{a.id}", a.resolution)
    msg = f"✓ decision #{a.id} resolved"
    if unstuck:
        msg += (f" — {row['stream']} → ACTIVE; your answer reaches it on its next "
                f"`fleet brief --as {row['stream']}`")
    elif row["stream"]:
        msg += f" — {row['stream']} still has other open decisions; left as-is"
    print(msg)
    return 0


def cmd_resolve_expired(con, a) -> int:
    """Auto-proceed expired REVERSIBLE decisions to their default (the self-draining queue)."""
    now = now_iso()
    rows = con.execute(
        """SELECT * FROM decisions WHERE resolved_ts IS NULL AND reversible=1
           AND expires_ts IS NOT NULL AND expires_ts <= ?""", (now,)).fetchall()
    for r in rows:
        with write_txn(con):
            con.execute("""UPDATE decisions SET resolved_ts=?, resolution=?, auto_resolved=1
                           WHERE id=?""",
                        (now, f"[auto-proceeded on default] {r['default_recommendation']}", r["id"]))
            _release_stream(con, r["stream"])
            if r["task_id"]:
                con.execute("""UPDATE tasks SET state='IN_PROGRESS', updated_ts=?
                               WHERE id=? AND state='NEEDS_HUMAN'""", (now, r["task_id"]))
            _event(con, "conductor", "auto-resolve", f"decision:{r['id']}",
                   r["default_recommendation"])
        print(f"✓ auto-proceeded #{r['id']} on default: {r['default_recommendation']}")
    if not rows:
        print("(no expired reversible decisions)")
    return 0


def cmd_ack(con, a) -> int:
    row = con.execute("SELECT * FROM read_state WHERE slug=?", (a.slug,)).fetchone()
    acked = set(_loads(row["acked_ids"], []) if row else [])
    acked.add(a.id)
    last_read = row["last_read_ts"] if row else None
    with write_txn(con):
        con.execute("""INSERT INTO read_state (slug,last_read_ts,acked_ids) VALUES (?,?,?)
                       ON CONFLICT(slug) DO UPDATE SET acked_ids=excluded.acked_ids""",
                    (a.slug, last_read, json.dumps(sorted(acked))))
    print(f"✓ {a.slug} acked #{a.id}")
    return 0


def cmd_route(con, a) -> int:
    row = con.execute("SELECT * FROM messages WHERE id=?", (a.msg,)).fetchone()
    if not row:
        print(f"ERROR: no message #{a.msg}", file=sys.stderr)
        return 2
    with write_txn(con):
        con.execute("UPDATE messages SET routed_ts=? WHERE id=?", (now_iso(), a.msg))
        _event(con, "conductor", "route", f"msg:{a.msg}", a.note)
    print(f"✓ msg #{a.msg} routed" + (f" — {a.note}" if a.note else ""))
    return 0


# ── brief / inspect (v1 + supersedes filtering + active-task header) ─────────────────────────
def _superseded_ids(con) -> set[int]:
    return {r["supersedes_id"] for r in
            con.execute("SELECT supersedes_id FROM messages WHERE supersedes_id IS NOT NULL")}


def _brief_data(con, slug: str) -> dict:
    me = con.execute("SELECT * FROM workstreams WHERE slug=?", (slug,)).fetchone()
    layer = me["layer"] if me else None
    rs = con.execute("SELECT * FROM read_state WHERE slug=?", (slug,)).fetchone()
    last_read = rs["last_read_ts"] if rs else None
    acked = set(_loads(rs["acked_ids"], []) if rs else [])
    superseded = _superseded_ids(con)

    decisions = con.execute(
        "SELECT * FROM decisions WHERE stream=? AND resolved_ts IS NULL ORDER BY raised_ts",
        (slug,)).fetchall()
    # >= not >: timestamps are second-resolution, and a resolve landing in the same second as
    # the agent's last brief must still be DELIVERED (showing an answer twice beats never).
    answered = con.execute(
        "SELECT * FROM decisions WHERE stream=? AND resolved_ts IS NOT NULL "
        "AND (? IS NULL OR resolved_ts >= ?) ORDER BY resolved_ts",
        (slug, last_read, last_read)).fetchall()
    last_update = con.execute(
        "SELECT * FROM updates WHERE slug=? ORDER BY ts DESC LIMIT 1", (slug,)).fetchone()
    tasks = con.execute(
        "SELECT * FROM tasks WHERE owner_slug=? AND state NOT IN ('DONE','PARKED') "
        "ORDER BY id", (slug,)).fetchall()

    msgs = con.execute("SELECT * FROM messages ORDER BY ts").fetchall()
    lr = _parse(last_read)
    blocked_on_me, protocol, unread = [], [], []
    for m in msgs:
        if m["id"] in superseded:      # a later message retracted this claim — never surface it
            continue
        if not addressed_to(m["to_addr"], slug, layer):
            continue
        if m["tier"] in PERSISTENT_TIERS:
            if m["resolved_ts"] is None and m["id"] not in acked:
                (blocked_on_me if m["tier"] == "BLOCKER" else protocol).append(m)
        else:
            mt = _parse(m["ts"])
            if lr is None or (mt and mt > lr):
                unread.append(m)
    return {"me": me, "decisions": decisions, "answered": answered, "blocked_on_me": blocked_on_me,
            "protocol": protocol, "unread": unread, "last_update": last_update, "tasks": tasks}


def _ref(m) -> str:
    return f"  ↳ {m['ref']}" if m["ref"] else ""


def _print_brief(d: dict, slug: str, advance_note: bool = True):
    me = d["me"]
    if me:
        flag = "  ⚠ STALE" if is_stale(me["updated_ts"]) else ""
        print(f"YOUR ROW   {me['layer'] or '-'} · {me['state']} · updated {ago(me['updated_ts'])}{flag}")
        if me["next"]:
            print(f"   next: {me['next']}")
        if me["blockers"] and (me["blockers"] or "").lower() != "none":
            print(f"   blockers: {me['blockers']}")
    else:
        print(f"YOUR ROW   (none yet — the Conductor usually creates this via spawn; "
              f"or `fleet set --slug {slug} --state ACTIVE`)")
    for t in d["tasks"]:
        print(f"🎯 TASK #{t['id']} [{t['state']}] {t['objective']}")
        print(f"   done means: {t['done_criteria']}")
        if t["dont_touch"]:
            print(f"   do NOT touch: {t['dont_touch']}")
    lu = d["last_update"]
    if lu:
        print(f"📌 last checkpoint  ({ago(lu['ts'])}, {lu['ts']})")
        for label in ("did", "files", "found", "next", "review", "blockers"):
            if lu[label] and (label != "blockers" or lu[label].lower() != "none"):
                print(f"   {label}: {lu[label]}")
        if lu["ref"]:
            print(f"   ↳ {lu['ref']}")
    print(f"🔔 NEEDS-HUMAN you raised ({len(d['decisions'])})")
    for x in d["decisions"]:
        print(f"   #{x['id']}  {x['question']}   (raised {ago(x['raised_ts'])})")
    if d["answered"]:
        print(f"✅ ANSWERED since you last looked ({len(d['answered'])}) — your next-action")
        for x in d["answered"]:
            print(f"   #{x['id']}  Q: {x['question']}")
            print(f"        → {x['resolution'] or '(no resolution text recorded — ask the Conductor)'}")
    print(f"⛔ BLOCKED on you ({len(d['blocked_on_me'])})")
    for m in d["blocked_on_me"]:
        print(f"   #{m['id']}  {m['body']}   [{m['from_stream']}]{_ref(m)}")
    print(f"📢 PROTOCOL since you last looked ({len(d['protocol'])})")
    for m in d["protocol"]:
        print(f"   #{m['id']}  {m['body']}   [{m['from_stream']}]{_ref(m)}")
    print(f"✉ unread for you ({len(d['unread'])})")
    for m in d["unread"]:
        k = f"/{m['kind']}" if m["kind"] and m["kind"] != "fyi" else ""
        print(f"   #{m['id']}  [{m['tier']}{k}] {m['body']}   [{m['from_stream']}→{m['to_addr']}]{_ref(m)}")
    total = (len(d["decisions"]) + len(d["answered"]) + len(d["blocked_on_me"])
             + len(d["protocol"]) + len(d["unread"]))
    if total == 0:
        print("— nothing addressed to you is open. You're clear. —")
    else:
        print(f"— that's everything addressed to you ({total} item(s)). Nothing else. —")
        if advance_note:
            print(f"  (ack persistent items: `fleet ack --as {slug} --id <n>`)")


def cmd_brief(con, a) -> int:
    d = _brief_data(con, a.slug)
    _print_brief(d, a.slug)
    with write_txn(con):
        con.execute("""INSERT INTO read_state (slug,last_read_ts,acked_ids) VALUES (?,?, '[]')
                       ON CONFLICT(slug) DO UPDATE SET last_read_ts=excluded.last_read_ts""",
                    (a.slug, now_iso()))
    return 0


def cmd_inspect(con, a) -> int:
    """Read-only Conductor view of ONE agent — does NOT advance its read cursor."""
    d = _brief_data(con, a.slug)
    if not d["me"]:
        print(f"(no agent row for {a.slug} — nothing to inspect)")
        return 2
    print(f"INSPECT {a.slug} — read-only; cursor untouched")
    _print_brief(d, a.slug, advance_note=False)
    hist = con.execute("SELECT * FROM updates WHERE slug=? ORDER BY ts DESC LIMIT ?",
                       (a.slug, a.limit)).fetchall()
    print(f"\n📒 last {len(hist)} checkpoints")
    for u in hist:
        rv = f"  ⚑{u['review']}" if u["review"] else ""
        print(f"   {u['ts']}  {u['state'] or ''}: {u['did'] or ''}{rv}")
    return 0


def cmd_status(con, a) -> int:
    rows = con.execute("SELECT * FROM workstreams").fetchall()
    order = {"NEEDS-USER": 0, "BLOCKED": 1, "ACTIVE": 2, "PARKED": 3}
    rows = sorted(rows, key=lambda r: (order.get(r["state"], 9), r["slug"]))
    open_decisions = con.execute(
        "SELECT * FROM decisions WHERE resolved_ts IS NULL ORDER BY raised_ts").fetchall()
    print(f"🔔 WAITING ON HUMAN ({len(open_decisions)})")
    for x in open_decisions:
        exp = f"  ⏳auto {x['expires_ts'][:16]}" if x["expires_ts"] and not x["resolved_ts"] else ""
        print(f"   #{x['id']}  [{x['category'] or '?'}] {x['question']}   "
              f"[{x['stream'] or '—'}]  raised {ago(x['raised_ts'])}{exp}")
    counts = {s: sum(1 for r in rows if r["state"] == s) for s in AGENT_STATES}
    print(f"\nAGENTS   ⚙ ACTIVE {counts['ACTIVE']}   ⛔ BLOCKED {counts['BLOCKED']}   "
          f"🔔 NEEDS-USER {counts['NEEDS-USER']}   ✅ PARKED {counts['PARKED']}   ({len(rows)})")
    for r in rows:
        flag = "  ⚠STALE" if is_stale(r["updated_ts"]) else ""
        blk = f"  ⛔{r['blockers']}" if r["blockers"] and (r["blockers"] or "").lower() != "none" else ""
        print(f"  [{r['state']:>10}] {r['slug']:<30} {r['repo'] or r['layer'] or '-':<20} "
              f"{r['model'] or '-':<8} {ago(r['updated_ts']):>9}{flag}{blk}")
        if r["next"]:
            print(f"               next: {r['next']}")
    trows = con.execute("SELECT * FROM tasks WHERE state NOT IN ('DONE','PARKED') ORDER BY id").fetchall()
    print(f"\nTASKS open ({len(trows)})")
    for t in trows:
        spend = f"  ${t['spent_usd']:.2f}" + (f"/${t['budget_usd']:.0f}" if t["budget_usd"] else "")
        print(f"  #{t['id']:>3} [{t['state']:>11}] ({t['kind']}) {t['objective']:<58} "
              f"{t['owner_slug'] or 'unassigned':<24}{spend}")
    return 0


def cmd_log(con, a) -> int:
    clauses, params = [], []
    if a.slug:
        clauses.append("slug=?"); params.append(a.slug)
    if a.days:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=a.days)).isoformat(timespec="seconds")
        clauses.append("ts>=?"); params.append(cutoff)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(a.limit)
    rows = con.execute(f"SELECT * FROM updates{where} ORDER BY ts DESC LIMIT ?", params).fetchall()
    print(f"📒 checkpoints — {a.slug or 'all agents'} ({len(rows)})")
    for u in rows:
        flag = "  ⚑REVIEW" if u["review"] else ""
        print(f"\n  {u['ts']}  [{u['slug']}] {u['state'] or ''}{flag}")
        for key in ("did", "files", "found", "next"):
            if u[key]:
                print(f"     {key}: {u[key]}")
        if u["review"]:
            print(f"     ⚑review: {u['review']}")
        if u["ref"]:
            print(f"     ↳ {u['ref']}")
    return 0


def cmd_events(con, a) -> int:
    rows = con.execute("SELECT * FROM events ORDER BY ts DESC LIMIT ?", (a.limit,)).fetchall()
    print(f"⚙ events (last {len(rows)})")
    for e in rows:
        print(f"  {e['ts']}  {e['actor']:<10} {e['action']:<14} {e['subject'] or '':<22} "
              f"{e['detail'] or ''}")
    return 0


# ── tasks (v2) ───────────────────────────────────────────────────────────────────────────────
def _get_task(con, tid: int):
    return con.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()


def _territories_overlap(t1: str | None, t2: str | None) -> bool:
    """Conservative overlap check on comma-separated path globs: equal, or one prefixes the other
    (dir vs file within it). Globs are compared on their non-wildcard prefix."""
    if not t1 or not t2:
        return not t1 and not t2  # two unscoped tasks in the same repo DO overlap
    def prefixes(t):
        return [g.strip().split("*")[0].rstrip("/") for g in t.split(",") if g.strip()]
    for p1 in prefixes(t1):
        for p2 in prefixes(t2):
            if p1.startswith(p2) or p2.startswith(p1):
                return True
    return False


def cmd_task_add(con, a) -> int:
    if a.kind not in TASK_KINDS:
        print(f"ERROR: --kind must be one of {TASK_KINDS}", file=sys.stderr)
        return 2
    deps = [int(x) for x in a.depends.split(",") if x.strip()] if a.depends else []
    for d in deps:
        if not _get_task(con, d):
            print(f"ERROR: --depends {d}: no such task", file=sys.stderr)
            return 2
    ctx = [{"ptr": p.split("::")[0].strip(), "why": (p.split("::")[1].strip() if "::" in p else "")}
           for p in (a.context or [])]
    ts = now_iso()
    with write_txn(con):
        c = con.execute(
            """INSERT INTO tasks (objective,kind,state,owner_slug,repo,territory,branch,depends_on,
                 done_criteria,dont_touch,context_refs,budget_usd,parent_id,created_ts,updated_ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (a.objective, a.kind, "QUEUED", a.owner, a.repo, a.territory, a.branch,
             json.dumps(deps), a.done, a.dont_touch, json.dumps(ctx), a.budget, a.parent, ts, ts))
        _event(con, a.actor or "conductor", "task-add", f"task:{c.lastrowid}", a.objective)
    print(f"✓ task #{c.lastrowid} [{a.kind}] QUEUED — {a.objective}")
    return 0


def cmd_task_list(con, a) -> int:
    clauses, params = [], []
    if a.state:
        clauses.append("state=?"); params.append(a.state)
    if a.owner:
        clauses.append("owner_slug=?"); params.append(a.owner)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = con.execute(f"SELECT * FROM tasks{where} ORDER BY id", params).fetchall()
    for t in rows:
        deps = _loads(t["depends_on"], [])
        dep = f"  deps:{deps}" if deps else ""
        spend = f"  ${t['spent_usd']:.2f}" + (f"/${t['budget_usd']:.0f}" if t["budget_usd"] else "")
        print(f"#{t['id']:>3} [{t['state']:>11}] ({t['kind']}) {t['objective']}  "
              f"→ {t['owner_slug'] or 'unassigned'} @ {t['repo'] or '-'}{dep}{spend}")
    if not rows:
        print("(no tasks match)")
    return 0


def cmd_task_show(con, a) -> int:
    t = _get_task(con, a.id)
    if not t:
        print(f"ERROR: no task #{a.id}", file=sys.stderr)
        return 2
    d = dict(t)
    d["depends_on"] = _loads(t["depends_on"], [])
    d["context_refs"] = _loads(t["context_refs"], [])
    d["evidence"] = _loads(t["evidence"], {})
    print(json.dumps(d, indent=2, default=str))
    return 0


def cmd_task_assign(con, a) -> int:
    t = _get_task(con, a.id)
    if not t:
        print(f"ERROR: no task #{a.id}", file=sys.stderr)
        return 2
    sets, params = [], []
    for col, val in (("owner_slug", a.owner), ("branch", a.branch), ("budget_usd", a.budget),
                     ("territory", a.territory), ("repo", a.repo)):
        if val is not None:
            sets.append(f"{col}=?"); params.append(val)
    if not sets:
        print("nothing to assign (use --owner/--branch/--budget/--territory/--repo)")
        return 2
    params += [now_iso(), a.id]
    with write_txn(con):
        con.execute(f"UPDATE tasks SET {', '.join(sets)}, updated_ts=? WHERE id=?", params)
        _event(con, "conductor", "task-assign", f"task:{a.id}",
               " ".join(f"{s.split('=')[0]}={p}" for s, p in zip(sets, params)))
    print(f"✓ task #{a.id} updated")
    return 0


def cmd_task_evidence(con, a) -> int:
    """Worker CLAIMS evidence. Claims are leads; `fleet verify` turns them into gates."""
    t = _get_task(con, a.id)
    if not t:
        print(f"ERROR: no task #{a.id}", file=sys.stderr)
        return 2
    ev = _loads(t["evidence"], {})
    for key, val in (("test_cmd", a.test_cmd), ("pr", a.pr), ("ci_url", a.ci_url),
                     ("notes", a.notes)):
        if val is not None:
            ev[key] = val
    if a.review_requested:
        ev["review_requested"] = True
    ev.pop("verified_by_conductor", None)  # any new claim invalidates prior verification
    ev.pop("verified_ts", None)
    with write_txn(con):
        con.execute("UPDATE tasks SET evidence=?, updated_ts=? WHERE id=?",
                    (json.dumps(ev), now_iso(), a.id))
    print(f"✓ task #{a.id} evidence claimed (unverified until `fleet verify --task {a.id}`)")
    return 0


def _dep_states(con, t) -> list[tuple[int, str]]:
    return [(d, (_get_task(con, d) or {"state": "MISSING"})["state"])
            for d in _loads(t["depends_on"], [])]


def _open_review_blockers(con, tid: int) -> list[int]:
    """Review tasks that depend on this task and aren't DONE — they block this task's DONE."""
    out = []
    for r in con.execute("SELECT id, depends_on, state FROM tasks WHERE kind='review'").fetchall():
        if tid in _loads(r["depends_on"], []) and r["state"] not in ("DONE", "PARKED"):
            out.append(r["id"])
    return out


def cmd_task_set_state(con, a) -> int:
    t = _get_task(con, a.id)
    if not t:
        print(f"ERROR: no task #{a.id}", file=sys.stderr)
        return 2
    if a.state not in TASK_STATES:
        print(f"ERROR: --state must be one of {TASK_STATES}", file=sys.stderr)
        return 2
    actor = a.actor or "conductor"
    ev = _loads(t["evidence"], {})
    if a.state in GATED_STATES:
        if actor not in CONDUCTOR_ACTORS:
            print(f"REFUSED: workers propose; the Conductor disposes. Ask for the gate:\n"
                  f"  fleet msg --from {actor} --to conductor --kind request --requires-action "
                  f"--tier FYI --body \"task #{a.id} ready for {a.state}: <evidence summary>\"",
                  file=sys.stderr)
            return 2
        if not ev.get("verified_by_conductor") and not a.force:
            print(f"REFUSED: evidence not verified. Claims are leads; verified evidence is the "
                  f"gate.\n  Run: fleet verify --task {a.id}   (or gh run view <ci_url>, then "
                  f"re-run set-state with --force to accept CI as the verifier)", file=sys.stderr)
            return 2
    if a.state == "DONE":
        blockers = _open_review_blockers(con, a.id)
        if blockers and not a.force:
            print(f"REFUSED: open review task(s) {blockers} block DONE (review blocks the merge, "
                  f"never the author — but it does block DONE).", file=sys.stderr)
            return 2
    if a.state == "IN_PROGRESS":
        unmet = [(d, s) for d, s in _dep_states(con, t) if s not in ("DONE", "PARKED")]
        if unmet and not a.force:
            print(f"REFUSED: unmet dependencies {unmet}", file=sys.stderr)
            return 2
        if t["repo"]:
            for o in con.execute(
                    "SELECT * FROM tasks WHERE state='IN_PROGRESS' AND repo=? AND id!=?",
                    (t["repo"], a.id)).fetchall():
                if _territories_overlap(t["territory"], o["territory"]):
                    print(f"REFUSED: territory overlaps IN_PROGRESS task #{o['id']} "
                          f"({o['territory'] or 'unscoped'}) in {t['repo']}. Two agents, one "
                          f"tree = the clip_analysis.py story. Re-scope or wait.", file=sys.stderr)
                    return 2
    with write_txn(con):
        con.execute("UPDATE tasks SET state=?, updated_ts=? WHERE id=?",
                    (a.state, now_iso(), a.id))
        _event(con, actor, "task-state", f"task:{a.id}",
               f"{t['state']} → {a.state}" + (" (forced)" if a.force else ""))
    print(f"✓ task #{a.id}: {t['state']} → {a.state}")
    return 0


def cmd_verify(con, a) -> int:
    """Conductor gate: re-run the claimed test_cmd in the OWNER'S worktree. Exit 0 → verified."""
    t = _get_task(con, a.id)
    if not t:
        print(f"ERROR: no task #{a.id}", file=sys.stderr)
        return 2
    ev = _loads(t["evidence"], {})
    cmd = ev.get("test_cmd")
    if not cmd:
        print(f"ERROR: task #{a.id} has no claimed test_cmd (worker: `fleet task evidence "
              f"--id {a.id} --test-cmd …`)", file=sys.stderr)
        return 2
    owner = con.execute("SELECT * FROM workstreams WHERE slug=?", (t["owner_slug"],)).fetchone() \
        if t["owner_slug"] else None
    cwd = None
    if owner and owner["worktree"] and Path(owner["worktree"]).is_dir():
        cwd = owner["worktree"]
    elif t["repo"] and (WORKSPACE / t["repo"]).is_dir():
        cwd = str(WORKSPACE / t["repo"])
        print(f"⚠ no worker worktree on record — verifying in primary checkout {cwd}", file=sys.stderr)
    try:
        proc = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True,
                              timeout=a.timeout)
        ok, tail = proc.returncode == 0, (proc.stdout + proc.stderr)[-1500:]
    except subprocess.TimeoutExpired:
        ok, tail = False, f"(timed out after {a.timeout}s)"
    with write_txn(con):
        if ok:
            ev["verified_by_conductor"] = True
            ev["verified_ts"] = now_iso()
            _event(con, "conductor", "gate-pass", f"task:{a.id}", cmd)
        else:
            ev.pop("verified_by_conductor", None)
            _event(con, "conductor", "gate-bounce", f"task:{a.id}", f"{cmd} → FAILED")
        con.execute("UPDATE tasks SET evidence=?, updated_ts=? WHERE id=?",
                    (json.dumps(ev), now_iso(), a.id))
    if ok:
        print(f"✓ VERIFIED task #{a.id}: `{cmd}` green in {cwd or 'cwd'}")
        return 0
    print(f"✗ GATE BOUNCE task #{a.id}: `{cmd}` failed. Output tail:\n{tail}")
    return 1


def cmd_review_policy(con, a) -> int:
    """Deterministic tier-1 review trigger check (no LLM). See brief F5."""
    t = _get_task(con, a.id)
    if not t:
        print(f"ERROR: no task #{a.id}", file=sys.stderr)
        return 2
    ev = _loads(t["evidence"], {})
    triggers = []
    if t["kind"] == "deploy":
        triggers.append("deploy-kind")
    if t["repo"] in PROD_REPOS:
        triggers.append(f"prod-facing-repo:{t['repo']}")
    terr = (t["territory"] or "").lower()
    hits = [h for h in SENSITIVE_PATH_HINTS if h in terr]
    if hits:
        triggers.append(f"sensitive-paths:{','.join(hits)}")
    if a.diff_lines and a.diff_lines > 300:
        triggers.append(f"large-diff:{a.diff_lines}")
    if ev.get("review_requested"):
        triggers.append("author-self-flag")
    print(json.dumps({"task": a.id, "tier1_required": bool(triggers), "triggers": triggers}))
    return 0


# ── worker runtime (v2): spawn / run / harvest / retire ─────────────────────────────────────
def _launcher(product: str, starter: str, model: str | None, max_turns: int,
              permission_mode: str | None, session_id: str | None = None,
              system_append: str | None = None) -> list[str]:
    """Build the headless CLI invocation. Env overrides let tests fake the binary.
    `system_append` carries the agent's standing role prompt: for claude it rides the system
    prompt on EVERY invocation (so the role survives resumes and compaction); codex has no
    equivalent flag, so it's prepended to the prompt on fresh sessions only."""
    if product == "claude":
        binary = os.environ.get("FLEET_LAUNCHER_CLAUDE", "claude")
        cmd = shlex.split(binary)
        if session_id:
            cmd += ["-p", "--resume", session_id, starter]
        else:
            cmd += ["-p", starter]
        cmd += ["--output-format", "json", "--max-turns", str(max_turns)]
        if model:
            cmd += ["--model", _resolve_model(model)]
        if permission_mode:
            cmd += ["--permission-mode", permission_mode]
        if system_append:
            cmd += ["--append-system-prompt", system_append]
        return cmd
    binary = os.environ.get("FLEET_LAUNCHER_CODEX", "codex")
    cmd = shlex.split(binary) + ["exec"]
    if session_id:
        cmd += ["resume", session_id, "--json", starter]
    else:
        prompt = (f"=== YOUR STANDING ROLE ===\n{system_append}\n=== END ROLE ===\n\n{starter}"
                  if system_append else starter)
        cmd += ["--json", prompt]
    return cmd


def _worktree_for(slug: str, repo: str) -> Path:
    return WORKTREES / f"{slug}--{repo}"


def _ensure_worktree(repo: str, slug: str, branch: str) -> Path:
    """fetch-then-origin/main: workers NEVER run in a primary checkout (stale-lane-branch gotcha)."""
    primary = WORKSPACE / repo
    if not (primary / ".git").exists():
        raise RuntimeError(f"no git repo at {primary}")
    wt = _worktree_for(slug, repo)
    if wt.exists():
        return wt
    WORKTREES.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "-C", str(primary), "fetch", "origin"], check=False,
                   capture_output=True, text=True)
    base = "origin/main"
    probe = subprocess.run(["git", "-C", str(primary), "rev-parse", "--verify", base],
                           capture_output=True, text=True)
    if probe.returncode != 0:
        base = "HEAD"   # repo has no origin (e.g. local-only) — fall back loudly
        print(f"⚠ {repo} has no origin/main; worktree bases on HEAD", file=sys.stderr)
    r = subprocess.run(["git", "-C", str(primary), "worktree", "add", str(wt), "-b", branch, base],
                       capture_output=True, text=True)
    if r.returncode != 0:
        # branch may already exist (respawn): attach without -b
        r2 = subprocess.run(["git", "-C", str(primary), "worktree", "add", str(wt), branch],
                            capture_output=True, text=True)
        if r2.returncode != 0:
            raise RuntimeError(f"worktree add failed:\n{r.stderr}\n{r2.stderr}")
    return wt


def _write_worker_settings(wt: Path):
    tpl = FLEET_HOME / "templates" / "worker-settings.json"
    if not tpl.exists():
        return
    dst = wt / ".claude"
    dst.mkdir(exist_ok=True)
    (dst / "settings.json").write_text(tpl.read_text().replace("FLEET_HOME", str(FLEET_HOME)))


def _render_starter(con, t, agent_row) -> str:
    ctx = _loads(t["context_refs"], [])
    ev = _loads(t["evidence"], {})
    slug = agent_row["slug"]
    d = _brief_data(con, slug)
    unread_n = len(d["unread"]) + len(d["blocked_on_me"]) + len(d["protocol"])
    profile_txt = ""
    model = agent_row["model"] or ""
    pf = FLEET_HOME / "profiles" / f"{model}.md"
    if model and pf.exists():
        lines = [ln for ln in pf.read_text().splitlines() if ln.strip() and not ln.startswith("#")]
        profile_txt = "\n".join(lines[:6])
    must = "\n".join(f"  {i+1}. {c['ptr']}" + (f" — {c['why']}" if c.get("why") else "")
                     for i, c in enumerate(ctx)) or "  (none listed — the task row is your spec)"
    budget = (f"${t['budget_usd']:.2f} (spent ${t['spent_usd']:.2f})"
              if t["budget_usd"] else "unset — be frugal")
    role_spec = agent_row["role_prompt"] if "role_prompt" in agent_row.keys() else None
    L = []
    if role_spec:
        L += [f"ROLE: your standing role prompt is loaded from `{role_spec}` and rides your "
              "system prompt on every session — it defines who you are; this brief defines "
              "the task.", ""]
    L += [
        f"You are {slug}, working {t['repo'] or 'the workspace'}"
        + (f":{t['territory']}" if t["territory"] else "") + ".",
        f"Worktree: {agent_row['worktree'] or '(assigned at spawn)'} · branch: "
        + (t["branch"] or agent_row["branch"] or "(assigned at spawn)"),
        "",
        f"TASK #{t['id']} [{t['kind']}]: {t['objective']}",
        f"DONE means: {t['done_criteria']}",
    ]
    if t["dont_touch"]:
        L.append(f"DO NOT touch: {t['dont_touch']}")
    L += ["", "MUST read first (complete list — nothing else is addressed to you"
          + (f"; plus {unread_n} unread fleet item(s) your session-start brief will show" if unread_n else "")
          + "):", must]
    if profile_txt:
        L += ["", f"Model notes ({model}):", profile_txt]
    L += [
        "",
        "Protocol (hook-enforced; listed so you know):",
        f"  - checkpoint before every stop: fleet checkpoint --as {slug} --did … --next …",
        f"  - blockers: fleet msg --from {slug} --to <who-can-clear-it> --tier BLOCKER "
        "--kind request --requires-action --body …",
        f"  - claim completion evidence: fleet task evidence --id {t['id']} --test-cmd \"…\" "
        "[--pr …]; then ask the Conductor for the gate (you cannot set IN_REVIEW/DONE yourself).",
        "  - propose decisions WITH a default: fleet decide --stream " + slug +
        " --question … --default … [--reversible --expires 24] --category …",
        "",
        "Escalation ladder: repo/docs → this brief → the owning agent (fleet msg) → the "
        "Conductor → a proposed decision with a default. You cannot contact the founder; "
        "the Conductor escalates for you.",
        f"Budget: {budget}. Failing the same way twice? Stop and message the Conductor.",
        "If this brief seems wrong or insufficient, say so via msg — do not go spelunking "
        "fleet-wide.",
    ]
    if ev:
        L += ["", f"Prior evidence on this task: {json.dumps(ev)}"]
    return "\n".join(L)


def cmd_starter(con, a) -> int:
    t = _get_task(con, a.task)
    if not t:
        print(f"ERROR: no task #{a.task}", file=sys.stderr)
        return 2
    slug = a.agent or t["owner_slug"]
    if not slug:
        print("ERROR: task has no owner; pass --agent", file=sys.stderr)
        return 2
    agent = con.execute("SELECT * FROM workstreams WHERE slug=?", (slug,)).fetchone()
    if not agent:
        # Render against a synthetic row so starter works pre-spawn.
        agent = {"slug": slug, "model": a.model, "worktree": None, "branch": None,
                 "role_prompt": None}
    print(_render_starter(con, t, agent))
    return 0


def cmd_spawn(con, a) -> int:
    t = _get_task(con, a.task)
    if not t:
        print(f"ERROR: no task #{a.task}", file=sys.stderr)
        return 2
    slug = a.agent or t["owner_slug"] or f"{t['kind']}-{(t['repo'] or 'ws').lower()}-t{t['id']}"
    product = a.product or "claude"
    if product not in PRODUCTS:
        print(f"ERROR: --product must be one of {PRODUCTS}", file=sys.stderr)
        return 2
    running = con.execute(
        "SELECT id FROM runs WHERE agent_slug=? AND status='running'", (slug,)).fetchone()
    if running and not a.dry_run:
        print(f"REFUSED: run #{running['id']} already running for {slug} (harvest first)",
              file=sys.stderr)
        return 2
    branch = t["branch"] or f"agent/{slug}/t{t['id']}"
    wt = None
    if t["repo"]:
        if a.dry_run:
            wt = _worktree_for(slug, t["repo"])
        else:
            wt = _ensure_worktree(t["repo"], slug, branch)
            _write_worker_settings(wt)
    # upsert the agent row first so the starter renders with worktree/branch filled in
    lifetime = a.lifetime or ("ephemeral" if not t["owner_slug"] else "territory")
    if not a.dry_run:
        with write_txn(con):
            con.execute(
                """INSERT INTO workstreams (slug,state,status_line,branch,product,model,repo,
                     worktree,lifetime,role_prompt,updated_ts)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(slug) DO UPDATE SET
                     state='ACTIVE', branch=excluded.branch, product=excluded.product,
                     model=COALESCE(excluded.model, workstreams.model), repo=excluded.repo,
                     worktree=excluded.worktree,
                     role_prompt=COALESCE(excluded.role_prompt, workstreams.role_prompt),
                     updated_ts=excluded.updated_ts""",
                (slug, "ACTIVE", f"spawned for task #{t['id']}", branch, product, a.model,
                 t["repo"], str(wt) if wt else None, lifetime, a.role_prompt, now_iso()))
            con.execute("UPDATE tasks SET owner_slug=?, branch=?, updated_ts=? WHERE id=?",
                        (slug, branch, now_iso(), t["id"]))
        t = _get_task(con, a.task)
    agent = con.execute("SELECT * FROM workstreams WHERE slug=?", (slug,)).fetchone() or \
        {"slug": slug, "model": a.model, "worktree": str(wt) if wt else None, "branch": branch,
         "role_prompt": a.role_prompt}
    role_text = _load_role_text(agent["role_prompt"] if "role_prompt" in agent.keys() else None)
    starter = _render_starter(con, t, agent)
    cmd = _launcher(product, starter, a.model, a.max_turns, a.permission_mode,
                    system_append=role_text)
    if a.dry_run:
        def _mask(c):
            if c == starter:
                return f"'<starter prompt, {len(c)} chars — see fleet starter --task {t['id']}>'"
            if role_text and c == role_text:
                return f"'<role prompt, {len(c)} chars>'"
            return shlex.quote(c)
        print("DRY RUN — would execute (cwd=" + (str(wt) if wt else "fleet home") + "):")
        print("  " + " ".join(_mask(c) for c in cmd))
        return 0
    if a.no_launch:
        print(f"✓ prepared {slug} (worktree {wt}); launch skipped (--no-launch)")
        return 0
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    out = RUNS_DIR / f"{slug}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    env = {**os.environ, "FLEET_SLUG": slug, "FLEET_DB": str(DB_PATH),
           "FLEET_TASK": str(t["id"])}
    launch_model = a.model or (agent["model"] if "model" in agent.keys() else None)
    proc = subprocess.Popen(cmd, cwd=str(wt) if wt else str(FLEET_HOME), env=env,
                            stdout=open(out, "w"), stderr=subprocess.STDOUT,
                            start_new_session=True)
    with write_txn(con):
        con.execute("""INSERT INTO runs (agent_slug,task_id,product,model,status,pid,result_ref,
                        started_ts) VALUES (?,?,?,?,?,?,?,?)""",
                    (slug, t["id"], product, launch_model, "running", proc.pid, str(out),
                     now_iso()))
        _event(con, "conductor", "spawn", f"agent:{slug}",
               f"task:{t['id']} pid:{proc.pid} model:{launch_model or 'default'}")
    print(f"✓ spawned {slug} (pid {proc.pid}, task #{t['id']}) → {out}\n  harvest later: fleet harvest")
    return 0


def cmd_run(con, a) -> int:
    agent = con.execute("SELECT * FROM workstreams WHERE slug=?", (a.agent,)).fetchone()
    if not agent:
        print(f"ERROR: no agent {a.agent} (spawn first)", file=sys.stderr)
        return 2
    running = con.execute(
        "SELECT id FROM runs WHERE agent_slug=? AND status='running'", (a.agent,)).fetchone()
    if running:
        print(f"REFUSED: run #{running['id']} already running for {a.agent}", file=sys.stderr)
        return 2
    if not agent["session_id"] and not a.fresh:
        print(f"ERROR: {a.agent} has no session to resume (pass --fresh to start one, or spawn)",
              file=sys.stderr)
        return 2
    text = sys.stdin.read() if a.input == "-" else a.input
    if not text:
        print("ERROR: empty --input", file=sys.stderr)
        return 2
    if a.model and a.model != agent["model"]:
        # Persistent model switch (works mid-session: --resume + --model re-powers the same
        # conversation). Logged so the report's per-model numbers stay interpretable.
        with write_txn(con):
            con.execute("UPDATE workstreams SET model=?, updated_ts=? WHERE slug=?",
                        (a.model, now_iso(), a.agent))
            _event(con, "conductor", "model-switch", f"agent:{a.agent}",
                   f"{agent['model'] or 'default'} → {a.model}")
        agent = con.execute("SELECT * FROM workstreams WHERE slug=?", (a.agent,)).fetchone()
    role_text = _load_role_text(agent["role_prompt"])
    cmd = _launcher(agent["product"] or "claude", text, agent["model"], a.max_turns,
                    a.permission_mode, session_id=None if a.fresh else agent["session_id"],
                    system_append=role_text)
    cwd = agent["worktree"] if agent["worktree"] and Path(agent["worktree"]).is_dir() else str(FLEET_HOME)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    out = RUNS_DIR / f"{a.agent}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    env = {**os.environ, "FLEET_SLUG": a.agent, "FLEET_DB": str(DB_PATH)}
    proc = subprocess.Popen(cmd, cwd=cwd, env=env, stdout=open(out, "w"),
                            stderr=subprocess.STDOUT, start_new_session=True)
    with write_txn(con):
        con.execute("""INSERT INTO runs (agent_slug,task_id,product,model,status,pid,result_ref,
                        started_ts) VALUES (?,?,?,?,?,?,?,?)""",
                    (a.agent, a.task, agent["product"] or "claude", agent["model"], "running",
                     proc.pid, str(out), now_iso()))
        _event(con, "conductor", "resume", f"agent:{a.agent}", f"pid:{proc.pid}")
    print(f"✓ resumed {a.agent} (pid {proc.pid}) → {out}")
    return 0


def _pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False
    except OSError:
        return False


def _parse_result(path: Path) -> dict | None:
    """claude -p --output-format json emits one JSON object; codex --json emits JSONL. Be lenient."""
    try:
        text = path.read_text().strip()
    except OSError:
        return None
    if not text:
        return None
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except ValueError:
        for line in reversed(text.splitlines()):
            line = line.strip()
            if line.startswith("{"):
                try:
                    return json.loads(line)
                except ValueError:
                    continue
    return None


def cmd_harvest(con, a) -> int:
    rows = con.execute("SELECT * FROM runs WHERE status='running'").fetchall()
    if not rows:
        print("(no running sessions)")
        return 0
    n_done = 0
    for r in rows:
        if _pid_alive(r["pid"]):
            print(f"  ⏳ run #{r['id']} {r['agent_slug']} still running (pid {r['pid']})")
            continue
        res = _parse_result(Path(r["result_ref"])) if r["result_ref"] else None
        with write_txn(con):
            if res is None:
                con.execute("UPDATE runs SET status='failed', ended_ts=? WHERE id=?",
                            (now_iso(), r["id"]))
                _event(con, "conductor", "harvest-fail", f"run:{r['id']}",
                       f"{r['agent_slug']}: unparseable/empty result")
                print(f"  ✗ run #{r['id']} {r['agent_slug']} FAILED (no parseable result "
                      f"— see {r['result_ref']})")
                continue
            cost = res.get("total_cost_usd")
            sess = res.get("session_id")
            status = "failed" if res.get("is_error") else "finished"
            con.execute("""UPDATE runs SET status=?, session_id=?, cost_usd=?, num_turns=?,
                            ended_ts=? WHERE id=?""",
                        (status, sess, cost, res.get("num_turns"), now_iso(), r["id"]))
            if sess:
                con.execute("UPDATE workstreams SET session_id=?, updated_ts=? WHERE slug=?",
                            (sess, now_iso(), r["agent_slug"]))
            if r["task_id"] and cost:
                con.execute("UPDATE tasks SET spent_usd=spent_usd+?, updated_ts=? WHERE id=?",
                            (cost, now_iso(), r["task_id"]))
            _event(con, "conductor", "harvest", f"run:{r['id']}",
                   f"{r['agent_slug']} {status} ${cost or 0:.2f}")
        n_done += 1
        print(f"  ✓ run #{r['id']} {r['agent_slug']} {status}"
              + (f"  ${cost:.2f}" if cost else "") + (f"  session {sess[:8]}…" if sess else ""))
        if r["task_id"]:
            t = _get_task(con, r["task_id"])
            if t and t["budget_usd"] and t["spent_usd"] > t["budget_usd"]:
                print(f"    ⚠ OVER BUDGET: task #{t['id']} ${t['spent_usd']:.2f} > "
                      f"${t['budget_usd']:.2f} — Conductor should stop resuming and escalate")
    print(f"harvested {n_done}/{len(rows)}")
    return 0


def cmd_retire(con, a) -> int:
    agent = con.execute("SELECT * FROM workstreams WHERE slug=?", (a.agent,)).fetchone()
    if not agent:
        print(f"ERROR: no agent {a.agent}", file=sys.stderr)
        return 2
    with write_txn(con):
        con.execute("UPDATE workstreams SET state='PARKED', updated_ts=? WHERE slug=?",
                    (now_iso(), a.agent))
        _event(con, "conductor", "retire", f"agent:{a.agent}", None)
    if a.rm_worktree and agent["worktree"] and agent["repo"]:
        primary = WORKSPACE / agent["repo"]
        subprocess.run(["git", "-C", str(primary), "worktree", "remove", "--force",
                        agent["worktree"]], capture_output=True, text=True)
        print(f"✓ retired {a.agent}; worktree removed")
    else:
        print(f"✓ retired {a.agent}")
    return 0


# ── digest / tick / projections ──────────────────────────────────────────────────────────────
def _digest_data(con) -> dict:
    now = datetime.now(timezone.utc)
    superseded = _superseded_ids(con)
    agents = [dict(r) for r in con.execute("SELECT * FROM workstreams").fetchall()]
    running = {r["agent_slug"]: dict(r) for r in
               con.execute("SELECT * FROM runs WHERE status='running'").fetchall()}
    for ag in agents:
        ag["running_run"] = running.get(ag["slug"])
    tasks = [dict(t) for t in con.execute(
        "SELECT * FROM tasks WHERE state NOT IN ('DONE','PARKED') ORDER BY id").fetchall()]
    for t in tasks:
        t["depends_on"] = _loads(t["depends_on"], [])
        t["evidence"] = _loads(t["evidence"], {})
    unrouted = [dict(m) for m in con.execute(
        """SELECT * FROM messages WHERE requires_action=1 AND routed_ts IS NULL
           AND resolved_ts IS NULL ORDER BY ts""").fetchall()
        if dict(m)["id"] not in superseded]
    decisions = [dict(d) for d in con.execute(
        "SELECT * FROM decisions WHERE resolved_ts IS NULL ORDER BY raised_ts").fetchall()]
    for d in decisions:
        exp = _parse(d["expires_ts"])
        d["expiring_24h"] = bool(exp and exp <= now + timedelta(hours=24))
    # stale: IN_PROGRESS task, no running session, last owner checkpoint older than threshold
    stale_tasks = []
    for t in tasks:
        if t["state"] != "IN_PROGRESS" or not t["owner_slug"] or t["owner_slug"] in running:
            continue
        cp = con.execute("SELECT ts FROM updates WHERE slug=? ORDER BY ts DESC LIMIT 1",
                         (t["owner_slug"],)).fetchone()
        last = _parse(cp["ts"]) if cp else _parse(t["updated_ts"])
        if not last or (now - last) > timedelta(hours=STALE_TASK_HOURS):
            stale_tasks.append({"task": t["id"], "owner": t["owner_slug"],
                                "last_signal": cp["ts"] if cp else t["updated_ts"]})
    # dependency cycles (DFS over open tasks)
    graph = {t["id"]: [d for d in t["depends_on"]] for t in tasks}
    cycles = []
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {k: WHITE for k in graph}
    def dfs(u, path):
        color[u] = GRAY
        for v in graph.get(u, []):
            if color.get(v, BLACK) == GRAY:
                cycles.append(path[path.index(v):] + [v] if v in path else [u, v])
            elif color.get(v) == WHITE:
                dfs(v, path + [v])
        color[u] = BLACK
    for k in list(graph):
        if color[k] == WHITE:
            dfs(k, [k])
    ready = [t["id"] for t in tasks if t["state"] == "QUEUED" and all(
        (_get_task(con, d) or {"state": "MISSING"})["state"] in ("DONE", "PARKED")
        for d in t["depends_on"])]
    over_budget = [t["id"] for t in tasks if t["budget_usd"] and t["spent_usd"] > t["budget_usd"]]
    gate_requests = [dict(m) for m in unrouted
                     if m["to_addr"] == "conductor" and m["kind"] == "request"]
    return {"generated_ts": now_iso(), "agents": agents, "open_tasks": tasks,
            "unrouted_messages": unrouted, "open_decisions": decisions,
            "stale_tasks": stale_tasks, "dependency_cycles": cycles,
            "ready_queued_tasks": ready, "over_budget_tasks": over_budget,
            "gate_requests": gate_requests,
            "running_sessions": [dict(v) for v in running.values()]}


def cmd_digest(con, a) -> int:
    d = _digest_data(con)
    if a.json:
        print(json.dumps(d, indent=2, default=str))
        return 0
    print(f"DIGEST @ {d['generated_ts']}")
    print(f"  agents: {len(d['agents'])} ({len(d['running_sessions'])} running)")
    print(f"  open tasks: {len(d['open_tasks'])} (ready: {d['ready_queued_tasks']} · "
          f"stale: {[s['task'] for s in d['stale_tasks']]} · over-budget: {d['over_budget_tasks']})")
    print(f"  unrouted ⚡ messages: {len(d['unrouted_messages'])} "
          f"(gate requests: {len(d['gate_requests'])})")
    print(f"  open decisions: {len(d['open_decisions'])} "
          f"({sum(1 for x in d['open_decisions'] if x['expiring_24h'])} expiring <24h)")
    if d["dependency_cycles"]:
        print(f"  ⚠ DEPENDENCY CYCLES: {d['dependency_cycles']}")
    return 0


def cmd_render_waiting(con, a) -> int:
    now = now_iso()
    open_d = con.execute(
        "SELECT * FROM decisions WHERE resolved_ts IS NULL ORDER BY raised_ts").fetchall()
    auto = con.execute(
        "SELECT * FROM decisions WHERE auto_resolved=1 AND shown_ts IS NULL "
        "ORDER BY resolved_ts").fetchall()
    nh_tasks = con.execute("SELECT * FROM tasks WHERE state='NEEDS_HUMAN' ORDER BY id").fetchall()
    cat_order = {c: i for i, c in enumerate(
        ("authorization", "verification", "architecture", "taste", "other", None))}
    open_d = sorted(open_d, key=lambda r: (cat_order.get(r["category"], 4), r["raised_ts"]))

    L = ["# WAITING ON YOU — generated by `fleet render-waiting`", "",
         f"> {now} · respond: `fleet resolve --id N --resolution \"…\"` — or just tell the "
         "Conductor. Reversible items auto-proceed on their default at expiry.", ""]
    if auto:
        L += [f"## ✅ Auto-proceeded since your last visit ({len(auto)})", ""]
        for x in auto:
            L.append(f"- **#{x['id']}** {x['question']}  → **{x['default_recommendation']}** "
                     f"(reversible; expired {x['expires_ts'][:16] if x['expires_ts'] else '?'}; "
                     f"undo by telling the Conductor)")
        L.append("")
    L.append(f"## 🔔 Open ({len(open_d)})")
    if not open_d:
        L.append("\n_Inbox zero._")
    for x in open_d:
        cat = x["category"] or "uncategorized — Conductor should bounce or categorize"
        L += ["", f"### [{cat}] #{x['id']} — {x['question']}"]
        if x["detail"]:
            L.append(f"- **Context:** {x['detail']}")
        L.append(f"- **Raised by:** {x['stream'] or '—'}"
                 + (f" · task #{x['task_id']}" if x["task_id"] else "")
                 + f" · {ago(x['raised_ts'])}")
        if x["default_recommendation"]:
            L.append(f"- **Recommendation:** {x['default_recommendation']}")
        if x["reversible"] and x["expires_ts"]:
            overdue = " **(OVERDUE — run `fleet resolve-expired`)**" if x["expires_ts"] <= now else ""
            L.append(f"- **If you do nothing:** auto-proceeds on the recommendation at "
                     f"{x['expires_ts'][:16]}{overdue}")
        else:
            L.append("- **If you do nothing:** waits for you — "
                     + ("no default was given" if not x["default_recommendation"] else "irreversible")
                     + ", it will not auto-proceed"
                     + (" **(OVERDUE)**" if is_stale(x["raised_ts"]) else ""))
        L.append(f"- **Respond:** `fleet resolve --id {x['id']} --resolution \"…\"`")
    if nh_tasks:
        L += ["", f"## ⏸ Tasks blocked on you ({len(nh_tasks)})", ""]
        for t in nh_tasks:
            L.append(f"- #{t['id']} ({t['kind']}) {t['objective']} — owner {t['owner_slug'] or '—'}")
    out = Path(a.out) if a.out else WAITING_OUT
    out.write_text("\n".join(L) + "\n")
    with write_txn(con):
        for x in auto:
            con.execute("UPDATE decisions SET shown_ts=? WHERE id=?", (now, x["id"]))
    print(f"✓ wrote {out} ({len(open_d)} open, {len(auto)} auto-proceeded shown)")
    return 0


def cmd_report(con, a) -> int:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=a.days)).isoformat(timespec="seconds")
    shipped = con.execute(
        """SELECT COUNT(*) FROM events WHERE action='task-state' AND detail LIKE '%→ DONE%'
           AND ts>=?""", (cutoff,)).fetchone()[0]
    founder_touches = con.execute(
        "SELECT COUNT(*) FROM events WHERE actor='founder' AND ts>=?", (cutoff,)).fetchone()[0]
    human_resolved = con.execute(
        "SELECT COUNT(*) FROM decisions WHERE resolved_ts>=? AND auto_resolved=0",
        (cutoff,)).fetchone()[0]
    auto_resolved = con.execute(
        "SELECT COUNT(*) FROM decisions WHERE resolved_ts>=? AND auto_resolved=1",
        (cutoff,)).fetchone()[0]
    bounces = con.execute(
        "SELECT COUNT(*) FROM events WHERE action='gate-bounce' AND ts>=?", (cutoff,)).fetchone()[0]
    spend = con.execute(
        "SELECT COALESCE(SUM(cost_usd),0) FROM runs WHERE started_ts>=?", (cutoff,)).fetchone()[0]
    by_lane = con.execute(
        """SELECT agent_slug, COALESCE(SUM(cost_usd),0) c, COUNT(*) n FROM runs
           WHERE started_ts>=? GROUP BY agent_slug ORDER BY c DESC""", (cutoff,)).fetchall()
    by_model = con.execute(
        """SELECT COALESCE(model,'(default)') m, COALESCE(SUM(cost_usd),0) c, COUNT(*) n
           FROM runs WHERE started_ts>=? GROUP BY m ORDER BY c DESC""", (cutoff,)).fetchall()
    touches_per = (human_resolved + founder_touches) / shipped if shipped else float("nan")
    print(f"REPORT — last {a.days}d")
    print(f"  tasks shipped: {shipped}")
    print(f"  human touches: {human_resolved} decisions + {founder_touches} direct "
          f"→ {touches_per:.1f}/shipped" if shipped else
          f"  human touches: {human_resolved} decisions + {founder_touches} direct (0 shipped)")
    print(f"  auto-proceeded decisions: {auto_resolved} · gate bounces: {bounces}")
    print(f"  spend: ${spend:.2f}")
    for r in by_lane:
        print(f"    {r['agent_slug']:<30} ${r['c']:.2f}  ({r['n']} runs)")
    if by_model:
        print("  spend by model:")
        for r in by_model:
            print(f"    {r['m']:<30} ${r['c']:.2f}  ({r['n']} runs)")
    return 0


def cmd_models(con, a) -> int:
    """Model tracking view: the registry, which agents run on what, spend by model."""
    print("MODEL REGISTRY (alias → launch id; unknown aliases pass through untouched)")
    for alias, m in MODELS.items():
        prof = FLEET_HOME / "profiles" / f"{alias}.md"
        print(f"  {alias:<8} → {m['id']:<30} [{m['tier']:>9}]  "
              f"profile: {'yes' if prof.exists() else 'NO — add profiles/' + alias + '.md'}")
    rows = con.execute(
        "SELECT COALESCE(model,'(default)') m, COUNT(*) n FROM workstreams "
        "WHERE state != 'PARKED' GROUP BY m ORDER BY n DESC").fetchall()
    print("\nACTIVE AGENTS by model")
    for r in rows:
        agents = [x["slug"] for x in con.execute(
            "SELECT slug FROM workstreams WHERE COALESCE(model,'(default)')=? AND state!='PARKED'",
            (r["m"],)).fetchall()]
        print(f"  {r['m']:<10} {r['n']:>3}  {', '.join(agents)}")
    spend = con.execute(
        """SELECT COALESCE(model,'(default)') m, COUNT(*) n, COALESCE(SUM(cost_usd),0) c,
                  COALESCE(AVG(cost_usd),0) avg_c
           FROM runs WHERE status IN ('finished','failed') GROUP BY m ORDER BY c DESC""").fetchall()
    print("\nSPEND by model (all time)")
    for r in spend:
        print(f"  {r['m']:<10} ${r['c']:>8.2f}  ({r['n']} runs, avg ${r['avg_c']:.2f})")
    switches = con.execute(
        "SELECT * FROM events WHERE action='model-switch' ORDER BY ts DESC LIMIT ?",
        (a.limit,)).fetchall()
    if switches:
        print("\nRECENT SWITCHES")
        for e in switches:
            print(f"  {e['ts']}  {e['subject']:<28} {e['detail']}")
    return 0


def cmd_tick(con, a) -> int:
    """The MECHANICAL half of the Conductor tick. Judgment (route/drive/gate) stays with the
    Conductor session per conductor/PLAYBOOK.md — this prints its judgment queue."""
    print("── tick: harvest ──")
    cmd_harvest(con, argparse.Namespace())
    print("── tick: expired reversible decisions ──")
    cmd_resolve_expired(con, argparse.Namespace())
    d = _digest_data(con)
    print("── tick: reconcile scan ──")
    for s in d["stale_tasks"]:
        print(f"  ⚠ STALE task #{s['task']} (owner {s['owner']}; last signal {s['last_signal']})")
    for c in d["dependency_cycles"]:
        print(f"  ⚠ DEPENDENCY CYCLE {c}")
    for t in d["over_budget_tasks"]:
        print(f"  ⚠ OVER BUDGET task #{t}")
    if not (d["stale_tasks"] or d["dependency_cycles"] or d["over_budget_tasks"]):
        print("  clean")
    print("── tick: founder queue ──")
    cmd_render_waiting(con, argparse.Namespace(out=None))
    print("── judgment queue (for the Conductor, per PLAYBOOK.md) ──")
    print(f"  route: {len(d['unrouted_messages'])} unrouted ⚡ message(s)"
          + (f" — ids {[m['id'] for m in d['unrouted_messages']]}" if d["unrouted_messages"] else ""))
    print(f"  gate:  {len(d['gate_requests'])} request(s) to conductor"
          + (f" — msg ids {[m['id'] for m in d['gate_requests']]}" if d["gate_requests"] else ""))
    print(f"  drive: ready QUEUED tasks {d['ready_queued_tasks'] or '—'}; "
          f"idle agents with open tasks: "
          f"{[t['owner_slug'] for t in d['open_tasks'] if t['state'] == 'IN_PROGRESS' and t['owner_slug'] and not next((x for x in d['running_sessions'] if x['agent_slug'] == t['owner_slug']), None)] or '—'}")
    print(f"  check reality: `gh pr list` / `gh run view` for branches "
          f"{sorted({t['branch'] for t in d['open_tasks'] if t['branch']}) or '—'}")
    return 0


# ── parser ───────────────────────────────────────────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="fleet", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("brief", help="session-start: the focused+complete view for one agent")
    s.add_argument("--as", dest="slug", required=True)
    s.set_defaults(fn=cmd_brief)

    s = sub.add_parser("set", help="create/update an agent row (atomic, locked)")
    s.add_argument("--slug", required=True)
    for f in ("owner", "layer", "state", "status", "next", "blockers", "branch", "product",
              "model", "repo", "worktree", "session-id", "lifetime", "role-prompt"):
        s.add_argument(f"--{f}", dest=f.replace("-", "_"))
    s.set_defaults(fn=cmd_set)

    s = sub.add_parser("checkpoint", aliases=["cp"], help="journal a hand-back + refresh snapshot")
    s.add_argument("--as", dest="slug", required=True)
    for f in ("did", "files", "found", "next", "review", "blockers", "state", "ref"):
        s.add_argument(f"--{f}")
    s.add_argument("--ask", action="store_true", help="also raise --review as a decision")
    s.set_defaults(fn=cmd_checkpoint)

    s = sub.add_parser("msg", help="post an addressed message")
    s.add_argument("--from", dest="from_stream", required=True)
    s.add_argument("--to", required=True, help="all | <slug> | <layer> | conductor")
    s.add_argument("--tier", default="FYI", choices=TIERS)
    s.add_argument("--kind", default="fyi", choices=MSG_KINDS)
    s.add_argument("--body", required=True)
    s.add_argument("--ref")
    s.add_argument("--requires-action", action="store_true")
    s.add_argument("--supersedes", type=int, help="retract/replace an earlier message id")
    s.add_argument("--verified", action="store_true", help="claim is machine-verified (cite in --ref)")
    s.set_defaults(fn=cmd_msg)

    s = sub.add_parser("decide", help="raise a human decision (WITH a default when possible)")
    s.add_argument("--stream")
    s.add_argument("--question", required=True)
    s.add_argument("--detail")
    s.add_argument("--default", help="recommended resolution")
    s.add_argument("--reversible", action="store_true")
    s.add_argument("--expires", type=float, help="hours until auto-proceed (reversible only)")
    s.add_argument("--category", choices=DECISION_CATEGORIES)
    s.add_argument("--task", type=int, help="task this decision blocks")
    s.set_defaults(fn=cmd_decide)

    s = sub.add_parser("resolve", help="close a decision (delivers --resolution to the raiser)")
    s.add_argument("--id", type=int, required=True)
    s.add_argument("--resolution")
    s.add_argument("--actor", default="founder")
    s.set_defaults(fn=cmd_resolve)

    s = sub.add_parser("resolve-expired", help="auto-proceed expired reversible decisions")
    s.set_defaults(fn=cmd_resolve_expired)

    s = sub.add_parser("ack", help="acknowledge a persistent blocker/protocol item")
    s.add_argument("--as", dest="slug", required=True)
    s.add_argument("--id", type=int, required=True)
    s.set_defaults(fn=cmd_ack)

    s = sub.add_parser("route", help="conductor: mark a ⚡ message disposed")
    s.add_argument("--msg", type=int, required=True)
    s.add_argument("--note")
    s.set_defaults(fn=cmd_route)

    s = sub.add_parser("inspect", help="read-only deep view of one agent (cursor untouched)")
    s.add_argument("--slug", required=True)
    s.add_argument("--limit", type=int, default=8)
    s.set_defaults(fn=cmd_inspect)

    s = sub.add_parser("status", help="whole-fleet digest (human)")
    s.set_defaults(fn=cmd_status)

    s = sub.add_parser("log", help="checkpoint journal")
    s.add_argument("--as", dest="slug")
    s.add_argument("--days", type=int)
    s.add_argument("--limit", type=int, default=30)
    s.set_defaults(fn=cmd_log)

    s = sub.add_parser("events", help="conductor action log")
    s.add_argument("--limit", type=int, default=40)
    s.set_defaults(fn=cmd_events)

    t = sub.add_parser("task", help="task graph").add_subparsers(dest="task_cmd", required=True)
    s = t.add_parser("add")
    s.add_argument("--objective", required=True)
    s.add_argument("--kind", required=True, choices=TASK_KINDS)
    s.add_argument("--done", required=True, help="falsifiable done criteria")
    s.add_argument("--owner"); s.add_argument("--repo"); s.add_argument("--territory")
    s.add_argument("--branch"); s.add_argument("--depends"); s.add_argument("--dont-touch")
    s.add_argument("--context", action="append", help="'<ptr> :: <why>' (repeatable)")
    s.add_argument("--budget", type=float); s.add_argument("--parent", type=int)
    s.add_argument("--actor")
    s.set_defaults(fn=cmd_task_add)
    s = t.add_parser("list")
    s.add_argument("--state", choices=TASK_STATES); s.add_argument("--owner")
    s.set_defaults(fn=cmd_task_list)
    s = t.add_parser("show")
    s.add_argument("--id", type=int, required=True)
    s.set_defaults(fn=cmd_task_show)
    s = t.add_parser("assign")
    s.add_argument("--id", type=int, required=True)
    s.add_argument("--owner"); s.add_argument("--branch"); s.add_argument("--budget", type=float)
    s.add_argument("--territory"); s.add_argument("--repo")
    s.set_defaults(fn=cmd_task_assign)
    s = t.add_parser("evidence")
    s.add_argument("--id", type=int, required=True)
    s.add_argument("--test-cmd"); s.add_argument("--pr"); s.add_argument("--ci-url")
    s.add_argument("--notes"); s.add_argument("--review-requested", action="store_true")
    s.set_defaults(fn=cmd_task_evidence)
    s = t.add_parser("set-state")
    s.add_argument("--id", type=int, required=True)
    s.add_argument("--state", required=True, choices=TASK_STATES)
    s.add_argument("--actor", required=True, help="your slug, or conductor/founder")
    s.add_argument("--force", action="store_true")
    s.set_defaults(fn=cmd_task_set_state)

    s = sub.add_parser("verify", help="conductor gate: re-run claimed evidence in the worktree")
    s.add_argument("--task", dest="id", type=int, required=True)
    s.add_argument("--timeout", type=int, default=600)
    s.set_defaults(fn=cmd_verify)

    s = sub.add_parser("review-policy", help="deterministic tier-1 review trigger check")
    s.add_argument("--task", dest="id", type=int, required=True)
    s.add_argument("--diff-lines", type=int)
    s.set_defaults(fn=cmd_review_policy)

    s = sub.add_parser("starter", help="render a task's starter prompt")
    s.add_argument("--task", type=int, required=True)
    s.add_argument("--agent"); s.add_argument("--model")
    s.set_defaults(fn=cmd_starter)

    s = sub.add_parser("spawn", help="worktree + headless worker for a task")
    s.add_argument("--task", type=int, required=True)
    s.add_argument("--agent"); s.add_argument("--model")
    s.add_argument("--product", choices=PRODUCTS)
    s.add_argument("--lifetime", choices=LIFETIMES)
    s.add_argument("--role-prompt", help="'path[#heading],…' standing role spec (persists on the agent)")
    s.add_argument("--max-turns", type=int, default=40)
    s.add_argument("--permission-mode", default="acceptEdits")
    s.add_argument("--dry-run", action="store_true")
    s.add_argument("--no-launch", action="store_true")
    s.set_defaults(fn=cmd_spawn)

    s = sub.add_parser("run", help="resume an agent's session with routed input")
    s.add_argument("--agent", required=True)
    s.add_argument("--input", required=True, help="text, or '-' for stdin")
    s.add_argument("--task", type=int)
    s.add_argument("--model", help="switch the agent's model (persists; works on resume)")
    s.add_argument("--fresh", action="store_true", help="start a new session instead of resuming")
    s.add_argument("--max-turns", type=int, default=40)
    s.add_argument("--permission-mode", default="acceptEdits")
    s.set_defaults(fn=cmd_run)

    s = sub.add_parser("harvest", help="collect finished background runs into the ledger")
    s.set_defaults(fn=cmd_harvest)

    s = sub.add_parser("retire", help="park an agent (optionally remove its worktree)")
    s.add_argument("--agent", required=True)
    s.add_argument("--rm-worktree", action="store_true")
    s.set_defaults(fn=cmd_retire)

    s = sub.add_parser("models", help="model registry, per-agent assignment, spend by model")
    s.add_argument("--limit", type=int, default=10)
    s.set_defaults(fn=cmd_models)

    s = sub.add_parser("digest", help="the Conductor's one-call gather")
    s.add_argument("--json", action="store_true")
    s.set_defaults(fn=cmd_digest)

    s = sub.add_parser("tick", help="mechanical tick: harvest+expire+reconcile+queue")
    s.set_defaults(fn=cmd_tick)

    s = sub.add_parser("render-waiting", help="regenerate WAITING_ON_YOU.md")
    s.add_argument("--out")
    s.set_defaults(fn=cmd_render_waiting)

    s = sub.add_parser("report", help="weekly founder report")
    s.add_argument("--days", type=int, default=7)
    s.set_defaults(fn=cmd_report)
    return ap


def main(argv=None) -> int:
    a = build_parser().parse_args(argv)
    con = connect()
    try:
        return a.fn(con, a)
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
