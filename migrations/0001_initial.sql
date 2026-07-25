-- Fleet-OS initial schema (v2). The complete current shape of every table + index.
--
-- Written in SQLite dialect (INTEGER PRIMARY KEY AUTOINCREMENT, `?`/`:name` handled at runtime).
-- Under Postgres, run_migrations() rewrites AUTOINCREMENT integer PKs to identity columns via
-- _pg_ddl(); the rest (TEXT/REAL/INTEGER, CHECK, DEFAULT, IF NOT EXISTS) is already portable.
--
-- This file supersedes the old per-invocation executescript(SCHEMA) + _MIGRATIONS ALTER loop:
-- it is applied exactly once, tracked in schema_migrations. It folds in every column that used to
-- be added incrementally by _MIGRATIONS, so a fresh DB gets the full schema in one step. Keep the
-- column set here in sync with the _MIGRATIONS reconciliation list in fleet.py.

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
  updated_ts   TEXT NOT NULL,
  product      TEXT DEFAULT 'claude',
  model        TEXT,
  repo         TEXT,
  worktree     TEXT,
  session_id   TEXT,
  lifetime     TEXT DEFAULT 'territory',
  role_prompt  TEXT
);

CREATE TABLE IF NOT EXISTS decisions (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  stream       TEXT,
  question     TEXT NOT NULL,
  detail       TEXT,
  raised_ts    TEXT NOT NULL,
  resolved_ts  TEXT,
  resolution   TEXT,
  kestral_id   TEXT,
  default_recommendation TEXT,
  reversible   INTEGER DEFAULT 0,
  expires_ts   TEXT,
  auto_resolved INTEGER DEFAULT 0,
  category     TEXT,
  shown_ts     TEXT,
  task_id      INTEGER
);

CREATE TABLE IF NOT EXISTS messages (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  from_stream  TEXT NOT NULL,
  to_addr      TEXT NOT NULL,
  tier         TEXT NOT NULL CHECK (tier IN ('FYI','BLOCKER','NEEDS-USER','PROTOCOL')),
  body         TEXT NOT NULL,
  ref          TEXT,
  ts           TEXT NOT NULL,
  resolved_ts  TEXT,
  kind         TEXT DEFAULT 'fyi',
  requires_action INTEGER DEFAULT 0,
  supersedes_id INTEGER,
  verified     INTEGER DEFAULT 0,
  routed_ts    TEXT
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
  updated_ts    TEXT NOT NULL,
  kestral_id    TEXT
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

-- v2: exclusive shared-resource mutex (e.g. one iOS simulator many lanes must take turns on).
-- One row per acquisition (not one row per resource) so the history stays an audit trail;
-- "the" lease on a resource is its newest row with released_ts IS NULL.
CREATE TABLE IF NOT EXISTS leases (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  resource     TEXT NOT NULL,
  holder_slug  TEXT NOT NULL,
  device       TEXT,
  acquired_ts  TEXT NOT NULL,
  ttl_seconds  INTEGER NOT NULL,
  released_ts  TEXT
);

CREATE INDEX IF NOT EXISTS idx_messages_to ON messages(to_addr, ts);
CREATE INDEX IF NOT EXISTS idx_messages_tier ON messages(tier, resolved_ts);
CREATE INDEX IF NOT EXISTS idx_updates_slug ON updates(slug, ts);
CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_leases_resource ON leases(resource, released_ts);
