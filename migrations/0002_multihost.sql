-- Fleet-OS v3: multi-host correctness on the shared (Postgres) backend.
--
-- Under one shared DB the rows that are machine-scoped must say WHICH machine owns them, so a
-- second commander's harvest/reap can be scoped to its own host and never touch another host's
-- live runs or leases (architecture §1.2 items 1-2). And the lease mutex — previously a social
-- read-then-write in fleet.py — becomes DB-enforced (§1.2 item 3).
--
-- Portable across both backends: ALTER TABLE ADD COLUMN and partial (WHERE) unique indexes are
-- supported by SQLite and Postgres alike. Applied exactly once, tracked in schema_migrations.

-- Host identity. runs/leases/workstreams each own resources tied to one machine (a live pid, a
-- device lease, an absolute-path worktree + a resumable session_id). NULL = legacy/unknown,
-- treated as "local" by the liveness code so pre-migration single-machine rows keep working.
ALTER TABLE runs ADD COLUMN host TEXT;
ALTER TABLE workstreams ADD COLUMN host TEXT;
ALTER TABLE leases ADD COLUMN host TEXT;

-- Cross-host liveness. A run's pid is meaningless on another host; heartbeat_ts (bumped by the
-- run's own brief/checkpoint CLI calls) + a TTL is the signal a remote commander uses instead.
ALTER TABLE runs ADD COLUMN heartbeat_ts TEXT;

-- Commander (machine+human) identity on the audit log, distinct from the ROLE in `actor`.
ALTER TABLE events ADD COLUMN commander TEXT;

-- The lease mutex, DB-enforced. At most one UNRELEASED lease per resource; the one-row-per-
-- acquisition audit trail (many released rows) is preserved because the index is partial. Two
-- commanders racing `lease acquire` both INSERT a released_ts-NULL row — exactly one survives,
-- the other gets a unique violation and is told the resource is taken.
CREATE UNIQUE INDEX IF NOT EXISTS uq_leases_active ON leases(resource) WHERE released_ts IS NULL;

CREATE INDEX IF NOT EXISTS idx_runs_host ON runs(host, status);
