-- Fleet-OS v4: work hierarchy on the existing flat tasks table (architecture §2.1, decision D-2).
--
-- No separate initiatives/epics tables (founder decision D-2, 2026-07-25): one row per work item,
-- with `level` naming its rung in the initiative→epic→story→task tree and the long-dormant
-- `parent_id` column finally given meaning as the edge to its parent row. Roll-ups over the tree
-- are a recursive CTE (fleet.py _subtree_rollups); parent-level ordering is validated in
-- cmd_task_add rather than by a DB foreign key — SQLite can't ALTER-ADD one in place and legacy
-- rows predate the constraint, so parent_id stays a plain INTEGER self-reference the app polices.
--
-- Portable across both backends: ALTER TABLE ADD COLUMN with a CHECK + DEFAULT is accepted by
-- SQLite and Postgres alike, and the DEFAULT backfills every existing row to 'task' (leaves the
-- flat world unchanged). Applied exactly once, tracked in schema_migrations.

ALTER TABLE tasks ADD COLUMN level TEXT NOT NULL DEFAULT 'task'
  CHECK (level IN ('initiative','epic','story','task'));

CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_id);
