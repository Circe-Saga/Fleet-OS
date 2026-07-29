# BRIEF F3: Protocol hooks — enforced-or-absent

Class: additive      Repo: `<workspace>/Fleet-OS`      Depends: F0

Design: `docs/DESIGN.md` C1 + §4.6. Any rule living only as README prose decays; these
hooks make brief-reading and checkpointing physics instead of habits.

## INSPECT FIRST

- Claude Code hooks: configured in the worktree's `.claude/settings.json`; hook processes get
  JSON on stdin (`session_id`, `cwd`, `stop_hook_active`, …) and inherit the worker's env
  (so `FLEET_SLUG`, `FLEET_DB` set by `fleet spawn` are visible).
  - SessionStart: stdout is injected as context.
  - Stop: JSON stdout `{"decision":"block","reason":"…"}` forces the agent to continue.
- `acme-evallab/docs/workstreams/README.md` rules 1+2 and the checkpoint habit — exactly what
  gets mechanized here.

## DO NOT CHANGE

- Hooks must be fail-open: any exception, missing env, or missing DB → exit 0 silently. A
  broken hook must never brick a worker session.
- Never auto-write a checkpoint on the agent's behalf — the guard demands one, it doesn't
  fabricate one (a fabricated checkpoint is worse than none).
- Honor `stop_hook_active`: if a previous Stop block already fired this stop, allow the stop
  (block once, remind loudly, never loop).

## DELIVERABLES

1. `hooks/session_start.py` — reads stdin JSON; prints `fleet brief --as $FLEET_SLUG` output
   (→ injected context: the agent cannot forget its brief); writes marker
   `<cwd>/.claude/.fleet_session_start` = now_iso.
2. `hooks/stop_guard.py` — allow if `stop_hook_active`; else query `updates` for a checkpoint
   with `slug=$FLEET_SLUG AND ts >= marker`; missing → block with the checkpoint template
   (`fleet checkpoint --as $FLEET_SLUG --did … --next … [--review … --ask]`).
3. `hooks/precompact_reminder.py` — same query; prints a loud reminder (PreCompact can't block).
4. `templates/worker-settings.json` — wires the three hooks with `FLEET_HOME` substituted by
   `fleet spawn` (F1 step 3).

## TESTS / CONTRACTS

Drive each hook as a subprocess with synthetic stdin JSON + env: no-checkpoint stop → block
JSON with reason; fresh-checkpoint stop → exit 0 empty; `stop_hook_active: true` → allow;
unset FLEET_SLUG → exit 0 (fail-open); session_start writes the marker and emits the brief.

## GOLDEN PATH

Spawned worker finishes a unit of work, tries to end its turn without checkpointing → Stop
hook blocks with the exact command → agent runs `fleet checkpoint …` → stop allowed → harvest
finds the checkpoint the Conductor needs.

## FAILURE CASE

fleet.db locked or missing when stop_guard runs → guard exits 0 (fail-open) and the miss
surfaces later as a stale-checkpoint flag in the Conductor's digest — degraded to v1 behavior,
never a bricked session.

## REVIEW FOCUS

Fail-open on every path; the block reason contains a copy-pasteable command; marker semantics
survive `--resume` (each resumed session rewrites it via SessionStart).
