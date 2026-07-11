# BRIEF F1: Worker runtime — spawn / run / harvest, worktrees, session ledger

Class: additive      Repo: `<workspace>/Fleet-OS`      Depends: F0

Design: `docs/agent-os-design.md` §4.2. This is the piece that retires hand-tended tabs.

## INSPECT FIRST

- `docs/agent-os-design.md` §4.2 (spawn mechanics, resume semantics, cross-product rule).
- Memory/gotcha: primary checkouts under `<workspace>/` sit on stale lane branches with other
  agents' WIP — worktrees MUST branch from `origin/main` after a `git fetch`, never from the
  primary tree's HEAD.
- `claude -p --output-format json` result shape: `{type:"result", is_error, result,
  session_id, total_cost_usd, num_turns}`. Resume: `claude -p --resume <session_id> "<input>"`.

## DO NOT CHANGE

- Never run a worker in a primary checkout. Worktrees live at `<workspace>/.worktrees/<slug>--<repo>`.
- Do not parse worker transcripts; the ledger stores the result JSON path (`result_ref`), not prose.
- Do not auto-retry a failed run — that's a Conductor judgment (tick), not runner behavior.
- No daemons. `spawn`/`run` are fire-and-forget subprocesses; `harvest` collects.

## COMMANDS

- `fleet spawn --task N [--agent slug] [--model M] [--product claude|codex] [--permission-mode
  acceptEdits] [--max-turns 40] [--dry-run] [--no-launch]`
  1. Resolve/create agent row (slug defaults from task: `<kind>-<repo>-t<N>` for ephemerals).
  2. `git fetch origin` in the primary repo, then `git worktree add
     ../.worktrees/<slug>--<repo> -b <branch> origin/main` (branch = task.branch or
     `agent/<slug>/t<N>`). Idempotent if the worktree exists.
  3. Write worker `.claude/settings.json` from `templates/worker-settings.json` (F3 hooks),
     substituting FLEET_HOME.
  4. Launch: env `FLEET_SLUG`, `FLEET_DB`, `FLEET_TASK`; cwd = worktree; stdout →
     `runs/<slug>-<ts>.json`; detached Popen; record `runs` row (status=running, pid).
     `--dry-run` prints the exact command instead.
- `fleet run --agent slug (--input "…" | --input -)` — resume `agents.session_id` with routed
  input, same ledger mechanics. Refuse if a run for this agent is already `running`.
- `fleet harvest` — for each running row: pid alive → skip; dead → parse the output JSON,
  update runs (status, cost_usd, num_turns, session_id), `agents.session_id`, and
  `tasks.spent_usd += cost`; over-budget tasks flagged in output. Unparseable → status=failed.
- `fleet retire --agent slug [--rm-worktree]` — park agent, optionally `git worktree remove`.

## TESTS / CONTRACTS

Unit-test with a fake launcher (`FLEET_LAUNCHER_CLAUDE="python3 tests/fake_claude.py"` env
override) that emits a canned result JSON: spawn creates worktree (against a scratch git repo)
+ runs row; harvest ingests cost into task.spent_usd; second `run` while running is refused;
budget overrun surfaces in harvest output; `--dry-run` touches nothing.

## GOLDEN PATH

```
fleet spawn --task 1 --agent lane-render --model opus --dry-run   # inspect the command
fleet spawn --task 1 --agent lane-render --model opus             # launch for real
fleet harvest                                                     # after it finishes
fleet run --agent lane-render --input "$(fleet starter --task 2)" # next task, same session
```

## FAILURE CASE

Worker crashes mid-run (pid dies, truncated JSON): harvest marks the run `failed` with
`result_ref` preserved, does NOT touch task state; the Conductor sees it in the next digest
and decides (resume with nudge / respawn — state lives in the DB and git, not the session).

## REVIEW FOCUS

Worktree creation is fetch-then-`origin/main` (the stale-branch gotcha); detached process
survives the spawning shell; harvest is idempotent (re-running never double-counts spend).
