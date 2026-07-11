# BRIEF F2: Conductor playbook + tick

Class: doc+cli      Repo: `<workspace>/Fleet-OS`      Depends: F0, F1

Design: `docs/agent-os-design.md` §3 (authority model), §4.3 (tick), §6 (spawn policy), §8.

## INSPECT FIRST

- Design §4.3 — the 7-step tick. Steps 1 (gather), 2-mechanical (stale/cycle/expiry), 6-partial
  (render queue) are deterministic → `fleet tick`. Steps 2-judgment, 3 (route), 4 (drive),
  5 (gate) are LLM judgment → the playbook.
- `acme-evallab/docs/workstreams/README.md` "review loop" — the human↔coordinator↔agent loop
  v2 automates; the Conductor inherits the coordinator role with `inspect` (cursor-preserving).

## DO NOT CHANGE

- The Conductor never edits worker code or worktrees; it routes, gates, spawns, escalates.
- Ticks are stateless: everything re-derived from `fleet digest` + `gh`; nothing may depend on
  the Conductor's conversational memory.
- Workers never transition tasks; every transition the Conductor grants cites verified evidence.
- Only `authorization` + prod-deploy items may interrupt the founder push-style.

## DELIVERABLES

1. **`fleet tick`** (mechanical): harvest → resolve-expired → stale-run/stale-checkpoint scan →
   depends_on cycle detection → regenerate WAITING_ON_YOU.md → print a compact judgment queue
   ("N unrouted msgs, N gate requests, N stale, N queued-with-deps-met") for the LLM half.
2. **`conductor/PLAYBOOK.md`** — the procedure a Claude Code session follows per tick:
   - Gather: `fleet tick`, `fleet digest --json`, `gh pr list --json` / `gh run view` per
     tracked branch (reality wins over DB).
   - Route every unrouted requires_action message: fold into recipient brief (`fleet route`),
     spawn a task, or supersede. Retractions knock superseded claims out of unread briefs.
   - Drive: agents with pending input + no running session → `fleet run` with rendered input;
     QUEUED tasks with deps met → assign per spawn policy (§6: territory owner unless
     boundary/independence/parallel-seam/saturation) → `fleet spawn`.
   - Gate: for each IN_REVIEW/DONE request → `fleet verify --task N` (re-run test_cmd) or
     `gh run view <ci_url>`; grant via `task set-state --actor conductor` or bounce with a
     `review-finding` message. Sincere-but-wrong "tests pass" is expected, not an anomaly.
   - Escalate: decisions lacking category/default get bounced back to the raiser once;
     categorize the rest; only authorization/deploy → PushNotification.
   - Log judgment calls as events (`route`, `gate-bounce`, `escalate`, `spawn`); the founder
     samples this weekly.
3. **`fleet verify --task N`** — run `evidence.test_cmd` in the owner's worktree, capture exit
   + tail, set `verified_by_conductor`/`verified_ts` on pass, event `gate-pass`/`gate-bounce`.

## TESTS / CONTRACTS

tick: stale detection fires on an IN_PROGRESS task with no running run and old checkpoint;
cycle A→B→A detected and reported (not crash); verify sets the flag only on exit 0 and
records a gate-bounce event with output tail on failure.

## GOLDEN PATH

Founder types "tick" in the Conductor session → Conductor runs `fleet tick`, routes 2
messages, resumes 1 idle agent, verifies 1 gate request, bounces 1 (test actually red),
regenerates the queue — founder reads a 6-line summary.

## FAILURE CASE

Conductor session compacts or dies mid-tick → next tick re-derives identical state from the
DB + gh; the only loss is the summary prose. Verify by running tick twice: second run is a
no-op (idempotent) apart from timestamps.

## REVIEW FOCUS

No hidden state in the playbook (every instruction consumes digest/gh output, not memory);
verify runs in the *worker's* worktree, not the primary checkout; events written for every
judgment call.
