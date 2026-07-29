# Agent OS — Design

**Status:** PROPOSED (2026-07-06) · **Author:** Claude (Fable 5, design review session) ·
**Decision owner:** the founder ·
**Supersedes-in-part:** the v1 fleet-coordination design (the state layer there
survives intact; this doc adds the layers above it and promotes the system to workspace scope).

> One-line thesis: **the fleet's state layer already works; what was missing is a driver.**
> Nothing wakes an idle agent. A perfectly addressed message in fleet.db does nothing until a
> human presses enter in the right tab — which is why the founder was still hand-copying
> stopping-point messages despite having addressed messages, tiers, and completeness-signaled
> briefs. This design centers on an execution driver (the **Conductor**) that spawns, resumes,
> routes, gates, and escalates — and keeps the state schema as small as that driver actually needs.

---

## 1. What v1 proved and what it didn't

The v1 fleet (fleet.py + fleet.db + BOARD/STATUS + DISCOVERIES) proved:

- **P1. Writers-address-correctly works.** Agents write excellent, correctly addressed handoffs.
  The BOARD's cross-stream inbox is better than most human teams' handoffs.
- **P2. Local SQLite + generated projections is the right substrate.** Concurrent-write safety,
  offline, focused-but-complete briefs — all validated in daily use.
- **P3. Machine-checkable contracts catch what LLM review misses.** `check_contract.py`,
  `check_render.py`, the blank-frame gate — every serious incident ended with a new machine gate,
  and that pattern kept working.

What v1 did **not** solve — the founder's actual overhead:

- **U1.** Copying stopping-point messages between agent sessions.
- **U2.** Telling agents who to update, and pushing blocked agents forward.
- **U3.** Deciding who needed which context; naming tabs; remembering which tab is which.
- **U4.** Being the scheduler: noticing an agent finished, deciding what it does next.

U1–U4 are one problem: **agent sessions are turn-based and human-tended.** State was never the
bottleneck; *delivery and drive* were. So v2 is not "more schema" — it is a driver plus the
minimum schema the driver needs.

Two design commitments follow from failure analysis of v1:

- **C1. Enforced-or-absent.** Any protocol rule that lives only as README prose decays with
  context length and compaction. Every obligation is either mechanically enforced (hooks,
  wrappers, CLI validation, Conductor verification) or deleted from the protocol.
- **C2. Derive, don't store.** Any state that duplicates an observable fact (PR open? CI green?
  merged?) will diverge from reality. The DB stores only what cannot be observed — intent,
  addressing, judgments, budgets. GitHub is queried live via `gh`; git is queried live.

---

## 2. Goals / non-goals

**Goals**

- G1. A feature can go from "founder + Conductor agree on an architecture" to "merged, tested,
  deployed" with human touches only in the five sanctioned categories: human-only decisions,
  authorization, ambiguous product taste, end-to-end verification, architecture violations.
- G2. Workers are machine-driven: spawned, resumed, and retired programmatically. No hand-tended
  tabs, no hand-copied messages, no tab naming.
- G3. Every worker turn starts with a generated, budgeted, complete-for-it brief.
- G4. State transitions that matter (review, merge, done) are **evidence-verified by the
  Conductor**, not self-declared by workers.
- G5. Cost is ledgered per task with ceilings; runaway loops stop themselves.
- G6. The founder's surface is two things: the Conductor conversation and a single
  `WAITING_ON_YOU.md` queue where reversible items auto-proceed on a default after expiry.

**Non-goals**

- N1. Not a general PM tool. No sprints, points, burndown. (Kestral remains the leadership
  mirror, one-way, never load-bearing.)
- N2. Not moving prose into the DB. DISCOVERIES, architecture docs, decision *rationale*,
  workstream narratives stay markdown. The DB is envelopes, queues, cursors, ledgers.
- N3. Not a message bus, daemon, or web service in v2. The Conductor tick is a command; the
  transport is a SQLite file; workers are subprocesses. Event-driven upgrades come later if
  tick latency actually hurts.
- N4. ~~Not multi-machine. One workstation, one DB.~~ **Superseded (E-A):** with `FLEET_DB=postgres://`
  the DB is shared across machines. No HTTP shim was needed — coordination goes through the shared
  DB directly. Machine-scoped rows (runs, leases, an agent's worktree/session) carry a `host`;
  harvest/reap are host-scoped; the lease mutex is a DB partial-unique index; a run's cross-host
  liveness is a heartbeat TTL (its pid is meaningless off-box); verify/resume are host-pinned. The
  governing invariant: a machine-scoped fact is written only by the host that owns it and never
  inferred cross-host from a pid, a worktree path, or a local clock — so anything another host
  must act on is either in the DB or does not exist. See migrations/0002_multihost.sql. SQLite
  (one workstation) stays the default.

---

## 3. Architecture

```
 FOUNDER ──── talks to ────┐                       ┌──── WAITING_ON_YOU.md ◄── founder queue
                           ▼                       │     (decisions w/ defaults; verification
                    ┌─────────────┐                │      items with artifact links)
                    │  CONDUCTOR   │── renders ────┘
                    │ (Claude Code │
                    │  session;    │── queries live ──► gh (PRs, CI, merges) · git (branches)
                    │  stateless   │
                    │  ticks)      │── reads/writes ──► fleet.db  (SQLite, WAL — the record)
                    └──────┬──────┘                        │
            spawns/resumes │ harvests                      │ brief/checkpoint/msg (CLI)
                           ▼                               │
        ┌──────────────────────────────────────┐           │
        │  WORKERS — headless resumable sessions│◄──────────┘
        │  `claude -p --resume …` / `codex exec`│
        │  one git WORKTREE per (agent, repo)   │
        │  hooks enforce brief + checkpoint     │
        └──────────────────────────────────────┘
                           │
                           ▼
              PRs / commits / artifacts / CI  (reality; never cached in the DB)
```

Authority model (single-writer discipline, which removes most consistency questions):

| Actor | May write |
|---|---|
| Workers | messages, checkpoints, their own agent row, `decide` proposals, task evidence *claims* |
| Conductor | task states, routing, spawns/retires, decision auto-resolutions, WAITING_ON_YOU, events |
| Founder | decision resolutions, task creation/kill, anything (root) |

Workers **propose**; the Conductor **disposes**. Workers never transition a task to
`IN_REVIEW`/`DONE` themselves and never contact the founder directly — escalation flows through
the Conductor.

---

## 4. Components

### 4.1 State layer — fleet.db v2 (delta, not rewrite)

Home moves to workspace scope: `<workspace>/Fleet-OS/fleet.db` (see §14). Existing tables
(`workstreams`, `messages`, `decisions`, `updates`, `read_state`) survive; deltas:

```sql
-- The spine. Tasks flow; agents execute. Agents do not "own" a state machine — tasks do.
CREATE TABLE tasks (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  objective     TEXT NOT NULL,             -- one line
  kind          TEXT NOT NULL CHECK (kind IN
                  ('implement','review','test','deploy','doc','investigate','decide')),
  state         TEXT NOT NULL DEFAULT 'QUEUED' CHECK (state IN
                  ('QUEUED','IN_PROGRESS','IN_REVIEW','BLOCKED','NEEDS_HUMAN','DONE','PARKED')),
  owner_slug    TEXT,                      -- agent slug; NULL = unassigned
  repo          TEXT,                      -- e.g. 'acme-api'
  territory     TEXT,                      -- path globs within repo; Conductor enforces no
                                           -- overlap between concurrently IN_PROGRESS tasks
  branch        TEXT,
  depends_on    TEXT NOT NULL DEFAULT '[]',-- JSON array of task ids
  done_criteria TEXT NOT NULL,             -- falsifiable, written at creation
  dont_touch    TEXT,
  context_refs  TEXT NOT NULL DEFAULT '[]',-- JSON array of {path|pr|doc, why} pointers
  evidence      TEXT NOT NULL DEFAULT '{}',-- JSON {test_cmd, test_output_ref, pr, ci_url,
                                           --       verified_by_conductor: bool, verified_ts}
  budget_usd    REAL, spent_usd REAL NOT NULL DEFAULT 0,
  parent_id     INTEGER,                   -- decomposition lineage
  created_ts    TEXT NOT NULL, updated_ts TEXT NOT NULL
);

-- Session ledger: one row per headless worker run. THE cost + liveness record.
CREATE TABLE runs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  agent_slug    TEXT NOT NULL,
  task_id       INTEGER,
  product       TEXT NOT NULL CHECK (product IN ('claude','codex')),
  session_id    TEXT,                      -- resumable session id from the CLI
  status        TEXT NOT NULL CHECK (status IN ('running','finished','failed','abandoned')),
  cost_usd      REAL, num_turns INTEGER,
  result_ref    TEXT,                      -- path to captured JSON output
  started_ts    TEXT NOT NULL, ended_ts TEXT
);

-- workstreams → agents (extend in place; slug stays primary key):
ALTER TABLE workstreams ADD COLUMN product     TEXT DEFAULT 'claude'; -- claude | codex
ALTER TABLE workstreams ADD COLUMN model      TEXT;                  -- e.g. 'opus', 'fable'
ALTER TABLE workstreams ADD COLUMN repo       TEXT;
ALTER TABLE workstreams ADD COLUMN worktree   TEXT;                  -- its isolated checkout
ALTER TABLE workstreams ADD COLUMN session_id TEXT;                  -- current resumable session
ALTER TABLE workstreams ADD COLUMN lifetime   TEXT DEFAULT 'territory'; -- territory | ephemeral

-- messages: typed envelopes (payload stays prose + refs — v1's proven form):
ALTER TABLE messages ADD COLUMN kind            TEXT DEFAULT 'fyi';
  -- 'handoff' | 'request' | 'fyi' | 'contract-change' | 'review-finding' | 'retraction'
ALTER TABLE messages ADD COLUMN requires_action INTEGER DEFAULT 0;
ALTER TABLE messages ADD COLUMN supersedes_id   INTEGER;  -- retract/replace an earlier claim
ALTER TABLE messages ADD COLUMN verified        INTEGER DEFAULT 0; -- machine-verified vs claimed

-- decisions: defaults + expiry = the queue that drains itself:
ALTER TABLE decisions ADD COLUMN default_recommendation TEXT;
ALTER TABLE decisions ADD COLUMN reversible  INTEGER DEFAULT 0;
ALTER TABLE decisions ADD COLUMN expires_ts  TEXT;
ALTER TABLE decisions ADD COLUMN auto_resolved INTEGER DEFAULT 0;
ALTER TABLE decisions ADD COLUMN category TEXT;  -- 'authorization'|'taste'|'architecture'|
                                                 -- 'verification'|'other'

-- Conductor action log (append-only; the founder's audit surface for Conductor judgment):
CREATE TABLE events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  actor TEXT NOT NULL,            -- 'conductor' | slug | 'founder'
  action TEXT NOT NULL,           -- 'route','spawn','resume','gate-pass','gate-bounce',
                                  -- 'auto-resolve','escalate','retire','reconcile'
  subject TEXT,                   -- task:12 | msg:44 | agent:lane-f-audio | decision:7
  detail TEXT, ts TEXT NOT NULL
);
```

Deliberately **absent** (v1 review conclusions, held): `context_packets` (generated views, never
stored — they rot), `artifacts` (GitHub is queried live), `capability_profiles` (markdown:
`Fleet-OS/profiles/<model>.md`, sourced from DISCOVERIES entries), `handoffs`/`blockers`/
`spawn_requests` as tables (they are message kinds / task rows). Every column above has a named
consumer in §4.2–4.6; anything unconsumed after two weeks of operation gets dropped.

### 4.2 Worker runtime — headless, resumable, isolated

The mechanism that retires U1–U4. One command surface:

```
fleet spawn  --task <id> [--agent <slug>] [--model <m>] [--product claude|codex]
fleet run    --agent <slug> --input -        # resume the agent's session with routed input
fleet harvest                                # collect finished background runs → ledger
```

**Spawn** (new agent or ephemeral): create a worktree from a **fresh `origin/main`** —
never a primary checkout; the workspace's standing gotcha (stale lane branches with other
agents' WIP in primary trees) becomes structurally impossible —

```
git -C <workspace>/<repo> worktree add ../.worktrees/<slug>--<repo> -b <branch> origin/main
```

then render the starter prompt (§4.6) and launch headless:

```
FLEET_SLUG=<slug> claude -p "$(fleet starter --task <id>)" \
   --output-format json --max-turns 40 [--permission-mode acceptEdits] \
   > runs/<slug>-<ts>.json          # run in background; harvest on tick
```

The JSON result carries `session_id`, `total_cost_usd`, `num_turns`, `is_error` → written to
`runs` + `agents.session_id`. **Resume** re-enters the same session with new routed input:
`claude -p --resume <session_id> "<rendered brief>"`. Codex workers use `codex exec` /
`codex exec resume` symmetrically; the `product` column picks the launcher. This is the
cross-product answer: *the CLI + DB is the lingua franca; any agent that can run a shell
participates; anything that can't (browser agents) goes through the Conductor as gateway.*

Session-resume gives workers durable identity without tabs. Tab naming is moot: nobody looks at
tabs; the founder looks at the Conductor and the queue. Long-lived **territory agents**
(fastapi, webapp, render…) accumulate judgment across many tasks in one resumable session
(compacting as needed — checkpoints + brief re-ground them, v1's proven pattern);
**ephemeral agents** (a review, a migration) spawn, finish, and are retired with their worktree
removed.

### 4.3 The Conductor — stateless ticks, single authority

A Claude Code session (the only one the founder addresses) with a playbook skill. The tick is
re-runnable, idempotent, and derives everything fresh — a crashed or compacted Conductor loses
nothing but its conversational charm:

```
fleet tick        # deterministic gather → Conductor reasons → actions → events logged
```

1. **Gather** — `fleet digest --json`: agents + current runs, tasks by state, unrouted
   `requires_action` messages, open/expiring decisions, stale checkpoints, per-task spend;
   plus live `gh pr list/checks` for tracked branches.
2. **Reconcile** — derived truths override stored state: PR merged → task DONE-candidate;
   CI red → bounce to owner with the failure; branch deleted → flag. Dependency cycle
   detection on `tasks.depends_on` → break or escalate. A task IN_PROGRESS with no running
   session and no checkpoint for N hours → stale → resume-with-nudge or reassign.
3. **Route** — every `requires_action` message gets a disposition: fold into the recipient's
   next brief, spawn a task, or supersede. Retractions (`supersedes_id`) knock the superseded
   item out of every unread brief — the mechanism that stops confident-wrong claims from
   propagating (the green-frame "it's the xfade → no, decode race" case is the canonical
   example this must handle).
4. **Drive** — for each agent with pending input and no running session: render brief → resume.
   For each QUEUED task with satisfied deps: assign to territory owner or spawn (policy §8).
5. **Gate** — tasks whose owners claim review/done: **verify evidence before granting the
   transition.** Claimed evidence is a lead; verified evidence is a gate: the Conductor re-runs
   `evidence.test_cmd` in the worker's worktree (or checks `ci_url` via `gh run view`) and only
   then sets `verified_by_conductor`. Sincere-but-wrong "my tests pass" is expected LLM
   behavior, not an anomaly; the design assumes it.
   The cwd contract (task #50): `fleet verify` runs in the OWNER's registered worktree
   (`workstreams.worktree`), **full stop** — no fallback to the fleet dir or a primary
   checkout; an empty/missing registration is an exit-2 config error with the remedy printed,
   never a gate bounce. `fleet spawn` persists the worktree it creates or reuses onto the
   agent row (repo resolved from the task's `--repo`, the agent row, or the territory's
   `Repo:` prefix, in that order) and never clobbers a `fleet set --worktree` registration.
   Multi-repo territories: the first-listed repo is the owner's; evidence cmds reference
   sibling repos relative to the owner's worktree (`../../<Repo>/…`).
6. **Escalate** — regenerate `WAITING_ON_YOU.md`; auto-resolve expired reversible decisions to
   their default (logged, reported); push-notify only `authorization` and prod-deploy items.
7. Every action → one `events` row. The founder samples the event log weekly; any worker may
   flag a Conductor decision as wrong (a legitimate `request` message — the Conductor is not
   exempt from the system's own skepticism).

Cadence: manual at first (the founder typing "tick" is the correct MVP of a daemon), then a
/loop or cron every 15–30 min, then event-driven (PR webhooks) only if latency actually hurts.

### 4.4 Evidence & gates

- **Tier 0 (always, free):** CI, contract/schema checkers, tests. Growing this tier is the
  highest-ROI review investment; every incident postmortem asks "what machine gate would have
  caught this" (the v1 fleet already lives this — generalize it per repo).
- **Tier 1 (LLM review, triggered):** auto-created `review` task when a PR is prod-facing,
  touches a locked contract/schema_id, touches auth/privacy/consent, exceeds ~300 changed
  lines, or the author self-flags. Reviewer = a *different* agent, gets diff + contract +
  done_criteria (not the repo). **Findings must be falsifiable** — each names a failure
  scenario and, where possible, a command demonstrating it. "LGTM" with no executed checks is
  a protocol violation for a reviewer.
- **Tier 2 (founder):** end-to-end product verification and taste — arrives as queue items
  with the artifact attached (render path, staging URL, PR link).

Review is **async and never blocks the author** — it blocks only the merge. The author moves on
to the next task immediately.

### 4.5 Human surface

`WAITING_ON_YOU.md` (workspace root; mirrored to Kestral one-way). One item = one screen:

```
## [taste] Make the new sort order the product default?         expires: never (irreversible)
Context: +6% task completion in the A/B, but turning it on rewrites every existing user's saved
view once, and there is no quiet way back. Built, flag-gated, currently off.
Recommendation: default it on for accounts created from today; leave existing saved views alone.
If you do nothing: stays off (this one waits for you — irreversible).
Respond: fleet resolve --id 9 --resolution "…"        (or tell the Conductor in chat)
```

Reversible items carry `expires_ts` and **auto-proceed on the default**, so the queue drains
itself; the founder's morning is mostly "approve default" taps plus the genuinely human calls.
The Conductor reports weekly: **human-touches per shipped task**, the system's one KPI, which
should trend toward the five sanctioned categories and nothing else.

### 4.6 Protocol enforcement — hooks, not habits (C1)

Per worker worktree (templated by `fleet spawn` into `.claude/settings.json`):

- **SessionStart hook** → runs `fleet brief --as $FLEET_SLUG --context-pack` and injects it.
  The agent cannot forget to read its brief; it never *asks* for it.
- **Stop hook** → blocks ending the turn unless a `fleet checkpoint` row exists newer than
  session start (returns `decision: block` with the reminder + template). Checkpoints stop
  being a habit and become physics.
- **PreCompact hook** → same checkpoint guard before compaction.

The **starter prompt** (rendered by `fleet starter --task <id>`, ~1–2k tokens, pointers not
payloads):

```
You are <slug>, the <role> for <repo>:<territory>. Worktree: <path>, branch: <branch>.
TASK <id>: <objective>
DONE means: <done_criteria — falsifiable>
DO NOT touch: <dont_touch>
MUST read first (complete list — nothing else is addressed to you):
  1. <pointer + one line why>  …
Model notes for you: <2–3 lines from Fleet-OS/profiles/<model>.md>
Protocol (hook-enforced; listed so you know): checkpoint before every stop; blockers via
  `fleet msg --kind request --requires-action`; propose decisions WITH a default.
Escalation: repo/docs → context above → owning agent (fleet msg) → Conductor → proposed
  decision with default. You cannot contact the founder; the Conductor escalates for you.
Budget: $<n> (spent: $<m>). Failing the same way twice? Stop and message the Conductor.
If this brief seems wrong or insufficient, say so via msg — do not go spelunking fleet-wide.
```

Context packets are **generated at session start, never cached**; tiered MUST/MAY with the
completeness signal ("nothing else is addressed to you") inherited from v1's brief.

---

## 5. Task lifecycle

`QUEUED → IN_PROGRESS → IN_REVIEW → DONE`, with `BLOCKED`, `NEEDS_HUMAN`, `PARKED` reachable
from any active state. That's all — seven states. PR-open/CI-wait/merge-ready are **not states**;
they're live GitHub facts the Conductor reads at gate time (C2). Transition rules:

| Transition | Actor | Requirement (verified, not declared) |
|---|---|---|
| QUEUED → IN_PROGRESS | Conductor | deps DONE; territory free; budget assigned; worktree exists |
| IN_PROGRESS → IN_REVIEW | Conductor | evidence.test_cmd re-run green, or ci_url green via gh |
| IN_REVIEW → DONE | Conductor | review task DONE with no blocking findings; PR merged (if any) |
| any → BLOCKED | Conductor | a `request` message names the blocker + who can clear it |
| any → NEEDS_HUMAN | Conductor | an open decision row exists, categorized |
| any → PARKED | Conductor/founder | checkpoint with resume prompt exists |

## 6. Spawn & territory policy

Spawn a **new** agent only on: (a) territory boundary (different repo/branch/owned paths — in v1,
two agents sharing one tree reliably ended up negotiating over one file, which is the cautionary
tale); (b)
independence requirement (review/test must not share the author's context); (c) parallelizable
work with a clean contract seam; (d) owner context saturation (>~60% budget) with a separable
subtask. Otherwise route to the territory owner — a fresh spawn re-derives cold context, the
expensive path. Two concurrently IN_PROGRESS tasks may not overlap territories; the Conductor
enforces at assignment.

## 7. Cost control

Every run's `total_cost_usd` lands in the ledger; `tasks.spent_usd` accumulates. Budget
exceeded → the Conductor stops resuming and escalates with the spend history. Same-failure-twice
→ workers are instructed (and the Conductor watches checkpoints) to stop and ask. Spawns carry
ceilings. The dashboard shows burn per lane.

## 8. Failure modes

| Failure | Mitigation |
|---|---|
| DB state diverges from git/GitHub | C2 derive-don't-store + tick reconcile pass |
| Confident-wrong claim propagates to briefs | `supersedes_id` retractions; `verified` flag on handoffs; provenance in briefs |
| Two agents edit one tree | worktree-per-agent at spawn; territory locks at assignment |
| Deadlock (A waits B waits A) | tick cycle detection on depends_on |
| Livelock (retry same failure forever) | budget ceilings; same-failure-twice rule; checkpoint-watch |
| Cost runaway | per-task ledger + ceilings (§7); weekly burn report |
| Review theater (rubber stamps / nitpicks) | falsifiable-findings rule; machine gates first; reviewer gets contract + done_criteria |
| Human bottleneck relocates (interrupts) | batched queue; defaults + expiry auto-proceed; push only authorization/deploy |
| Conductor bad judgment propagates | stateless ticks (restartable); events log sampled weekly; workers may flag decisions |
| Protocol decay under compaction | hooks re-ground every session start; Stop-hook checkpoint guard (C1) |
| Worker session lost/corrupt | checkpoints + brief re-derive context; spawn fresh with same starter — state is in the DB and git, not the session |

## 9. What we are explicitly not building (yet)

Stored context packets · capability-profile tables · artifacts table · 12-state lifecycle ·
message bus/daemon/web UI · multi-machine transport · browser-controlling manager · Kestral
two-way sync. Each has a trigger condition (e.g., profiles automate only when model gotchas are
being hand-copied into prompts more than weekly). Schema follows pain, never anticipates it.

## 10. Home & migration

New tiny repo **`<workspace>/Fleet-OS/`** (git init; versioned, unlike the workspace root):

```
Fleet-OS/
  fleet.py            # ported from the v1 fleet script + v2 deltas
  fleet.db            # gitignored; THE workspace record
  runs/               # gitignored; captured worker JSON outputs
  profiles/<model>.md # capability notes, curated from DISCOVERIES
  templates/          # starter prompt, worker settings.json (hooks), WAITING_ON_YOU item
  conductor/PLAYBOOK.md  # the Conductor's tick procedure + policies (this doc §4.3–§8 distilled)
  docs/briefs/        # the implementation briefs this was built from (F0–F5)
```

A v1 fleet keeps running as-is during the transition; its DB seeds the workspace DB
(`fleet seed` already exists and is idempotent). Its agents cut over by pointing `FLEET_DB` at
the workspace copy, and the old v1 script becomes a thin shim → retired.

## 11. Rollout — build order (briefs in `docs/briefs/`)

| # | Brief | Delivers | Depends on |
|---|---|---|---|
| F0 | `F0-fleet-v2-schema-and-cli.md` | Fleet-OS repo, schema v2, task/digest/starter/decide-v2 commands | — |
| F1 | `F1-worker-runner.md` | spawn/run/harvest, worktrees, session ledger, cross-product launchers | F0 |
| F2 | `F2-conductor-playbook.md` | Conductor skill + tick procedure + routing/gating policies | F0, F1 |
| F3 | `F3-protocol-hooks.md` | SessionStart/Stop/PreCompact hooks + settings template | F0 |
| F4 | `F4-human-surface.md` | WAITING_ON_YOU renderer, decision defaults/expiry/auto-resolve, notify | F0 |
| F5 | `F5-review-gates.md` | auto review-task policy, evidence verification via gh, falsifiable-findings protocol | F0–F2 |

Acceptance for the whole rollout (the only test that matters): **one real feature** — pick a
small cross-repo one — goes founder-brief → Conductor decomposition → two workers + one reviewer,
headless → merged, with founder touches limited to the five categories. Run that before
polishing anything.

## 12. Founder experience (day in the life)

Morning: open `WAITING_ON_YOU.md` — 3–5 items, mostly "approve default" (the reversible ones
already auto-proceeded overnight and are listed as FYIs with their outcomes). One taste call
with the render attached. One authorization tap.
Midday: one Conductor check-in — "how's the audio lane?" — answered from the digest, not from
memory.
Evening: the verification queue — artifacts the fleet staged for taste review.
Weekly: the Conductor's report — tasks shipped, human-touches per task, spend per lane, gate
bounces, decisions auto-proceeded — plus a 10-minute sample of the events log to audit the
Conductor's own judgment.

New feature: founder and Conductor flush out the problem and architecture in conversation
(genuinely human-led — categories 3 and 5 by definition); the Conductor writes the plan doc,
decomposes it into a task DAG with done-criteria and territories, and the fleet takes it from
there.

## 13. Open questions

- O1. Conductor model/effort: strongest model at low cadence vs cheaper model at high cadence.
  Start strongest-model manual ticks; measure.
- O2. Worker permission mode: `acceptEdits` in isolated worktrees is probably right (blast
  radius = the worktree + branch); prod-deploy tasks stay gated. Decide per task kind.
- O3. Subscription vs API billing for headless workers — affects ledger semantics
  (`total_cost_usd` is still reported; treat as accounting signal either way).
- O4. When `acme-evallab`'s specialized v1 fleet (stage agents) merges into the workspace fleet vs stays
  a federated island with its own DB. Leaning: federate until the workspace fleet is proven.
