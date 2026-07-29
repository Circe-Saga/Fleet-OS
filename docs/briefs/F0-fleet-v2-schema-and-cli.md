# BRIEF F0: Fleet-OS repo, schema v2, task/digest/starter CLI

Class: new-repo-foundation      Repo: `<workspace>/Fleet-OS` (create)

Design: `docs/DESIGN.md` §4.1, §5, §10.

## INSPECT FIRST

- `<workspace>/acme-evallab/evals/scripts/fleet.py`
  - The v1 substrate being ported. Keep verbatim where proven: `connect`/`write_txn` (WAL +
    BEGIN IMMEDIATE, fleet.py:150-171), `addressed_to` (174), `cmd_brief`/`_brief_data`
    (339-434, incl. the ✅ ANSWERED show-once delivery), `cmd_checkpoint` (209), `cmd_resolve`
    un-stick logic (287-321), `cmd_inspect` cursor-preserving read (441).
- `<workspace>/acme-evallab/docs/FLEET_COORDINATION_DESIGN.md`
  - §3 writers-address-correctly; §7 concurrency model. Both principles carry over unchanged.

## DO NOT CHANGE

- Do not break v1 command signatures (`brief`, `set`, `checkpoint`, `msg`, `decide`, `resolve`,
  `ack`, `status`, `log`, `inspect`) — v1 agents may cut over with muscle memory intact.
- Do not store anything observable from git/GitHub (no PR-state, CI-state, merged columns).
- Do not put prose in the DB — envelopes, queues, cursors, ledgers only.
- Do not touch `acme-evallab`'s fleet.py or fleet.db; the workspace fleet is a parallel install.

## SCHEMA DELTAS

Exactly the DDL in design §4.1: new tables `tasks`, `runs`, `events`; new columns on
`workstreams` (product, model, repo, worktree, session_id, lifetime), `messages` (kind,
requires_action, supersedes_id, verified, routed_ts), `decisions` (default_recommendation,
reversible, expires_ts, auto_resolved, category). All column adds idempotent (guard with
`PRAGMA table_info`). DB path: `$FLEET_DB` env else `<Fleet-OS>/fleet.db`.

## COMMANDS (new)

- `fleet task add --objective … --kind … --done "…" [--repo --territory --owner --branch
  --depends 1,2 --dont-touch … --context "ptr :: why" (repeatable) --budget N --parent N]`
- `fleet task list [--state S] [--owner slug]` · `fleet task show --id N`
- `fleet task set-state --id N --state S --actor <slug|conductor|founder>` — IN_REVIEW/DONE
  refuse unless actor is conductor/founder AND evidence verified (design §5 table).
- `fleet task evidence --id N [--test-cmd …] [--pr …] [--ci-url …]` — worker *claims*.
- `fleet digest --json` — agents+runs, tasks by state, unrouted requires_action messages,
  open/expiring decisions, stale checkpoints, per-task spend. The Conductor's one-call input.
- `fleet starter --task N` — render the starter prompt (design §4.6 template) from task row +
  agent row + `profiles/<model>.md` + unread addressed items.
- `fleet route --msg N` — conductor marks a requires_action message disposed (`routed_ts`).
- `fleet resolve-expired` — auto-resolve expired reversible decisions to their default; log events.
- `fleet events [--limit N]` — read the Conductor action log.
- `msg` gains `--kind --requires-action --supersedes N`; superseded items drop out of all
  unread briefs and show as retractions once. `decide` gains `--default --reversible
  --expires <hours> --category`.

## TESTS / CONTRACTS

`tests/test_fleet.py` (stdlib unittest, `FLEET_DB` → tmpdir): schema idempotency (connect
twice); task lifecycle happy path + gated-transition refusal (worker actor cannot DONE;
conductor without verified evidence cannot IN_REVIEW); supersedes removes a message from a
brief; expired reversible decision auto-resolves with event row; digest JSON has every
section; concurrent `msg` writes from two connections both land.

## GOLDEN PATH

```
fleet task add --objective "port decode-race fix" --kind implement --repo acme-editor \
  --done "atomic temp+rename in prod decode path; regression test green" --budget 8
fleet task set-state --id 1 --state IN_PROGRESS --actor conductor
fleet task evidence --id 1 --test-cmd "pytest tests/test_decode_race.py"
fleet task set-state --id 1 --state IN_REVIEW --actor conductor   # refused until verified
```

## FAILURE CASE

A worker runs `fleet task set-state --id 1 --state DONE --actor lane-render` → exit 2 with
"workers propose; the Conductor disposes" and the exact `msg --kind request` incantation.

## REVIEW FOCUS

Transition-gate enforcement actually checks `evidence.verified_by_conductor`; supersedes
filtering can't hide an *unrelated* message; ALTERs are idempotent on an existing v1 DB.
