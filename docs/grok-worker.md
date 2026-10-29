# External worker contract (Grok)

`fleet dispatch` hands one investigate task to an external worker. `fleet ingest` records
the result. There is no HTTP listener in this repo: the worker returns the result JSON by
a git push or an email attachment, and the operator runs `ingest`. Dispatch does not change
task state. Ingest does not either. `IN_REVIEW` / `DONE` stay with the Conductor, after
`fleet verify`.

Dispatch is refused unless the task is `kind=investigate` and has no `repo` and no
`territory`. The contract is built only from the fields below. It never includes `FLEET_DB`,
the webhook bearer, a worktree path, or the fleet environment. The bearer lives in
`FLEET_GROK_WEBHOOK_TOKEN` and is sent only as the `Authorization` header.

`runs.product` stays `claude` because that column's check constraint only allows `claude`
and `codex`. The run's `model` is `grok`, which is what `fleet report` groups on.

## Dispatch

```bash
export FLEET_GROK_WEBHOOK_URL='https://example.invalid/routines/fleet'
export FLEET_GROK_WEBHOOK_TOKEN='…'          # never put this in the task text or the URL
python3 fleet.py task set-state --id N --state IN_PROGRESS --actor conductor
python3 fleet.py dispatch --task N
```

`dispatch` POSTs the contract with `Content-Type: application/json` and
`Authorization: Bearer $FLEET_GROK_WEBHOOK_TOKEN`. Redirects are not followed. Proxies are
not used. A non-2xx response marks the run `failed`.

On success the one-time callback token is printed once on stdout and is also inside the
contract (the worker has to hand it back). Only its SHA-256 hex digest is stored. The worker
must show that token to the operator next to the result file. It is not a field of the
result JSON.

While the callback is outstanding the run stays `running` with no pid. `fleet harvest` /
`fleet tick` skip it. After 12 hours with no checkpoint, the existing stale-task signal
fires (`fleet digest` → `stale_tasks`). The run is left open.

### Contract JSON

| Field | Type | Meaning |
|---|---|---|
| `task_id` | int | Fleet task id |
| `run_id` | int | Fleet run id. Pass this to `ingest --run` |
| `objective` | string | Task objective |
| `kind` | string | Always `investigate` |
| `done_criteria` | string | Falsifiable done text. Not a task-state change |
| `dont_touch` | string or null | Exclusions from the task row |
| `context_refs` | array of `{ptr, why}` | Pointers already on the task. Not file contents |
| `budget_usd` | number or null | Task budget, if one was set. Usually null |
| `callback_token` | string | One-time ingest token. Treat as a secret |
| `nonce` | string | Per-dispatch id the worker can echo in its own logs |

No other fields are sent. Example: [`examples/grok-pilot-contract.json`](../examples/grok-pilot-contract.json).

## Result JSON

`fleet ingest --run N --token <callback_token> --file result.json`

The file must be one JSON object, UTF-8, at most 256 KiB, with exactly these keys:

| Field | Type | Meaning |
|---|---|---|
| `status` | `completed` \| `blocked` \| `failed` | Worker outcome. Not a fleet task state |
| `artifact_links` | array of `{type, ref}` | `type` is `git`, `url`, or `email`. `ref` is a non-empty string. May be `[]` |
| `evidence` | `{claim, test_cmd}` | Both strings. `test_cmd` must be non-empty when `status` is `completed` |
| `blockers` | array of `{summary, needs}` | `needs` is `conductor` or `founder`. May be `[]` |
| `cost_usd` | number ≥ 0 | Added to `tasks.spent_usd` once. Not a bool |
| `external_ref` | string | Worker idempotency key (1–200 chars) |

Unknown keys at any level are rejected. Example: [`examples/grok-pilot-result.json`](../examples/grok-pilot-result.json).

### What ingest writes

- The run closes: `completed` and `blocked` → `finished`; `failed` → `failed`. There is no `blocked` run status (the existing check constraint), so a blocker is a message, not a status.
- `cost_usd` is stored on the run and added to the task spend.
- Evidence is merged. `claim`, `test_cmd` (when non-empty), and `artifact_links` are stored. Any previous `verified_by_conductor` flag is cleared. The Conductor must `fleet verify` again.
- A checkpoint is appended for the agent.
- Each blocker becomes one `BLOCKER` / `request` message to `conductor` with `requires_action`. It shows up in `fleet digest` as a gate request. Ingest does not open a founder decision, even when `needs` is `founder`.
- The task `state` column is not updated.

### Idempotency

The callback token is checked on every call (including replays) with a constant-time compare against the stored hash.

- Same `external_ref` as the one already stored: exit 0, no writes.
- A different `external_ref`: exit 2, no writes. Dispatch a new run instead of reusing this one.
- A bad token or a schema error: exit 2, no writes.

## Pilot

Investigate task, no repo. The contract inlines nothing the worker must keep secret. The result's `evidence.claim` is the first heading of `README.md`, and `evidence.test_cmd` re-reads that file. `fleet verify` runs the command in the owner's registered worktree. The worker's JSON is a claim until that command exits 0.
