# Adopting this fleet for your own agents

Everything here is generic except where this guide says otherwise. `fleet.py` is a single
stdlib-only file; there is nothing to install. The runtime assumption is one machine, one
workspace directory containing your git repos side by side, and the Claude Code CLI (and/or
Codex CLI) authenticated — the fleet drives agents **exclusively through `claude -p --resume`
headless sessions**, so your existing CLI auth (subscription or key) is the only credential.

## The 15-minute adoption path

1. **Copy the repo next to your own repos** (the fleet expects `FLEET_WORKSPACE` to be the
   parent directory of the repos agents will work in — default: this repo's parent).
2. **Wire the hooks for your own sessions (optional).** The repo intentionally ships no
   `.claude/settings.json` — it can only contain absolute paths to one machine, so it is
   gitignored. Copy `templates/worker-settings.json` to `.claude/settings.json` and replace
   every `FLEET_HOME` with the absolute path to your Fleet-OS checkout. (`spawn` already does
   this substitution for worker worktrees; this step is only for the session *you* drive.)
3. **Smoke it**: `python3 -m unittest discover -s tests` (fake launcher, tmp DBs — free), then
   `python3 tests/e2e.py` (full golden path, simulated worker), then `python3 tests/e2e.py
   --live` for one real headless worker if you want proof on your auth.
4. **File a task and run it**:
   ```bash
   python3 fleet.py task add --objective "…" --kind implement --repo Your-Repo \
       --done "<falsifiable criteria>" --context "path/to/spec.md :: why it matters"
   python3 fleet.py task set-state --id 1 --state IN_PROGRESS --actor conductor
   python3 fleet.py spawn --task 1 --dry-run   # inspect the exact command, then drop --dry-run
   python3 fleet.py tick && python3 fleet.py status
   ```
5. **Read `conductor/PLAYBOOK.md`** and run its tick procedure yourself (or hand it to a
   Claude session acting as your Conductor — that is how it is meant to be run).

## What to replace with your own

| Piece | What it is | What you do |
|---|---|---|
| Leadership role prompts | A role-prompt spec points at a heading in any markdown file: `--role-prompt "path#heading"` | Write your own role docs anywhere in your workspace; point `--role-prompt` at them. The mechanism (section extraction, system-prompt injection on every spawn/resume) is generic. One cabinet file can serve many roles unsplit. |
| `conductor/PLAYBOOK.md` §leadership, §Kestral | Org conventions and a one-way project-tracker mirror | Keep the tick procedure and gates; rewrite the org sections to match your team. If you don't run a tracker mirror, ignore `kestral-*` commands — nothing else depends on them. |
| `PROD_REPOS` / `FLEET_PROD_REPOS` | Repos whose changes trigger tier-1 review | Set `FLEET_PROD_REPOS` to a comma-separated list of your prod-facing repos. Empty (the default) means no repo is treated as prod-facing. |
| `LEASE_DEVICES` / `FLEET_LEASE_DEVICES` | The ONE named device per shared resource (e.g. simulator) lanes may lease | Set `FLEET_LEASE_DEVICES="ios-sim=iPhone17-t111,android-emu=Pixel8"` if you have a shared device workers occasionally need (`fleet lease acquire/release/status`, `conductor/worker-rules.md`). Empty (the default) means no device constraint — leases are still mutex-enforced, just not device-pinned. |
| `profiles/<alias>.md` | Curated per-model gotchas injected into worker starters | Keep the files, replace the content with what *you* learn about each model. Empty profiles are fine (fail-soft). The docs cite **DISCOVERIES** as the source — that is just a house convention for one append-only log of dated, empirically-observed findings, of which a profile is the curated per-model slice; keep your own under any name. |
| `MODELS` registry in `fleet.py` | Alias → model-id map | One line per new model. Unknown aliases pass through untouched, so you can use raw model ids immediately. |
| Budget policy | The reference fleet runs on subscription auth: budgets unset, `cost_usd` treated as telemetry | If you run on API-key billing, set `--budget` on tasks — over-budget stops resumption and escalates (see PLAYBOOK standing rules). |

## The parts worth stealing even if you keep nothing else

- **Workers propose, the Conductor disposes.** Workers cannot set task states; claims become
  DONE only after `fleet verify` re-runs the claimed test command in the worker's own worktree.
- **Enforced-or-absent.** Protocol rules live in hooks (`hooks/` — SessionStart brief
  injection, Stop checkpoint guard, PreCompact reminder) or CLI gates. A rule that is merely
  written down does not exist.
- **Derive, don't store.** Nothing cached that git/GitHub can answer live.
- **Decisions with defaults.** `fleet decide --reversible --default "…" --expires H`
  auto-proceeds; irreversible never does. Your judgment queue drains itself.
- **Worktree isolation.** Every worker gets `.worktrees/<agent>--<repo>` off `origin/main` on
  branch `agent/<slug>/t<N>`; territory overlap between in-flight tasks is refused at spawn.
- **Push before you report.** A worker commits, pushes, and opens its PR (draft if it still needs
  a gate) *before* recording evidence or messaging the Conductor — work stranded uncommitted in a
  worktree is invisible. That plus the "worker never drives a shared/human-owned/non-headless
  resource — it hands the gatekeeper a manual-check note" rule live in `conductor/worker-rules.md`.

## Design rationale

`docs/DESIGN.md` is the full design doc (thesis: agent coordination fails on the *driver*, not
the state layer). `docs/briefs/F0–F5` are the implementation briefs it was built from, in the
house brief format — useful as worked examples of task-shaped agent briefs.
