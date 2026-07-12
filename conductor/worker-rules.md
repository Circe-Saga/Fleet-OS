# Worker rules

These rules apply to every worker — an implementation lane, a reviewer, a doc author: any agent
that executes a task. The Conductor delivers them to workers (folded into the task starter/context,
or as a `--role-prompt` pointer) and enforces the reciprocal checks in `PLAYBOOK.md`. Where a task's
objective conflicts with a rule here, **the rule overrides the objective.**

## Finish protocol: push and open the PR BEFORE you report

A unit of work is not "done" when the tests pass in your worktree — it is done when the change is
**committed, pushed, and has an open PR.** In order, at the end of every run that produced code:

1. `git commit` your work. (Commit early and often *during* the run too — a turn-cap with
   uncommitted work loses the tail.)
2. `git push` your branch.
3. Open the PR (`gh pr create` or equivalent). If the change still needs a human/Conductor
   verification gate before it can merge, open it as a **draft** and put the gate request in the PR
   body — do NOT hold the push waiting for that gate.
4. ONLY THEN record fleet evidence and message the Conductor.

Why the order is strict: work that is only committed locally, or — worse — left uncommitted in the
worktree, is **invisible.** There is no PR, so the Conductor has nothing to gate and the founder
sees nothing shipped, even though your run reported success. This has actually bitten the fleet:
workers finished green, recorded evidence, messaged the Conductor, and left the changes stranded in
the worktree with no PR. If you are about to run out of turns, `git commit` + `git push` +
`gh pr create` FIRST, then spend any remaining turns on tests and notes. Never end a run with an
unpushed branch.

The Conductor's reciprocal check (`PLAYBOOK.md` §1): a run that reports success but has no PR is
inspected for stranded worktree work rather than trusted.

## Never operate a shared, human-owned, or non-headlessly-verifiable resource

Some verification needs a resource a worker must not drive: a shared device or emulator a human is
logged into, a singleton dev environment, a staging deploy, a production console, or any
visual/manual check that cannot be done headlessly. **That verification is the gatekeeper's job —
the Conductor's or the founder's — not yours.** If your objective tells you to operate such a
resource ("test it live on the shared device", "screenshot each screen", "verify in staging"), skip
that part; booting a second copy of a shared resource can also degrade it for the human who owns it.

What you deliver instead:

1. The code change per your objective.
2. Every check you *can* run headlessly — typecheck, unit/integration tests, the linters and
   formatters CI will enforce — green.
3. The PR (per the finish protocol above), with a **"MANUAL/VISUAL CHECK (for the gatekeeper)"**
   section: exactly what to open or run, the precise expected result, and any before/after you
   predict. The gatekeeper performs that verification and merges.

Budget your turns for CODE and headless checks. Do not spend turns trying to boot or drive a
resource you are not allowed to — you will run out of turns before the work is delivered. Deliver
code + headless-green + the manual-check note, then STOP.

### Exception: leasing a shared device (e.g. a simulator/emulator)

The rule above is **deny-by-default: the Conductor owns visual/manual QA on shared devices.** The
only exception is when YOUR task's brief explicitly grants sim/emulator access — a context ref or
the objective/done-criteria says so in plain terms. If it doesn't say so, you are not granted it;
follow the rule above (manual-check note, skip the device) and don't ask "can I use it" — that's a
brief-quality question for whoever wrote it, not something to negotiate mid-run.

When a brief DOES grant it, the fleet has exactly ONE persistent instance of the shared device —
booting a second one (a second simulator, a second emulator) slows the whole host machine for
every lane sharing it, including yours. Protocol, in order:

1. **Acquire before you touch it.** Before any `xcrun simctl`, `expo run:ios`, or other
   boot/launch of the device: `python3 fleet.py lease acquire --resource <name> --as <slug>`
   (e.g. `--resource ios-sim`). A refusal means someone else holds it — do not boot a second
   instance to work around a busy one; wait, or `fleet msg` them to ask when they'll release it.
2. **Use only the device the lease names.** `lease acquire` refuses a `--device` that doesn't
   match the fleet's pinned device for that resource (`FLEET_LEASE_DEVICES`); omit `--device` and
   it resolves to the pin automatically. Never boot a second device under any name.
3. **Release the moment you're done — success or failure.** `python3 fleet.py lease release
   --resource <name> --as <slug>`, as part of your finish protocol, before you report. A crashed
   or turn-capped run's lease is auto-reaped by the next `fleet tick` (dead holder run → released),
   but that is a safety net, not a substitute for releasing explicitly.
