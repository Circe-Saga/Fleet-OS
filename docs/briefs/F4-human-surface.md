# BRIEF F4: Human surface — WAITING_ON_YOU.md + self-draining decisions

Class: additive      Repo: `/Users/jackychen/Workspace/Circe-Saga/Saga-Fleet`      Depends: F0

Design: `docs/agent-os-design.md` §4.5, §7 (escalation), §12 (founder experience).

## INSPECT FIRST

- Design §4.5 — the one-screen item format (category, context, recommendation, "if you do
  nothing", respond hint).
- v1 `fleet status` "🔔 WAITING ON YOU" lane + `_build_md` (fleet.py:493-588) — the projection
  pattern to extend, not replace.

## DO NOT CHANGE

- The queue is a generated projection; never hand-edited, never a second source of truth.
- Irreversible items never auto-proceed, no matter how old.
- Auto-proceeded outcomes must remain visible (an "auto-proceeded since your last visit"
  section) — silent defaults are how trust in the system dies.

## DELIVERABLES

1. `fleet render-waiting [--out PATH]` — writes `Circe-Saga/WAITING_ON_YOU.md`:
   - Open decisions, grouped by category (`authorization` first), each one screen: question,
     detail, raiser + task link, **default recommendation**, expiry + what-happens-if-nothing,
     respond hint (`fleet resolve --id N --resolution "…"` / "tell the Conductor").
   - Tasks in NEEDS_HUMAN with their blocking decision.
   - "Auto-proceeded since last render" — decisions `auto_resolved=1` not yet shown.
   - Verification queue: `decide --category verification` items with artifact refs inline.
2. `fleet resolve-expired` behavior (F0) reported here: expired+reversible → resolution =
   default, `auto_resolved=1`, event `auto-resolve`, raiser released via existing resolve logic.
3. Weekly report `fleet report --days 7`: tasks shipped, human-touches per shipped task (count
   of founder-resolved decisions + founder events), spend per lane, gate bounces, auto-proceeds.

## TESTS / CONTRACTS

Render with a fixture DB: authorization items sort first; an expired irreversible decision
stays open and is flagged `OVERDUE — waiting on you`; auto-proceeded section shows once and
clears on next render; report math (touches/shipped) verified on a known fixture.

## GOLDEN PATH

Morning: founder opens WAITING_ON_YOU.md — 2 auto-proceeded FYIs, 1 authorization tap, 1
taste call with a render path. Total interaction: one `fleet resolve`, one "approved" to the
Conductor.

## FAILURE CASE

A decision raised without `--default` and without category: renderer shows it under
`uncategorized — Conductor should bounce or categorize`, so malformed escalations are visible
instead of lost.

## REVIEW FOCUS

"If you do nothing" line is always accurate (derived from reversible+expires, not free text);
auto-proceed cannot fire on irreversible items (test it); the file reads well in a terminal
and on GitHub.
