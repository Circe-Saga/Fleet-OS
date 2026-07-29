# BRIEF F5: Review gates — auto review tasks + evidence verification

Class: additive      Repo: `<workspace>/Fleet-OS`      Depends: F0–F2

Design: `docs/DESIGN.md` §4.4 (tiers), §5 (transition table).

## INSPECT FIRST

- Design §4.4 — tier 0 machine gates / tier 1 triggered LLM review / tier 2 founder.
- The v1 fleet's `check_contract.py` / `check_render.py` pattern — tier 0 exemplars; the postmortem
  rule ("what machine gate would have caught this") is policy, encoded in the playbook.
- `gh pr view/diff --json`, `gh run view` — the evidence sources.

## DO NOT CHANGE

- Review never blocks the author — only the merge. Author moves on immediately.
- Reviewer is a different agent from the author, always (independence is the point).
- A review with zero executed checks cannot pass the gate ("LGTM" is a protocol violation).

## DELIVERABLES

1. `fleet review-policy --task N [--diff-stat "files,+lines,-lines"]` — pure function → JSON
   verdict `{tier1_required: bool, triggers: [...]}`. Triggers: task.kind==deploy or
   prod-facing repo (the configured `FLEET_PROD_REPOS` mainline set — e.g.
   `acme-api,acme-editor,acme-web`), territory matches
   locked contracts/schemas or auth/privacy/consent paths, diff > 300 lines, author self-flag
   (`evidence.review_requested`).
2. Playbook amendment (F2): when a gate request arrives and policy says tier1_required →
   Conductor creates `task add --kind review --depends <author-task>` with context_refs =
   {PR, contract, done_criteria}, assigns a non-author agent; IN_REVIEW → DONE additionally
   requires the review task DONE with no blocking findings.
3. Reviewer protocol (in the review task's starter, rendered by `fleet starter`): findings as
   `msg --kind review-finding --to <author>` — each names a **failure scenario** and, where
   possible, a command demonstrating it; verdict via `task evidence` on the review task
   (commands run, findings count, blocking y/n).

## TESTS / CONTRACTS

review-policy: prod repo → required; `acme-evallab` scratch script 40 lines → not required; 500-line
eval diff → required (size trigger); self-flag → required. Gate integration: DONE refused
while a depends-on review task is not DONE (uses F0's transition gate + depends_on).

## GOLDEN PATH

Author task hits IN_REVIEW request → policy fires (prod repo) → review task spawned to an
ephemeral reviewer with diff+contract only → 2 findings (1 blocking, with repro command) →
author fixes, re-requests → verify green, review DONE → merge, task DONE.

## FAILURE CASE

Reviewer returns "looks good" with no executed checks recorded in its evidence → Conductor
bounces the *review* task (gate applies to reviewers too) with the falsifiable-findings rule
quoted.

## REVIEW FOCUS

Policy is deterministic and cheap (no LLM call inside `review-policy`); the reviewer's context
is diff+contract+criteria, not the whole repo; author-reviewer distinctness enforced at
assignment, not by convention.
