# Model profile: Haiku (4.5)

- Fast/cheap tier: bulk mechanical sweeps (renames, fixture regeneration, format migrations),
  first-pass triage, high-volume classification. Not for multi-file reasoning or review duty.
- Give it fully-specified, single-territory tasks with an exact done-check command; ambiguity
  that Sonnet absorbs will derail Haiku — escalate the task spec, not just the model.
- Pair with tier-0 machine gates: its error mode is confident small mistakes at volume, which
  contract checkers catch cheaply.
