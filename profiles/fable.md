# Model profile: Fable (5)

- Strongest available tier (above Opus). Use for: Conductor duty, architecture decomposition,
  gnarly cross-repo debugging, final pre-prod reviews — not for routine implementation (cost).
- Long-context relational tracking still degrades with size (GraphWalks-class effect applies to
  every model): externalize relational state into the task graph, contracts, and checklists
  rather than one giant context.
- Includes dual-use safety measures; security-review tasks should cite authorization context
  (own-infra, defensive) explicitly in the starter.
