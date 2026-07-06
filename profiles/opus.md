# Model profile: Opus (4.8)

- On long image-laden prompts, output completeness DEGRADES tail-first (observed: 46% of items
  silently dropped, last 100 indices 82% dropped; NOT truncation — stop=end_turn). Cheap
  insurance: an explicit "account for EVERY item 0..N-1; verify all N before finishing" clause.
  (DISCOVERIES 2026-06-27, deduper coverage collapse.)
- Rejects `temperature` on Bedrock Converse (400) — never pass it.
- In agentic tool loops on editorial tasks, MORE agency hurt quality (tool_loop 77.8 vs
  single-shot 96.6 on chapter editing) — prefer single-shot with complete context.
- Bedrock inference-profile grants are REGION-SCOPED (us-west-2 for prod) — escalation calls
  must originate in the granted region.
