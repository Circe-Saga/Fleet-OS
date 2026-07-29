# Model profile: Sonnet (5)

- Rejects `temperature` on Bedrock Converse (400, same as Opus 4.8) — never pass it.
- Extended thinking on Bedrock had an empty-JSON response bug in structured-output paths —
  disable thinking for strict-JSON calls (worked around prod-side; stay wary).
- On video frames, 2×2@360 tiling HURT moment/boundary quality (0.75→0.65 / 0.78→0.48);
  single-frame is the default for video analysis.
- Strong default for implementation + judge work at ~1/5 Opus cost; escalate to Opus for long
  cross-file relational reasoning or when a review keeps bouncing.
