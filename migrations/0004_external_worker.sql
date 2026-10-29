-- Fleet-OS: external webhook workers (Grok and anything else that cannot run the fleet CLI).
--
-- Nullable columns only. The runs product/status CHECKs are unchanged (claude|codex and
-- running|finished|failed|abandoned). A dispatched task is a running row with no pid and
-- await_external=1. Harvest must not treat the missing pid as death; the callback token is
-- stored as a hash; external_ref is the worker's idempotency key, set when a result is ingested.

ALTER TABLE runs ADD COLUMN await_external INTEGER DEFAULT 0;
ALTER TABLE runs ADD COLUMN callback_token_hash TEXT;
ALTER TABLE runs ADD COLUMN external_ref TEXT;
