#!/usr/bin/env python3
"""Fake `claude -p` launcher for tests: ignores its args, emits a canned result JSON."""
import json
import sys

print(json.dumps({
    "type": "result", "subtype": "success", "is_error": False,
    "result": "fake work done",
    "session_id": "sess-fake-0001",
    "total_cost_usd": 1.25,
    "num_turns": 3,
}))
sys.exit(0)
