"""JSON window document. Encode and decode are state access, not join work."""

from __future__ import annotations

import json

from nexmark_join.window import empty_state


def encode_state(state: dict) -> bytes:
    body = {
        "version": state.get("version"),
        "last_ts": state.get("last_ts"),
        "persons": state.get("persons") or {},
        "auctions": state.get("auctions") or {},
        "outputs": state.get("outputs") or {},
    }
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


def decode_state(raw: bytes | None) -> dict:
    if not raw:
        return empty_state()
    body = json.loads(raw)
    return {
        "version": body.get("version"),
        "last_ts": body.get("last_ts"),
        "persons": body.get("persons") or {},
        "auctions": body.get("auctions") or {},
        "outputs": {str(key): value for key, value in (body.get("outputs") or {}).items()},
    }
