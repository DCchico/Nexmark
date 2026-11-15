"""Load, join, and commit one Q8 window. Codec time stays inside S."""

from __future__ import annotations

import os
import time

from nexmark_join.codec import decode_state, encode_state
from nexmark_join.readiness import post_commit_semaphore
from nexmark_join.window import LateEvent, MissingPredecessor, apply_event, empty_state, project


class DirectCache:
    def __init__(self) -> None:
        self.live: dict[str, tuple] = {}

    def fresh(self, namespace: str, version) -> bool:
        found = self.live.get(namespace)
        return found is not None and found[0] == version

    def put(self, namespace: str, state: dict) -> None:
        self.live[namespace] = (state.get("version"), state)


def execute(body: dict, headers: dict, backend) -> dict:
    namespace = body["namespace"]
    seq = int(body["seq"])
    predecessor = body.get("predecessor")
    predecessor = None if predecessor is None else int(predecessor)
    event = body["event"]
    started = time.perf_counter()
    location = headers.get("state-location") or ""
    dependent = headers.get("dependent-state-location") or ""
    direct_validate_s = 0.0
    load_s = 0.0
    publish_s = 0.0
    load_bytes = 0
    publish_bytes = 0
    access = body.get("backend") or "memory"
    # The scheduler can ask for shared memory while still naming the previous
    # container. That container's live object is the committed version; the
    # shared-memory file is stale until a non-direct schedule publishes it.
    cache_hit = (location == "direct" or dependent == "direct") and backend.direct.fresh(namespace, predecessor)
    if cache_hit:
        tick = time.perf_counter()
        _version, state = backend.direct.live[namespace]
        state = {
            "version": state.get("version"),
            "last_ts": state.get("last_ts"),
            "persons": {k: dict(v) for k, v in state.get("persons", {}).items()},
            "auctions": {k: dict(v) for k, v in state.get("auctions", {}).items()},
            "outputs": {k: list(v) for k, v in state.get("outputs", {}).items()},
        }
        direct_validate_s = time.perf_counter() - tick
        access = "direct"
    else:
        tick = time.perf_counter()
        raw, access = backend.load(namespace, headers)
        state = decode_state(raw)
        load_s = time.perf_counter() - tick
        load_bytes = 0 if raw is None else len(raw)
    try:
        result = apply_event(state, event, seq, predecessor)
    except (MissingPredecessor, LateEvent) as exc:
        return {"ok": False, "error": str(exc), "op": "join_update", "namespace": namespace}
    tick = time.perf_counter()
    hold_direct = cache_hit and location == "direct"
    if hold_direct:
        backend.direct.put(namespace, state)
        publish_bytes = 0
        access = "direct"
    else:
        payload = encode_state(state)
        publish_bytes, access = backend.commit(namespace, headers, payload, access)
        backend.direct.put(namespace, state)
    if headers.get("vdagid"):
        post_commit_semaphore(str(headers["vdagid"]), seq)
    publish_s = time.perf_counter() - tick
    elapsed = time.perf_counter() - started
    return {
        "ok": True,
        "op": "join_update",
        "namespace": namespace,
        "seq": seq,
        "predecessor": predecessor,
        "version": result["version"],
        "duplicate": result["duplicate"],
        "matches": result["matches"],
        "persons": result["persons"],
        "auctions": result["auctions"],
        "access": access,
        "load_bytes": load_bytes,
        "publish_bytes": publish_bytes,
        "state_bytes": 0 if access == "direct" else publish_bytes or load_bytes,
        "window": project(state),
        "timings_raw": {
            "load_s": load_s,
            "publish_s": publish_s,
            "direct_validate_s": direct_validate_s,
            "handler_s": elapsed,
        },
    }


def local_backend():
    class Local:
        def __init__(self) -> None:
            self.docs: dict[str, bytes] = {}
            self.direct = DirectCache()

        def load(self, namespace, headers):
            raw = self.docs.get(namespace)
            return raw, "memory"

        def commit(self, namespace, headers, payload, access):
            self.docs[namespace] = payload
            return len(payload), "memory"

    return Local()
