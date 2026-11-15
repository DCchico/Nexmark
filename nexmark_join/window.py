"""Apache Beam Nexmark Query 8, incremental and in order.

RANGE 12 HOUR on person and auction event time. A person matches auctions
whose seller id is that person. An auction matches the person with the same
id, if that person is still in the window. This is not the cloned
per-auction bid list, and it does not insert synthetic window entries.
"""

from __future__ import annotations

WINDOW_S = 12 * 3600


class MissingPredecessor(RuntimeError):
    pass


class LateEvent(RuntimeError):
    pass


def empty_state() -> dict:
    return {"version": None, "last_ts": None, "persons": {}, "auctions": {}, "outputs": {}}


def apply_event(state: dict, event: dict, seq: int, predecessor: int | None) -> dict:
    outputs = state.get("outputs") or {}
    if str(seq) in outputs:
        return {
            "ok": True,
            "duplicate": True,
            "version": state.get("version"),
            "matches": list(outputs[str(seq)]),
            "persons": len(state.get("persons") or {}),
            "auctions": len(state.get("auctions") or {}),
        }
    if state.get("version") != predecessor:
        raise MissingPredecessor(
            f"sequence {seq} requires predecessor {predecessor} but committed version is {state.get('version')}"
        )
    ts = int(event["ts"])
    last = state.get("last_ts")
    if last is not None and ts < int(last):
        raise LateEvent(f"event time {ts} is older than committed time {last}")
    persons = {key: dict(value) for key, value in (state.get("persons") or {}).items()}
    auctions = {key: dict(value) for key, value in (state.get("auctions") or {}).items()}
    cutoff = ts - WINDOW_S
    persons = {key: value for key, value in persons.items() if int(value["ts"]) > cutoff}
    auctions = {key: value for key, value in auctions.items() if int(value["ts"]) > cutoff}
    matches = []
    kind = event["kind"]
    if kind == "person":
        persons[str(event["id"])] = {"name": str(event["name"]), "ts": ts}
        for auction_id, auction in sorted(auctions.items()):
            if auction["seller"] == str(event["id"]):
                matches.append({
                    "person_id": str(event["id"]),
                    "name": str(event["name"]),
                    "auction_id": auction_id,
                    "reserve": auction["reserve"],
                })
    elif kind == "auction":
        auctions[str(event["id"])] = {"seller": str(event["seller"]), "reserve": event["reserve"], "ts": ts}
        person = persons.get(str(event["seller"]))
        if person is not None:
            matches.append({
                "person_id": str(event["seller"]),
                "name": person["name"],
                "auction_id": str(event["id"]),
                "reserve": event["reserve"],
            })
    else:
        raise ValueError(f"unknown event kind {kind}")
    outputs = dict(outputs)
    outputs[str(seq)] = matches
    state["version"] = seq
    state["last_ts"] = ts
    state["persons"] = persons
    state["auctions"] = auctions
    state["outputs"] = outputs
    return {
        "ok": True,
        "duplicate": False,
        "version": seq,
        "matches": matches,
        "persons": len(persons),
        "auctions": len(auctions),
    }


def project(state: dict) -> dict:
    return {
        "version": state.get("version"),
        "last_ts": state.get("last_ts"),
        "persons": {key: dict(value) for key, value in sorted((state.get("persons") or {}).items())},
        "auctions": {key: dict(value) for key, value in sorted((state.get("auctions") or {}).items())},
    }
