"""stateful-e2e-v1. One instance per request. Residual C excludes timed state."""

from __future__ import annotations

SCHEMA = "stateful-e2e-v1"


class Phases:
    def __init__(self) -> None:
        self.state_managed_s = 0.0
        self.immutable_s = 0.0
        self.direct_validate_s = 0.0
        self.stall_s = 0.0

    def add_managed(self, seconds: float) -> None:
        self.state_managed_s += seconds

    def add_direct_validate(self, seconds: float) -> None:
        self.direct_validate_s += seconds

    def add_stall(self, seconds: float) -> None:
        self.stall_s += seconds


def close_handler(handler_s: float, phases: Phases) -> dict:
    state_s = phases.state_managed_s + phases.immutable_s + phases.direct_validate_s
    stall_s = phases.stall_s
    compute_s = handler_s - state_s - stall_s
    return {
        "schema": SCHEMA,
        "handler_s": handler_s,
        "compute_s": compute_s,
        "state_s": state_s,
        "stall_s": stall_s,
        "state_managed_s": phases.state_managed_s,
        "immutable_s": phases.immutable_s,
        "direct_validate_s": phases.direct_validate_s,
    }


def close_operation(*, release: float, dispatch: float, completion: float, handler: dict | None) -> dict:
    e2e = completion - release
    handler_s = None if handler is None else handler.get("handler_s")
    return {
        "timing_schema": SCHEMA,
        "timing_origin": "driver",
        "e2e_s": e2e,
        "http_s": completion - dispatch,
        "driver_queue_s": dispatch - release,
        "orchestration_s": None if handler_s is None else e2e - handler_s,
        "handler_s": handler_s,
        "compute_s": None if handler is None else handler.get("compute_s"),
        "state_s": None if handler is None else handler.get("state_s"),
        "stall_s": None if handler is None else handler.get("stall_s"),
        "state_managed_s": None if handler is None else handler.get("state_managed_s"),
        "immutable_s": None if handler is None else handler.get("immutable_s"),
        "direct_validate_s": None if handler is None else handler.get("direct_validate_s"),
        "timing_complete": handler_s is not None,
    }
