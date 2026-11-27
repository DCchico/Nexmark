"""One 60-second Nexmark reference cell. Importing this module does not open a socket."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from nexmark_join.trace import MEASURE_S, NAMESPACES, RATE, SEED, WARMUP_S
from nexmark_join.window import WINDOW_S

POLICIES = ("RR", "Faast", "Cloudburst", "Data-centric", "Stateflow")
FLAGS = {"--policy", "--run-id", "--output", "--redis", "--dry-run", "--validate-only", "--execute"}


def full_trace_kwargs() -> dict:
    return {
        "seed": SEED,
        "namespaces": NAMESPACES,
        "rate": RATE,
        "warmup_s": WARMUP_S,
        "measure_s": MEASURE_S,
    }


def _accepted(path: Path) -> bool:
    result = path / "result.json"
    if not result.is_file():
        return False
    try:
        return json.loads(result.read_text(encoding="utf-8")).get("complete") is True
    except (OSError, json.JSONDecodeError):
        return False


def _unsafe(path: Path) -> bool:
    if not path.parts or path.parts[-1] in {"", ".", ".."}:
        return True
    return ".." in path.parts


def parse(argv: list[str]) -> dict:
    if not argv or argv[0] in {"-h", "--help"}:
        return {"action": "help", "code": 0 if argv else 2}
    if argv[0] != "cell":
        return {"action": "reject", "code": 2, "error": "single-cell mode is 'cell'; probe and campaign stay separate"}
    values: dict[str, str] = {}
    switches = set()
    index = 1
    while index < len(argv):
        token = argv[index]
        if token not in FLAGS:
            return {"action": "reject", "code": 2, "error": f"unknown option {token}"}
        if token in {"--dry-run", "--validate-only", "--execute"}:
            switches.add(token)
            index += 1
            continue
        if index + 1 >= len(argv) or argv[index + 1].startswith("--"):
            return {"action": "reject", "code": 2, "error": f"missing value for {token}"}
        values[token] = argv[index + 1]
        index += 2
    if len(switches) != 1:
        return {"action": "reject", "code": 2, "error": "choose exactly one of --dry-run, --validate-only, or --execute"}
    policy = values.get("--policy", "")
    if policy not in POLICIES:
        return {"action": "reject", "code": 2, "error": f"unsupported policy {policy}"}
    run_id = values.get("--run-id", "")
    if not run_id or any(mark in run_id for mark in " /\\"):
        return {"action": "reject", "code": 2, "error": "run-id must be one safe identifier"}
    if not values.get("--output") or not values.get("--redis"):
        return {"action": "reject", "code": 2, "error": "missing --output or --redis"}
    output = Path(values["--output"])
    if _unsafe(output):
        return {"action": "reject", "code": 3, "error": "unsafe output path"}
    execute = "--execute" in switches
    if execute and _accepted(output):
        return {"action": "reject", "code": 3, "error": f"accepted result already exists at {output}"}
    spec = full_trace_kwargs()
    return {
        "action": "cell",
        "execute": execute,
        "policy": policy,
        "run_id": run_id,
        "output": str(output),
        "redis": values["--redis"],
        "seed": spec["seed"],
        "namespaces": spec["namespaces"],
        "rate_per_namespace": spec["rate"],
        "warmup_s": spec["warmup_s"],
        "measure_s": spec["measure_s"],
        "event_time_window_s": WINDOW_S,
        "oracle": "nexmark_join.window.apply_event",
        "probe": False,
        "runner": "cell",
        "calls": 1 if execute else 0,
    }


def _write_failure(plan: dict, failure: str) -> None:
    dest = Path(plan["output"])
    dest.mkdir(parents=True, exist_ok=True)
    record = dest / "result.json"
    if record.exists():
        return
    record.write_text(json.dumps({
        "complete": False,
        "failure": failure,
        "policy": plan["policy"],
        "run_id": plan["run_id"],
        "measure_s": plan["measure_s"],
        "warmup_s": plan["warmup_s"],
        "event_time_window_s": plan["event_time_window_s"],
    }) + "\n", encoding="utf-8")


def dispatch(argv: list[str], generate=None, executor=None) -> int:
    plan = parse(argv)
    if plan.get("action") == "help":
        print("cell --policy POLICY --run-id ID --output DIR --redis HOST:PORT (--dry-run | --validate-only | --execute)")
        return plan["code"]
    if plan.get("action") == "reject":
        print(plan["error"])
        return plan["code"]
    if not plan["execute"]:
        print(json.dumps(plan))
        return 0
    if generate is None or executor is None:
        print("no executor")
        return 2
    trace = generate(**full_trace_kwargs())
    if trace.get("measure_s") != MEASURE_S or trace.get("warmup_s") != WARMUP_S or trace.get("namespaces") != NAMESPACES:
        print("refusing a trace that is not the 60-second reference")
        return 2
    output = Path(plan["output"])
    output.mkdir(parents=True, exist_ok=True)
    failed = output / "result.json"
    if failed.exists() and not _accepted(output):
        slot = 1
        while (output / f"result-failed{slot}.json").exists():
            slot += 1
        failed.rename(output / f"result-failed{slot}.json")
    try:
        result = executor(plan, trace)
    except (Exception, SystemExit) as exc:
        _write_failure(plan, str(exc))
        return 1
    if not isinstance(result, dict) or result.get("complete") is not True:
        _write_failure(plan, "cell returned incomplete evidence")
        return 1
    return 0
