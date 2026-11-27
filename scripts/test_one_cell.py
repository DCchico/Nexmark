"""Windows-safe checks. They do not register a workflow or open a socket."""

import importlib.util
import json
import socket
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

socket.socket.connect = lambda self, *args, **kwargs: (_ for _ in ()).throw(AssertionError("network"))
socket.create_connection = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("network"))

from nexmark_join.trace import MEASURE_S, NAMESPACES, RATE, SEED, WARMUP_S, generate
from nexmark_join.window import WINDOW_S
from one_cell import dispatch


def load_campaign():
    path = Path(__file__).with_name("run_campaign.py")
    spec = importlib.util.spec_from_file_location("nexmark_campaign_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def cell_args(root: Path, policy: str = "RR", mode: str = "--execute") -> list[str]:
    return ["cell", "--policy", policy, "--run-id", f"core60{policy[:2].lower()}", "--output", str(root / policy), "--redis", "10.80.1.4:6379", mode]


class OneCellTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_dry_run_does_not_generate_or_call(self):
        calls = []

        def fake_generate(**kwargs):
            calls.append(kwargs)
            return {}

        code = dispatch(cell_args(self.root, mode="--dry-run"), generate=fake_generate, executor=lambda plan, trace: calls.append(plan))
        self.assertEqual(code, 0)
        self.assertEqual(calls, [])
        self.assertEqual(dispatch(["--help"], generate=fake_generate, executor=lambda plan, trace: calls.append(plan)), 0)
        self.assertEqual(dispatch([], generate=fake_generate, executor=lambda plan, trace: calls.append(plan)), 2)

    def test_execute_requests_the_full_trace_once(self):
        probes = generate(measure_s=5.0, warmup_s=1.0)
        for policy in ("RR", "Faast", "Cloudburst", "Data-centric", "Stateflow"):
            generated = []
            executed = []

            def fake_generate(**kwargs):
                generated.append(kwargs)
                return generate(**kwargs)

            def fake_cell(plan, trace):
                executed.append((plan, trace))
                return {"complete": True}

            code = dispatch(cell_args(self.root, policy), generate=fake_generate, executor=fake_cell)
            self.assertEqual(code, 0, policy)
            self.assertEqual(generated, [{"seed": SEED, "namespaces": NAMESPACES, "rate": RATE, "warmup_s": WARMUP_S, "measure_s": MEASURE_S}])
            self.assertEqual(len(executed), 1)
            plan, trace = executed[0]
            self.assertEqual(plan["policy"], policy)
            self.assertEqual(plan["event_time_window_s"], WINDOW_S)
            self.assertEqual(trace["measure_s"], 60.0)
            self.assertEqual(trace["warmup_s"], 10.0)
            self.assertEqual(trace["namespaces"], 6)
            self.assertNotEqual(len(trace["rows"]), len(probes["rows"]))
            self.assertFalse(plan["probe"])

    def test_rejects_probe_flag_missing_binding_and_duplicate(self):
        calls = []
        executor = lambda plan, trace: calls.append(plan) or {"complete": True}
        self.assertEqual(dispatch(["cell", "--policy", "RR", "--probe", "--execute"], generate=generate, executor=executor), 2)
        self.assertEqual(dispatch(["cell", "--policy", "RR", "--run-id", "core60rr", "--output", str(self.root / "RR"), "--execute"], generate=generate, executor=executor), 2)
        output = self.root / "RR"
        output.mkdir()
        (output / "result.json").write_text(json.dumps({"complete": True}), encoding="utf-8")
        self.assertEqual(dispatch(cell_args(self.root), generate=generate, executor=executor), 3)
        self.assertEqual(calls, [])

    def test_child_failure_records_one_attempt(self):
        def fail(plan, trace):
            raise RuntimeError("child failed")

        code = dispatch(cell_args(self.root), generate=generate, executor=fail)
        self.assertEqual(code, 1)
        saved = json.loads((self.root / "RR" / "result.json").read_text(encoding="utf-8"))
        self.assertFalse(saved["complete"])
        self.assertEqual(saved["measure_s"], 60.0)
        self.assertEqual(saved["event_time_window_s"], WINDOW_S)

    def test_launcher_calls_cell_once_with_the_full_trace(self):
        module = load_campaign()
        calls = []
        module.prepare_one = lambda policy, dest: calls.append(("prepare", policy))
        module.cleanup = lambda: calls.append(("cleanup",))

        def fake_cell(run_id, policy, dest, trace):
            calls.append(("cell", run_id, policy, trace["measure_s"], trace["warmup_s"], trace["namespaces"]))
            return {"complete": True}

        module.cell = fake_cell
        code = module.main(cell_args(self.root))
        self.assertEqual(code, 0)
        self.assertEqual([item[0] for item in calls], ["prepare", "cell", "cleanup"])
        self.assertEqual(calls[1][3:], (60.0, 10.0, 6))
        self.assertEqual(module.main(cell_args(self.root, mode="--dry-run")), 0)
        self.assertEqual(len([item for item in calls if item[0] == "cell"]), 1)


if __name__ == "__main__":
    unittest.main()
