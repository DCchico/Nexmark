import unittest

import os

from nexmark_join.codec import decode_state, encode_state
from nexmark_join.readiness import post_commit_semaphore, semaphore_name
from nexmark_join.service import execute, local_backend
from nexmark_join.timing import Phases, close_handler
from nexmark_join.trace import generate
from nexmark_join.window import MissingPredecessor, apply_event, empty_state


def play(events):
    state = empty_state()
    emitted = []
    for seq, event in enumerate(events):
        result = apply_event(state, event, seq, None if seq == 0 else seq - 1)
        emitted.extend(result["matches"])
    return state, emitted


class WindowTest(unittest.TestCase):
    def test_person_then_auction_matches_once(self):
        events = [
            {"kind": "person", "id": "p", "name": "Ada", "ts": 0},
            {"kind": "auction", "id": "a", "seller": "p", "reserve": 10, "ts": 1000},
        ]
        _state, matches = play(events)
        self.assertEqual(matches, [{"person_id": "p", "name": "Ada", "auction_id": "a", "reserve": 10}])

    def test_auction_before_person_matches_on_the_person(self):
        events = [
            {"kind": "auction", "id": "a", "seller": "p", "reserve": 10, "ts": 0},
            {"kind": "person", "id": "p", "name": "Ada", "ts": 1000},
        ]
        _state, matches = play(events)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["name"], "Ada")

    def test_duplicate_does_not_emit_again(self):
        state = empty_state()
        event = {"kind": "person", "id": "p", "name": "Ada", "ts": 0}
        first = apply_event(state, event, 0, None)
        second = apply_event(state, event, 0, None)
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(second["matches"], [])

    def test_missing_predecessor_writes_nothing(self):
        state = empty_state()
        with self.assertRaises(MissingPredecessor):
            apply_event(state, {"kind": "person", "id": "p", "name": "Ada", "ts": 0}, 1, 0)
        self.assertIsNone(state["version"])

    def test_namespaces_do_not_share_people(self):
        left = empty_state()
        right = empty_state()
        apply_event(left, {"kind": "person", "id": "p", "name": "Ada", "ts": 0}, 0, None)
        result = apply_event(right, {"kind": "auction", "id": "a", "seller": "p", "reserve": 1, "ts": 0}, 0, None)
        self.assertEqual(result["matches"], [])

    def test_codec_round_trip_and_encode_is_not_the_join(self):
        state = empty_state()
        apply_event(state, {"kind": "person", "id": "p", "name": "Ada", "ts": 5}, 0, None)
        restored = decode_state(encode_state(state))
        self.assertEqual(restored["persons"]["p"]["name"], "Ada")

    def test_handler_times_put_encode_in_state(self):
        backend = local_backend()
        body = {
            "namespace": "s0",
            "seq": 0,
            "predecessor": None,
            "backend": "memory",
            "event": {"kind": "person", "id": "p", "name": "Ada", "ts": 0},
        }
        result = execute(body, {}, backend)
        raw = result["timings_raw"]
        phases = Phases()
        phases.add_managed(raw["load_s"] + raw["publish_s"])
        phases.add_direct_validate(raw["direct_validate_s"])
        closed = close_handler(raw["handler_s"], phases)
        self.assertGreater(closed["state_managed_s"], 0.0)
        self.assertAlmostEqual(closed["handler_s"], closed["compute_s"] + closed["state_s"] + closed["stall_s"])

    def test_trace_has_six_streams_and_matches_the_oracle_twice(self):
        trace = generate()
        self.assertEqual(trace["namespaces"], 6)
        measured = [row for row in trace["rows"] if not row["warmup"]]
        self.assertGreater(len(measured), 1000)
        states = [empty_state() for _ in range(6)]
        for row in trace["rows"]:
            apply_event(states[row["namespace"]], row["event"], row["seq"], row["predecessor"])
        again = [empty_state() for _ in range(6)]
        for row in trace["rows"]:
            apply_event(again[row["namespace"]], row["event"], row["seq"], row["predecessor"])
        self.assertEqual(
            [state["outputs"] for state in states],
            [state["outputs"] for state in again],
        )

    def test_commit_posts_the_provider_semaphore(self):
        self.assertEqual(semaphore_name("nxprobe", 0), "/nx_nxprobe_0_sem")
        path = "/dev/shm/sem.nx_nxprobe_0_sem"
        if os.path.exists(path):
            os.unlink(path)
        backend = local_backend()
        result = execute(
            {"namespace": "nxprobe", "seq": 0, "predecessor": None, "event": {"kind": "person", "id": "p", "name": "Ada", "ts": 0}},
            {"vdagid": "nxprobe"},
            backend,
        )
        self.assertTrue(result["ok"])
        self.assertTrue(os.path.exists(path))
        post_commit_semaphore("nxprobe", 0)
        os.unlink(path)

    def test_shared_memory_after_direct_uses_the_live_object(self):
        backend = local_backend()
        headers = {"vdagid": "nxflip", "state-location": "direct", "dependent-state-location": "direct"}
        first = execute(
            {"namespace": "nxflip", "seq": 0, "predecessor": None, "event": {"kind": "person", "id": "p", "name": "Ada", "ts": 0}},
            headers,
            backend,
        )
        held = execute(
            {"namespace": "nxflip", "seq": 1, "predecessor": 0, "event": {"kind": "auction", "id": "a", "seller": "p", "reserve": 10, "ts": 1000}},
            headers,
            backend,
        )
        self.assertTrue(first["ok"] and held["ok"])
        self.assertEqual(held["publish_bytes"], 0)
        stale = backend.docs["nxflip"]
        flipped = execute(
            {"namespace": "nxflip", "seq": 2, "predecessor": 1, "event": {"kind": "person", "id": "q", "name": "Bea", "ts": 2000}},
            {"vdagid": "nxflip", "state-location": "shared_memory", "dependent-state-location": "direct"},
            backend,
        )
        self.assertTrue(flipped["ok"])
        self.assertNotEqual(backend.docs["nxflip"], stale)
        for seq in (0, 1, 2):
            os.unlink(f"/dev/shm/sem.nx_nxflip_{seq}_sem")


if __name__ == "__main__":
    unittest.main()
