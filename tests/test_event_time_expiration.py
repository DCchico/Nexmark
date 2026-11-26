"""Tiny event-time oracle for the implemented 12-hour Q8 window.

A person or auction older than the window is not a member after a later
event. A person still inside the window still matches a new auction.
"""

from __future__ import annotations

import unittest

from nexmark_join.window import WINDOW_S, apply_event, empty_state, project


class EventTimeExpirationTest(unittest.TestCase):
    def test_old_person_expires_and_current_person_still_matches(self) -> None:
        state = empty_state()
        apply_event(state, {"kind": "person", "id": "old", "name": "Old", "ts": 0}, 0, None)
        # Membership keeps a record only while its event time is strictly newer
        # than the arriving event minus the 12-hour window.
        apply_event(state, {"kind": "person", "id": "ada", "name": "Ada", "ts": WINDOW_S}, 1, 0)
        later = WINDOW_S + 50
        result = apply_event(
            state,
            {"kind": "auction", "id": "a-ada", "seller": "ada", "reserve": 10, "ts": later},
            2,
            1,
        )
        self.assertEqual(result["matches"], [{
            "person_id": "ada",
            "name": "Ada",
            "auction_id": "a-ada",
            "reserve": 10,
        }])
        visible = project(state)
        self.assertNotIn("old", visible["persons"])
        self.assertEqual(set(visible["persons"]), {"ada"})
        stale = apply_event(
            state,
            {"kind": "auction", "id": "a-old", "seller": "old", "reserve": 1, "ts": later + 1},
            3,
            2,
        )
        self.assertEqual(stale["matches"], [])
        self.assertNotIn("old", project(state)["persons"])


if __name__ == "__main__":
    unittest.main()
