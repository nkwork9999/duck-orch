import unittest
from datetime import datetime, timezone

import duckdb

from duckorch_embedded import VariantEvent, VariantEventLog


class VariantEventTests(unittest.TestCase):
    def test_round_trip_distinguishes_json_types_and_missing_and_promotes_a_key(self):
        when = datetime(2026, 1, 1, tzinfo=timezone.utc)
        with duckdb.connect() as db:
            log = VariantEventLog(db, "sensor", promoted_text="label")
            log.append([
                VariantEvent("1", "note", when, '{"label":"warm","data":[1,"1",null]}'),
                VariantEvent("2", "note", when, '{"label":3}'),
                VariantEvent("3", "note", when, '{"label":null}'),
                VariantEvent("4", "note", when, '{}'),
            ])
            rows = log.read("note")
            self.assertEqual([event.attributes for event in rows], [
                {"label": "warm", "data": [1, "1", None]},
                {"label": 3}, {"label": None}, {},
            ])
            self.assertEqual([event.event_id for event in log.read("note", key="label", value="warm")], ["1"])
            self.assertEqual(log.read("note", key="label", value="3"), [])
            self.assertEqual(VariantEventLog(db, "other").read("note"), [])
            with self.assertRaises(ValueError):
                VariantEventLog(db, "sensor", promoted_text="other")
            with self.assertRaises(ValueError):
                log.append([VariantEvent("5", "note", when, '{"label":"ok"}'),
                            VariantEvent("6", "note", when, '[1,2]')])
            self.assertEqual(len(log.read("note")), 4)
