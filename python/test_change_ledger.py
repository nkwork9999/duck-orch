import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from duckorch_embedded import ChangeLedger, ChangeWindowPlanner, DateWindow, PartitionGrain


class ChangeLedgerTests(unittest.TestCase):
    def test_revision_and_tenant_databases_isolate_durable_change_windows(self):
        horizon = DateWindow(date(2026, 1, 1), date(2027, 1, 1))
        planner = ChangeWindowPlanner({
            "orders": {}, "daily": {"orders": PartitionGrain.DAY},
            "monthly": {"daily": PartitionGrain.MONTH}, "full": {"daily": PartitionGrain.ALL},
        }, horizon)
        feb = DateWindow(date(2026, 2, 18), date(2026, 2, 19))
        jan = DateWindow(date(2026, 1, 2), date(2026, 1, 3))
        with TemporaryDirectory() as directory:
            first = str(Path(directory) / "a.duckdb")
            second = str(Path(directory) / "b.duckdb")
            with duckdb.connect(first) as db:
                ledger = ChangeLedger(db, {"orders": "sales.orders"})
                self.assertTrue(ledger.record("orders", "correction-a", feb))
                self.assertFalse(ledger.record("orders", "correction-a", feb))
                self.assertTrue(ledger.record("orders", "correction-b", jan))
            with duckdb.connect(first) as a, duckdb.connect(second) as b:
                local = ChangeLedger(a, {"orders": "sales.orders"})
                other = ChangeLedger(b, {"orders": "sales.orders"})
                self.assertEqual(other.plan("correction-a", planner), {})
                self.assertEqual(local.plan("correction-a", planner), {
                    "daily": (feb,), "monthly": (DateWindow(date(2026, 2, 1), date(2026, 3, 1)),),
                    "full": (horizon,),
                })
                self.assertEqual(local.plan("correction-b", planner)["daily"], (jan,))
                plan = list(local.explain("correction-a", planner, tenant_id="tenant-a", bytes_per_day={"daily": 100}))
                output = {step.model_id: step for step in plan}
                self.assertEqual(set(output), {"orders", "daily", "monthly", "full"})
                self.assertEqual(output["monthly"].parent_id, "daily")
                self.assertEqual(output["daily"].parent_id, "orders")
                self.assertEqual(output["orders"].window, feb)
                self.assertEqual(output["orders"].reason, "source_change")
                self.assertEqual(output["full"].window, horizon)
                self.assertIsNone(output["monthly"].estimated_bytes)
                self.assertEqual(output["daily"].estimated_bytes, 100)
                self.assertTrue(all(step.tenant_id == "tenant-a" for step in plan))
