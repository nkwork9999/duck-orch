import os
import unittest
from datetime import date

import duckdb
from duckorch_embedded import DailyPartitionPlanner


class PlannerTests(unittest.TestCase):
    def setUp(self):
        artifact = os.getenv("DUCKORCH_TEST_EXTENSION")
        config = {"allow_unsigned_extensions": bool(artifact)}
        if directory := os.getenv("DUCKDB_EXTENSION_DIRECTORY"):
            config["extension_directory"] = directory
        self.db = duckdb.connect(config=config)
        self.addCleanup(self.db.close)
        self.db.load_extension(artifact or "duckorch")
        self.db.execute("PRAGMA orch_asset_list")
        self.asset = "owner's 日次"
        self.planner = DailyPartitionPlanner(self.db, {"sales": self.asset})
        self.days = [date(2026, 1, day) for day in range(1, 6)]
        for asset in (self.asset, "private"):
            self.db.executemany(
                "INSERT INTO __orch__.asset_partitions(asset_name, partition_key) VALUES (?, ?)",
                [(asset, day.isoformat()) for day in self.days],
            )

    def test_only_registered_days_with_latest_unsuccessful_result(self):
        self.db.executemany(
            "INSERT INTO __orch__.asset_materializations(asset_name, partition_key, materialized_at, status) VALUES (?, ?, ?, ?)",
            [(self.asset, "2026-01-02", "2026-02-01", "success"),
             (self.asset, "2026-01-03", "2026-02-01", "success"),
             (self.asset, "2026-01-03", "2026-02-02", "failed"),
             ("private", "2026-01-01", "2026-02-01", "success")],
        )
        self.assertEqual(self.planner.pending("sales", self.days[0], self.days[3]), [self.days[0], self.days[2]])

    def test_unregistered_model_and_invalid_range(self):
        with self.assertRaises(KeyError):
            self.planner.pending("private", self.days[0], self.days[1])
        with self.assertRaises(ValueError):
            self.planner.pending("sales", self.days[1], self.days[0])
        self.assertEqual(self.planner.pending("sales", self.days[1], self.days[1]), [])

    def test_empty_and_non_date_arguments(self):
        self.assertEqual(self.planner.pending("sales", date(2027, 1, 1), date(2027, 1, 2)), [])
        with self.assertRaises(TypeError):
            self.planner.pending("sales", "2026-01-01", self.days[1])
