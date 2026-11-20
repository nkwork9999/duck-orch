import unittest
from datetime import date

import duckdb
from duckorch_embedded import DailyPartitionPlanner


class CoverageTests(unittest.TestCase):
    def test_pending_ranges_keep_holes_half_open(self):
        with duckdb.connect() as db:
            db.execute("CREATE SCHEMA __orch__")
            db.execute("CREATE TABLE __orch__.asset_partitions(asset_name VARCHAR, partition_key VARCHAR)")
            db.execute("""CREATE TABLE __orch__.asset_materializations(
                        asset_name VARCHAR, partition_key VARCHAR, materialized_at TIMESTAMP, status VARCHAR)""")
            db.execute("""INSERT INTO __orch__.asset_partitions
                SELECT 'sales', '2026-01-0' || i FROM range(1, 8) t(i) WHERE i <> 6""")
            db.execute("""INSERT INTO __orch__.asset_materializations VALUES
                ('sales', '2026-01-03', '2026-02-01', 'success'),
                ('sales', '2026-01-04', '2026-02-01', 'success'),
                ('sales', '2026-01-04', '2026-02-02', 'failed')""")
            planner = DailyPartitionPlanner(db, {"daily": "sales"})
            days = [date(2026, 1, i) for i in range(1, 9)]
            self.assertEqual(planner.pending_ranges("daily", days[0], days[7]),
                             [(days[0], days[2]), (days[3], days[5]), (days[6], days[7])])
            self.assertEqual(planner.pending_ranges("daily", days[1], days[1]), [])
