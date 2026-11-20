import unittest
from datetime import date

import duckdb
from duckorch_embedded import ChangeWindowPlanner, DateWindow, PartitionGrain


class ChangeWindowTests(unittest.TestCase):
    def test_late_day_correction_equals_full_rebuild(self):
        day = date(2026, 2, 3)
        next_day = date(2026, 2, 4)
        horizon = DateWindow(date(2026, 1, 1), date(2027, 1, 1))
        planner = ChangeWindowPlanner(
            {"input": {}, "daily": {"input": PartitionGrain.DAY},
             "monthly": {"daily": PartitionGrain.MONTH}, "legacy": {"daily": PartitionGrain.ALL}},
            horizon,
        )
        plan = planner.affected("input", DateWindow(day, next_day))
        self.assertEqual(plan, {
            "daily": (DateWindow(day, next_day),),
            "monthly": (DateWindow(date(2026, 2, 1), date(2026, 3, 1)),),
            "legacy": (horizon,),
        })
        with duckdb.connect() as db:
            db.execute("""
                CREATE TABLE sales AS SELECT d::DATE AS d, i AS amount
                FROM generate_series(DATE '2026-01-01', DATE '2026-12-31', INTERVAL 1 DAY)
                WITH ORDINALITY t(d,i)
            """)
            daily = "SELECT d, sum(amount) AS total FROM sales {} GROUP BY d"
            monthly = "SELECT date_trunc('month', d)::DATE AS m, sum(total) AS total FROM daily {} GROUP BY m"
            db.execute("CREATE TABLE daily AS " + daily.format(""))
            db.execute("CREATE TABLE monthly AS " + monthly.format(""))
            db.execute("UPDATE sales SET amount = amount + 10 WHERE d = ?", [day])
            for table, key, template in [("daily", "d", daily), ("monthly", "m", monthly)]:
                window, = plan[table]
                args = [window.start, window.end]
                db.execute(f"DELETE FROM {table} WHERE {key} >= ? AND {key} < ?", args)
                db.execute(f"INSERT INTO {table} " + template.format("WHERE d >= ? AND d < ?"), args)
                actual = db.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
                expected = db.execute(template.format("") + " ORDER BY 1").fetchall()
                self.assertEqual(actual, expected)

    def test_cycle_unknown_model_and_boundary(self):
        horizon = DateWindow(date(2026, 1, 10), date(2026, 3, 1))
        with self.assertRaises(ValueError):
            ChangeWindowPlanner({"cycle": {"cycle": PartitionGrain.DAY}}, horizon)
        planner = ChangeWindowPlanner({"a": {}, "b": {"a": PartitionGrain.MONTH}}, horizon)
        with self.assertRaises(KeyError):
            planner.affected("missing", horizon)
        self.assertEqual(planner.affected("a", DateWindow(date(2026, 3, 1), date(2026, 3, 2))), {})
        self.assertEqual(planner.affected("a", DateWindow(date(2026, 1, 1), date(2026, 1, 12))),
                         {"b": (DateWindow(date(2026, 1, 10), date(2026, 2, 1)),)})
