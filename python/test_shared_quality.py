from datetime import date
import unittest

import duckdb

from duckorch_embedded import DateWindow, NullRule, NumericRangeRule, RowCountRule, SharedQualityScan


class SharedQualityTests(unittest.TestCase):
    def test_windowed_counts_and_ranges_match_individual_checks(self):
        with duckdb.connect() as db:
            db.execute("""
                CREATE TABLE observed(day DATE, "gross sales" DOUBLE, units BIGINT);
                INSERT INTO observed VALUES
                    ('2026-01-01', 1, NULL), ('2026-01-01', NULL, 8), ('2026-01-01', 7, 3),
                    ('2026-01-02', NULL, NULL), ('2026-01-03', 500, 50);
            """)
            scanner = SharedQualityScan(db, "observed", date_column="day", columns={
                "sales": "gross sales", "units": "units",
            })
            checks = {
                "presence": RowCountRule(minimum=1),
                "sales-null": NullRule("sales", maximum=1),
                "sales-range": NumericRangeRule("sales", minimum=0, maximum=5),
                "units-range": NumericRangeRule("units", minimum=0, maximum=10),
            }
            runs = [
                (DateWindow(date(2026, 1, 1), date(2026, 1, 2)), [(3, True), (1, True), (1, False), (0, True)]),
                (DateWindow(date(2026, 1, 2), date(2026, 1, 3)), [(1, True), (1, True), (0, True), (0, True)]),
                (DateWindow(date(2026, 1, 4), date(2026, 1, 5)), [(0, False), (0, True), (0, True), (0, True)]),
            ]
            for window, expected in runs:
                together = scanner.evaluate(window, checks)
                self.assertEqual([(r.observed, r.passed) for r in together], expected)
                separate = [scanner.evaluate(window, {name: rule})[0] for name, rule in checks.items()]
                self.assertEqual(together, separate)
            with self.assertRaises(KeyError):
                scanner.evaluate(runs[0][0], {"internal": NullRule("day")})
