import unittest

import duckdb

from duckorch_embedded import GroupDelta, MembershipChange


class GroupDeltaTests(unittest.TestCase):
    def test_reaggregate_old_and_new_memberships_and_remove_empty_groups(self):
        with duckdb.connect() as db:
            db.execute("""
                CREATE TABLE current_sales(id BIGINT PRIMARY KEY, region VARCHAR NOT NULL, amount DOUBLE);
                INSERT INTO current_sales VALUES (1, 'east', 5), (2, 'west', 6), (3, 'north', 7);
                CREATE TABLE totals AS SELECT region, sum(amount) AS value, count(*) AS n
                FROM current_sales GROUP BY region;
            """)
            delta = GroupDelta(db, "current_sales", "totals")
            self.assertEqual(delta.refresh("v1", []), 0)
            db.execute("""
                UPDATE current_sales SET region='west' WHERE id=1;
                DELETE FROM current_sales WHERE id=3;
                INSERT INTO current_sales VALUES (3, 'south', 8);
                UPDATE current_sales SET amount=10 WHERE id=2;
            """)
            events = [MembershipChange(1, "east", "west"), MembershipChange(2, "west", "west"),
                      MembershipChange(3, "north", None), MembershipChange(3, None, "south")]
            self.assertEqual(delta.refresh("v2", events), 4)
            self.assertEqual(delta.refresh("v2", events), 4)
            self.assertEqual(db.execute("SELECT * FROM totals ORDER BY region").fetchall(), db.execute("""
                SELECT region, sum(amount), count(*) FROM current_sales GROUP BY region ORDER BY region
            """).fetchall())
