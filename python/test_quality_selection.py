import unittest

import duckdb

from duckorch_embedded import ForeignKeyCheck, QualityCheckSelector


class QualityCheckTests(unittest.TestCase):
    def test_parent_only_change_selects_referential_check(self):
        selector = QualityCheckSelector({
            "fk": {"orders": frozenset({"customer_id"}), "customers": frozenset({"id"})},
            "unrelated": {"invoices": None},
            "unresolved": None,
        })
        self.assertEqual(selector.select({"customers": frozenset({"name"})}), ("unresolved",))
        self.assertEqual(selector.select({"customers": frozenset({"id"})}), ("fk", "unresolved"))
        self.assertEqual(selector.select({"orders": None}), ("fk", "unresolved"))
        self.assertEqual(selector.select({}), ())

        with duckdb.connect() as db:
            db.execute("""
                CREATE TABLE customers(id INT);
                INSERT INTO customers VALUES (1), (2);
                CREATE TABLE orders(customer_id INT, amount INT);
                INSERT INTO orders VALUES (1, 5), (1, 6), (2, 7), (NULL, 8);
            """)
            check = ForeignKeyCheck("orders", "customer_id", "customers", "id")
            self.assertEqual(check.violation_count(db), 0)
            db.execute("DELETE FROM customers WHERE id = 1")
            self.assertEqual(check.violation_count(db), 2)
