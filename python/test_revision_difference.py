import unittest

import duckdb

from duckorch_embedded import RevisionDifference


class RevisionDifferenceTests(unittest.TestCase):
    def test_keys_nulls_and_bounded_examples_match_rows_without_ambiguous_keys(self):
        with duckdb.connect() as db:
            db.execute("""
                CREATE TABLE old_data(id BIGINT, "display label" VARCHAR, amount INTEGER);
                CREATE TABLE new_data(id BIGINT, "display label" VARCHAR, amount INTEGER);
                INSERT INTO old_data VALUES (1, NULL, 10), (2, 'remove', 2), (3, 'equal', NULL),
                    (4, 'duplicate a', 11), (4, 'duplicate b', 12), (6, 'to duplicate', 6), (NULL, 'invalid', 1);
                INSERT INTO new_data VALUES (1, '', 10), (3, 'equal', NULL), (5, 'add', 5),
                    (4, 'cannot join', 11), (6, 'duplicate c', 6), (6, 'duplicate d', 6), (NULL, 'invalid', 2);
            """)
            comparator = RevisionDifference(db, "old_data", "new_data", key="id",
                                            columns={"label": "display label", "amount": "amount"})
            result = comparator.compare(example_limit=2)
            self.assertEqual((result.added, result.removed, result.changed, result.unchanged), (1, 1, 1, 1))
            self.assertEqual(result.column_changes, {"label": 1, "amount": 0})
            self.assertEqual((result.previous_duplicate_keys, result.current_duplicate_keys), (1, 1))
            self.assertEqual((result.previous_null_keys, result.current_null_keys), (1, 1))
            self.assertEqual([(e.key,e.kind) for e in result.examples], [(1,"changed"),(2,"removed")])
            self.assertEqual((result.examples[0].previous, result.examples[0].current), (
                {"label": None, "amount": 10}, {"label": "", "amount": 10},
            ))
            self.assertEqual(comparator.compare(example_limit=0).examples, ())
