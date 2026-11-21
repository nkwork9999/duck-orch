import unittest

import duckdb

from duckorch_embedded import ColumnContract, ColumnType, RelationContract


class SchemaContractTests(unittest.TestCase):
    def test_checks_names_types_and_nullability_on_an_empty_relation(self):
        with duckdb.connect() as db:
            db.execute("CREATE TABLE release(id BIGINT NOT NULL, price DOUBLE, day DATE)")
            contract = RelationContract({
                "id": ColumnContract(ColumnType.BIGINT, nullable=False),
                "price": ColumnContract(ColumnType.DOUBLE),
                "day": ColumnContract(ColumnType.DATE),
            }, allow_extra=False)
            self.assertEqual(contract.inspect(db, "release"), ())
            db.execute("CREATE TABLE candidate(id INTEGER, renamed DOUBLE, extra BOOLEAN)")
            result = contract.inspect(db, "candidate")
            self.assertEqual({(problem.column, problem.kind) for problem in result}, {
                ("id", "type"), ("id", "nullability"), ("price", "missing"), ("day", "missing"),
                ("renamed", "unexpected"), ("extra", "unexpected"),
            })
            with self.assertRaises(KeyError):
                contract.inspect(db, "does not exist")
