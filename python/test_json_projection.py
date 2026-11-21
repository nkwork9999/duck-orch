import unittest

import duckdb

from duckorch_embedded import JsonField, JsonProjector, JsonType


class JsonProjectionTests(unittest.TestCase):
    def test_contract_distinguishes_missing_null_and_wrong_type(self):
        data = [
            '{"metrics":{"count":4},"value":"x","a/b":{"~field":true},"extra":12}',
            '{"metrics":{"count":null},"value":"null"}',
            '{"metrics":{"count":"4"},"value":3}',
            '{"metrics":{"count":9223372036854775808}}',
            '{invalid',
        ]
        with duckdb.connect() as db:
            projector = JsonProjector(db, {
                "count": JsonField(("metrics", "count"), JsonType.INTEGER),
                "value": JsonField(("value",), JsonType.TEXT),
                "flag": JsonField(("a/b", "~field"), JsonType.BOOLEAN),
            }, keep_source=True)
            rows = projector.project(data)
            self.assertEqual(rows[0].values, {"count": 4, "value": "x", "flag": True})
            self.assertEqual(rows[0].source, data[0])
            self.assertEqual(rows[0].issues, {})
            self.assertEqual(rows[1].values["value"], "null")
            self.assertEqual(rows[1].issues, {"count": "null", "flag": "missing"})
            self.assertEqual(rows[2].issues, {"count": "invalid_type", "value": "invalid_type", "flag": "missing"})
            self.assertEqual(rows[3].issues["count"], "out_of_range")
            self.assertEqual(rows[4].issues, dict.fromkeys(("count", "value", "flag"), "invalid_document"))
            old = db.execute("SELECT json_extract(?::JSON, '$.metrics.count')::BIGINT", [data[0]]).fetchone()[0]
            self.assertEqual(rows[0].values["count"], old)
            self.assertEqual(projector.project([]), [])
