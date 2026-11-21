import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from duckorch_embedded import FingerprintPlanner, FingerprintStore, ModelFingerprint


class ModelFingerprintTests(unittest.TestCase):
    def test_macro_change_selects_model_and_dependents(self):
        planner = FingerprintPlanner({"source": (), "staging": ("source",), "report": ("staging",), "other": ()})
        def fingerprint(sql, macros=None, upstream_versions=None):
            return ModelFingerprint.capture(sql, macros=macros, upstream_versions=upstream_versions,
                                            settings={"timezone": "UTC", "threads": "1"}, engine_version="2.0.0")
        initial = {"source": fingerprint("select 1"),
                   "staging": fingerprint("select prepare(value) from source", {"prepare": "v1"}, {"source": "r1"}),
                   "report": fingerprint("select sum(value) from staging", upstream_versions={"staging": "r1"}),
                   "other": fingerprint("select 2")}
        revised = dict(initial)
        revised["staging"] = fingerprint("select prepare(value) from source", {"prepare": "v2"}, {"source": "r1"})
        self.assertEqual(planner.plan(initial, revised), {"staging": ("macros",), "report": ("dependency:staging",)})
        self.assertEqual(planner.plan(initial, initial), {})
        unresolved = dict(initial)
        unresolved["report"] = fingerprint("select sum(value) from staging", upstream_versions={"staging": None})
        self.assertEqual(planner.plan(unresolved, unresolved), {"report": ("unresolved:staging",)})

    def test_capture_is_deterministic_and_executable_fields_are_independent(self):
        first = ModelFingerprint.capture("select 1", settings={"z":"7", "a":"3"}, engine_version="2.0.0")
        same = ModelFingerprint.capture("select 1", settings={"a":"3", "z":"7"}, engine_version="2.0.0")
        self.assertEqual(first, same)
        sql = ModelFingerprint.capture("select 2", settings={"a":"3", "z":"7"}, engine_version="2.0.0")
        self.assertEqual(first.differences(sql), ("definition",))

    def test_named_snapshots_survive_reopen_and_cannot_be_overwritten(self):
        baseline = ModelFingerprint.capture("select 1", engine_version="2.0.0", upstream_versions={"raw": None})
        with TemporaryDirectory() as directory:
            path = str(Path(directory) / "tenant.duckdb")
            with duckdb.connect(path) as db:
                FingerprintStore(db).save("first", {"model": baseline})
            with duckdb.connect(path) as db:
                store = FingerprintStore(db)
                with self.assertRaises(ValueError):
                    store.save("first", {"different": baseline})
                self.assertEqual(store.load("first"), {"model": baseline})
