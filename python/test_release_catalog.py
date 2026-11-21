import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import sys
from threading import Barrier

import duckdb

from duckorch_embedded import GenerationCatalog, ReleaseModel, StaleReleasePlan


class GenerationCatalogTests(unittest.TestCase):
    def test_publication_switches_all_models_and_rollback_restores_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "tenant.duckdb")
            with duckdb.connect(database) as db:
                catalog = GenerationCatalog(db, "production", {"orders", "report"})
                v1 = {
                    "orders": ReleaseModel("orders-v1", "schema-a"),
                    "report": ReleaseModel("report-v1", "schema-b"),
                }
                v2 = {
                    "orders": ReleaseModel("orders-v2", "schema-a"),
                    "report": ReleaseModel("report-v2", "schema-b"),
                }
                catalog.set_inputs({"raw": "source-v1"})
                catalog.stage("v1", v1, inputs={"raw": "source-v1"}, checks={"quality": True})
                initial = catalog.publish(catalog.prepare("v1"))
                self.assertEqual(dict(initial.models), v1)
                self.assertEqual(dict(initial.inputs), {"raw": "source-v1"})
                with self.assertRaises(ValueError):
                    catalog.stage("invalid", v2, inputs={"raw": "source-v2"}, checks={"quality": False})
                catalog.set_inputs({"raw": "source-v2"})
                catalog.stage("v2", v2, inputs={"raw": "source-v2"}, checks={"quality": True})
                plan = catalog.prepare("v2")
            with duckdb.connect(database) as db:
                catalog = GenerationCatalog(db, "production", {"orders", "report"})
                self.assertEqual(catalog.active(), initial)
                second = catalog.publish(plan)
                self.assertEqual(dict(second.models), v2)
                self.assertEqual(dict(second.inputs), {"raw": "source-v2"})
                back = catalog.rollback("v1", expected_epoch=second.epoch)
                self.assertEqual((dict(back.models), dict(back.inputs)), (v1, {"raw": "source-v1"}))
                self.assertGreater(back.epoch, initial.epoch)
                with self.assertRaises(StaleReleasePlan):
                    catalog.publish(plan)

    def test_parallel_plans_and_input_changes_cannot_overwrite_active_generation(self):
        with duckdb.connect() as db:
            catalog = GenerationCatalog(db, "production", {"orders"})
            catalog.set_inputs({"raw": "a"})
            for release in ("left", "right"):
                catalog.stage(release, {"orders": ReleaseModel(release, "schema")},
                              inputs={"raw": "a"}, checks={"quality": True})
            left = catalog.prepare("left")
            right = catalog.prepare("right")
            catalog.publish(right)
            with self.assertRaises(StaleReleasePlan):
                catalog.publish(left)
            retry = catalog.prepare("left")
            catalog.set_inputs({"raw": "b"})
            with self.assertRaises(StaleReleasePlan):
                catalog.publish(retry)
            self.assertEqual(catalog.active().revision, "right")

    def test_parallel_publication_has_one_winner(self):
        with duckdb.connect() as db:
            catalog = GenerationCatalog(db, "production", {"orders"})
            plans = []
            for i in range(5):
                release = str(i)
                catalog.stage(release, {"orders": ReleaseModel(release, "schema")},
                              inputs={}, checks={"quality": True})
                plans.append(catalog.prepare(release))
            barrier = Barrier(len(plans))

            def publish(plan):
                barrier.wait()
                try:
                    return catalog.publish(plan)
                except StaleReleasePlan:
                    return None

            with ThreadPoolExecutor(len(plans)) as pool:
                winners = [snapshot for snapshot in pool.map(publish, plans) if snapshot is not None]
            self.assertEqual(len(winners), 1)
            self.assertEqual(winners[0], catalog.active())

    def test_abort_before_commit_and_exit_after_commit_on_disk(self):
        child = """
import os
import sys
sys.path.insert(0, sys.argv[3])
import duckdb
from duckorch_embedded import GenerationCatalog

class ExitOnSnapshot(GenerationCatalog):
    @staticmethod
    def _snapshot(*args):
        os._exit(41)

catalog_type = ExitOnSnapshot if sys.argv[2] == 'before' else GenerationCatalog
db = duckdb.connect(sys.argv[1])
catalog = catalog_type(db, 'production', {'orders', 'report'})
catalog.publish(catalog.prepare('v2'))
os._exit(42)
"""
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "tenant.duckdb")
            with duckdb.connect(path) as db:
                catalog = GenerationCatalog(db, "production", {"orders", "report"})
                for version in ("v1", "v2"):
                    models = {name: ReleaseModel(f"{name}-{version}", "contract")
                              for name in ("orders", "report")}
                    catalog.stage(version, models, inputs={}, checks={"quality": True})
                catalog.publish(catalog.prepare("v1"))
            for phase, code, version in (("before", 41, "v1"), ("after", 42, "v2")):
                finished = subprocess.run([sys.executable, "-c", child, path, phase,
                                           str(Path(__file__).resolve().parent)], timeout=30)
                self.assertEqual(finished.returncode, code)
                with duckdb.connect(path) as db:
                    catalog = GenerationCatalog(db, "production", {"orders", "report"})
                    release = catalog.active()
                    self.assertEqual([event.revision for event in catalog.pending_notifications(limit=10)],
                                     ["v1"] if phase == "before" else ["v1", "v2"])
                    self.assertEqual(release.revision, version)
                    self.assertEqual([model.physical_version for _, model in release.models],
                                     [f"orders-{version}", f"report-{version}"])

    def test_outbox_replays_stable_notifications_until_acknowledged(self):
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "tenant.duckdb")
            with duckdb.connect(database) as db:
                catalog = GenerationCatalog(db, "production", {"orders"})
                for revision in ("v1", "v2"):
                    catalog.stage(revision, {"orders": ReleaseModel(revision, "schema")},
                                  inputs={}, checks={"quality": True})
                    active = catalog.publish(catalog.prepare(revision))
                catalog.rollback("v1", expected_epoch=active.epoch)
                first, = catalog.pending_notifications(limit=1)
                trial = catalog.fork("trial")
                self.assertFalse(trial.pending_notifications(limit=10))
                self.assertEqual(trial.acknowledge_notifications([first.event_id]), 0)
            with duckdb.connect(database) as db:
                catalog = GenerationCatalog(db, "production", {"orders"})
                self.assertEqual(catalog.pending_notifications(limit=1), [first])
                all_events = catalog.pending_notifications(limit=10)
                self.assertEqual([(e.epoch, e.revision, e.kind) for e in all_events],
                                 [(1, "v1", "publish"), (2, "v2", "publish"), (3, "v1", "rollback")])
                self.assertEqual(catalog.acknowledge_notifications([first.event_id, first.event_id]), 1)
                self.assertEqual(catalog.acknowledge_notifications([first.event_id]), 0)
                self.assertEqual(catalog.pending_notifications(limit=10), all_events[1:])

    def test_parallel_request_replay_returns_one_durable_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "tenant.duckdb")
            with duckdb.connect(database) as db:
                catalog = GenerationCatalog(db, "production", {"orders"})
                for revision in ("v1", "v2"):
                    catalog.stage(revision, {"orders": ReleaseModel(revision, "schema")},
                                  inputs={}, checks={"quality": True})
                plan = catalog.prepare("v1")
                barrier = Barrier(10)

                def publish(attempt):
                    if attempt < 10:
                        barrier.wait()
                    return catalog.publish(plan, request_key="run-1")

                with ThreadPoolExecutor(10) as pool:
                    results = list(pool.map(publish, range(100)))
                self.assertTrue(all(result == results[0] for result in results))
                self.assertEqual(len(catalog.pending_notifications(limit=10)), 1)
                with self.assertRaises(ValueError):
                    catalog.publish(catalog.prepare("v2"), request_key="run-1")
                catalog.publish(catalog.prepare("v2"), request_key="run-2")
            with duckdb.connect(database) as db:
                catalog = GenerationCatalog(db, "production", {"orders"})
                self.assertEqual(catalog.active().revision, "v2")
                self.assertEqual(catalog.publish(plan, request_key="run-1"), results[0])
                self.assertEqual(len(catalog.pending_notifications(limit=10)), 2)

    def test_trial_reuses_unchanged_versions_and_stays_pinned(self):
        with duckdb.connect() as db:
            production = GenerationCatalog(db, "production", {"sales", "report"})
            snapshot = {"sales": ReleaseModel("sales-v1", "contract"),
                        "report": ReleaseModel("report-v1", "contract")}
            production.stage("v1", snapshot, inputs={}, checks={"quality": True})
            production.publish(production.prepare("v1"))
            trial = production.fork("trial")
            self.assertEqual(dict(trial.active().models), snapshot)
            changed = dict(snapshot, report=ReleaseModel("trial-report", "contract"))
            trial.stage("trial-v2", changed, inputs={}, checks={"quality": True})
            trial.publish(trial.prepare("trial-v2"))
            next_production = dict(snapshot, sales=ReleaseModel("sales-v2", "contract"))
            production.stage("v2", next_production, inputs={}, checks={"quality": True})
            production.publish(production.prepare("v2"))
            self.assertEqual(dict(trial.active().models), changed)
            self.assertEqual(dict(production.active().models), next_production)
            with self.assertRaises(ValueError):
                production.fork("trial")
