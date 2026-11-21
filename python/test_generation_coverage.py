import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path
from threading import Barrier

import duckdb

from duckorch_embedded import DateWindow, GenerationCoverage


START = date(2026, 1, 1)


def window(start, end):
    return DateWindow(START + timedelta(days=start), START + timedelta(days=end))


class GenerationCoverageTests(unittest.TestCase):
    def test_adjacent_claims_share_one_job_and_keep_their_own_outcomes(self):
        with duckdb.connect() as db:
            queue = GenerationCoverage(db, {"daily": "sales"})
            first, = queue.enqueue("daily", "v1", window(0, 2))
            second, = queue.enqueue("daily", "v1", window(2, 5))
            different, = queue.enqueue("daily", "v2", window(5, 6))
            batches = queue.batches([second, different, first])
            self.assertEqual([(b.generation, b.window) for b in batches],
                             [("v1", window(0, 5)), ("v2", window(5, 6))])
            self.assertEqual(batches[0].claims, (first, second))
            self.assertEqual(queue.settle(batches[0], {first.claim_id}), (True, True))
            retry, = queue.enqueue("daily", "v1", window(0, 5))
            self.assertEqual(retry.window, second.window)
            self.assertNotEqual(retry.claim_id, second.claim_id)
            self.assertEqual(queue.settle(batches[0], {first.claim_id}), (False, False))

    def test_overlapping_requests_retries_and_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "tenant.duckdb")
            with duckdb.connect(path) as db:
                queue = GenerationCoverage(db, {"daily": "owner's sales"})
                left, = queue.enqueue("daily", "v1", window(2, 4))
                right, = queue.enqueue("daily", "v1", window(4, 6))
                self.assertTrue(queue.finish(left, success=True))
                self.assertEqual(queue.enqueue("daily", "v1", window(2, 6)), [])
                self.assertEqual(queue.enqueue("daily", "v1", window(4, 4)), [])
                self.assertTrue(queue.finish(right, success=False))
                self.assertFalse(queue.finish(right, success=False))
            with duckdb.connect(path) as db:
                queue = GenerationCoverage(db, {"daily": "owner's sales"})
                work = queue.enqueue("daily", "v1", window(0, 8))
                self.assertEqual([r.window for r in work], [window(0, 2), window(4, 8)])
                self.assertFalse(queue.finish(right, success=True))
                self.assertEqual(queue.enqueue("daily", "v1", window(0, 8)), [])
                for claim in work:
                    self.assertTrue(queue.finish(claim, success=True))
                self.assertEqual(queue.enqueue("daily", "v1", window(0, 8)), [])
                next_version, = queue.enqueue("daily", "v2", window(0, 8))
                self.assertEqual(next_version.window, window(0, 8))
                with self.assertRaises(KeyError):
                    queue.enqueue("unregistered", "v1", window(0, 8))

    def test_concurrent_overlapping_requests_cover_each_date_once(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "tenant.duckdb")
            with duckdb.connect(path) as db:
                queue = GenerationCoverage(db, {"model": "sales"})
                intervals = [(0, 6), (3, 9), (2, 4), (4, 7), (5, 9), (1, 3)]
                barrier = Barrier(len(intervals))

                def claim(bounds):
                    barrier.wait()
                    return queue.enqueue("model", "v1", window(*bounds))

                with ThreadPoolExecutor(len(intervals)) as pool:
                    batches = list(pool.map(claim, intervals))
                claimed = []
                for batch in batches:
                    for item in batch:
                        claimed.extend(range((item.window.start - START).days, (item.window.end - START).days))
                self.assertEqual(sorted(claimed), list(range(9)))
                self.assertEqual(queue.enqueue("model", "v1", window(0, 9)), [])
