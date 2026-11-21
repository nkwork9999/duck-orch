from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import duckdb

from duckorch_embedded import FileIngestionLedger, FileRevision


class FileIngestionLedgerTests(unittest.TestCase):
    def test_same_file_name_with_new_version_is_pending_after_restart(self):
        with TemporaryDirectory() as directory:
            database = Path(directory) / "tenant.duckdb"
            old = FileRevision("s3://example-bucket/warehouse/data.parquet", "version-1")
            replaced = FileRevision(old.uri, "version-2")
            other = FileRevision("s3://example-bucket/warehouse/more.parquet", "version-1")
            with duckdb.connect(database) as db:
                first = FileIngestionLedger(db, "orders")
                self.assertEqual(first.pending([old, other, old]), [old, other])
                self.assertEqual(first.complete([old, old]), 1)
                self.assertEqual(first.complete([old]), 0)
                self.assertEqual(FileIngestionLedger(db, "returns").pending([old]), [old])
            with duckdb.connect(database) as db:
                ledger = FileIngestionLedger(db, "orders")
                self.assertEqual(ledger.pending([old, replaced, other, replaced]), [replaced, other])
                self.assertEqual(ledger.pending([]), [])
                self.assertEqual(ledger.complete([replaced, other]), 2)
                self.assertEqual(ledger.pending([old, replaced, other]), [])

    def test_revision_requires_nonempty_plain_host_metadata(self):
        for invalid in ["", "\0", None]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                FileRevision("s3://example-bucket/data.parquet", invalid)
