"""Measure durable reservation for 10,000 gaps. Run from the repository root."""
import csv
import sys
from datetime import date, timedelta
from time import perf_counter_ns

import duckdb

from duckorch_embedded import DateWindow, GenerationCoverage


def main():
    fragments = 10_000
    start = date(2000, 1, 1)
    requested = DateWindow(start, start + timedelta(days=2 * fragments))
    expected = [DateWindow(start + timedelta(days=i), start + timedelta(days=i + 1))
                for i in range(1, 2 * fragments, 2)]
    writer = csv.writer(sys.stdout)
    writer.writerow(["run", "duckdb_version", "existing_intervals", "reserved_gaps", "elapsed_ns"])
    for trial in range(-3, 30):
        with duckdb.connect(config={"threads": 1}) as db:
            queue = GenerationCoverage(db, {"daily": "sales"})
            db.execute("""
                INSERT INTO __orch__.coverage_claims
                SELECT uuid(), 'sales', 'v1', ?::DATE + (2*i)::INT, ?::DATE + (2*i+1)::INT, true
                FROM range(?) AS t(i)
            """, [start, start, fragments])
            before = perf_counter_ns()
            claims = queue.enqueue("daily", "v1", requested)
            elapsed = perf_counter_ns() - before
            if [claim.window for claim in claims] != expected or queue.enqueue("daily", "v1", requested):
                raise AssertionError("coverage mismatch or duplicate reservations")
            if trial >= 0:
                writer.writerow([trial + 1, duckdb.__version__, fragments, len(claims), elapsed])


if __name__ == "__main__":
    main()
