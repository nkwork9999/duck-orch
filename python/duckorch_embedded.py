"""Read-side planning for a host-provisioned, tenant-scoped duckOrch connection."""
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from graphlib import CycleError, TopologicalSorter
from time import sleep
from uuid import UUID

from duckdb import ConstraintException, DuckDBPyConnection, TransactionException


class DailyPartitionPlanner:
    """Map application model IDs to registered daily assets on one connection."""

    def __init__(self, connection: DuckDBPyConnection, models: Mapping[str, str]):
        self._db = connection
        self._models = dict(models)

    def pending(self, model_id: str, start: date, end: date) -> list[date]:
        """Registered days in [start, end) whose latest attempt has not succeeded."""
        asset = self._models[model_id]
        if type(start) is not date or type(end) is not date:
            raise TypeError("start and end must be dates")
        if end < start:
            raise ValueError("end must be on or after start")
        days = self._db.execute("""
            WITH requested AS (
                SELECT partition_key FROM __orch__.asset_partitions
                WHERE asset_name = ? AND partition_key >= ? AND partition_key < ?
            ), latest AS (
                SELECT partition_key, status
                FROM __orch__.asset_materializations SEMI JOIN requested USING (partition_key)
                WHERE asset_name = ?
                QUALIFY row_number() OVER (
                    PARTITION BY partition_key ORDER BY materialized_at DESC
                ) = 1
            )
            SELECT partition_key FROM requested LEFT JOIN latest USING (partition_key)
            WHERE status IS DISTINCT FROM 'success' ORDER BY partition_key
        """, [asset, start.isoformat(), end.isoformat(), asset]).fetchall()
        return [date.fromisoformat(key) for (key,) in days]

    def pending_ranges(self, model_id: str, start: date, end: date) -> list[tuple[date, date]]:
        """Coalesce adjacent pending registered days into half-open ranges."""
        ranges: list[tuple[date, date]] = []
        for day in self.pending(model_id, start, end):
            following_day = day + timedelta(days=1)
            if ranges and ranges[-1][1] == day:
                ranges[-1] = (ranges[-1][0], following_day)
            else:
                ranges.append((day, following_day))
        return ranges


@dataclass(frozen=True)
class DateWindow:
    """Half-open calendar date range."""
    start: date
    end: date

    def __post_init__(self):
        if type(self.start) is not date or type(self.end) is not date:
            raise TypeError("window boundaries must be dates")
        if self.end < self.start:
            raise ValueError("window end must be on or after start")


class PartitionGrain(Enum):
    DAY = "day"
    MONTH = "month"
    ALL = "all"


@dataclass(frozen=True)
class RollingDependency:
    """Days before and after an output date that its calculation reads."""

    lookback: int = 0
    lookahead: int = 0

    def __post_init__(self):
        if any(type(value) is not int or value < 0 for value in (self.lookback, self.lookahead)):
            raise ValueError("rolling dependency widths must be nonnegative integer days")


@dataclass(frozen=True)
class CoverageClaim:
    claim_id: UUID
    model_id: str
    generation: str
    window: DateWindow


class GenerationCoverage:
    """Durable half-open work intervals on a host-owned, tenant-specific database."""

    def __init__(self, connection: DuckDBPyConnection, models: Mapping[str, str]):
        self._db = connection
        self._models = dict(models)
        self._write(self._initialize)

    @staticmethod
    def _initialize(tx):
        tx.execute("CREATE SCHEMA IF NOT EXISTS __orch__")
        tx.execute("""
            CREATE TABLE IF NOT EXISTS __orch__.coverage_generations (
                asset VARCHAR, generation VARCHAR, revision BIGINT NOT NULL,
                PRIMARY KEY (asset, generation)
            )
        """)
        tx.execute("""
            CREATE TABLE IF NOT EXISTS __orch__.coverage_claims (
                claim_id UUID PRIMARY KEY, asset VARCHAR NOT NULL, generation VARCHAR NOT NULL,
                lo DATE NOT NULL, hi DATE NOT NULL, completed BOOLEAN NOT NULL,
                CHECK (lo < hi)
            )
        """)

    def _write(self, operation):
        # An independent cursor owns the transaction. Retrying after a concurrent
        # writer commits must recompute the gaps from a fresh snapshot.
        for attempt in range(12):
            try:
                with self._db.cursor() as tx:
                    tx.execute("BEGIN TRANSACTION")
                    result = operation(tx)
                    tx.execute("COMMIT")
                    return result
            except (TransactionException, ConstraintException) as error:
                # Concurrent first inserts can surface as a duplicate-key error
                # even with ON CONFLICT when their snapshots started together.
                if attempt == 11 or (isinstance(error, ConstraintException)
                                     and "duplicate key" not in str(error).lower()):
                    raise
                sleep(min(0.002 * 2**attempt, 0.1))

    def enqueue(self, model_id: str, generation: str, requested: DateWindow) -> list[CoverageClaim]:
        """Atomically reserve just the pieces not already queued or completed."""
        asset = self._models[model_id]
        if type(generation) is not str or not generation:
            raise ValueError("generation must be a nonempty version string")
        if type(requested) is not DateWindow:
            raise TypeError("requested must be a DateWindow")
        if requested.start == requested.end:
            return []

        def reserve(tx):
            # Force overlapping reservations for this generation to conflict,
            # including two writers that initially see an empty interval table.
            tx.execute("""
                INSERT INTO __orch__.coverage_generations VALUES (?, ?, 1)
                ON CONFLICT (asset, generation) DO UPDATE SET revision = revision + 1
            """, [asset, generation])
            rows = tx.execute("""
                INSERT INTO __orch__.coverage_claims
                WITH req AS (SELECT ?::DATE AS first_day, ?::DATE AS last_day),
                occupied AS (
                    SELECT greatest(lo, first_day) AS lo, least(hi, last_day) AS hi
                    FROM __orch__.coverage_claims, req
                    WHERE asset = ? AND generation = ? AND lo < last_day AND hi > first_day
                    UNION ALL SELECT last_day, last_day FROM req
                ), gaps AS (
                    SELECT coalesce(max(hi) OVER (ORDER BY lo, hi ROWS BETWEEN
                               UNBOUNDED PRECEDING AND 1 PRECEDING), first_day) AS gap_lo,
                           lo AS gap_hi FROM occupied, req
                )
                SELECT uuid(), ?, ?, gap_lo, gap_hi, false FROM gaps WHERE gap_lo < gap_hi
                RETURNING claim_id, lo, hi
            """, [requested.start, requested.end, asset, generation, asset, generation]).fetchall()
            return [CoverageClaim(key, model_id, generation, DateWindow(lo, hi))
                    for key, lo, hi in sorted(rows, key=lambda row: row[1])]

        return self._write(reserve)

    def finish(self, claim: CoverageClaim, *, success: bool) -> bool:
        """Keep successful coverage or release a failed reservation for retry."""
        if type(claim) is not CoverageClaim or type(success) is not bool:
            raise TypeError("finish expects a CoverageClaim and a boolean outcome")
        asset = self._models[claim.model_id]
        operation = ("UPDATE __orch__.coverage_claims SET completed = true" if success
                     else "DELETE FROM __orch__.coverage_claims")
        sql = operation + """
            WHERE claim_id = ? AND asset = ? AND generation = ? AND lo = ? AND hi = ?
              AND NOT completed RETURNING claim_id
        """
        args = [claim.claim_id, asset, claim.generation, claim.window.start, claim.window.end]
        return self._write(lambda tx: tx.execute(sql, args).fetchone() is not None)


class ChangeWindowPlanner:
    """Read-side change plan for a host-registered calendar DAG.

    The caller supplies a tenant-specific graph and retained date horizon at
    construction. Use ALL when an edge has no established partition mapping.
    """

    def __init__(self, dependencies: Mapping[str, Mapping[str, PartitionGrain | RollingDependency]],
                 horizon: DateWindow):
        if type(horizon) is not DateWindow:
            raise TypeError("horizon must be a DateWindow")
        self._horizon = horizon
        self._dependencies = {model: dict(parents) for model, parents in dependencies.items()}
        for parents in self._dependencies.values():
            if not parents.keys() <= self._dependencies.keys():
                raise ValueError("all upstream models must be registered")
            if any(type(grain) not in (PartitionGrain, RollingDependency) for grain in parents.values()):
                raise TypeError("dependencies must use PartitionGrain or RollingDependency")
        try:
            self._order = tuple(TopologicalSorter(self._dependencies).static_order())
        except CycleError as exc:
            raise ValueError("model dependencies contain a cycle") from exc

    def _expand(self, window: DateWindow, grain: PartitionGrain | RollingDependency) -> DateWindow:
        if type(grain) is RollingDependency:
            before = min(grain.lookahead, (window.start - self._horizon.start).days)
            after = min(grain.lookback, (self._horizon.end - window.end).days)
            return DateWindow(window.start - timedelta(days=before), window.end + timedelta(days=after))
        if grain is PartitionGrain.DAY:
            return window
        if grain is PartitionGrain.ALL:
            return self._horizon

        start = window.start.replace(day=1)
        end = window.end
        if end.day != 1:
            if end.month < 12:
                end = end.replace(month=end.month + 1, day=1)
            elif end.year < date.max.year:
                end = date(end.year + 1, 1, 1)
            else:
                end = self._horizon.end
        return DateWindow(max(start, self._horizon.start), min(end, self._horizon.end))

    def affected(self, source_id: str, changed: DateWindow) -> dict[str, tuple[DateWindow, ...]]:
        """Propagate one changed window to downstream models in topological order."""
        return self.affected_many({source_id: (changed,)})

    @staticmethod
    def _merge(windows: Iterable[DateWindow]) -> tuple[DateWindow, ...]:
        result = []
        for window in sorted(windows, key=lambda span: (span.start, span.end)):
            if result and window.start <= result[-1].end:
                prev = result[-1]
                result[-1] = DateWindow(prev.start, max(prev.end, window.end))
            else:
                result.append(window)
        return tuple(result)

    def affected_many(self, changes: Mapping[str, Iterable[DateWindow]]) -> dict[str, tuple[DateWindow, ...]]:
        """Propagate disjoint source changes and merge touching output intervals."""
        direct = {}
        for model, changes_for_model in changes.items():
            if model not in self._dependencies:
                raise KeyError(model)
            clipped = []
            for changed in changes_for_model:
                if type(changed) is not DateWindow:
                    raise TypeError("changed must be a DateWindow")
                start = max(changed.start, self._horizon.start)
                end = min(changed.end, self._horizon.end)
                if start < end:
                    clipped.append(DateWindow(start, end))
            direct[model] = self._merge(clipped)

        propagated = {}
        outputs = {}
        for model in self._order:
            incoming = self._merge(
                self._expand(window, grain)
                for parent, grain in self._dependencies[model].items()
                for window in propagated.get(parent, ())
            )
            if incoming:
                outputs[model] = incoming
            propagated[model] = self._merge((*incoming, *direct.get(model, ())))
        return outputs


class QualityCheckSelector:
    """Choose registered checks from changes to their model and column inputs."""

    def __init__(self, checks: Mapping[str, Mapping[str, Iterable[str] | None] | None]):
        self._unresolved = set()
        self._dependencies = {}
        for check_id, inputs in checks.items():
            if inputs is None or not inputs:
                self._unresolved.add(check_id)
                continue
            for model, columns in inputs.items():
                dependencies = self._dependencies.setdefault(model, [])
                dependencies.append((check_id, None if columns is None else frozenset(columns)))

    def select(self, changes: Mapping[str, Iterable[str] | None]) -> tuple[str, ...]:
        """None marks a whole-model change or a dependency with unknown columns."""
        if not changes:
            return ()
        selected = set(self._unresolved)
        for model, columns in changes.items():
            changed = None if columns is None else frozenset(columns)
            for check_id, referenced in self._dependencies.get(model, ()):
                if changed is None or referenced is None or not referenced.isdisjoint(changed):
                    selected.add(check_id)
        return tuple(sorted(selected))


@dataclass(frozen=True)
class ForeignKeyCheck:
    """Count non-null child keys missing from a registered parent relation."""

    child: str
    child_key: str
    parent: str
    parent_key: str

    def __post_init__(self):
        for name in (self.child, self.child_key, self.parent, self.parent_key):
            if type(name) is not str or not name or "\0" in name:
                raise ValueError("check identifiers must be nonempty strings without NUL")

    def violation_count(self, connection: DuckDBPyConnection) -> int:
        def quote(identifier):
            return '"' + identifier.replace('"', '""') + '"'

        child, child_key, parent, parent_key = (quote(name) for name in (
            self.child, self.child_key, self.parent, self.parent_key,
        ))
        missing, = connection.execute(f"""
            SELECT count(*) FROM main.{child} AS c
            ANTI JOIN main.{parent} AS p ON c.{child_key} = p.{parent_key}
            WHERE c.{child_key} IS NOT NULL
        """).fetchone()
        return missing
