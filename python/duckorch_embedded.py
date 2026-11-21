"""Read-side planning for a host-provisioned, tenant-scoped duckOrch connection."""
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from graphlib import CycleError, TopologicalSorter
from hashlib import sha256
from json import dumps, loads
from math import isfinite
from time import sleep
from uuid import UUID

from duckdb import ConstraintException, DuckDBPyConnection, InvalidInputException, TransactionException


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
class PlanStep:
    tenant_id: str
    model_id: str
    window: DateWindow
    reason: str
    parent_id: str | None
    estimated_bytes: int | None


@dataclass(frozen=True)
class CoverageClaim:
    claim_id: UUID
    model_id: str
    generation: str
    window: DateWindow


@dataclass(frozen=True)
class CoverageBatch:
    claims: tuple[CoverageClaim, ...]

    def __post_init__(self):
        if type(self.claims) is not tuple or not self.claims or any(type(c) is not CoverageClaim for c in self.claims):
            raise ValueError("a batch must include coverage claims")
        if any((a.model_id, a.generation, a.window.end) != (b.model_id, b.generation, b.window.start)
               for a, b in zip(self.claims, self.claims[1:])):
            raise ValueError("a batch must be contiguous and fixed to one model and generation")

    @property
    def generation(self):
        return self.claims[0].generation

    @property
    def window(self):
        return DateWindow(self.claims[0].window.start, self.claims[-1].window.end)


def _write_transaction(connection, operation):
    # Retrying after a concurrent writer commits requires a fresh snapshot.
    for attempt in range(12):
        try:
            with connection.cursor() as tx:
                tx.execute("BEGIN TRANSACTION")
                result = operation(tx)
                tx.execute("COMMIT")
                return result
        except (TransactionException, ConstraintException) as error:
            # Concurrent first inserts can report a duplicate key after both
            # transactions observed the key as absent.
            if attempt == 11 or (isinstance(error, ConstraintException)
                                 and "duplicate key" not in str(error).lower()):
                raise
            sleep(min(0.002 * 2**attempt, 0.1))


class GenerationCoverage:
    """Durable half-open work intervals on a host-owned, tenant-specific database."""

    def __init__(self, connection: DuckDBPyConnection, models: Mapping[str, str]):
        self._db = connection
        self._models = dict(models)
        _write_transaction(self._db, self._initialize)

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

        return _write_transaction(self._db, reserve)

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
        return _write_transaction(self._db, lambda tx: tx.execute(sql, args).fetchone() is not None)

    @staticmethod
    def batches(claims: list[CoverageClaim]) -> list[CoverageBatch]:
        """Combine adjacent intervals from separate requests into one job."""
        if type(claims) is not list or any(type(claim) is not CoverageClaim for claim in claims):
            raise TypeError("claims must be a list of CoverageClaim values")
        ordered = sorted(claims, key=lambda c: (c.model_id, c.generation, c.window.start, c.window.end))
        grouped = []
        for claim in ordered:
            if grouped:
                previous = grouped[-1][-1]
                if (previous.model_id, previous.generation) == (claim.model_id, claim.generation):
                    if claim.window.start < previous.window.end:
                        raise ValueError("claims for a model version cannot overlap")
                    if claim.window.start == previous.window.end:
                        grouped[-1].append(claim)
                        continue
            grouped.append([claim])
        return [CoverageBatch(tuple(group)) for group in grouped]

    def settle(self, batch: CoverageBatch, succeeded: set[UUID]) -> tuple[bool, ...]:
        """Commit successful portions and release failed portions in one transaction."""
        if type(batch) is not CoverageBatch or type(succeeded) is not set:
            raise TypeError("expected a CoverageBatch and a set of successful claim IDs")
        keys = {c.claim_id for c in batch.claims}
        if not succeeded <= keys:
            raise ValueError("successful claims must belong to this batch")
        asset = self._models[batch.claims[0].model_id]
        selected = """
            SELECT unnest(?::UUID[]) AS claim_key, unnest(?::DATE[]) AS first_day,
                   unnest(?::DATE[]) AS last_day
        """
        condition = """
            c.claim_id = r.claim_key AND c.asset = ? AND c.generation = ?
            AND c.lo = r.first_day AND c.hi = r.last_day AND NOT c.completed
        """
        def update(tx):
            applied = set()
            for success, operation in (
                (True, f"UPDATE __orch__.coverage_claims AS c SET completed = true FROM ({selected}) AS r"),
                (False, f"DELETE FROM __orch__.coverage_claims AS c USING ({selected}) AS r"),
            ):
                claims = [c for c in batch.claims if (c.claim_id in succeeded) == success]
                if not claims:
                    continue
                args = [[c.claim_id for c in claims], [c.window.start for c in claims],
                        [c.window.end for c in claims], asset, batch.generation]
                rows = tx.execute(f"{operation} WHERE {condition} RETURNING claim_id", args).fetchall()
                applied.update(row[0] for row in rows)
            return tuple(c.claim_id in applied for c in batch.claims)
        return _write_transaction(self._db, update)


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

    def _walk(self, changes: Mapping[str, Iterable[DateWindow]]):
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
        for model in self._order:
            incoming = [
                (parent, grain, self._expand(window, grain))
                for parent, grain in self._dependencies[model].items()
                for window in propagated.get(parent, ())
            ]
            sources = direct.get(model, ())
            propagated[model] = self._merge((*sources, *(window for _, _, window in incoming)))
            yield model, sources, incoming

    def affected_many(self, changes: Mapping[str, Iterable[DateWindow]]) -> dict[str, tuple[DateWindow, ...]]:
        """Propagate disjoint source changes and merge touching output intervals."""
        outputs = {}
        for model, _, incoming in self._walk(changes):
            if incoming:
                outputs[model] = self._merge(window for _, _, window in incoming)
        return outputs

    def explain(self, changes: Mapping[str, Iterable[DateWindow]], *, tenant_id: str,
                bytes_per_day: Mapping[str, int] | None = None) -> Iterator[PlanStep]:
        """Stream source and parent-edge evidence in dependency order."""
        if type(tenant_id) is not str or not tenant_id:
            raise ValueError("tenant_id must be a nonempty ID from the host")
        estimates = dict(bytes_per_day or {})
        if any(type(size) is not int or size < 0 for size in estimates.values()):
            raise ValueError("per-day estimates must be nonnegative bytes")
        for model, direct, edges in self._walk(changes):
            cost = estimates.get(model)
            def estimate(interval):
                return None if cost is None else cost * (interval.end - interval.start).days
            for interval in direct:
                yield PlanStep(tenant_id, model, interval, "source_change", None, estimate(interval))
            for parent, grain, interval in edges:
                reason = grain.value if type(grain) is PartitionGrain else "rolling_window"
                yield PlanStep(tenant_id, model, interval, reason, parent, estimate(interval))


class ChangeLedger:
    """Persist source corrections by revision in a tenant-scoped DuckDB file."""

    def __init__(self, connection: DuckDBPyConnection, models: Mapping[str, str]):
        self._db = connection
        self._models = dict(models)
        if any(type(model) is not str or type(asset) is not str or not model or not asset
               for model, asset in self._models.items()):
            raise ValueError("model IDs and asset names must be nonempty strings")
        self._by_asset = {}
        for model, asset in self._models.items():
            self._by_asset.setdefault(asset, []).append(model)

        def initialize(tx):
            tx.execute("CREATE SCHEMA IF NOT EXISTS __orch__")
            tx.execute("""
                CREATE TABLE IF NOT EXISTS __orch__.changed_input_windows (
                    revision VARCHAR, asset VARCHAR, lo DATE, hi DATE,
                    PRIMARY KEY (revision, asset, lo, hi), CHECK (lo < hi)
                )
            """)
        _write_transaction(self._db, initialize)

    @staticmethod
    def _require_revision(revision):
        if type(revision) is not str or not revision:
            raise ValueError("revision must be a nonempty string")

    def record(self, model_id: str, revision: str, changed: DateWindow) -> bool:
        """Insert a source interval once and return whether it was new."""
        asset = self._models[model_id]
        self._require_revision(revision)
        if type(changed) is not DateWindow:
            raise TypeError("expected a DateWindow")
        if changed.start == changed.end:
            return False
        def insert(tx):
            return tx.execute("""
                INSERT INTO __orch__.changed_input_windows VALUES (?, ?, ?, ?)
                ON CONFLICT DO NOTHING RETURNING 1
            """, [revision, asset, changed.start, changed.end]).fetchone() is not None
        return _write_transaction(self._db, insert)

    def _changes(self, revision, planner):
        self._require_revision(revision)
        if type(planner) is not ChangeWindowPlanner:
            raise TypeError("expected a ChangeWindowPlanner")
        rows = self._db.execute("""
            SELECT asset, lo, hi FROM __orch__.changed_input_windows
            WHERE revision = ? AND asset IN (SELECT unnest(?::VARCHAR[]))
        """, [revision, list(self._by_asset)]).fetchall()
        changes = {}
        for asset, start, end in rows:
            window = DateWindow(start, end)
            for model in self._by_asset[asset]:
                changes.setdefault(model, []).append(window)
        return changes

    def plan(self, revision: str, planner: ChangeWindowPlanner) -> dict[str, tuple[DateWindow, ...]]:
        """Map the recorded source ranges to their downstream windows."""
        changes = self._changes(revision, planner)
        return planner.affected_many(changes)

    def explain(self, revision: str, planner: ChangeWindowPlanner, *, tenant_id: str,
                bytes_per_day: Mapping[str, int] | None = None) -> Iterator[PlanStep]:
        changes = self._changes(revision, planner)
        return planner.explain(changes, tenant_id=tenant_id, bytes_per_day=bytes_per_day)


@dataclass(frozen=True)
class FileRevision:
    uri: str
    version: str

    def __post_init__(self):
        if any(type(value) is not str or not value or "\0" in value for value in (self.uri, self.version)):
            raise ValueError("provide the URI and immutable version or verified content digest")


class FileIngestionLedger:
    """Track immutable file revisions for one registered input on a tenant database."""

    def __init__(self, connection: DuckDBPyConnection, input_id: str):
        if type(input_id) is not str or not input_id:
            raise ValueError("input_id must be a nonempty string")
        self._db = connection
        self._input = input_id

        def initialize(tx):
            tx.execute("CREATE SCHEMA IF NOT EXISTS __orch__")
            tx.execute("""
                CREATE TABLE IF NOT EXISTS __orch__.ingested_files (
                    input_id VARCHAR, uri VARCHAR, version VARCHAR,
                    completed_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
                    PRIMARY KEY (input_id, uri, version)
                )
            """)

        _write_transaction(connection, initialize)

    @staticmethod
    def _parameters(files):
        if type(files) is not list or any(type(file) is not FileRevision for file in files):
            raise TypeError("expected a list of FileRevision values")
        return [{"uri": file.uri, "version": file.version} for file in dict.fromkeys(files)]

    def pending(self, files: list[FileRevision]) -> list[FileRevision]:
        """Return distinct, unprocessed revisions in inventory order."""
        with self._db.cursor() as reader:
            missing = reader.execute("""
                WITH inventory AS (
                    SELECT f.uri AS uri, f.version AS version, ord
                    FROM unnest(?::STRUCT(uri VARCHAR, version VARCHAR)[]) WITH ORDINALITY AS t(f, ord)
                ), complete AS (
                    SELECT uri, version FROM __orch__.ingested_files WHERE input_id = ?
                )
                SELECT uri, version FROM inventory ANTI JOIN complete USING (uri, version) ORDER BY ord
            """, [self._parameters(files), self._input]).fetchall()
        return [FileRevision(uri, version) for uri, version in missing]

    def complete(self, files: list[FileRevision]) -> int:
        """Confirm file revisions after their ingest output has committed."""
        params = [self._input, self._parameters(files)]

        def commit(tx):
            return len(tx.execute("""
                INSERT INTO __orch__.ingested_files(input_id, uri, version)
                SELECT ?, f.uri, f.version FROM unnest(?::STRUCT(uri VARCHAR, version VARCHAR)[]) AS t(f)
                ON CONFLICT DO NOTHING RETURNING 1
            """, params).fetchall())

        return _write_transaction(self._db, commit)


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


@dataclass(frozen=True)
class RowCountRule:
    minimum: int = 0
    maximum: int | None = None

    def __post_init__(self):
        if type(self.minimum) is not int or self.minimum < 0:
            raise ValueError("minimum must be a nonnegative integer")
        if self.maximum is not None and (type(self.maximum) is not int or self.maximum < self.minimum):
            raise ValueError("maximum must equal or exceed minimum")


@dataclass(frozen=True)
class NullRule:
    column_id: str
    maximum: int = 0

    def __post_init__(self):
        if type(self.maximum) is not int or self.maximum < 0:
            raise ValueError("maximum must be a nonnegative integer")


@dataclass(frozen=True)
class NumericRangeRule:
    column_id: str
    minimum: float
    maximum: float
    allowed: int = 0

    def __post_init__(self):
        for value in (self.minimum, self.maximum):
            valid_integer = type(value) is int and -(1 << 63) <= value < (1 << 63)
            valid_double = type(value) is float and isfinite(value)
            if not (valid_integer or valid_double):
                raise ValueError("numeric bounds must be signed 64-bit integers or finite doubles")
        if self.minimum > self.maximum:
            raise ValueError("maximum must equal or exceed minimum")
        if type(self.allowed) is not int or self.allowed < 0:
            raise ValueError("allowed must be a nonnegative integer")


@dataclass(frozen=True)
class QualityObservation:
    check_id: str
    observed: int
    passed: bool


class SharedQualityScan:
    """Validate an interval against host-registered columns with one scan."""

    def __init__(self, connection: DuckDBPyConnection, relation: str, *,
                 date_column: str, columns: Mapping[str, str]):
        self._db = connection
        self._relation = self._identifier(relation)
        self._date = self._identifier(date_column)
        self._columns = {name: self._identifier(column) for name, column in columns.items()}

    @staticmethod
    def _identifier(name):
        if type(name) is not str or not name or "\0" in name:
            raise ValueError("provide a registered column or relation identifier")
        return '"' + name.replace('"', '""') + '"'

    def evaluate(self, window: DateWindow,
                 checks: Mapping[str, RowCountRule | NullRule | NumericRangeRule]) -> list[QualityObservation]:
        if type(window) is not DateWindow:
            raise TypeError("expected a DateWindow")
        if len(checks) > 256:
            raise ValueError("evaluate at most 256 checks together")
        if not checks:
            return []
        aggregates, parameters, ranges = [], [], []
        for check in checks.values():
            if type(check) is RowCountRule:
                aggregates.append("count(*)")
                ranges.append((check.minimum, check.maximum))
            elif type(check) is NullRule:
                column = self._columns[check.column_id]
                aggregates.append(f"count(*) FILTER (WHERE {column} IS NULL)")
                ranges.append((0, check.maximum))
            elif type(check) is NumericRangeRule:
                column = self._columns[check.column_id]
                aggregates.append(f"""
                    count(*) FILTER (WHERE {column} IS NOT NULL
                                     AND (NOT isfinite({column}) OR {column} < ? OR {column} > ?))
                """)
                parameters.extend([check.minimum, check.maximum])
                ranges.append((0, check.allowed))
            else:
                raise TypeError("expected a typed quality check")
        observed = self._db.execute(f"""
            SELECT {", ".join(aggregates)} FROM main.{self._relation}
            WHERE {self._date} >= ?::DATE AND {self._date} < ?::DATE
        """, [*parameters, window.start, window.end]).fetchone()
        return [QualityObservation(name, count, count >= lower and (upper is None or count <= upper))
                for name, count, (lower, upper) in zip(checks, observed, ranges)]


def _fingerprint(value):
    return sha256(dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()).hexdigest()


@dataclass(frozen=True)
class ModelFingerprint:
    """Hashes of executable inputs and the versions of referenced models."""

    definition: str
    upstream_versions: tuple[tuple[str, str | None], ...]
    macros: str
    settings: str
    engine: str

    @classmethod
    def capture(cls, sql: str, *, engine_version: str, macros: Mapping[str, str] | None = None,
                settings: Mapping[str, str] | None = None,
                upstream_versions: Mapping[str, str | None] | None = None):
        if type(sql) is not str or not sql.strip() or type(engine_version) is not str or not engine_version:
            raise ValueError("provide the executable SQL definition and engine version")
        macros, settings, versions = dict(macros or {}), dict(settings or {}), dict(upstream_versions or {})
        for mapping in (macros, settings):
            if any(type(key) is not str or type(value) is not str for key, value in mapping.items()):
                raise TypeError("macro definitions and settings must map strings to strings")
        if any(type(key) is not str or (value is not None and type(value) is not str)
               for key, value in versions.items()):
            raise TypeError("reference versions must map string IDs to strings or None")
        return cls(_fingerprint(sql), tuple(sorted(versions.items())), _fingerprint(macros),
                   _fingerprint(settings), _fingerprint(engine_version))

    def differences(self, other: "ModelFingerprint") -> tuple[str, ...]:
        if type(other) is not ModelFingerprint:
            raise TypeError("expected ModelFingerprint")
        components = ("definition", "upstream_versions", "macros", "settings", "engine")
        return tuple(name for name in components if getattr(self, name) != getattr(other, name))


class FingerprintPlanner:
    """Return invalidation reasons from consecutive executable model snapshots."""

    def __init__(self, dependencies: Mapping[str, Iterable[str]]):
        self._dependencies = {model: tuple(sorted(set(parents))) for model, parents in dependencies.items()}
        internal = {model: tuple(parent for parent in parents if parent in self._dependencies)
                    for model, parents in self._dependencies.items()}
        try:
            self._order = tuple(TopologicalSorter(internal).static_order())
        except CycleError as exc:
            raise ValueError("model dependencies contain a cycle") from exc

    def plan(self, previous: Mapping[str, ModelFingerprint],
             current: Mapping[str, ModelFingerprint]) -> dict[str, tuple[str, ...]]:
        if current.keys() != self._dependencies.keys():
            raise ValueError("current fingerprints must match the registered model graph")
        changes = {}
        for model in self._order:
            snapshot = current[model]
            baseline = previous.get(model)
            reasons = list(baseline.differences(snapshot) if baseline is not None else ("new",))
            upstream = dict(snapshot.upstream_versions)
            unresolved = {parent for parent in self._dependencies[model] if not upstream.get(parent)}
            unresolved.update(parent for parent, version in upstream.items() if version is None)
            reasons.extend(f"unresolved:{parent}" for parent in sorted(unresolved))
            reasons.extend(f"dependency:{parent}" for parent in self._dependencies[model] if parent in changes)
            if reasons:
                changes[model] = tuple(reasons)
        return changes


class FingerprintStore:
    """Keep immutable named fingerprints in the tenant's DuckDB file."""

    def __init__(self, connection: DuckDBPyConnection):
        self._db = connection
        connection.execute("CREATE SCHEMA IF NOT EXISTS __orch__")
        connection.execute("CREATE TABLE IF NOT EXISTS __orch__.fingerprint_revisions (revision VARCHAR PRIMARY KEY)")
        connection.execute("""
            CREATE TABLE IF NOT EXISTS __orch__.model_fingerprints (
              revision VARCHAR, model VARCHAR, definition VARCHAR NOT NULL, upstream JSON NOT NULL,
              macros VARCHAR NOT NULL, settings VARCHAR NOT NULL, engine VARCHAR NOT NULL,
              PRIMARY KEY (revision, model)
            )
        """)

    def save(self, revision: str, models: Mapping[str, ModelFingerprint]) -> None:
        if type(revision) is not str or not revision or not models:
            raise ValueError("provide a revision name and at least one model fingerprint")
        if any(type(model) is not str or not model or type(value) is not ModelFingerprint
               for model, value in models.items()):
            raise TypeError("expected model IDs and ModelFingerprint values")
        entries = [{"model": name, "definition": f.definition, "upstream": dumps(f.upstream_versions),
                    "macros": f.macros, "settings": f.settings, "engine": f.engine}
                   for name, f in models.items()]
        with self._db.cursor() as tx:
            tx.execute("BEGIN TRANSACTION")
            if tx.execute("SELECT 1 FROM __orch__.fingerprint_revisions WHERE revision = ?",
                          [revision]).fetchone():
                raise ValueError("revision already exists")
            tx.execute("INSERT INTO __orch__.fingerprint_revisions VALUES (?)", [revision])
            tx.execute("""
                INSERT INTO __orch__.model_fingerprints
                SELECT ?, f.model, f.definition, f.upstream::JSON, f.macros, f.settings, f.engine
                FROM unnest(?::STRUCT(model VARCHAR, definition VARCHAR, upstream VARCHAR,
                                      macros VARCHAR, settings VARCHAR, engine VARCHAR)[]) AS t(f)
            """, [revision, entries])
            tx.execute("COMMIT")

    def load(self, revision: str) -> dict[str, ModelFingerprint]:
        rows = self._db.execute("""
            SELECT model, definition, upstream, macros, settings, engine
            FROM __orch__.model_fingerprints WHERE revision = ?
        """, [revision]).fetchall()
        if not rows:
            raise KeyError(revision)
        return {name: ModelFingerprint(definition, tuple(tuple(pair) for pair in loads(upstream)),
                                       macros, settings, engine)
                for name, definition, upstream, macros, settings, engine in rows}


@dataclass(frozen=True)
class ReleaseModel:
    physical_version: str
    contract: str

    def __post_init__(self):
        if any(type(value) is not str or not value for value in (self.physical_version, self.contract)):
            raise ValueError("provide a physical version and contract for each model")


@dataclass(frozen=True)
class ReleasePlan:
    environment_id: UUID
    revision: str
    base_epoch: int
    input_epoch: int


@dataclass(frozen=True)
class PublishedGeneration:
    revision: str
    epoch: int
    models: tuple[tuple[str, ReleaseModel], ...]
    inputs: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class ReleaseNotification:
    event_id: UUID
    environment: str
    epoch: int
    revision: str
    kind: str


class StaleReleasePlan(ValueError):
    """The active generation or source inputs changed since preparation."""


class ReleaseRequestConflict(ValueError):
    """A publication key was already used with a different request."""


class GenerationCatalog:
    """Publish immutable checked manifests through one DuckDB transaction per environment."""

    def __init__(self, connection: DuckDBPyConnection, environment: str, model_ids: set[str]):
        if type(environment) is not str or not environment:
            raise ValueError("environment must be a nonempty string")
        models = frozenset(model_ids)
        if not models or any(type(model) is not str or not model for model in models):
            raise ValueError("register at least one model ID")
        self._db = connection
        self._environment = environment
        self._models = models

        def initialize(tx):
            tx.execute("CREATE SCHEMA IF NOT EXISTS __orch__")
            tx.execute("""
                CREATE TABLE IF NOT EXISTS __orch__.release_environments (
                    environment VARCHAR PRIMARY KEY, id UUID NOT NULL, active VARCHAR,
                    epoch BIGINT NOT NULL, input_epoch BIGINT NOT NULL, observed_inputs JSON NOT NULL,
                    model_ids JSON NOT NULL
                )
            """)
            tx.execute("""
                CREATE TABLE IF NOT EXISTS __orch__.release_manifests (
                    environment VARCHAR, revision VARCHAR, models JSON NOT NULL,
                    inputs JSON NOT NULL, checks JSON NOT NULL, published BOOLEAN NOT NULL,
                    PRIMARY KEY (environment, revision)
                )
            """)
            tx.execute("""
                CREATE TABLE IF NOT EXISTS __orch__.release_outbox (
                    environment VARCHAR NOT NULL, epoch BIGINT NOT NULL, event_id UUID UNIQUE NOT NULL,
                    revision VARCHAR NOT NULL, kind VARCHAR NOT NULL, delivered BOOLEAN NOT NULL,
                    PRIMARY KEY (environment, epoch)
                )
            """)
            tx.execute("""
                CREATE TABLE IF NOT EXISTS __orch__.release_requests (
                    environment VARCHAR, request_key VARCHAR, digest VARCHAR NOT NULL,
                    revision VARCHAR NOT NULL, epoch BIGINT NOT NULL,
                    PRIMARY KEY (environment, request_key)
                )
            """)
            tx.execute("""
                INSERT INTO __orch__.release_environments VALUES (?, uuid(), NULL, 0, 0, '{}', ?)
                ON CONFLICT DO NOTHING
            """, [self._environment, dumps(sorted(self._models))])
            registered, = tx.execute("""
                SELECT model_ids FROM __orch__.release_environments WHERE environment = ?
            """, [self._environment]).fetchone()
            if set(loads(registered)) != self._models:
                raise ValueError("model IDs do not match the existing environment")
        _write_transaction(self._db, initialize)

    @staticmethod
    def _input_json(inputs):
        values = dict(inputs)
        if any(type(key) is not str or not key or type(version) is not str or not version
               for key, version in values.items()):
            raise ValueError("source versions must map nonempty strings to nonempty strings")
        return dumps(values, sort_keys=True, separators=(",", ":"))

    def set_inputs(self, inputs: Mapping[str, str]) -> None:
        """Record the currently visible source versions without changing the active release."""
        encoded = self._input_json(inputs)
        def update(tx):
            tx.execute("""
                UPDATE __orch__.release_environments SET observed_inputs = ?, input_epoch = input_epoch + 1
                WHERE environment = ? AND observed_inputs IS DISTINCT FROM ?::JSON
            """, [encoded, self._environment, encoded])
        _write_transaction(self._db, update)

    def stage(self, revision: str, models: Mapping[str, ReleaseModel], *,
              inputs: Mapping[str, str], checks: Mapping[str, bool]) -> None:
        """Record all already-prepared physical versions and their passed checks."""
        if type(revision) is not str or not revision:
            raise ValueError("revision must be a nonempty string")
        if models.keys() != self._models or any(type(model) is not ReleaseModel for model in models.values()):
            raise ValueError("stage must include a physical version for every registered model")
        if not checks or any(type(key) is not str or not key or value is not True for key, value in checks.items()):
            raise ValueError("all registered checks must pass before staging")
        manifest = {name: [model.physical_version, model.contract] for name, model in models.items()}
        args = [self._environment, revision, dumps(manifest, sort_keys=True), self._input_json(inputs),
                dumps(dict(checks), sort_keys=True)]
        self._db.execute("INSERT INTO __orch__.release_manifests VALUES (?, ?, ?, ?, ?, false)", args)

    def prepare(self, revision: str) -> ReleasePlan:
        if type(revision) is not str or not revision:
            raise ValueError("revision must be a nonempty string")
        current = self._db.execute("""
            SELECT e.id, e.epoch, e.input_epoch, e.observed_inputs, m.inputs
            FROM __orch__.release_environments e JOIN __orch__.release_manifests m USING (environment)
            WHERE e.environment = ? AND m.revision = ?
        """, [self._environment, revision]).fetchone()
        if current is None:
            raise KeyError(revision)
        identity, epoch, input_epoch, observed, staged = current
        if loads(observed) != loads(staged):
            raise StaleReleasePlan("staged source versions differ from current sources")
        return ReleasePlan(identity, revision, epoch, input_epoch)

    def validate(self, plan: ReleasePlan) -> None:
        """Verify the plan before applying work. Publication checks the same versions again."""
        if type(plan) is not ReleasePlan:
            raise TypeError("expected a ReleasePlan")
        if self.prepare(plan.revision) != plan:
            raise StaleReleasePlan("active generation or source version changed")

    def publish(self, plan: ReleasePlan, *, request_key: str | None = None) -> PublishedGeneration:
        if type(plan) is not ReleasePlan:
            raise TypeError("expected a ReleasePlan")
        if request_key is not None and (type(request_key) is not str or not 1 <= len(request_key) <= 255):
            raise ValueError("request_key must contain between 1 and 255 characters")
        signature = None
        if request_key is not None:
            payload = dumps([str(plan.environment_id), plan.revision, plan.base_epoch, plan.input_epoch],
                            separators=(",", ":")).encode("utf-8")
            signature = sha256(payload).hexdigest()

        def switch(tx):
            if request_key is not None:
                prior = tx.execute("""
                    SELECT digest, revision, epoch FROM __orch__.release_requests
                    WHERE environment = ? AND request_key = ?
                """, [self._environment, request_key]).fetchone()
                if prior is not None:
                    digest, revision, epoch = prior
                    if digest != signature:
                        raise ReleaseRequestConflict("request_key belongs to another publication plan")
                    manifest = self._manifest(tx, revision, published_only=True)
                    return self._snapshot(revision, epoch, *manifest)
            manifest = self._manifest(tx, plan.revision, published_only=False)
            epoch = tx.execute("""
                UPDATE __orch__.release_environments AS e SET active = ?, epoch = epoch + 1
                WHERE environment = ? AND id = ? AND epoch = ? AND input_epoch = ?
                    AND observed_inputs = ?::JSON
                RETURNING epoch
            """, [plan.revision, self._environment, plan.environment_id, plan.base_epoch,
                  plan.input_epoch, manifest[1]]).fetchone()
            if epoch is None:
                raise StaleReleasePlan("active generation or source version changed")
            tx.execute("""
                UPDATE __orch__.release_manifests SET published = true WHERE environment = ? AND revision = ?
            """, [self._environment, plan.revision])
            self._notify(tx, epoch[0], plan.revision, "publish")
            if request_key is not None:
                tx.execute("""
                    INSERT INTO __orch__.release_requests VALUES (?, ?, ?, ?, ?)
                """, [self._environment, request_key, signature, plan.revision, epoch[0]])
            return self._snapshot(plan.revision, epoch[0], *manifest)
        return _write_transaction(self._db, switch)

    def rollback(self, revision: str, *, expected_epoch: int) -> PublishedGeneration:
        """Repoint the active release to an already published manifest."""
        if type(expected_epoch) is not int or expected_epoch < 0:
            raise ValueError("expected_epoch must be nonnegative")
        def switch(tx):
            models, inputs = self._manifest(tx, revision, published_only=True)
            found = tx.execute("""
                UPDATE __orch__.release_environments SET active = ?, epoch = epoch + 1
                WHERE environment = ? AND epoch = ? RETURNING epoch
            """, [revision, self._environment, expected_epoch]).fetchone()
            if found is None:
                raise StaleReleasePlan("active generation changed before rollback")
            self._notify(tx, found[0], revision, "rollback")
            return self._snapshot(revision, found[0], models, inputs)
        return _write_transaction(self._db, switch)

    def _notify(self, tx, epoch: int, revision: str, kind: str) -> None:
        tx.execute("""
            INSERT INTO __orch__.release_outbox VALUES (?, ?, uuid(), ?, ?, false)
        """, [self._environment, epoch, revision, kind])

    def pending_notifications(self, *, limit: int) -> list[ReleaseNotification]:
        """Read a bounded batch. The caller may deliver the same event again until ACK."""
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ValueError("limit must be an integer between 1 and 10000")
        with self._db.cursor() as reader:
            rows = reader.execute("""
                SELECT event_id, environment, epoch, revision, kind
                FROM __orch__.release_outbox
                WHERE environment = ? AND NOT delivered ORDER BY epoch LIMIT ?
            """, [self._environment, limit]).fetchall()
        return [ReleaseNotification(*row) for row in rows]

    def acknowledge_notifications(self, events: list[UUID]) -> int:
        """Mark previously delivered IDs from this environment as acknowledged."""
        if type(events) is not list or len(events) > 10_000 or any(type(event) is not UUID for event in events):
            raise TypeError("expected a list of up to 10000 UUIDs")

        def acknowledge(tx):
            rows = tx.execute("""
                UPDATE __orch__.release_outbox SET delivered = true
                WHERE environment = ? AND NOT delivered AND event_id IN (SELECT unnest(?::UUID[]))
                RETURNING event_id
            """, [self._environment, events]).fetchall()
            return len(rows)

        return _write_transaction(self._db, acknowledge)

    def _manifest(self, tx, revision, *, published_only):
        if type(revision) is not str or not revision:
            raise ValueError("revision must be a nonempty string")
        row = tx.execute("""
            SELECT models, inputs FROM __orch__.release_manifests
            WHERE environment = ? AND revision = ? AND (published OR NOT ?)
        """, [self._environment, revision, published_only]).fetchone()
        if row is None:
            raise KeyError(revision)
        return row

    def active(self) -> PublishedGeneration | None:
        row = self._db.execute("""
            SELECT e.active, e.epoch, m.models, m.inputs
            FROM __orch__.release_environments e JOIN __orch__.release_manifests m
                ON e.environment = m.environment AND e.active = m.revision
            WHERE e.environment = ?
        """, [self._environment]).fetchone()
        return self._snapshot(*row) if row is not None else None

    def fork(self, target_environment: str) -> "GenerationCatalog":
        """Create a trial environment pinned to the current immutable references."""
        if type(target_environment) is not str or not target_environment:
            raise ValueError("target environment must be a nonempty string")
        def branch(tx):
            if tx.execute("SELECT 1 FROM __orch__.release_environments WHERE environment = ?",
                          [target_environment]).fetchone():
                raise ValueError("target environment already exists")
            copied = tx.execute("""
                INSERT INTO __orch__.release_environments
                SELECT ?, uuid(), source.active, 1, 0, m.inputs, source.model_ids
                FROM __orch__.release_environments AS source JOIN __orch__.release_manifests AS m
                     ON m.environment = source.environment AND m.revision = source.active
                WHERE source.environment = ? RETURNING active
            """, [target_environment, self._environment]).fetchone()
            if copied is None:
                raise ValueError("publish a generation before forking its environment")
            tx.execute("""
                INSERT INTO __orch__.release_manifests
                SELECT ?, revision, models, inputs, checks, true
                FROM __orch__.release_manifests WHERE environment = ? AND revision = ?
            """, [target_environment, self._environment, copied[0]])
        _write_transaction(self._db, branch)
        return GenerationCatalog(self._db, target_environment, self._models)

    @staticmethod
    def _snapshot(revision, epoch, models, inputs):
        return PublishedGeneration(revision, epoch,
                                   tuple((name, ReleaseModel(*value)) for name, value in sorted(loads(models).items())),
                                   tuple(sorted(loads(inputs).items())))


class JsonType(Enum):
    INTEGER = ("BIGINT", ("BIGINT", "UBIGINT"))
    NUMBER = ("DOUBLE", ("BIGINT", "UBIGINT", "DOUBLE"))
    TEXT = ("VARCHAR", ("VARCHAR",))
    BOOLEAN = ("BOOLEAN", ("BOOLEAN",))


@dataclass(frozen=True)
class JsonField:
    path: tuple[str, ...]
    kind: JsonType

    def __post_init__(self):
        if type(self.path) is not tuple or not self.path or any(type(part) is not str for part in self.path):
            raise ValueError("path must be a nonempty tuple of JSON object keys or array offsets")
        if type(self.kind) is not JsonType:
            raise TypeError("expected a JsonType")


@dataclass(frozen=True)
class ProjectedJson:
    values: dict[str, int | float | str | bool | None]
    issues: dict[str, str]
    source: str | None


class JsonProjector:
    """Project a host-registered set of JSON paths to typed DuckDB STRUCT fields."""

    def __init__(self, connection: DuckDBPyConnection, fields: Mapping[str, JsonField], *,
                 keep_source: bool = False):
        if type(keep_source) is not bool or not 1 <= len(fields) <= 256:
            raise ValueError("provide at least one and at most 256 registered fields")
        self._db = connection
        self._paths = []
        casts, values, issues = [], [], []
        seen = set()
        for index, (name, field) in enumerate(fields.items(), 1):
            if type(name) is not str or not name or "\0" in name or type(field) is not JsonField:
                raise ValueError("expected nonempty column IDs and JsonField definitions")
            if name.casefold() in seen:
                raise ValueError("field IDs must be distinct")
            seen.add(name.casefold())
            quoted = '"' + name.replace('"', '""') + '"'
            self._paths.append("/" + "/".join(part.replace("~", "~0").replace("/", "~1") for part in field.path))
            value = f"extracted[{index}]"
            type_name, allowed = field.kind.value
            matching_types = ", ".join(f"'{item}'" for item in allowed)
            conversion = (f"json_extract_string({value}, '$')" if field.kind is JsonType.TEXT
                          else f"try_cast({value} AS {type_name})")
            if field.kind is JsonType.NUMBER:
                conversion = f"CASE WHEN isfinite({conversion}) THEN {conversion} END"
            casts.extend([f"json_type({value}) AS kind_{index}", f"{conversion} AS value_{index}"])
            reason = f"""
                CASE WHEN decoded IS NULL THEN 'invalid_document'
                     WHEN kind_{index} IS NULL THEN 'missing'
                     WHEN kind_{index} = 'NULL' THEN 'null'
                     WHEN kind_{index} NOT IN ({matching_types}) THEN 'invalid_type'
                     WHEN value_{index} IS NULL THEN 'out_of_range' END
            """
            issues.append(f"{quoted} := {reason}")
            values.append(f"{quoted} := CASE WHEN ({reason}) IS NULL THEN value_{index} END")
        retained = "raw" if keep_source else "NULL::VARCHAR"
        self._sql = f"""
            WITH docs AS (
                SELECT raw, ord, try_cast(raw AS JSON) AS decoded
                FROM unnest(?::VARCHAR[]) WITH ORDINALITY AS t(raw, ord)
            ), projected AS MATERIALIZED (
                SELECT raw, ord, decoded, json_extract(decoded, ?::VARCHAR[]) AS extracted FROM docs
            ), typed AS (
                SELECT raw, ord, decoded, {", ".join(casts)} FROM projected
            )
            SELECT struct_pack({", ".join(values)}), struct_pack({", ".join(issues)}), {retained}
            FROM typed ORDER BY ord
        """

    def project(self, documents: list[str]) -> list[ProjectedJson]:
        """Expose missing, null, type and range issues without dropping a row."""
        if type(documents) is not list or any(type(item) is not str for item in documents):
            raise TypeError("expected a list of JSON strings")
        return [ProjectedJson(values, {key: error for key, error in issues.items() if error}, raw)
                for values, issues, raw in self._db.execute(self._sql, [documents, self._paths]).fetchall()]


@dataclass(frozen=True)
class VariantEvent:
    event_id: str
    kind: str
    occurred_at: datetime
    json_attributes: str

    def __post_init__(self):
        if any(type(value) is not str or not value for value in (self.event_id, self.kind, self.json_attributes)):
            raise ValueError("provide an event ID, kind and JSON attributes")
        if type(self.occurred_at) is not datetime or self.occurred_at.utcoffset() is None:
            raise ValueError("event time must carry a timezone")


@dataclass(frozen=True)
class ReadVariantEvent:
    event_id: str
    kind: str
    occurred_at: datetime
    attributes: dict


class VariantEventLog:
    """Keep event keys in typed columns and tenant-specific attributes in VARIANT."""

    def __init__(self, connection: DuckDBPyConnection, stream: str, *, promoted_text: str | None = None):
        if type(stream) is not str or not stream:
            raise ValueError("stream must be a nonempty string")
        if promoted_text is not None and (type(promoted_text) is not str or not promoted_text):
            raise ValueError("the promoted key must be a nonempty string or None")
        self._db = connection
        self._stream = stream
        self._promoted = promoted_text
        def create(tx):
            tx.execute("CREATE SCHEMA IF NOT EXISTS __orch__")
            tx.execute("""
                CREATE TABLE IF NOT EXISTS __orch__.event_streams (stream VARCHAR PRIMARY KEY, promoted VARCHAR)
            """)
            tx.execute("""
                CREATE TABLE IF NOT EXISTS __orch__.variant_events (
                    stream VARCHAR, event_id VARCHAR, kind VARCHAR NOT NULL, occurred_at TIMESTAMPTZ NOT NULL,
                    attributes VARIANT NOT NULL, promoted_text VARCHAR, PRIMARY KEY (stream, event_id)
                )
            """)
            tx.execute("INSERT INTO __orch__.event_streams VALUES (?, ?) ON CONFLICT DO NOTHING",
                       [stream, promoted_text])
            registered, = tx.execute("SELECT promoted FROM __orch__.event_streams WHERE stream = ?", [stream]).fetchone()
            if registered != promoted_text:
                raise ValueError("the stream already has a different promoted key")
        _write_transaction(self._db, create)

    def append(self, events: list[VariantEvent]) -> None:
        if type(events) is not list or any(type(event) is not VariantEvent for event in events):
            raise TypeError("expected a list of VariantEvent values")
        rows = [{"id": e.event_id, "kind": e.kind, "created": e.occurred_at, "content": e.json_attributes}
                for e in events]
        try:
            self._db.execute("""
                INSERT INTO __orch__.variant_events
                WITH parsed AS (
                    SELECT e.id AS id, e.kind AS kind, e.created AS created, try_cast(e.content AS JSON) AS doc
                    FROM unnest(?::STRUCT(id VARCHAR, kind VARCHAR, created TIMESTAMPTZ, content VARCHAR)[]) AS t(e)
                ), typed AS MATERIALIZED (
                    SELECT id, kind, created, CASE WHEN json_type(doc) IS DISTINCT FROM 'OBJECT'
                        THEN error('event attributes must be a valid JSON object') ELSE doc::VARIANT END AS attributes
                    FROM parsed
                ), promoted AS (
                    SELECT *, variant_extract(attributes, ?::VARCHAR) AS candidate FROM typed
                )
                SELECT ?, id, kind, created, attributes,
                       CASE WHEN variant_typeof(candidate) = 'VARCHAR' THEN candidate::VARCHAR END
                FROM promoted
            """, [rows, self._promoted, self._stream])
        except InvalidInputException as error:
            if "event attributes must be a valid JSON object" not in str(error):
                raise
            raise ValueError("event attributes must be a valid JSON object") from error

    def read(self, kind: str, *, key: str | None = None, value: str | None = None) -> list[ReadVariantEvent]:
        if type(kind) is not str or not kind:
            raise ValueError("kind must be a nonempty string")
        predicates = ["stream = ?", "kind = ?"]
        args = [self._stream, kind]
        if key is not None or value is not None:
            if type(key) is not str or not key or type(value) is not str:
                raise ValueError("a text match requires a key and a string value")
            if key == self._promoted:
                predicates.append("promoted_text = ?")
                args.append(value)
            else:
                predicates.append("""
                    CASE WHEN variant_typeof(variant_extract(attributes, ?)) = 'VARCHAR'
                         THEN variant_extract(attributes, ?)::VARCHAR END = ?
                """)
                args.extend([key, key, value])
        where = " AND ".join(predicates)
        rows = self._db.execute(f"""
            SELECT event_id, kind, occurred_at AT TIME ZONE 'UTC', to_json(attributes)
            FROM __orch__.variant_events WHERE {where} ORDER BY occurred_at, event_id
        """, args).fetchall()
        return [ReadVariantEvent(i, k, when.replace(tzinfo=timezone.utc), loads(attributes))
                for i, k, when, attributes in rows]


class ColumnType(Enum):
    BOOLEAN = "BOOLEAN"
    SMALLINT = "SMALLINT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    HUGEINT = "HUGEINT"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    TEXT = "VARCHAR"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    INSTANT = "TIMESTAMP WITH TIME ZONE"
    JSON = "JSON"


@dataclass(frozen=True)
class ColumnContract:
    kind: ColumnType
    nullable: bool = True

    def __post_init__(self):
        if type(self.kind) is not ColumnType or type(self.nullable) is not bool:
            raise TypeError("a column declaration needs a ColumnType and nullability")


@dataclass(frozen=True)
class SchemaViolation:
    column: str
    kind: str
    expected: str | bool | None
    actual: str | bool | None


class RelationContract:
    """Check declared column names, types and nullability against DuckDB metadata."""

    def __init__(self, columns: Mapping[str, ColumnContract], *, allow_extra: bool = True):
        self._columns = dict(columns)
        if type(allow_extra) is not bool or not self._columns or any(
            type(name) is not str or not name or type(rule) is not ColumnContract
            for name, rule in self._columns.items()
        ):
            raise ValueError("register logical column names and their ColumnContract values")
        self._allow_extra = allow_extra

    def inspect(self, connection: DuckDBPyConnection, relation: str) -> tuple[SchemaViolation, ...]:
        if type(relation) is not str or not relation:
            raise ValueError("relation must be a nonempty name")
        rows = connection.execute("""
            SELECT column_name, data_type, is_nullable FROM duckdb_columns()
            WHERE database_name = current_database() AND schema_name = 'main' AND table_name = ?
            ORDER BY column_index
        """, [relation]).fetchall()
        if not rows:
            raise KeyError(relation)
        actual = {name: (kind, nullable) for name, kind, nullable in rows}
        problems = []
        for name, rule in self._columns.items():
            kind, nullable = actual.get(name, (None, None))
            if kind is None:
                problems.append(SchemaViolation(name, "missing", rule.kind.value, None))
                continue
            if kind != rule.kind.value:
                problems.append(SchemaViolation(name, "type", rule.kind.value, kind))
            if nullable and not rule.nullable:
                problems.append(SchemaViolation(name, "nullability", False, True))
        if not self._allow_extra:
            for name, kind, _ in rows:
                if name not in self._columns:
                    problems.append(SchemaViolation(name, "unexpected", None, kind))
        return tuple(problems)


@dataclass(frozen=True)
class ScheduledTask:
    tenant: str
    depends_on: tuple[str, ...]
    duration: int | None

    def __post_init__(self):
        if type(self.tenant) is not str or not self.tenant:
            raise ValueError("task tenant must be a nonempty string")
        if type(self.depends_on) is not tuple or any(type(name) is not str or not name for name in self.depends_on):
            raise ValueError("task dependencies must be a tuple of task IDs")
        if self.duration is not None and (type(self.duration) is not int or self.duration <= 0):
            raise ValueError("task duration must be positive or unavailable")


class CriticalPathScheduler:
    """Prioritize ready tasks by estimated remaining path under tenant quotas."""

    def __init__(self, tasks: Mapping[str, ScheduledTask], quotas: Mapping[str, int],
                 *, default_duration: int = 1):
        self._tasks = dict(tasks)
        self._quotas = dict(quotas)
        if type(default_duration) is not int or default_duration <= 0 or any(
            type(value) is not int or value <= 0 for value in self._quotas.values()
        ):
            raise ValueError("task durations and tenant quotas must be positive integers")
        if any(type(name) is not str or not name or type(task) is not ScheduledTask
               for name, task in self._tasks.items()):
            raise TypeError("register task IDs and ScheduledTask values")
        children = {name: [] for name in tasks}
        parents = {}
        for name, task in tasks.items():
            if task.tenant not in self._quotas:
                raise ValueError("task tenant has no slot quota")
            parents[name] = frozenset(task.depends_on)
            for parent in parents[name]:
                if parent not in tasks or tasks[parent].tenant != task.tenant:
                    raise ValueError("task dependencies must be registered in the same tenant")
                children[parent].append(name)
        try:
            order = tuple(TopologicalSorter(parents).static_order())
        except CycleError as error:
            raise ValueError("task graph contains a dependency cycle") from error
        lengths = {}
        for name in reversed(order):
            lengths[name] = (tasks[name].duration or default_duration) + max(
                (lengths[child] for child in children[name]), default=0,
            )
        self._parents = parents
        self._priority = sorted(tasks, key=lambda name: (-lengths[name], name))

    def ready(self, *, completed: set[str], running: set[str], slots: int) -> tuple[str, ...]:
        """Return a deterministic dispatch order with no more than total slots."""
        if type(slots) is not int or slots < 0:
            raise ValueError("slots must be a nonnegative integer")
        busy = completed | running
        if completed & running or not busy <= self._tasks.keys():
            raise ValueError("task state contains invalid or unknown IDs")
        remaining = slots - len(running)
        counts = dict.fromkeys(self._quotas, 0)
        for name in running:
            counts[self._tasks[name].tenant] += 1
        ready = []
        for name in self._priority:
            if len(ready) >= remaining:
                break
            tenant = self._tasks[name].tenant
            if name not in busy and counts[tenant] < self._quotas[tenant] and self._parents[name] <= completed:
                ready.append(name)
                counts[tenant] += 1
        return tuple(ready)


@dataclass(frozen=True)
class MembershipChange:
    key: int
    previous_group: str | None
    current_group: str | None

    def __post_init__(self):
        if type(self.key) is not int or not -(1 << 63) <= self.key < (1 << 63):
            raise ValueError("the row key must be a signed 64-bit integer")
        if self.previous_group is None and self.current_group is None:
            raise ValueError("a change must affect at least one group")
        if any(value is not None and type(value) is not str for value in (self.previous_group, self.current_group)):
            raise TypeError("group keys must be strings or absent")


class GroupDelta:
    """Refresh region sums and row counts from the committed current relation."""

    def __init__(self, connection: DuckDBPyConnection, source: str, target: str):
        for name in (source, target):
            if type(name) is not str or not name or "\0" in name:
                raise ValueError("source and target must be nonempty identifiers without NUL")
        if source.casefold() == target.casefold():
            raise ValueError("source and target must be distinct relations")
        contract = RelationContract({"region": ColumnContract(ColumnType.TEXT, nullable=False),
                                     "amount": ColumnContract(ColumnType.DOUBLE)})
        if contract.inspect(connection, source):
            raise ValueError("source must have a non-null VARCHAR region and DOUBLE amount")
        self._db = connection
        self._model = (source, target)
        self._source = '"' + source.replace('"', '""') + '"'
        self._target = '"' + target.replace('"', '""') + '"'
        def initialize(tx):
            tx.execute("CREATE SCHEMA IF NOT EXISTS __orch__")
            tx.execute("""
                CREATE TABLE IF NOT EXISTS __orch__.group_changes (
                   model_source VARCHAR NOT NULL, model_target VARCHAR NOT NULL, revision VARCHAR NOT NULL,
                   row_key BIGINT NOT NULL, previous_group VARCHAR, current_group VARCHAR
                )
            """)
            tx.execute("""
                CREATE TABLE IF NOT EXISTS __orch__.group_refresh_state (
                    target VARCHAR PRIMARY KEY, sequence BIGINT NOT NULL
                )
            """)
        _write_transaction(connection, initialize)

    def refresh(self, revision: str, changes: list[MembershipChange]) -> int:
        """Rebuild the union of current and former groups for a revision."""
        if type(revision) is not str or not revision:
            raise ValueError("revision must be a nonempty string")
        if type(changes) is not list or any(type(item) is not MembershipChange for item in changes):
            raise TypeError("expected a list of MembershipChange values")
        params = [*self._model, revision]
        rows = [{"id": c.key, "prior": c.previous_group, "next_group": c.current_group} for c in changes]

        def refresh(tx):
            tx.execute("""
                INSERT INTO __orch__.group_refresh_state VALUES (?, 1)
                ON CONFLICT (target) DO UPDATE SET sequence = sequence + 1
            """, [self._model[1].lower()])
            tx.execute("""
                INSERT INTO __orch__.group_changes
                WITH input AS (
                    SELECT DISTINCT c.id AS row_key, c.prior AS previous_group, c.next_group AS current_group
                    FROM unnest(?::STRUCT(id BIGINT, prior VARCHAR, next_group VARCHAR)[]) AS t(c)
                )
                SELECT ?, ?, ?, todo.row_key, todo.previous_group, todo.current_group FROM input AS todo
                WHERE NOT EXISTS (
                    SELECT 1 FROM __orch__.group_changes log
                    WHERE (log.model_source, log.model_target, log.revision) = (?, ?, ?)
                      AND log.row_key = todo.row_key
                      AND log.previous_group IS NOT DISTINCT FROM todo.previous_group
                      AND log.current_group IS NOT DISTINCT FROM todo.current_group
                )
            """, [rows, *params, *params])
            tx.execute("""
                CREATE TEMP TABLE affected_groups AS
                SELECT DISTINCT unnest([previous_group, current_group]) AS region
                FROM __orch__.group_changes WHERE (model_source, model_target, revision) = (?, ?, ?)
            """, params)
            count, = tx.execute("SELECT count(region) FROM affected_groups").fetchone()
            tx.execute(f"DELETE FROM main.{self._target} WHERE region IN (SELECT region FROM affected_groups)")
            tx.execute(f"""
                INSERT INTO main.{self._target}(region, value, n)
                SELECT region, sum(amount), count(*) FROM main.{self._source}
                SEMI JOIN affected_groups USING (region) GROUP BY region
            """)
            return count
        return _write_transaction(self._db, refresh)
