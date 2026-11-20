"""Read-side planning for a host-provisioned, tenant-scoped duckOrch connection."""
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from graphlib import CycleError, TopologicalSorter

from duckdb import DuckDBPyConnection


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


class ChangeWindowPlanner:
    """Read-side plan for one changed input in a host-registered calendar DAG.

    The caller supplies a tenant-specific graph and retained date horizon at
    construction. Use ALL when an edge has no established partition mapping.
    """

    def __init__(self, dependencies: Mapping[str, Mapping[str, PartitionGrain]],
                 horizon: DateWindow):
        if type(horizon) is not DateWindow:
            raise TypeError("horizon must be a DateWindow")
        self._horizon = horizon
        self._dependencies = {model: dict(parents) for model, parents in dependencies.items()}
        for parents in self._dependencies.values():
            if not parents.keys() <= self._dependencies.keys():
                raise ValueError("all upstream models must be registered")
            if any(type(grain) is not PartitionGrain for grain in parents.values()):
                raise TypeError("dependencies must use PartitionGrain")
        try:
            self._order = tuple(TopologicalSorter(self._dependencies).static_order())
        except CycleError as exc:
            raise ValueError("model dependencies contain a cycle") from exc

    def _expand(self, window: DateWindow, grain: PartitionGrain) -> DateWindow:
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
        if source_id not in self._dependencies:
            raise KeyError(source_id)
        if type(changed) is not DateWindow:
            raise TypeError("changed must be a DateWindow")
        start = max(changed.start, self._horizon.start)
        end = min(changed.end, self._horizon.end)
        if start >= end:
            return {}

        windows = {source_id: DateWindow(start, end)}
        for model in self._order:
            if model == source_id:
                continue
            candidates = [
                self._expand(windows[parent], grain)
                for parent, grain in self._dependencies[model].items()
                if parent in windows
            ]
            if candidates:
                # Each mapping contains the original changed window, so the
                # incoming windows overlap and their union is contiguous.
                windows[model] = DateWindow(min(w.start for w in candidates),
                                            max(w.end for w in candidates))
        del windows[source_id]
        return {model: (window,) for model, window in windows.items()}
