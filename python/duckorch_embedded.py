"""Read-side planning for a host-provisioned, tenant-scoped duckOrch connection."""
from collections.abc import Mapping
from datetime import date

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
