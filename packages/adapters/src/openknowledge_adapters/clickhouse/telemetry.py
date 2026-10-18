"""Read-only snapshots; unavailable gauges stay null and carry an error."""

from datetime import UTC, datetime

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_contracts.datasets import DatasetName
from openknowledge_contracts.tables.s2 import CONTRACTS
from openknowledge_core.ingestion.errors import IngestionError, TableAccessDenied
from openknowledge_core.ingestion.measurement import LoadSnapshot


class ClickHouseLoadTelemetry:
    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client

    async def snapshot(self, dataset: DatasetName) -> LoadSnapshot:
        snapshot = LoadSnapshot(captured_at=datetime.now(UTC))
        table = CONTRACTS[dataset].table
        # Independent queries retain partial evidence when a system table is unavailable.
        for system_table, fields, query in (
            (
                "system.parts",
                ("active_table_bytes",),
                "SELECT sum(bytes_on_disk) AS active_table_bytes FROM system.parts "
                "WHERE active AND database = currentDatabase() AND table = {table:String}",
            ),
            (
                "system.metrics",
                ("server_memory_tracking_bytes",),
                "SELECT maxOrNull(value) AS server_memory_tracking_bytes FROM system.metrics "
                "WHERE metric = 'MemoryTracking'",
            ),
            (
                "system.merges",
                ("table_running_merges", "table_merge_memory_bytes"),
                "SELECT count() AS table_running_merges, "
                "sum(memory_usage) AS table_merge_memory_bytes "
                "FROM system.merges WHERE database = currentDatabase() AND table = {table:String}",
            ),
            (
                "system.events",
                ("server_merged_rows_total", "server_merged_uncompressed_bytes_total"),
                "SELECT maxOrNullIf(value, event = 'MergedRows') AS server_merged_rows_total, "
                "maxOrNullIf(value, event = 'MergedUncompressedBytes') "
                "AS server_merged_uncompressed_bytes_total FROM system.events",
            ),
        ):
            try:
                rows = await self._client.rows(query, table=table)
                for field in fields:
                    value = rows[0][field]
                    if value is None:
                        snapshot.errors.append(f"{field}: counter unavailable")
                    else:
                        setattr(snapshot, field, int(value))
            except TableAccessDenied:
                snapshot.errors.append(
                    f"{','.join(fields)}: access denied (code 497); "
                    f"ask an administrator to grant SELECT ON {system_table} to the load account"
                )
            except (IngestionError, ValueError, KeyError, IndexError, TypeError) as exc:
                snapshot.errors.append(f"{','.join(fields)}: {type(exc).__name__}")
        return snapshot
