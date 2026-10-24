from time import monotonic

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_adapters.clickhouse.tables import ClickHouseTables
from openknowledge_core.ingestion.citation_edges import (
    BuildCitationEdgesResult,
    CitationEdgeTableResult,
)
from openknowledge_core.ingestion.errors import TableRejected

TABLES = ("citation_edges_out", "citation_edges_in")


class ClickHouseCitationEdges:
    def __init__(self, client: ClickHouseClient, tables: ClickHouseTables) -> None:
        self._client = client
        self._tables = tables

    async def build(self) -> BuildCitationEdgesResult:
        await self._tables.table("citations")
        results: dict[str, CitationEdgeTableResult] = {}
        for table in TABLES:
            started = monotonic()
            await self._client.post(f"DROP TABLE IF EXISTS {table}_new")
            await self._client.post(f"CREATE TABLE {table}_new AS {table}")
            written = await self._client.written(
                f"INSERT INTO {table}_new (citing, cited, isinfluential, intents) "
                "SELECT citingcorpusid, assumeNotNull(citedcorpusid), isinfluential, "
                "toUInt8(bitOr(bitOr("
                "if(has(arrayFlatten(intents), 'background'), 1, 0), "
                "if(has(arrayFlatten(intents), 'methodology'), 2, 0)), "
                "if(has(arrayFlatten(intents), 'result'), 4, 0))) "
                "FROM citations FINAL WHERE citedcorpusid IS NOT NULL "
                "SETTINGS min_insert_block_size_rows = 50000000, "
                "min_insert_block_size_bytes = 0, max_insert_threads = 1"
            )
            rows = await self._client.rows(f"SELECT count() AS rows FROM {table}_new")
            counted = int(rows[0]["rows"])
            if counted != written:
                raise TableRejected(
                    f"{table}: staged count {counted} differs from inserted rows {written}; "
                    "no edge tables were swapped"
                )
            results[table] = CitationEdgeTableResult(rows=counted, seconds=monotonic() - started)
        if results[TABLES[0]].rows != results[TABLES[1]].rows:
            raise TableRejected("citation edge counts differ; no edge tables were swapped")
        # Validate both directions before publishing either; a failed build can be rerun.
        for table in TABLES:
            started = monotonic()
            await self._client.post(f"EXCHANGE TABLES {table}_new AND {table}")
            await self._client.post(f"DROP TABLE {table}_new")
            results[table] = results[table].model_copy(
                update={"seconds": results[table].seconds + monotonic() - started}
            )
        return BuildCitationEdgesResult(**results)
