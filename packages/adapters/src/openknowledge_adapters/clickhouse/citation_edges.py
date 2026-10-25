from collections.abc import Callable
from datetime import datetime
from time import monotonic
from uuid import UUID

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_adapters.clickhouse.tables import ClickHouseAppliedReleases, ClickHouseTables
from openknowledge_contracts.datasets import ReleaseId
from openknowledge_contracts.manifests import Producer
from openknowledge_core.ingestion.citation_edges import (
    EDGE_TABLES,
    BuildCitationEdgesResult,
    CitationEdgeTableResult,
)
from openknowledge_core.ingestion.errors import TableAccessDenied, TableRejected
from openknowledge_core.ingestion.models import AppliedRelease, held_release

OUT, IN = EDGE_TABLES
TABLES = EDGE_TABLES
QUERY_ID = "citation-edges"
SOURCES = {
    OUT: "SELECT citingcorpusid, assumeNotNull(citedcorpusid), isinfluential, "
    "toUInt8(bitOr(bitOr("
    "if(has(arrayFlatten(intents), 'background'), 1, 0), "
    "if(has(arrayFlatten(intents), 'methodology'), 2, 0)), "
    "if(has(arrayFlatten(intents), 'result'), 4, 0))) "
    "FROM citations FINAL WHERE citedcorpusid IS NOT NULL",
    # One scan of citations: the incoming table is the outgoing one, sorted the other way.
    IN: f"SELECT citing, cited, isinfluential, intents FROM {OUT}_new",
}
SETTINGS = (
    "SETTINGS max_memory_usage = 8000000000, max_execution_time = 14400, "
    "min_insert_block_size_rows = 50000000, min_insert_block_size_bytes = 0, "
    "max_insert_threads = 1"
)
BYTES_PER_CITATION = 12
"""Free disk for both new copies at about 4 bytes an edge each, with room for merges."""
MIN_KEPT = 0.9
"""A rebuild with fewer edges than this share of the live table is refused."""


class ClickHouseCitationEdges:
    def __init__(
        self,
        client: ClickHouseClient,
        tables: ClickHouseTables,
        applied: ClickHouseAppliedReleases,
    ) -> None:
        self._client = client
        self._tables = tables
        self._applied = applied

    async def is_current(self, release: ReleaseId) -> bool:
        rows = await self._client.rows(
            "SELECT name, uuid FROM system.tables WHERE database = currentDatabase() "
            f"AND name IN ('{OUT}', '{IN}')"
        )
        live = {row["name"]: UUID(row["uuid"]) for row in rows}
        for table in TABLES:
            applied = await self._applied.latest(table)
            if (
                applied is None
                or applied.release != release
                or live.get(table) != applied.target_uuid
            ):
                return False
        return True

    async def build(
        self, producer: Producer, clock: Callable[[], datetime]
    ) -> BuildCitationEdgesResult:
        source = await self._tables.table("citations")
        release = held_release(source, await self._applied.latest(source.name))
        if release is None:
            raise TableRejected(
                "citations holds no completed release; finish its load before building edges"
            )
        # A cancelled run's insert keeps running on the server. Replacing its query ID stops it,
        # unlike KILL QUERY, without a grant.
        await self._client.post("SELECT 1 SETTINGS replace_running_query = 1", query_id=QUERY_ID)
        # Reclaim abandoned copies, including an old live table after a partial swap.
        # Stop the old insert first; never drop the published names.
        for table in TABLES:
            await self._client.post(f"DROP TABLE IF EXISTS {table}_new SYNC")
        live = await self._preflight()
        results: dict[str, CitationEdgeTableResult] = {}
        for table in TABLES:
            started = monotonic()
            await self._client.post(f"CREATE TABLE {table}_new AS {table}")
            rows = await self._client.written(
                f"INSERT INTO {table}_new (citing, cited, isinfluential, intents) "
                f"{SOURCES[table]} {SETTINGS}",
                query_id=QUERY_ID,
            )
            results[table] = CitationEdgeTableResult(rows=rows, seconds=monotonic() - started)
        built = results[OUT].rows
        if results[IN].rows != built:
            raise TableRejected("citation edge counts differ; no edge tables were swapped")
        if built < live * MIN_KEPT:
            raise TableRejected(
                f"the rebuild holds {built} edges where {OUT} has {live}; no edge tables were "
                "swapped. If the drop is expected, truncate both edge tables and rerun"
            )
        # Validate both directions before publishing either; a failed build can be rerun.
        for table in TABLES:
            started = monotonic()
            await self._client.post(f"EXCHANGE TABLES {table}_new AND {table}")
            await self._client.post(f"DROP TABLE {table}_new SYNC")
            await self._applied.record(
                AppliedRelease(
                    target=table,
                    target_uuid=await self._uuid(table),
                    release=release,
                    from_release=None,
                    software_version=producer.software_version,
                    image_digest=producer.image_digest,
                    applied_at=clock(),
                )
            )
            results[table] = results[table].model_copy(
                update={"seconds": results[table].seconds + monotonic() - started}
            )
        return BuildCitationEdgesResult(source_release=release, **results)

    async def _uuid(self, table: str) -> UUID:
        rows = await self._client.rows(
            "SELECT uuid FROM system.tables "
            "WHERE database = currentDatabase() AND name = {table:String}",
            table=table,
        )
        return UUID(rows[0]["uuid"])

    async def _preflight(self) -> int:
        """Check the edge tables exist and the disk can hold new copies; return the live edges."""
        rows = await self._client.rows(
            "SELECT name, total_rows FROM system.tables WHERE database = currentDatabase() "
            f"AND name IN ('citations', '{OUT}', '{IN}')"
        )
        found = {row["name"]: int(row["total_rows"] or 0) for row in rows}
        if not set(TABLES) <= found.keys():
            raise TableRejected("the citation edge tables do not exist; run the migrations")
        try:
            disk = await self._client.rows(
                "SELECT free_space FROM system.disks WHERE name = 'default'"
            )
        except TableAccessDenied:
            raise TableAccessDenied(
                "the edge rebuild checks free disk first; grant SELECT ON system.disks"
            ) from None
        free, needed = int(disk[0]["free_space"]), found["citations"] * BYTES_PER_CITATION
        if free < needed:
            raise TableRejected(
                f"building citation edges needs about {needed // 2**30} GiB free, but the disk "
                f"has {free // 2**30} GiB; nothing was built"
            )
        return found[OUT]
