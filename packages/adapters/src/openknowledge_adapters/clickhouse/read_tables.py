from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager, nullcontext
from datetime import datetime
from time import monotonic
from uuid import UUID

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_adapters.clickhouse.tables import ClickHouseAppliedReleases, ClickHouseTables
from openknowledge_contracts.datasets import ReleaseId
from openknowledge_contracts.manifests import Producer
from openknowledge_core.ingestion.errors import TableAccessDenied, TableRejected
from openknowledge_core.ingestion.models import AppliedRelease, held_release
from openknowledge_core.ingestion.read_tables import (
    READ_TABLES,
    SOURCE_OF,
    BuildReadTablesResult,
    ReadTableResult,
)

PAPERS, OUT, IN = READ_TABLES
TABLES = READ_TABLES
QUERY_ID = "citation-edges"  # Keep the recovery key stable across the rename.
COLUMNS = {
    PAPERS: "corpusid, title, year, venue, citationcount, influentialcitationcount, referencecount",
    OUT: "citing, cited, isinfluential, intents",
    IN: "citing, cited, isinfluential, intents",
}
SOURCES = {
    PAPERS: f"SELECT {COLUMNS[PAPERS]} FROM papers FINAL WHERE is_deleted = 0",
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
    "SETTINGS max_memory_usage = 8000000000, max_execution_time = 14400, max_insert_threads = 1, "
)
EDGE_BLOCKS = "min_insert_block_size_rows = 50000000, min_insert_block_size_bytes = 0"
BLOCKS = {
    # Paper rows carry titles: 50M of them would not fit the memory limit, so cap blocks by bytes.
    PAPERS: "min_insert_block_size_rows = 0, min_insert_block_size_bytes = 1073741824",
    OUT: EDGE_BLOCKS,
    IN: EDGE_BLOCKS,
}
BYTES_PER_CITATION = 12
"""Free disk for both new copies at about 4 bytes an edge each, with room for merges."""
BYTES_PER_PAPER = 100
"""Provisional free disk per papers row, pending a measured build."""
MIN_KEPT = 0.9
"""A rebuild below this share of any live table is refused."""


def _no_stage(name: str) -> AbstractAsyncContextManager[None]:
    return nullcontext()


class ClickHouseReadTables:
    def __init__(
        self,
        client: ClickHouseClient,
        tables: ClickHouseTables,
        applied: ClickHouseAppliedReleases,
        stage: Callable[[str], AbstractAsyncContextManager[None]] | None = None,
    ) -> None:
        self._client = client
        self._tables = tables
        self._applied = applied
        self._stage: Callable[[str], AbstractAsyncContextManager[None]] = stage or _no_stage

    async def is_current(self, releases: Mapping[str, ReleaseId]) -> bool:
        rows = await self._client.rows(
            "SELECT name, uuid FROM system.tables WHERE database = currentDatabase() "
            f"AND name IN ('{PAPERS}', '{OUT}', '{IN}')"
        )
        live = {row["name"]: UUID(row["uuid"]) for row in rows}
        for table in TABLES:
            applied = await self._applied.latest(table)
            if (
                applied is None
                or applied.release != releases.get(SOURCE_OF[table])
                or live.get(table) != applied.target_uuid
            ):
                return False
        return True

    async def build(
        self, producer: Producer, clock: Callable[[], datetime]
    ) -> BuildReadTablesResult:
        releases: dict[str, ReleaseId] = {}
        for name in dict.fromkeys(SOURCE_OF.values()):
            source = await self._tables.table(name)
            release = held_release(source, await self._applied.latest(name))
            if release is None:
                raise TableRejected(
                    f"{name} holds no completed release; "
                    "finish its load before building read tables"
                )
            releases[name] = release
        async with self._stage("read tables: preflight and recovery"):
            # A cancelled run's insert keeps running on the server. Replacing its query ID stops it,
            # unlike KILL QUERY, without a grant.
            await self._client.post(
                "SELECT 1 SETTINGS replace_running_query = 1", query_id=QUERY_ID
            )
            # Reclaim abandoned copies, including an old live table after a partial swap.
            # Stop the old insert first; never drop the published names.
            for table in TABLES:
                await self._client.post(f"DROP TABLE IF EXISTS {table}_new SYNC")
            live = await self._preflight()
        results: dict[str, ReadTableResult] = {}
        for table in TABLES:
            started = monotonic()
            async with self._stage(f"read tables: build {table}"):
                await self._client.post(f"CREATE TABLE {table}_new AS {table}")
                rows = await self._client.written(
                    f"INSERT INTO {table}_new ({COLUMNS[table]}) {SOURCES[table]} "
                    f"{SETTINGS}{BLOCKS[table]}",
                    query_id=QUERY_ID,
                )
                results[table] = ReadTableResult(rows=rows, seconds=monotonic() - started)
        async with self._stage("read tables: validate counts"):
            built = results[OUT].rows
            if results[IN].rows != built:
                raise TableRejected("citation edge counts differ; no read tables were swapped")
            for table in TABLES:
                if results[table].rows < live[table] * MIN_KEPT:
                    raise TableRejected(
                        f"the rebuild holds {results[table].rows} rows where {table} has "
                        f"{live[table]}; no read tables were swapped. If the drop is expected, "
                        "truncate the affected live table and rerun"
                    )
        # EXCHANGE is not atomic across tables, even in one statement. Undo a partial swap so a
        # failure leaves the old generation live, and record nothing until all three are live.
        async with self._stage("read tables: publish"):
            swapped: list[str] = []
            try:
                for table in TABLES:
                    await self._client.post(f"EXCHANGE TABLES {table}_new AND {table}")
                    swapped.append(table)
            except BaseException:
                for table in reversed(swapped):
                    await self._client.post(f"EXCHANGE TABLES {table}_new AND {table}")
                raise
            for table in TABLES:
                await self._applied.record(
                    AppliedRelease(
                        target=table,
                        target_uuid=await self._uuid(table),
                        release=releases[SOURCE_OF[table]],
                        from_release=None,
                        software_version=producer.software_version,
                        image_digest=producer.image_digest,
                        applied_at=clock(),
                    )
                )
            for table in TABLES:
                await self._client.post(f"DROP TABLE {table}_new SYNC")
        return BuildReadTablesResult(source_releases=releases, tables=results)

    async def _uuid(self, table: str) -> UUID:
        rows = await self._client.rows(
            "SELECT uuid FROM system.tables "
            "WHERE database = currentDatabase() AND name = {table:String}",
            table=table,
        )
        return UUID(rows[0]["uuid"])

    async def _preflight(self) -> dict[str, int]:
        """Check the read tables and capacity; return live row counts."""
        rows = await self._client.rows(
            "SELECT name, total_rows FROM system.tables WHERE database = currentDatabase() "
            f"AND name IN ('papers', 'citations', '{PAPERS}', '{OUT}', '{IN}')"
        )
        found = {row["name"]: int(row["total_rows"] or 0) for row in rows}
        if not set(TABLES) <= found.keys():
            raise TableRejected("the read tables do not exist; run the migrations")
        try:
            disk = await self._client.rows(
                "SELECT free_space FROM system.disks WHERE name = 'default'"
            )
        except TableAccessDenied:
            raise TableAccessDenied(
                "the read table rebuild checks free disk first; grant SELECT ON system.disks"
            ) from None
        free = int(disk[0]["free_space"])
        needed = found["citations"] * BYTES_PER_CITATION + found["papers"] * BYTES_PER_PAPER
        if free < needed:
            raise TableRejected(
                f"building read tables needs about {needed // 2**30} GiB free, but the disk "
                f"has {free // 2**30} GiB; nothing was built"
            )
        return {table: found[table] for table in TABLES}
