import logging
from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager, nullcontext
from datetime import datetime
from time import monotonic
from uuid import UUID

from pydantic import TypeAdapter

from openknowledge_adapters.clickhouse.client import ClickHouseClient
from openknowledge_adapters.clickhouse.tables import ClickHouseAppliedReleases, ClickHouseTables
from openknowledge_contracts.datasets import ReleaseId
from openknowledge_contracts.manifests import Producer
from openknowledge_core.ingestion.errors import TableAccessDenied, TableRejected
from openknowledge_core.ingestion.models import AppliedRelease, held_release
from openknowledge_core.ingestion.read_tables import (
    GROUPS,
    READ_TABLES,
    SOURCE_OF,
    BuildReadTablesResult,
    GroupPlan,
    GroupState,
    ReadTableResult,
    ReadTablesPath,
    plan_group,
)

PAPERS, OUT, IN = READ_TABLES
TABLES = READ_TABLES
QUERY_ID = "citation-edges"  # Keep the recovery key stable across the rename.
DELETE_QUERY_ID = "read-tables-delete"
COLUMNS = {
    PAPERS: "corpusid, title, year, venue, citationcount, influentialcitationcount, referencecount",
    OUT: "citing, cited, isinfluential, intents, citationid",
    IN: "citing, cited, isinfluential, intents, citationid",
}
EDGES_IN = f"SELECT {COLUMNS[IN]} FROM {OUT}"
SOURCES = {
    PAPERS: f"SELECT {COLUMNS[PAPERS]} FROM papers FINAL WHERE is_deleted = 0",
    OUT: "SELECT citingcorpusid, assumeNotNull(citedcorpusid), isinfluential, "
    "toUInt8(bitOr(bitOr("
    "if(has(arrayFlatten(intents), 'background'), 1, 0), "
    "if(has(arrayFlatten(intents), 'methodology'), 2, 0)), "
    "if(has(arrayFlatten(intents), 'result'), 4, 0))), citationid "
    "FROM citations FINAL WHERE citedcorpusid IS NOT NULL",
    # One scan of citations: the incoming table is the outgoing one, sorted the other way.
    IN: f"{EDGES_IN}_new",
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
BYTES_PER_CITATION = 28
"""(4 + 4 + 5.31 + 5.31) bytes for both new copies with IDs, with 50% merge room."""
BYTES_PER_PAPER = 100
"""Provisional free disk per papers row, pending a measured build."""
BYTES_PER_IN_ONLY = 14
"""(4 + 5.31) bytes for one new incoming copy with IDs, with 50% merge room."""
MIN_KEPT = 0.9
"""A rebuild below this share of any live table is refused."""
DELTA = {"papers": "read_delta_papers", "citations": "read_delta_citations"}
"""Scratch tables holding the keys a source took in since the read tables' release."""
KEY = {"papers": "corpusid", "citations": "citationid"}
BYTES_PER_KEY = {"papers": BYTES_PER_PAPER, "citations": BYTES_PER_CITATION}
_RELEASE = TypeAdapter(ReleaseId)
log = logging.getLogger(__name__)


def delta_sql(source: str, start: ReleaseId, end: ReleaseId) -> list[str]:
    """Collect the keys ``source`` wrote in ``(start, end]``, tombstones and superseded rows too."""
    start, end = _RELEASE.validate_python(start), _RELEASE.validate_python(end)
    return [
        f"DROP TABLE IF EXISTS {DELTA[source]}",
        f"CREATE TABLE {DELTA[source]} (key UInt64) ENGINE = MergeTree ORDER BY key",
        f"INSERT INTO {DELTA[source]} SELECT DISTINCT {KEY[source]} FROM {source} "
        f"WHERE source_release > '{start}' AND source_release <= '{end}'",
    ]


def apply_sql(source: str) -> list[str]:
    """Replace the rows of the delta keys in ``source``'s read tables with their current rows.

    Deleting every delta key first makes a rerun after a partial run reach the same state.
    """
    touched = f"{KEY[source]} IN (SELECT key FROM {DELTA[source]})"
    tables = [table for table in TABLES if SOURCE_OF[table] == source]
    # Waited on: until a delete finishes, the old rows still show next to the new ones.
    deletes = [
        f"DELETE FROM {table} WHERE {touched} SETTINGS lightweight_deletes_sync = 2"
        for table in tables
    ]
    # The incoming edges come from the outgoing ones, which TABLES updates first.
    selects = {
        table: f"{EDGES_IN} WHERE" if table == IN else f"{SOURCES[table]} AND" for table in tables
    }
    return deletes + [
        f"INSERT INTO {table} ({COLUMNS[table]}) {selects[table]} {touched} "
        f"{SETTINGS}{BLOCKS[table]}"
        for table in tables
    ]


def in_only_sql() -> list[str]:
    """Rebuild only the incoming edges into ``_new``, from the live outgoing ones."""
    return [
        f"CREATE TABLE {IN}_new AS {IN}",
        f"INSERT INTO {IN}_new ({COLUMNS[IN]}) {EDGES_IN} {SETTINGS}{BLOCKS[IN]}",
    ]


def _no_stage(name: str) -> AbstractAsyncContextManager[None]:
    return nullcontext()


class ClickHouseReadTables:
    def __init__(
        self,
        client: ClickHouseClient,
        tables: ClickHouseTables,
        applied: ClickHouseAppliedReleases,
        stage: Callable[[str], AbstractAsyncContextManager[None]] | None = None,
        max_delta_share: float = 0.3,
    ) -> None:
        self._client = client
        self._tables = tables
        self._applied = applied
        self._stage: Callable[[str], AbstractAsyncContextManager[None]] = stage or _no_stage
        self._max_delta_share = max_delta_share
        """Touched keys over this share of a source rebuild in full, the cheaper path then."""

    async def is_current(self, releases: Mapping[str, ReleaseId]) -> bool:
        live = await self._live()
        for table in TABLES:
            applied = await self._applied.latest(table)
            if (
                applied is None
                or applied.release != releases.get(SOURCE_OF[table])
                or live.get(table) != applied.target_uuid
            ):
                return False
        return True

    async def reclaim(self) -> None:
        # A cancelled run's statement keeps running on the server. Replacing its query ID stops
        # it, unlike KILL QUERY, without a grant.
        for query_id in (QUERY_ID, DELETE_QUERY_ID):
            await self._client.post(
                "SELECT 1 SETTINGS replace_running_query = 1", query_id=query_id
            )
        # Reclaim abandoned copies, including an old live table after a partial swap or a lost
        # marker answer. Stop the old insert first; never drop the published names.
        for table in TABLES:
            await self._client.post(f"DROP TABLE IF EXISTS {table}_new SYNC")
        for table in DELTA.values():
            await self._client.post(f"DROP TABLE IF EXISTS {table}")

    async def build(
        self, producer: Producer, clock: Callable[[], datetime], *, full: bool = False
    ) -> BuildReadTablesResult:
        releases: dict[str, ReleaseId] = {}
        uuids: dict[str, UUID] = {}
        for name in GROUPS:
            source = await self._tables.table(name)
            release = held_release(source, await self._applied.latest(name))
            if release is None:
                raise TableRejected(
                    f"{name} holds no completed release; "
                    "finish its load before building read tables"
                )
            releases[name], uuids[name] = release, source.uuid
        if full:
            return await self._full(producer, clock, releases)
        async with self._stage("read tables: recovery and plan"):
            await self.reclaim()
            live = await self._live()
            plans: dict[str, GroupPlan] = {}
            for name in GROUPS:
                state = await self._state(name, releases[name], uuids[name], live)
                plans[name] = plan_group(state, full=False)
        if any(plan.path is ReadTablesPath.FULL for plan in plans.values()):
            return await self._full(producer, clock, releases)
        results: dict[str, ReadTableResult] = {}
        touched: dict[str, int] = {}
        for name, plan in plans.items():
            if plan.path is ReadTablesPath.IN_ONLY or plan.in_only_first:
                results[IN] = await self._in_only(plan.start or plan.end, producer, clock)
            if plan.path is ReadTablesPath.INCREMENTAL and plan.start is not None:
                updated = await self._incremental(name, plan.start, plan.end, producer, clock)
                if updated is None:
                    return await self._full(producer, clock, releases)
                touched[name], tables = updated
                results |= tables
        return BuildReadTablesResult(
            source_releases=releases,
            tables=results,
            paths={name: plan.path for name, plan in plans.items()},
            touched=touched,
        )

    async def _state(
        self, group: str, source: ReleaseId, source_uuid: UUID, live: Mapping[str, UUID]
    ) -> GroupState:
        tables = GROUPS[group]
        markers = [await self._applied.latest(table) for table in tables]
        # A marker counts only for the live table it names, and the group's tables must agree.
        found = {
            None if applied is None or applied.target_uuid != live.get(table) else applied.release
            for table, applied in zip(tables, markers, strict=True)
        }
        marker = found.pop() if len(found) == 1 else None
        took = marker is not None and await self._applied.took(group, source_uuid, marker)
        edges = took and OUT in tables
        return GroupState(
            marker=marker,
            source=source,
            source_took_marker=took,
            ids_missing_out=edges and await self._missing_ids(OUT),
            ids_missing_in=edges and await self._missing_ids(IN),
        )

    async def _missing_ids(self, table: str) -> bool:
        """Whether rows from before ``citationid`` remain, which an update cannot key on."""
        return bool(await self._client.rows(f"SELECT 1 FROM {table} WHERE citationid = 0 LIMIT 1"))

    async def _in_only(
        self, release: ReleaseId, producer: Producer, clock: Callable[[], datetime]
    ) -> ReadTableResult:
        started = monotonic()
        async with self._stage("read tables: in-only"):
            outgoing = (await self._counts(OUT))[OUT]
            await self._require_free(outgoing * BYTES_PER_IN_ONLY, f"rebuilding {IN}")
            create, insert = in_only_sql()
            await self._run(create)
            rows = await self._run(insert)
            if (await self._counts(f"{IN}_new"))[f"{IN}_new"] != outgoing:
                raise TableRejected(f"citation edge counts differ; {IN} was not swapped")
            await self._client.post(f"EXCHANGE TABLES {IN}_new AND {IN}")
            await self._applied.record(await self._marker(IN, release, None, producer, clock))
            await self._client.post(f"DROP TABLE {IN}_new SYNC")
        return ReadTableResult(rows=rows, seconds=monotonic() - started)

    async def _incremental(
        self,
        source: str,
        start: ReleaseId,
        end: ReleaseId,
        producer: Producer,
        clock: Callable[[], datetime],
    ) -> tuple[int, dict[str, ReadTableResult]] | None:
        """Take ``source``'s diffs into its read tables in place, or None to rebuild them all."""
        started = monotonic()
        tables = GROUPS[source]
        async with self._stage(f"read tables: {source} delta"):
            for statement in delta_sql(source, start, end):
                await self._run(statement)
            touched = (await self._counts(DELTA[source]))[DELTA[source]]
            total = await self._source_rows(source)
            if touched > self._max_delta_share * total:
                log.info(
                    "%s touched %d of %d rows, over the %.0f%% share; rebuilding in full",
                    source,
                    touched,
                    total,
                    self._max_delta_share * 100,
                )
                return None
            await self._require_free(
                touched * BYTES_PER_KEY[source], f"updating the {source} read tables"
            )
        async with self._stage(f"read tables: {source} apply"):
            written: dict[str, int] = {}
            for statement in apply_sql(source):
                rows = await self._run(statement)
                if statement.startswith("INSERT"):
                    written[statement.split()[2]] = rows
            if OUT in tables:
                counts = await self._counts(OUT, IN)
                if counts[OUT] != counts[IN]:
                    log.warning(
                        "citation edge counts differ after the update (%d out, %d in); "
                        "rebuilding in full",
                        counts[OUT],
                        counts[IN],
                    )
                    return None
            await self._applied.record_all(
                [await self._marker(table, end, start, producer, clock) for table in tables]
            )
            await self._client.post(f"DROP TABLE IF EXISTS {DELTA[source]}")
        seconds = monotonic() - started
        return touched, {
            table: ReadTableResult(rows=written[table], seconds=seconds) for table in tables
        }

    async def _full(
        self,
        producer: Producer,
        clock: Callable[[], datetime],
        releases: Mapping[str, ReleaseId],
    ) -> BuildReadTablesResult:
        async with self._stage("read tables: preflight and recovery"):
            await self.reclaim()
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
        # An uncommitted marker insert leaves the new generation live but not current, so the next
        # sync rebuilds it; one whose answer was lost may have committed, and then it is current
        # and the next sync only reclaims the old tables.
        # Swapping back here could strand the edge tables a generation apart.
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
            await self._applied.record_all(
                [
                    await self._marker(table, releases[SOURCE_OF[table]], None, producer, clock)
                    for table in TABLES
                ]
            )
            for table in TABLES:
                await self._client.post(f"DROP TABLE {table}_new SYNC")
        return BuildReadTablesResult(
            source_releases=dict(releases),
            tables=results,
            paths=dict.fromkeys(GROUPS, ReadTablesPath.FULL),
        )

    async def _run(self, statement: str) -> int:
        """Run under the kind's fixed query ID, which a rerun replaces; return rows inserted."""
        if statement.startswith("INSERT"):
            return await self._client.written(statement, query_id=QUERY_ID)
        query_id = DELETE_QUERY_ID if statement.startswith("DELETE") else None
        await self._client.post(statement, query_id=query_id)
        return 0

    async def _marker(
        self,
        table: str,
        release: ReleaseId,
        from_release: ReleaseId | None,
        producer: Producer,
        clock: Callable[[], datetime],
    ) -> AppliedRelease:
        return AppliedRelease(
            target=table,
            target_uuid=await self._uuid(table),
            release=release,
            from_release=from_release,
            software_version=producer.software_version,
            image_digest=producer.image_digest,
            applied_at=clock(),
        )

    async def _live(self) -> dict[str, UUID]:
        rows = await self._client.rows(
            "SELECT name, uuid FROM system.tables WHERE database = currentDatabase() "
            f"AND name IN ('{PAPERS}', '{OUT}', '{IN}')"
        )
        return {row["name"]: UUID(row["uuid"]) for row in rows}

    async def _uuid(self, table: str) -> UUID:
        rows = await self._client.rows(
            "SELECT uuid FROM system.tables "
            "WHERE database = currentDatabase() AND name = {table:String}",
            table=table,
        )
        return UUID(rows[0]["uuid"])

    async def _counts(self, *tables: str) -> dict[str, int]:
        """Rows the tables hold; ``total_rows`` also counts rows deleted but not yet merged."""
        counts: dict[str, int] = {}
        for table in tables:
            rows = await self._client.rows(f"SELECT count() AS rows FROM {table}")
            counts[table] = int(rows[0]["rows"])
        return counts

    async def _source_rows(self, source: str) -> int:
        rows = await self._client.rows(
            "SELECT total_rows FROM system.tables "
            "WHERE database = currentDatabase() AND name = {table:String}",
            table=source,
        )
        return int(rows[0]["total_rows"] or 0)

    async def _preflight(self) -> dict[str, int]:
        """Check the read tables and capacity; return live row counts."""
        rows = await self._client.rows(
            "SELECT name, total_rows FROM system.tables WHERE database = currentDatabase() "
            f"AND name IN ('papers', 'citations', '{PAPERS}', '{OUT}', '{IN}')"
        )
        found = {row["name"]: int(row["total_rows"] or 0) for row in rows}
        if not set(TABLES) <= found.keys():
            raise TableRejected("the read tables do not exist; run the migrations")
        columns = await self._client.rows(
            "SELECT table FROM system.columns WHERE database = currentDatabase() "
            f"AND table IN ('{OUT}', '{IN}') AND name = 'citationid'"
        )
        have_ids = {row["table"] for row in columns}
        for table, migration in ((OUT, "00018"), (IN, "00019")):
            if table not in have_ids:
                raise TableRejected(
                    f"{table}.citationid is missing; run migration {migration} before rebuilding"
                )
        await self._require_free(
            found["citations"] * BYTES_PER_CITATION + found["papers"] * BYTES_PER_PAPER,
            "building read tables",
        )
        return await self._counts(*TABLES)

    async def _require_free(self, needed: int, action: str) -> None:
        try:
            disk = await self._client.rows(
                "SELECT free_space FROM system.disks WHERE name = 'default'"
            )
        except TableAccessDenied:
            raise TableAccessDenied(
                "the read table rebuild checks free disk first; grant SELECT ON system.disks"
            ) from None
        free = int(disk[0]["free_space"])
        if free < needed:
            raise TableRejected(
                f"{action} needs about {needed // 2**30} GiB free, but the disk "
                f"has {free // 2**30} GiB; nothing was built"
            )
