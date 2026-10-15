from uuid import UUID

import pytest

from openknowledge_contracts.manifests import ObjectPath
from openknowledge_core.ingestion import (
    DownloadDatasetService,
    LoadDataset,
    LoadDatasetResult,
    LoadDatasetService,
)
from openknowledge_core.ingestion.errors import (
    ConflictingKeys,
    IngestionError,
    ObjectChanged,
    RowCountMismatch,
)
from openknowledge_core.ingestion.models import Duplicates
from tests.fakes.ports import COMMAND, PAPERS, PRODUCER, FakeSource, FakeStore, FakeTables, clock

LOAD = LoadDataset(**COMMAND.model_dump())
PART_0 = ObjectPath.under(COMMAND.bucket, COMMAND.prefix, COMMAND.release, "papers", "part-0.gz")


@pytest.fixture
async def tables(
    service: DownloadDatasetService, source: FakeSource, store: FakeStore
) -> FakeTables:
    for name, data in PAPERS.items():
        source.add("papers", name, data)
    pinned = await service.pin_dataset(COMMAND)
    for shard in pinned.shards:
        await service.archive_shard(pinned, shard)
    await service.write_manifest(pinned)
    return FakeTables(store)


async def load(store: FakeStore, tables: FakeTables) -> LoadDatasetResult:
    loader = LoadDatasetService(store=store, tables=tables, producer=PRODUCER, clock=clock)
    prepared = await loader.prepare(LOAD)
    outcomes = [await loader.load_object(prepared, entry) for entry in prepared.manifest.entries]
    return await loader.finish(prepared, outcomes)


async def test_a_rerun_skips_every_loaded_object(store: FakeStore, tables: FakeTables) -> None:
    first = await load(store, tables)
    again = await load(store, tables)
    assert (first.loaded, first.already_loaded, first.live_rows) == (2, 0, 51)
    assert (again.loaded, again.already_loaded) == (0, 2)
    assert len(tables.loads) == 2
    assert tables.scanned == []


@pytest.mark.parametrize("change", ["recreated", "truncated"])
async def test_a_recreated_or_emptied_table_loads_again(
    store: FakeStore, tables: FakeTables, change: str
) -> None:
    await load(store, tables)
    if change == "recreated":
        tables.uuid = UUID(int=2)
    else:
        tables.rows.clear()
    assert (await load(store, tables)).loaded == 2


async def test_an_object_with_another_generation_is_not_loaded(
    store: FakeStore, tables: FakeTables
) -> None:
    store.put(PART_0, PAPERS["part-0.gz"])
    with pytest.raises(ObjectChanged):
        await load(store, tables)
    assert tables.loads == []


async def test_missing_rows_fail_the_load(store: FakeStore, tables: FakeTables) -> None:
    tables.short_by = 1
    with pytest.raises(RowCountMismatch, match="wrote 0 rows"):
        await load(store, tables)
    assert tables.records == {}


async def test_keys_repeated_identically_are_counted(store: FakeStore, tables: FakeTables) -> None:
    tables.collapsed = 1
    tables.found = Duplicates(identical_rows=1, conflicting_keys=0, examples=())
    result = await load(store, tables)
    assert (result.live_rows, result.identical_duplicates) == (50, 1)
    assert tables.scanned == ["part-0.gz", "part-1.gz"]


@pytest.mark.parametrize(
    ("found", "error", "message"),
    [
        (
            Duplicates(identical_rows=0, conflicting_keys=1, examples=("(7)",)),
            ConflictingKeys,
            r"\(1, such as \(7\)\)",
        ),
        (
            Duplicates(identical_rows=0, conflicting_keys=0, examples=()),
            RowCountMismatch,
            "0 of them repeat",
        ),
    ],
    ids=["conflicting", "unexplained"],
)
async def test_other_repeated_keys_fail_the_load(
    store: FakeStore,
    tables: FakeTables,
    found: Duplicates,
    error: type[IngestionError],
    message: str,
) -> None:
    tables.collapsed = 1
    tables.found = found
    with pytest.raises(error, match=message):
        await load(store, tables)
