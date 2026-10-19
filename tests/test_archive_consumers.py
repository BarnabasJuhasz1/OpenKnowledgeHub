"""Consumer reads cross the actual streaming storage adapter, without source clients."""

import json
from contextlib import aclosing

import pytest

from openknowledge_adapters import gcs
from openknowledge_core.ingestion.archive_progress import ArchiveProgress
from openknowledge_core.ingestion.consumers import ConsumeBase
from openknowledge_core.ingestion.errors import ObjectChanged
from openknowledge_core.ingestion.service import DownloadDatasetService
from openknowledge_pipelines import bootstrap
from openknowledge_pipelines.settings import CommonSettings
from tests.fakes.gcs_json_api import FakeGcs, running
from tests.fakes.ports import PRODUCER, FakeSource, clock, gz


@pytest.mark.parametrize("replace_before_media", [False, True])
async def test_completed_base_through_streaming_storage_adapter(replace_before_media: bool) -> None:
    command = ConsumeBase(dataset="s2orc_v2", release="2026-10-13", bucket="raw", max_records=None)
    source = FakeSource()
    source.add(
        "s2orc_v2",
        "part.gz",
        gz(
            (
                json.dumps({"corpusid": 7, "body": {"text": "Synthetic body", "annotations": None}})
                + "\n"
            ).encode()
        ),
    )
    async with running(FakeGcs()) as server:
        settings = CommonSettings(gcs_api_root=server.base_url)
        async with gcs.connect(
            gcs.GcsConfig(
                api_root=server.base_url, request_timeout_seconds=30, min_transfer_rate=1024**2
            )
        ) as store:
            service = DownloadDatasetService(
                catalog=source,
                downloads=source,
                store=store,
                producer=PRODUCER,
                clock=clock,
            )
            pinned = await service.pin_dataset(command)
            await service.archive_shard(pinned, pinned.shards[0])
            await ArchiveProgress(store, service, PRODUCER, clock).finalize(command)
        if replace_before_media:
            path = pinned.object_path(pinned.shards[0])
            original_generation = server.objects[(path.bucket, path.name)].generation

            def replace(key: tuple[str, str]) -> None:
                if key == (path.bucket, path.name):
                    server.put_object(*key, gz(b'{"corpusid":99,"body":{"text":"replacement"}}\n'))

            server.before_media = replace
            # The default bounded read must fail before attributing any replacement row.
            bounded = command.model_copy(update={"max_records": 1})
            async with bootstrap.open_archive_consumer(settings) as consumer:
                async with aclosing(consumer.stream(bounded)) as rows:
                    with pytest.raises(ObjectChanged):
                        await anext(rows)
            assert (path.name, str(original_generation)) in server.media_preconditions
            return
        async with bootstrap.open_archive_consumer(settings) as consumer:
            async with aclosing(consumer.stream(command)) as rows:
                result = [row async for row in rows]
        assert len(result) == 1 and result[0].paper.corpusid == 7
