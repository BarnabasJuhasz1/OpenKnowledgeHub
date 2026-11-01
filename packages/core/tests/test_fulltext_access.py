import json

import pytest
from pydantic import ValidationError

from openknowledge_core.ingestion.consumers import ArchiveConsumer, ConsumeBase
from openknowledge_core.ingestion.fulltext import LookupRequest, lookup, metadata
from packages.core.tests.test_consumers import FULLTEXT, TrackedStore, completed
from tests.fakes.ports import gz


async def test_lookup_retains_lineage_and_restricts_even_reported_ccby() -> None:
    command = ConsumeBase(dataset="s2orc_v2", release="2026-10-13", bucket="raw")
    store = TrackedStore()
    paper = {**FULLTEXT, "openaccessinfo": {"license": "CCBY", "status": "GREEN"}}
    await completed(command, store, gz((json.dumps(paper) + "\n").encode()))
    result = await lookup(ArchiveConsumer(store).stream(command), LookupRequest(corpusid=12))
    assert result.status == "found" and result.metadata is not None
    item = result.metadata
    assert item.source.object_ref.generation > 0 and item.source.line == 1
    assert item.source.kind == "base" and item.source.release == command.release
    assert item.source.completion.sha256 and item.source.manifest.sha256
    assert item.integrity == "completion-pinned-prefix"
    assert "later diffs/deletes unsupported" in item.source_scope
    assert item.license_reported == "CCBY"
    assert not item.display_allowed and not item.derivation_allowed
    assert "Synthetic text" not in item.model_dump_json()
    assert store.closed == 1


async def test_lookup_distinguishes_budget_from_stream_absence() -> None:
    command = ConsumeBase(dataset="s2orc_v2", release="2026-10-13", bucket="raw")
    store = TrackedStore()
    await completed(command, store, gz((json.dumps(FULLTEXT) + "\n").encode()) * 2)
    limited = await lookup(
        ArchiveConsumer(store).stream(command), LookupRequest(corpusid=99, max_records=1)
    )
    assert limited.status == "scan-limit-reached" and limited.scanned == 1
    absent = await lookup(ArchiveConsumer(store).stream(command), LookupRequest(corpusid=99))
    assert absent.status == "absent-in-stream" and absent.scanned == 2
    assert store.closed == 2


async def test_empty_text_and_unknown_license_are_explicit() -> None:
    command = ConsumeBase(dataset="s2orc_v2", release="2026-10-13", bucket="raw")
    store = TrackedStore()
    paper = {**FULLTEXT, "body": {"text": "  "}, "openaccessinfo": {"license": 42}}
    await completed(command, store, gz((json.dumps(paper) + "\n").encode()))
    rows = ArchiveConsumer(store).stream(command)
    try:
        item = metadata(await anext(rows))
    finally:
        await rows.aclose()
    assert item.text_state == "empty" and item.text_utf8_bytes == 2
    assert item.license_reported is None


@pytest.mark.parametrize("corpusid", [True, -1, 2**64, "12"])
def test_invalid_lookup_identifiers(corpusid: object) -> None:
    with pytest.raises(ValidationError):
        LookupRequest.model_validate({"corpusid": corpusid})


async def test_upstream_prefix_exhaustion_is_only_absence_in_supplied_stream() -> None:
    command = ConsumeBase(dataset="s2orc_v2", release="2026-10-13", bucket="raw", max_records=1)
    store = TrackedStore()
    other = {**FULLTEXT, "corpusid": 99}
    data = (json.dumps(FULLTEXT) + "\n" + json.dumps(other) + "\n").encode()
    await completed(command, store, gz(data))
    result = await lookup(ArchiveConsumer(store).stream(command), LookupRequest(corpusid=99))
    assert result.status == "absent-in-stream" and result.scanned == 1
    whole = command.model_copy(update={"max_records": None})
    found = await lookup(ArchiveConsumer(store).stream(whole), LookupRequest(corpusid=99))
    assert found.metadata is not None and found.metadata.source.line == 2
