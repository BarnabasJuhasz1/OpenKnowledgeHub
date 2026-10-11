import json
from datetime import UTC, datetime
from urllib.parse import quote

import pytest
from pydantic import TypeAdapter, ValidationError

from openknowledge_contracts.datasets import (
    DatasetInventory,
    DatasetName,
    DatasetSummary,
    ReleaseId,
    ReleaseMetadata,
    ReleaseRecord,
    ShardRef,
    SignedMaterialError,
)

NOW = datetime(2026, 10, 10, tzinfo=UTC)
SIGNED = "https://ai2.test/staging/p/part-0.gz?AWSAccessKeyId=AKIA&Signature=topsecret"


def record(readme: str = "Terms", **extra: object) -> ReleaseRecord:
    metadata = ReleaseMetadata.model_validate(
        {"release_id": "2026-10-06", "README": readme, "datasets": [], **extra}
    )
    return ReleaseRecord(retrieved_at=NOW, metadata=metadata)


def shard(name: str = "part-0.gz") -> ShardRef:
    return ShardRef(source_path=f"/staging/2026-10-06/papers/{name}", file_name=name)


@pytest.mark.parametrize("value", ["2026-10-06", "2022-05-10"])
def test_release_ids(value: str) -> None:
    assert TypeAdapter(ReleaseId).validate_python(value) == value


@pytest.mark.parametrize("value", ["latest", "2026-10-06/../x", "", "2026-10-6"])
def test_bad_release_ids(value: str) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(ReleaseId).validate_python(value)


@pytest.mark.parametrize("value", ["papers", "publication-venues", "embeddings-specter_v2"])
def test_dataset_names(value: str) -> None:
    assert TypeAdapter(DatasetName).validate_python(value) == value


@pytest.mark.parametrize("value", ["Papers", "../papers", "papers?x=1", "", "papers\n"])
def test_bad_dataset_names(value: str) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(DatasetName).validate_python(value)


@pytest.mark.parametrize(
    ("path", "name"),
    [
        ("staging/p/part-0.gz", "part-0.gz"),
        ("/staging/p/part-0.gz", "part-1.gz"),
        ("/staging/p/part\u200b0.gz", "part\u200b0.gz"),
        ("/staging/p/dataset.json", "dataset.json"),
        ("/staging/p/manifest.json", "manifest.json"),
        ("/staging/p/a.gz.receipt.json", "a.gz.receipt.json"),
    ],
)
def test_shard_refs_refuse_odd_paths_and_reserved_names(path: str, name: str) -> None:
    with pytest.raises(ValidationError):
        ShardRef(source_path=path, file_name=name)


def test_release_record_round_trips_with_its_readme_and_unknown_fields() -> None:
    document = record(license_notes="ODC-BY")
    stored = json.loads(document.to_json())
    assert stored["metadata"]["README"] == "Terms"
    assert stored["metadata"]["license_notes"] == "ODC-BY"
    assert ReleaseRecord.from_json(document.to_json()) == document


def test_inventory_round_trips_and_needs_unique_shards() -> None:
    summary = DatasetSummary.model_validate({"name": "papers", "description": "", "README": "x"})
    inventory = DatasetInventory(
        release_id="2026-10-06", retrieved_at=NOW, dataset=summary, shards=(shard(),)
    )
    assert DatasetInventory.from_json(inventory.to_json()) == inventory
    with pytest.raises(ValidationError):
        DatasetInventory(
            release_id="2026-10-06", retrieved_at=NOW, dataset=summary, shards=(shard(), shard())
        )
    with pytest.raises(ValidationError):
        DatasetInventory(release_id="2026-10-06", retrieved_at=NOW, dataset=summary, shards=())


@pytest.mark.parametrize(
    "readme",
    [
        f"Download {SIGNED} today",
        quote(SIGNED, safe=""),
        quote(quote(SIGNED, safe=""), safe=""),
        "https://x.test/a?X-Amz-Credential=c&X-Amz-Signature=topsecret",
        "token: x-amz-security-token=topsecret",
    ],
)
def test_signing_parameters_never_reach_the_archive(readme: str) -> None:
    with pytest.raises(SignedMaterialError) as info:
        record(readme).to_json()
    assert str(info.value) == "$.metadata.README holds a URL signing parameter"


def test_a_signing_parameter_in_a_key_is_reported_without_the_key() -> None:
    with pytest.raises(SignedMaterialError) as info:
        record(**{"Signature=topsecret": "x"}).to_json()
    assert "topsecret" not in str(info.value)


def test_ordinary_links_with_query_strings_archive() -> None:
    document = record("See https://example.org/docs?page=2&lang=en and mailto:a@b.test")
    assert ReleaseRecord.from_json(document.to_json()) == document


def test_a_stored_document_with_signed_material_is_refused_without_echoing_it() -> None:
    data = json.dumps({"retrieved_at": NOW.isoformat(), "metadata": {"note": SIGNED}}).encode()
    with pytest.raises(SignedMaterialError) as info:
        ReleaseRecord.from_json(data)
    assert "topsecret" not in str(info.value)


def test_validation_errors_do_not_echo_the_stored_text() -> None:
    with pytest.raises(ValidationError) as info:
        ReleaseRecord.from_json(b'{"retrieved_at": "never-topsecret", "metadata": {}}')
    assert "topsecret" not in str(info.value)


def test_a_deeply_nested_document_is_a_value_error() -> None:
    with pytest.raises(ValueError, match="nests too deeply"):
        ReleaseRecord.from_json(b"[" * 5000 + b"]" * 5000)


def test_a_number_too_long_for_python_is_not_json() -> None:
    with pytest.raises(ValueError, match="^the document could not be parsed as JSON$"):
        ReleaseRecord.from_json(b'{"retrieved_at": ' + b"1" * 5000 + b"}")


def test_the_path_to_signed_material_never_quotes_an_odd_key() -> None:
    with pytest.raises(SignedMaterialError) as info:
        record(**{"note with hunter2\nforged": SIGNED}).to_json()
    assert str(info.value) == "$.metadata.? holds a URL signing parameter"
