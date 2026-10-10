"""The S2 contracts against synthetic records shaped like the real ones.

``conform`` checks a fixture against its contract: an unknown key or a null in a required field
fails, which Arrow alone lets through. A record that conforms has to come back out of Arrow
unchanged, null and empty apart.
"""

import json
from collections.abc import Iterable
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest
from pydantic import TypeAdapter

from openknowledge_contracts.datasets import DatasetName
from openknowledge_contracts.tables import DatasetContract
from openknowledge_contracts.tables.fields import ArrowField, fields_of
from openknowledge_contracts.tables.s2 import CONTRACTS

FIXTURES = Path(__file__).parent / "fixtures" / "s2"

type Json = None | bool | int | float | str | list[Json] | dict[str, Json]


def conform(value: Json, field: ArrowField, path: str) -> object:
    if value is None:
        if not field.nullable:
            raise ValueError(f"{path} is null but required")
        return None
    type_ = field.type
    if isinstance(type_, pa.JsonType):
        return json.dumps(value)
    if isinstance(type_, pa.MapType) and isinstance(value, dict):
        map_type = cast("pa.MapType[pa.DataType, pa.DataType]", type_)
        return [(k, conform(v, map_type.item_field, f"{path}.{k}")) for k, v in value.items()]
    if isinstance(type_, pa.ListType) and isinstance(value, list):
        list_type = cast("pa.ListType[pa.DataType]", type_)
        return [conform(v, list_type.value_field, f"{path}[]") for v in value]
    if isinstance(type_, pa.StructType) and isinstance(value, dict):
        return conform_object(value, fields_of(type_), path)
    if isinstance(value, list | dict):
        raise ValueError(f"{path} holds a {type(value).__name__}, not {type_}")
    return value


def conform_object(
    value: dict[str, Json], fields: Iterable[ArrowField], path: str
) -> dict[str, object]:
    fields = list(fields)
    if unknown := sorted(set(value) - {f.name for f in fields}):
        raise ValueError(f"{path} has unknown keys {unknown}")
    return {f.name: conform(value.get(f.name), f, f"{path}.{f.name}") for f in fields}


def records(contract: DatasetContract) -> list[dict[str, Json]]:
    lines = (FIXTURES / f"{contract.name}.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


def test_the_registry_names_tables_and_keys() -> None:
    assert {name: (c.table, c.key) for name, c in CONTRACTS.items()} == {
        "papers": ("papers", ("corpusid",)),
        "paper-ids": ("paper_ids", ("sha",)),
        "abstracts": ("abstracts", ("corpusid",)),
        "tldrs": ("tldrs", ("corpusid",)),
        "authors": ("authors", ("authorid",)),
        "publication-venues": ("publication_venues", ("id",)),
        "citations": ("citations", ("citationid",)),
    }


def test_the_registry_is_keyed_by_valid_dataset_names() -> None:
    names = tuple(CONTRACTS)
    assert TypeAdapter(tuple[DatasetName, ...]).validate_python(names) == names


def test_every_dataset_has_fixtures() -> None:
    assert {path.stem for path in FIXTURES.glob("*.jsonl")} == set(CONTRACTS)


@pytest.mark.parametrize("contract", CONTRACTS.values(), ids=list(CONTRACTS))
def test_records_come_back_out_of_arrow_unchanged(contract: DatasetContract) -> None:
    rows = [conform_object(r, contract.source, contract.name) for r in records(contract)]
    assert pa.Table.from_pylist(rows, schema=contract.source).to_pylist() == rows


def test_the_fixture_check_refuses_unknown_keys_at_any_depth() -> None:
    tldr: dict[str, Json] = {"corpusid": 1, "model": "m", "text": None, "surprise": 1}
    with pytest.raises(ValueError, match="surprise"):
        conform_object(tldr, CONTRACTS["tldrs"].source, "tldrs")
    paper = records(CONTRACTS["papers"])[0]
    paper["journal"] = {"name": "J", "pages": None, "volume": None, "issue": "2"}
    with pytest.raises(ValueError, match="issue"):
        conform_object(paper, CONTRACTS["papers"].source, "papers")


def test_the_fixture_check_refuses_a_null_in_a_required_field() -> None:
    with pytest.raises(ValueError, match="tldrs.model"):
        conform_object({"corpusid": 1, "model": None}, CONTRACTS["tldrs"].source, "tldrs")


@pytest.mark.parametrize("citationid", [-1, 2**64])
def test_ids_outside_uint64_are_refused(citationid: int) -> None:
    citation = {**records(CONTRACTS["citations"])[0], "citationid": citationid}
    row = conform_object(citation, CONTRACTS["citations"].source, "citations")
    with pytest.raises((pa.ArrowInvalid, OverflowError)):
        pa.Table.from_pylist([row], schema=CONTRACTS["citations"].source)
