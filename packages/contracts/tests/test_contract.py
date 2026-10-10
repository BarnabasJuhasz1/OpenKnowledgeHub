import pyarrow as pa
import pytest

from openknowledge_contracts.tables import PROVENANCE, DatasetContract
from openknowledge_contracts.tables.fields import ArrowField, optional, required


def contract(*fields: ArrowField, key: tuple[str, ...] = ("id",)) -> DatasetContract:
    return DatasetContract(name="example", table="example", key=key, source=pa.schema(fields))


def test_a_load_writes_the_source_then_provenance() -> None:
    schema = contract(required("id", pa.uint64()), optional("text", pa.string())).schema
    assert schema.names == ["id", "text", *PROVENANCE.names]


def test_refuses_a_contract_without_key() -> None:
    with pytest.raises(ValueError, match="needs a key"):
        contract(required("id", pa.uint64()), key=())


def test_refuses_a_key_missing_from_the_source() -> None:
    with pytest.raises(ValueError, match="'other'"):
        contract(required("id", pa.uint64()), key=("other",))


def test_refuses_a_nullable_key() -> None:
    with pytest.raises(ValueError, match="'id'"):
        contract(optional("id", pa.uint64()))


def test_refuses_repeated_field_names() -> None:
    with pytest.raises(ValueError, match="repeat"):
        contract(required("id", pa.uint64()), required("id", pa.uint64()))


@pytest.mark.parametrize("name", ["source_release", "version"])
def test_refuses_source_fields_named_like_table_columns(name: str) -> None:
    with pytest.raises(ValueError, match=name):
        contract(required("id", pa.uint64()), required(name, pa.string()))
