import pytest

from openknowledge_contracts.identifiers import PaperId


def test_holds_the_full_unsigned_range() -> None:
    assert PaperId(0).value == 0
    assert PaperId(2**64 - 1).value == 2**64 - 1


def test_compares_and_hashes_by_value_only_against_paper_ids() -> None:
    assert PaperId(7) == PaperId(7)
    assert len({PaperId(7), PaperId(7)}) == 1
    assert PaperId(7) != 7
    assert sorted([PaperId(9), PaperId(2)]) == [PaperId(2), PaperId(9)]


def test_prints_the_bare_number() -> None:
    assert str(PaperId(253315119)) == "253315119"


@pytest.mark.parametrize("value", [True, 1.0, "1", None])
def test_refuses_non_integers(value: object) -> None:
    with pytest.raises(TypeError):
        PaperId(value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [-1, 2**64])
def test_refuses_values_outside_uint64(value: int) -> None:
    with pytest.raises(ValueError):
        PaperId(value)


def test_is_immutable() -> None:
    paper = PaperId(1)
    with pytest.raises(AttributeError):
        paper.value = 2  # type: ignore[misc]
