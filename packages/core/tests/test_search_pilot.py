import pytest
from pydantic import ValidationError

from openknowledge_core.search.pilot import PilotInput, PilotVector, evaluate
from openknowledge_core.search.ranking import reciprocal_rank_fusion


def payload() -> dict[str, object]:
    a = [1.0] + [0.0] * 767
    b = [0.0, 1.0] + [0.0] * 766
    return {
        "sample": {
            "dataset": "embeddings-specter_v2",
            "release": "2026-10-06",
            "model": "fixture",
            "document_adapter": "fixture",
            "model_revision": "fixture",
            "provenance": "synthetic behavior fixture only",
            "dtype": "float32",
            "vectors": [
                {"corpusid": 1, "vector": a},
                {"corpusid": 2, "vector": a},
                {"corpusid": 3, "vector": b},
            ],
        },
        "k": 1,
        "queries": [
            {
                "corpusid": 1,
                "vector": a,
                "kind": "paper",
                "compatibility_evidence": "same archived fixture vector",
                "candidates": [2],
            }
        ],
    }


def test_exact_baseline_excludes_self_and_counts_missing_candidates() -> None:
    pilot = PilotInput.model_validate(payload())
    assert evaluate(pilot).mean_recall_at_k == 1
    empty = pilot.queries[0].model_copy(update={"candidates": ()})
    result = evaluate(pilot.model_copy(update={"queries": (empty,)}))
    assert result.mean_recall_at_k == 0
    assert result.raw_float32_bytes == 3 * 768 * 4
    assert result.binary_code_bytes_lower_bound == 3 * 96


@pytest.mark.parametrize("candidates", [[1], [2, 2], [99]])
def test_rejects_self_duplicates_and_unknown_candidates(candidates: list[int]) -> None:
    pilot = PilotInput.model_validate(payload())
    raw = pilot.model_dump()
    raw["queries"][0]["candidates"] = candidates
    with pytest.raises(ValidationError):
        PilotInput.model_validate(raw)


@pytest.mark.parametrize("value", [0.0, float("nan"), float("inf"), 1e308])
def test_rejects_invalid_vectors(value: float) -> None:
    pilot = PilotInput.model_validate(payload())
    raw = pilot.model_dump()
    raw["sample"]["vectors"][0]["vector"] = [value] * 768
    with pytest.raises(ValidationError):
        PilotInput.model_validate(raw)


def test_rejects_paper_query_from_different_space() -> None:
    pilot = PilotInput.model_validate(payload())
    raw = pilot.model_dump()
    raw["queries"][0]["vector"] = raw["sample"]["vectors"][2]["vector"]
    with pytest.raises(ValidationError, match="archived vector"):
        PilotInput.model_validate(raw)


def test_hybrid_keeps_text_only_papers_and_fuses_ranks() -> None:
    assert reciprocal_rank_fusion((99, 2), (2, 3)) == (2, 99, 3)
    pilot = PilotInput.model_validate(payload())
    raw = pilot.model_dump()
    raw["queries"][0]["bm25_candidates"] = [99, 2]
    result = evaluate(PilotInput.model_validate(raw))
    assert result.hybrid_rankings == ((2,),)
    assert result.mean_recall_at_k == 1


def test_small_vectors_do_not_underflow_cosine() -> None:
    raw = PilotInput.model_validate(payload()).model_dump()
    extreme = [1e-40] * 768
    raw["sample"]["vectors"][0]["vector"] = extreme
    raw["queries"][0]["vector"] = extreme
    assert 0 <= evaluate(PilotInput.model_validate(raw)).mean_recall_at_k <= 1


def test_rejects_vectors_zero_after_float32_rounding() -> None:
    with pytest.raises(ValidationError, match="nonzero"):
        PilotVector(corpusid=1, vector=(1e-50,) * 768)


def test_canonical_float32_rounding_precedes_paper_query_equality() -> None:
    raw = PilotInput.model_validate(payload()).model_dump()
    rounded = 0.10000000149011612
    raw["sample"]["vectors"][0]["vector"] = (0.1,) + (0.0,) * 767
    raw["queries"][0]["vector"] = (rounded,) + (0.0,) * 767
    pilot = PilotInput.model_validate(raw)
    assert pilot.sample.vectors[0].vector[0] == rounded
    assert pilot.queries[0].vector == pilot.sample.vectors[0].vector
    assert 0 <= evaluate(pilot).mean_recall_at_k <= 1
