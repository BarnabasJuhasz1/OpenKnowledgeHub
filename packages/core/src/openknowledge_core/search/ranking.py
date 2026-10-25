"""Pure rank fusion shared by offline evaluation and opt-in retrieval."""


def reciprocal_rank_fusion(
    bm25: tuple[int, ...], vectors: tuple[int, ...], *, rank_constant: int = 60
) -> tuple[int, ...]:
    """Equal-weight RRF, preserving BM25-only papers; this is not a relevance metric."""
    if rank_constant < 1:
        raise ValueError("rank_constant must be positive")
    scores: dict[int, float] = {}
    for ranking in (bm25, vectors):
        if len(ranking) != len(set(ranking)):
            raise ValueError("ranking ids repeat")
        for rank, id_ in enumerate(ranking, start=1):
            scores[id_] = scores.get(id_, 0.0) + 1 / (rank_constant + rank)
    return tuple(sorted(scores, key=lambda id_: (-scores[id_], id_)))
