"""Bounded validation shared by staging writers and snapshot readers.

This resolves only the supplied rows. Publication completeness and durable replay
checks across batches remain the responsibility of a future adapter.
"""

from openknowledge_contracts.embeddings import (
    EmbeddingLookupResult,
    EmbeddingMutation,
    EmbeddingSnapshot,
    LookupEmbeddings,
)


def validate_batch(
    source: EmbeddingSnapshot, rows: tuple[EmbeddingMutation, ...]
) -> tuple[EmbeddingMutation, ...]:
    """Validate one bounded batch, retaining identical replays in source order."""
    if len(rows) > 3000:
        raise ValueError("embedding batches allow at most 3000 mutations")
    seen: dict[tuple[int, int], EmbeddingMutation] = {}
    for row in rows:
        if row.source != source:
            raise ValueError("embedding snapshot identity or metadata mismatch")
        key = (row.corpusid, row.sequence)
        if key in seen and seen[key] != row:
            raise ValueError("conflicting embedding replay for corpusid and sequence")
        seen[key] = row
    return rows


def resolve_lookup(
    command: LookupEmbeddings, rows: tuple[EmbeddingMutation, ...]
) -> EmbeddingLookupResult:
    """Resolve a bounded candidate set after the caller checks publication metadata.

    Candidates must include the highest sequence for each requested ID, including
    tombstones. Arrival order and background merges never decide the winner.
    """
    validate_batch(command.source, rows)
    requested = dict.fromkeys(command.corpusids)
    latest: dict[int, EmbeddingMutation] = {}
    for row in rows:
        if row.corpusid not in requested:
            raise ValueError("lookup candidates contain an unrequested corpusid")
        previous = latest.get(row.corpusid)
        if previous is None or row.sequence > previous.sequence:
            latest[row.corpusid] = row
    return EmbeddingLookupResult(
        source=command.source,
        found=tuple(
            latest[key] for key in requested if key in latest and not latest[key].is_deleted
        ),
        missing=tuple(key for key in requested if key not in latest or latest[key].is_deleted),
    )
