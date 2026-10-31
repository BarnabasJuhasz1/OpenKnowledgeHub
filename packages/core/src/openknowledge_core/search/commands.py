"""Inputs and results of the search use cases."""

from __future__ import annotations

from typing import Self

from pydantic import BaseModel, ConfigDict, NonNegativeInt, PositiveInt, model_validator

from openknowledge_contracts.datasets import ReleaseId
from openknowledge_core.search.models import CorpusSlice, IndexName


class IndexPapers(BaseModel):
    """Write latest papers into ``index``, over a corpus ID slice bounded at ``release``.

    With ``from_release``, only the papers with a row after it are written, as a diff pass over
    an index that holds ``from_release`` and so must exist. Without it, every live paper of the
    slice is written, as a base pass, which creates the index when it is missing.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    index: IndexName
    release: ReleaseId
    from_release: ReleaseId | None = None
    part: PositiveInt
    parts: PositiveInt
    bounds: tuple[int, int] | None = None

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        if self.from_release is not None and self.from_release >= self.release:
            raise ValueError("a diff must start from a release before the one it brings")
        if self.part > self.parts:
            raise ValueError("the part must not exceed the number of parts")
        return self

    @property
    def corpus_slice(self) -> CorpusSlice:
        return CorpusSlice(part=self.part, parts=self.parts)


class IndexPapersResult(BaseModel):
    """Operations sent by one completed part."""

    model_config = ConfigDict(frozen=True)

    index: IndexName
    release: ReleaseId
    from_release: ReleaseId | None
    part: PositiveInt
    parts: PositiveInt
    created: bool
    """Whether this pass created the index."""
    written: NonNegativeInt
    deleted: NonNegativeInt
    requests: NonNegativeInt
    """Bulk batches sent, which the client and the bulk helper may each send more than once."""


class SettleIndex(BaseModel):
    """Reset the refresh interval of ``index`` to the default and refresh it after a base build."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    index: IndexName


class SettleIndexResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    index: IndexName
    refresh_interval: str
