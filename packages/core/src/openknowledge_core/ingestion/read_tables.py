from pydantic import BaseModel, ConfigDict, NonNegativeFloat, NonNegativeInt

from openknowledge_contracts.datasets import ReleaseId

READ_TABLES = ("citation_edges_out", "citation_edges_in")
"""A rebuild is complete only when the latest applied row of both names the citations release."""


class ReadTableResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    rows: NonNegativeInt
    seconds: NonNegativeFloat


class BuildReadTablesResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source_release: ReleaseId
    """The completed citations release the edges were built from, also kept in ``applied``."""
    citation_edges_out: ReadTableResult
    citation_edges_in: ReadTableResult
