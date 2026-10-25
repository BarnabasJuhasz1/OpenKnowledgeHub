from pydantic import BaseModel, ConfigDict, NonNegativeFloat, NonNegativeInt

from openknowledge_contracts.datasets import ReleaseId


class CitationEdgeTableResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    rows: NonNegativeInt
    seconds: NonNegativeFloat


class BuildCitationEdgesResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source_release: ReleaseId
    """The completed citations release the edges were built from, also kept in ``applied``."""
    citation_edges_out: CitationEdgeTableResult
    citation_edges_in: CitationEdgeTableResult
