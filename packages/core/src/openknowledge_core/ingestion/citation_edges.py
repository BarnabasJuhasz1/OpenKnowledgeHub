from pydantic import BaseModel, ConfigDict, NonNegativeFloat, NonNegativeInt


class CitationEdgeTableResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    rows: NonNegativeInt
    seconds: NonNegativeFloat


class BuildCitationEdgesResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    citation_edges_out: CitationEdgeTableResult
    citation_edges_in: CitationEdgeTableResult
