from pydantic import BaseModel, ConfigDict, NonNegativeFloat, NonNegativeInt

from openknowledge_contracts.datasets import ReleaseId

READ_TABLES: tuple[str, ...] = ("papers_slim", "citation_edges_out", "citation_edges_in")
SOURCE_OF: dict[str, str] = {
    "papers_slim": "papers",
    "citation_edges_out": "citations",
    "citation_edges_in": "citations",
}
"""All read tables must name their source release and live UUID in applied."""


class ReadTableResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    rows: NonNegativeInt
    seconds: NonNegativeFloat


class BuildReadTablesResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source_releases: dict[str, ReleaseId]
    tables: dict[str, ReadTableResult]
