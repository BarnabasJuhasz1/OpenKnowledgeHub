from enum import StrEnum

from pydantic import BaseModel, ConfigDict, NonNegativeFloat, NonNegativeInt

from openknowledge_contracts.datasets import ReleaseId

READ_TABLES: tuple[str, ...] = ("papers_slim", "citation_edges_out", "citation_edges_in")
SOURCE_OF: dict[str, str] = {
    "papers_slim": "papers",
    "citation_edges_out": "citations",
    "citation_edges_in": "citations",
}
"""All read tables must name their source release and live UUID in applied."""


GROUPS: dict[str, tuple[str, ...]] = {
    "papers": ("papers_slim",),
    "citations": ("citation_edges_out", "citation_edges_in"),
}


class ReadTablesPath(StrEnum):
    CURRENT = "current"
    INCREMENTAL = "incremental"
    IN_ONLY = "in-only"
    FULL = "full"


class GroupState(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    marker: ReleaseId | None
    source: ReleaseId
    source_took_marker: bool
    ids_missing_out: bool
    ids_missing_in: bool


class GroupPlan(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    path: ReadTablesPath
    start: ReleaseId | None
    end: ReleaseId
    in_only_first: bool = False


def plan_group(state: GroupState, *, full: bool) -> GroupPlan:
    if (
        full
        or state.marker is None
        or not state.source_took_marker
        or state.marker > state.source
        or state.ids_missing_out
    ):
        return GroupPlan(path=ReadTablesPath.FULL, start=None, end=state.source)
    if state.marker == state.source:
        path = ReadTablesPath.IN_ONLY if state.ids_missing_in else ReadTablesPath.CURRENT
        return GroupPlan(path=path, start=None, end=state.source)
    return GroupPlan(
        path=ReadTablesPath.INCREMENTAL,
        start=state.marker,
        end=state.source,
        in_only_first=state.ids_missing_in,
    )


class ReadTableResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    rows: NonNegativeInt
    seconds: NonNegativeFloat


class BuildReadTablesResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source_releases: dict[str, ReleaseId]
    tables: dict[str, ReadTableResult]
    paths: dict[str, ReadTablesPath] = {}
    touched: dict[str, NonNegativeInt] = {}
