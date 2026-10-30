"""Plan managed passes and retain their partition boundaries across retries."""

from datetime import datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, NonNegativeInt, PositiveInt

from openknowledge_contracts.datasets import ReleaseId
from openknowledge_contracts.search import PAPERS
from openknowledge_core.search.models import IndexName


class SearchState(BaseModel):
    model_config = ConfigDict(frozen=True)

    releases: dict[str, ReleaseId]
    sources: dict[str, UUID]


class IndexPass(BaseModel):
    model_config = ConfigDict(frozen=True)

    index: IndexName
    contract_version: PositiveInt = PAPERS.version
    release: ReleaseId
    from_release: ReleaseId | None = None
    sources: dict[str, UUID]
    parts: PositiveInt
    low: NonNegativeInt
    high: NonNegativeInt


class Indexed(BaseModel):
    model_config = ConfigDict(frozen=True)

    plan: IndexPass
    kind: Literal["started", "part", "complete"]
    part: NonNegativeInt = 0
    recorded_at: datetime


class SearchPlan(BaseModel):
    plan: IndexPass | None = None
    reason: str
    finished: frozenset[int] = frozenset()


def plan_index(
    state: SearchState,
    receipts: list[Indexed],
    *,
    base_parts: int,
    diff_parts: int,
) -> SearchPlan:
    if (
        set(state.releases) != {"papers", "abstracts", "tldrs"}
        or len(set(state.releases.values())) != 1
    ):
        return SearchPlan(reason="papers, abstracts and tldrs are not applied at one release")
    release = state.releases["papers"]
    current = [r for r in receipts if r.plan.contract_version == PAPERS.version]
    complete = [r.plan for r in current if r.kind == "complete"]
    live = complete[-1] if complete else None
    base = live is None or live.sources != state.sources
    if not base and live is not None and live.release >= release:
        return SearchPlan(plan=live, reason="current")
    if base:
        unfinished = [
            r
            for r in current
            if r.kind == "started"
            and r.plan.from_release is None
            and r.plan.sources == state.sources
            and r.plan.parts == base_parts
            and r.plan not in complete
        ]
        if unfinished:
            plan = max(unfinished, key=lambda r: r.recorded_at).plan
            return SearchPlan(
                plan=plan,
                reason="base",
                finished=frozenset(r.part for r in current if r.kind == "part" and r.plan == plan),
            )
    start = None if base or live is None else live.release
    parts = base_parts if base else diff_parts
    matching = [
        r
        for r in current
        if r.plan.release == release
        and r.plan.sources == state.sources
        and r.plan.parts == parts
        and r.plan.from_release == start
    ]
    if matching:
        return SearchPlan(
            plan=matching[0].plan,
            reason="base" if base else "diff",
            finished=frozenset(r.part for r in matching if r.kind == "part"),
        )
    fingerprint = sha256(str(sorted(state.sources.items())).encode()).hexdigest()[:12]
    index = (
        f"papers-v{PAPERS.version}-{release}-{fingerprint}" if base else live.index if live else ""
    )
    return SearchPlan(
        plan=IndexPass(
            index=index,
            release=release,
            from_release=start,
            sources=state.sources,
            parts=parts,
            low=0,
            high=0,
        ),
        reason="base" if base else "diff",
    )
