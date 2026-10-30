from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from openknowledge_core.search.progress import Indexed, SearchState, plan_index

SOURCES = {name: UUID(int=i) for i, name in enumerate(("papers", "abstracts", "tldrs"), 1)}


def state(release: str = "2026-10-20") -> SearchState:
    return SearchState(releases=dict.fromkeys(SOURCES, release), sources=SOURCES)


def completed() -> Indexed:
    base = plan_index(state(), [], base_parts=256, diff_parts=256)
    assert base.plan is not None
    return Indexed(plan=base.plan, kind="complete", recorded_at=datetime.now(UTC))


@pytest.mark.parametrize("reason", ["missing", "contract", "sources"])
def test_base_builds_a_new_physical_index(reason: str) -> None:
    previous = completed()
    current = state()
    if reason == "contract":
        previous = previous.model_copy(
            update={
                "plan": previous.plan.model_copy(
                    update={"contract_version": 1, "index": "papers-v1-old"}
                )
            }
        )
    if reason == "sources":
        current = current.model_copy(update={"sources": {**SOURCES, "papers": UUID(int=99)}})
    result = plan_index(
        current, [] if reason == "missing" else [previous], base_parts=256, diff_parts=256
    )
    assert result.reason == "base" and result.plan is not None
    assert result.plan.from_release is None and result.plan.parts == 256
    if reason != "missing":
        assert result.plan.index != previous.plan.index


def test_diff_covers_missed_releases_on_the_same_index() -> None:
    previous = completed()
    diff = plan_index(state("2026-11-10"), [previous], base_parts=256, diff_parts=256)
    assert diff.reason == "diff" and diff.plan is not None
    assert diff.plan.from_release == "2026-10-20" and diff.plan.index == previous.plan.index


def test_current_needs_no_document_pass() -> None:
    result = plan_index(state(), [completed()], base_parts=256, diff_parts=256)
    assert result.reason == "current"


def test_unaligned_sources_do_not_plan_writes() -> None:
    unaligned = state().model_copy(
        update={"releases": {**state().releases, "papers": "2026-10-27"}}
    )
    assert plan_index(unaligned, [completed()], base_parts=256, diff_parts=256).plan is None


def test_release_move_resumes_latest_matching_unfinished_base_then_diffs() -> None:
    old = completed().plan
    latest = old.model_copy(update={"release": "2026-10-27", "index": "papers-v2-latest"})
    now = datetime.now(UTC)
    receipts = [
        Indexed(plan=old, kind="started", recorded_at=now),
        Indexed(plan=latest, kind="started", recorded_at=now + timedelta(seconds=1)),
        Indexed(plan=latest, kind="part", part=1, recorded_at=now + timedelta(seconds=2)),
    ]
    applied = state("2026-11-10")
    resumed = plan_index(applied, receipts, base_parts=256, diff_parts=18)
    assert resumed.reason == "base" and resumed.plan == latest
    assert resumed.finished == {1}
    receipts.append(Indexed(plan=latest, kind="complete", recorded_at=now + timedelta(seconds=3)))
    diff = plan_index(applied, receipts, base_parts=256, diff_parts=18)
    assert diff.reason == "diff" and diff.plan is not None
    assert (diff.plan.index, diff.plan.from_release, diff.plan.release) == (
        latest.index,
        latest.release,
        "2026-11-10",
    )
