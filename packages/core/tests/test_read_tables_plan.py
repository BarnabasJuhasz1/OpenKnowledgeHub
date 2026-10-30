import pytest

from openknowledge_core.ingestion.read_tables import (
    GroupPlan,
    GroupState,
    ReadTablesPath,
    plan_group,
)


@pytest.mark.parametrize(
    ("changes", "full", "expected"),
    [
        ({}, True, GroupPlan(path=ReadTablesPath.FULL, start=None, end="2026-10-13")),
        (
            {"marker": None},
            False,
            GroupPlan(path=ReadTablesPath.FULL, start=None, end="2026-10-13"),
        ),
        (
            {"source_took_marker": False},
            False,
            GroupPlan(path=ReadTablesPath.FULL, start=None, end="2026-10-13"),
        ),
        (
            {"marker": "2026-10-20"},
            False,
            GroupPlan(path=ReadTablesPath.FULL, start=None, end="2026-10-13"),
        ),
        (
            {"ids_missing_out": True},
            False,
            GroupPlan(path=ReadTablesPath.FULL, start=None, end="2026-10-13"),
        ),
        (
            {"marker": "2026-10-13", "ids_missing_in": True},
            True,
            GroupPlan(path=ReadTablesPath.FULL, start=None, end="2026-10-13"),
        ),
        (
            {"marker": "2026-10-13", "ids_missing_in": True, "source_took_marker": False},
            False,
            GroupPlan(path=ReadTablesPath.FULL, start=None, end="2026-10-13"),
        ),
        (
            {"marker": "2026-10-13", "ids_missing_in": True, "ids_missing_out": True},
            False,
            GroupPlan(path=ReadTablesPath.FULL, start=None, end="2026-10-13"),
        ),
        (
            {"marker": "2026-10-13"},
            False,
            GroupPlan(path=ReadTablesPath.CURRENT, start=None, end="2026-10-13"),
        ),
        (
            {"marker": "2026-10-13", "ids_missing_in": True},
            False,
            GroupPlan(path=ReadTablesPath.IN_ONLY, start=None, end="2026-10-13"),
        ),
        (
            {},
            False,
            GroupPlan(path=ReadTablesPath.INCREMENTAL, start="2026-10-06", end="2026-10-13"),
        ),
        (
            {"ids_missing_in": True},
            False,
            GroupPlan(
                path=ReadTablesPath.INCREMENTAL,
                start="2026-10-06",
                end="2026-10-13",
                in_only_first=True,
            ),
        ),
    ],
)
def test_plan_group(changes: dict[str, object], full: bool, expected: GroupPlan) -> None:
    state = GroupState.model_validate(
        {
            "marker": "2026-10-06",
            "source": "2026-10-13",
            "source_took_marker": True,
            "ids_missing_out": False,
            "ids_missing_in": False,
            **changes,
        }
    )
    assert plan_group(state, full=full) == expected
