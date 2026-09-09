from __future__ import annotations

from typing import Any


def _finding_order(item: dict[str, Any]) -> tuple[object, ...]:
    return (
        0 if item["status"] == "FAIL" else 1,
        item["code"],
        item["subject"],
        item["route_id"] or "",
        item["detail"],
    )


def _select_bounded_findings(candidates: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if len(candidates) <= limit:
        return sorted(candidates, key=_finding_order)
    retained = candidates[: limit - 1]
    smallest_fail = min(
        (item for item in candidates if item["status"] == "FAIL"),
        key=_finding_order,
        default=None,
    )
    if smallest_fail is not None and smallest_fail not in retained:
        retained[-1] = smallest_fail
    resource = {
        "code": "PRIVILEGE_RESOURCE_LIMIT",
        "status": "UNVERIFIED",
        "subject": "findings",
        "route_id": None,
        "detail": "observed findings exceed the closed limit",
        "recovery_command_id": "REDUCE_OR_PARTITION_WORKFLOWS",
    }
    return sorted([*retained, resource], key=_finding_order)


def _candidate(index: int, status: str = "UNVERIFIED") -> dict[str, Any]:
    return {
        "code": "PRIVILEGE_INVALID_WORKFLOW",
        "status": status,
        "subject": f"workflow-{index}",
        "route_id": None,
        "detail": f"candidate-{index}",
        "recovery_command_id": "REPAIR_WORKFLOW_SYNTAX",
    }


def test_pr3b_finding_overflow_selection_is_executable_and_status_preserving() -> None:
    ordinary = [_candidate(index) for index in range(5)]
    selected = _select_bounded_findings(ordinary, 4)
    assert len(selected) == 4
    assert {item["subject"] for item in selected} == {
        "workflow-0",
        "workflow-1",
        "workflow-2",
        "findings",
    }

    selected_with_fail = _select_bounded_findings([*ordinary, _candidate(5, "FAIL")], 4)
    assert len(selected_with_fail) == 4
    assert [item for item in selected_with_fail if item["status"] == "FAIL"] == [_candidate(5, "FAIL")]
    assert selected_with_fail[-1]["code"] == "PRIVILEGE_RESOURCE_LIMIT"

    early_and_later_fail = [_candidate(9, "FAIL"), *ordinary, _candidate(0, "FAIL")]
    selected_smallest_fail = _select_bounded_findings(early_and_later_fail, 4)
    assert [item for item in selected_smallest_fail if item["status"] == "FAIL"][0] == _candidate(0, "FAIL")
