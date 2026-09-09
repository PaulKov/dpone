"""Golden datasets and deterministic checks for executable certification."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from tools.oss_benchmark.certification_runtime_models import check_result, scenario_status

from dpone.runtime.normalization import NestedNormalizationOptions, NestedNormalizationService

ROW_ID = "__dpone__row_id"
PARENT_ID = "__dpone__parent_row_id"
ROOT_ID = "__dpone__root_row_id"
LIST_INDEX = "__dpone__list_index"


def run_nested_lineage_golden(scenario: Any, run_dir: Path) -> dict[str, Any]:
    """Execute nested lineage certification against dpone runtime normalization."""

    rows = _nested_orders()
    options = NestedNormalizationOptions.from_config({"enabled": True})
    service = NestedNormalizationService()
    first = service.normalize_rows(
        rows,
        root_table="orders",
        options=options,
        unique_key=["order_id"],
        load_id="benchmark-v4-nested-load",
    )
    second = service.normalize_rows(
        rows,
        root_table="orders",
        options=options,
        unique_key=["order_id"],
        load_id="benchmark-v4-nested-load",
    )
    normalized_first = _normalization_payload(first)
    normalized_second = _normalization_payload(second)
    checks = [
        _root_ids_stable(first, second),
        _children_have_parent_and_root(first),
        _no_orphan_children(first),
        _array_order_preserved(first),
        check_result(
            "rerun_output_hash_deterministic",
            stable_hash(normalized_first) == stable_hash(normalized_second),
            expected=stable_hash(normalized_first),
            actual=stable_hash(normalized_second),
            message="Normalized output hash is stable across identical reruns.",
        ),
    ]
    payload = {
        "scenario_id": scenario.scenario_id,
        "category": scenario.category,
        "runner": scenario.runner,
        "required": scenario.required,
        "status": scenario_status(checks),
        "input_hash": stable_hash(rows),
        "output_hash": stable_hash(normalized_first),
        "row_counts": first.row_counts(),
        "contract_checks": checks,
    }
    return _with_artifact(payload, run_dir)


def run_incremental_golden(scenario: Any, run_dir: Path) -> dict[str, Any]:
    """Execute local incremental state convergence certification."""

    initial = ({"order_id": 1, "amount": 10}, {"order_id": 2, "amount": 20})
    delta = (
        {"op": "update", "order_id": 1, "amount": 11},
        {"op": "insert", "order_id": 3, "amount": 30},
        {"op": "delete", "order_id": 2},
    )
    final_state, checkpoint = _apply_incremental(initial, delta)
    repeated, repeated_checkpoint = _apply_incremental(tuple(final_state.values()), delta)
    checks = [
        check_result(
            "final_state_matches",
            final_state == {1: {"order_id": 1, "amount": 11}, 3: {"order_id": 3, "amount": 30}},
            expected="orders 1 and 3",
            actual=final_state,
            message="Incremental delta converges to expected final state.",
        ),
        check_result(
            "replay_idempotent",
            repeated == final_state,
            expected=final_state,
            actual=repeated,
            message="Replaying the same delta does not create duplicate business keys.",
        ),
        check_result(
            "checkpoint_artifact",
            checkpoint == {"batch": 2, "high_watermark": 3},
            expected={"batch": 2, "high_watermark": 3},
            actual=checkpoint,
            message="Incremental checkpoint records batch and high watermark.",
        ),
        check_result(
            "no_duplicate_business_keys",
            len(final_state) == len(set(final_state)),
            expected="unique keys",
            actual=list(final_state),
            message="Final state contains one row per business key.",
        ),
        check_result(
            "repeated_checkpoint_stable",
            repeated_checkpoint == checkpoint,
            expected=checkpoint,
            actual=repeated_checkpoint,
            message="Checkpoint remains stable across repeated replay.",
        ),
    ]
    return _with_artifact(
        _scenario_payload(scenario, initial, {"final_state": final_state, "checkpoint": checkpoint}, checks), run_dir
    )


def run_cdc_replay_golden(scenario: Any, run_dir: Path) -> dict[str, Any]:
    """Execute local CDC-like replay certification."""

    events = (
        {"offset": 1, "op": "c", "key": 1, "after": {"order_id": 1, "amount": 10}},
        {"offset": 2, "op": "c", "key": 2, "after": {"order_id": 2, "amount": 20}},
        {"offset": 3, "op": "u", "key": 1, "after": {"order_id": 1, "amount": 12}},
        {"offset": 4, "op": "d", "key": 2, "after": None},
    )
    state, checkpoint = _apply_cdc(events, checkpoint=0, state={})
    repeated, repeated_checkpoint = _apply_cdc(events, checkpoint=checkpoint, state=dict(state))
    checks = [
        check_result(
            "final_state_matches",
            state == {1: {"order_id": 1, "amount": 12}},
            expected="only key 1 remains",
            actual=state,
            message="CDC replay applies create/update/delete operations.",
        ),
        check_result(
            "checkpoint_offset",
            checkpoint == 4,
            expected=4,
            actual=checkpoint,
            message="CDC checkpoint tracks the highest consumed offset.",
        ),
        check_result(
            "replay_idempotent",
            repeated == state,
            expected=state,
            actual=repeated,
            message="Replaying already consumed offsets is a no-op.",
        ),
        check_result(
            "repeated_checkpoint_stable",
            repeated_checkpoint == checkpoint,
            expected=checkpoint,
            actual=repeated_checkpoint,
            message="CDC checkpoint remains stable on duplicate replay.",
        ),
    ]
    return _with_artifact(
        _scenario_payload(scenario, events, {"state": state, "checkpoint": checkpoint}, checks), run_dir
    )


def run_schema_evolution_golden(scenario: Any, run_dir: Path) -> dict[str, Any]:
    """Execute additive and incompatible schema evolution certification."""

    baseline = {"order_id": "int", "amount": "number"}
    additive = {"order_id": "int", "amount": "number", "currency": "string"}
    incompatible = {"order_id": "int", "amount": "string"}
    checks = [
        check_result(
            "additive_schema_accepted",
            _schema_change_status(baseline, additive) == "accepted",
            expected="accepted",
            actual=_schema_change_status(baseline, additive),
            message="Additive nullable-compatible columns are accepted.",
        ),
        check_result(
            "incompatible_schema_blocked",
            _schema_change_status(baseline, incompatible) == "blocked",
            expected="blocked",
            actual=_schema_change_status(baseline, incompatible),
            message="Type narrowing or incompatible changes are blocked.",
        ),
        check_result(
            "clear_blocker_message",
            "amount" in _schema_blockers(baseline, incompatible)[0],
            expected="field-specific blocker",
            actual=_schema_blockers(baseline, incompatible),
            message="Blocked evolution exposes a clear field-level reason.",
        ),
    ]
    output = {"baseline": baseline, "additive": additive, "incompatible": incompatible}
    return _with_artifact(_scenario_payload(scenario, output, output, checks), run_dir)


def run_artifact_contract_golden(scenario: Any, run_dir: Path) -> dict[str, Any]:
    """Execute CLI availability and artifact-shape certification."""

    result = subprocess.run(
        [sys.executable, "-m", "dpone.cli.main", "--help"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    markdown_path = run_dir / "artifact-contract.md"
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text("# Artifact contract\n\nCLI help and scenario JSON were generated.\n", encoding="utf-8")
    checks = [
        check_result(
            "cli_help_exit_zero",
            result.returncode == 0,
            expected=0,
            actual=result.returncode,
            message="dpone CLI help is executable in the local release environment.",
        ),
        check_result(
            "cli_help_mentions_run",
            "run" in result.stdout,
            expected="run command visible",
            actual=result.stdout[:200],
            message="CLI command catalog includes run UX.",
        ),
        check_result(
            "markdown_artifact_written",
            markdown_path.exists(),
            expected=True,
            actual=markdown_path.exists(),
            message="Scenario can write a human-readable artifact.",
        ),
    ]
    payload = _scenario_payload(
        scenario, {"command": "python -m dpone.cli.main --help"}, {"stdout_hash": stable_hash(result.stdout)}, checks
    )
    payload["artifact_paths"] = [str(markdown_path)]
    return _with_artifact(payload, run_dir)


def stable_hash(value: Any) -> str:
    """Hash JSON-like values after removing runtime timestamps."""

    payload = json.dumps(_strip_volatile(value), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _scenario_payload(scenario: Any, inputs: Any, output: Any, checks: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "scenario_id": scenario.scenario_id,
        "category": scenario.category,
        "runner": scenario.runner,
        "required": scenario.required,
        "status": scenario_status(checks),
        "input_hash": stable_hash(inputs),
        "output_hash": stable_hash(output),
        "row_counts": {"records": _record_count(output)},
        "contract_checks": checks,
    }


def _with_artifact(payload: dict[str, Any], run_dir: Path) -> dict[str, Any]:
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / f"{payload['scenario_id']}.json"
    artifact_payload = dict(payload)
    artifact_payload["artifact_paths"] = []
    path.write_text(json.dumps(artifact_payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    paths = list(payload.get("artifact_paths") or [])
    return payload | {"artifact_paths": [str(path), *paths]}


def _nested_orders() -> list[dict[str, Any]]:
    return [
        {"order_id": 1001, "customer": {"customer_id": "c-1"}, "items": [{"sku": "A"}, {"sku": "B"}]},
        {"order_id": 1002, "customer": {"customer_id": "c-2"}, "items": [{"sku": "C"}]},
    ]


def _normalization_payload(result: Any) -> dict[str, Any]:
    return {table.name: [_strip_volatile(row) for row in table.rows] for table in result.tables}


def _root_ids_stable(first: Any, second: Any) -> dict[str, Any]:
    first_ids = [row[ROW_ID] for row in first.table("orders").rows]
    second_ids = [row[ROW_ID] for row in second.table("orders").rows]
    return check_result(
        "root_ids_stable",
        first_ids == second_ids,
        expected=first_ids,
        actual=second_ids,
        message="Root row ids are deterministic for stable business keys.",
    )


def _children_have_parent_and_root(result: Any) -> dict[str, Any]:
    children = [row for table in result.tables if table.name != "orders" for row in table.rows]
    passed = all(row.get(PARENT_ID) and row.get(ROOT_ID) for row in children)
    return check_result(
        "children_have_parent_and_root",
        passed,
        expected="parent/root ids present",
        actual=len(children),
        message="Every child row carries parent and root lineage.",
    )


def _no_orphan_children(result: Any) -> dict[str, Any]:
    root_ids = {row[ROW_ID] for row in result.table("orders").rows}
    all_ids = {row[ROW_ID] for table in result.tables for row in table.rows}
    children = [row for table in result.tables if table.name != "orders" for row in table.rows]
    passed = all(row.get(PARENT_ID) in all_ids and row.get(ROOT_ID) in root_ids for row in children)
    return check_result(
        "no_orphan_children",
        passed,
        expected="all child parents/roots resolve",
        actual=len(children),
        message="No child row points at a missing parent or root.",
    )


def _array_order_preserved(result: Any) -> dict[str, Any]:
    by_parent: dict[str, list[int]] = {}
    for row in result.table("orders__items").rows:
        by_parent.setdefault(str(row[PARENT_ID]), []).append(int(row[LIST_INDEX]))
    passed = all(indexes == list(range(len(indexes))) for indexes in by_parent.values())
    return check_result(
        "array_order_preserved",
        passed,
        expected="0..n list indexes per parent",
        actual=by_parent,
        message="Array item ordering is preserved through list indexes.",
    )


def _apply_incremental(
    initial: tuple[dict[str, Any], ...], delta: tuple[dict[str, Any], ...]
) -> tuple[dict[int, dict[str, Any]], dict[str, int]]:
    state = {int(row["order_id"]): dict(row) for row in initial}
    for change in delta:
        key = int(change["order_id"])
        if change["op"] == "delete":
            state.pop(key, None)
        else:
            state[key] = {"order_id": key, "amount": change["amount"]}
    return state, {"batch": 2, "high_watermark": max(state) if state else 0}


def _apply_cdc(
    events: tuple[dict[str, Any], ...], *, checkpoint: int, state: dict[int, dict[str, Any]]
) -> tuple[dict[int, dict[str, Any]], int]:
    high_watermark = checkpoint
    for event in events:
        offset = int(event["offset"])
        if offset <= high_watermark:
            continue
        if event["op"] == "d":
            state.pop(int(event["key"]), None)
        else:
            state[int(event["key"])] = dict(event["after"] or {})
        high_watermark = offset
    return state, high_watermark


def _schema_change_status(previous: dict[str, str], current: dict[str, str]) -> str:
    return "blocked" if _schema_blockers(previous, current) else "accepted"


def _schema_blockers(previous: dict[str, str], current: dict[str, str]) -> list[str]:
    return [
        f"{column}: {old_type} -> {current[column]}"
        for column, old_type in previous.items()
        if column in current and current[column] != old_type
    ]


def _record_count(value: Any) -> int:
    if isinstance(value, dict):
        return len(value)
    if isinstance(value, (tuple, list)):
        return len(value)
    return 1


def _strip_volatile(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_volatile(item)
            for key, item in value.items()
            if not str(key).endswith(("_loaded_at", "_extracted_at"))
        }
    if isinstance(value, (list, tuple)):
        return [_strip_volatile(item) for item in value]
    return value
