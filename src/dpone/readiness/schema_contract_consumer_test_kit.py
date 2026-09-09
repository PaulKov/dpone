"""Consumer contract test kit and certification contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

CONSUMER_TEST_KIT_SCHEMA = "dpone.schema_contract_consumer_test_kit.v1"
CONSUMER_CERTIFICATION_SCHEMA = "dpone.schema_contract_consumer_certification.v1"


@dataclass(frozen=True, slots=True)
class SchemaConsumerTestKitBuilder:
    """Builds executable consumer-owner test evidence from a compatibility matrix."""

    def build(
        self,
        *,
        matrix: Mapping[str, Any],
        compatibility_view_plan: Mapping[str, Any] | None = None,
        contract_version: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        blockers = list(_matrix_blockers(matrix))
        blockers.extend(_contract_blockers(matrix, contract_version))
        warnings = list(_view_warnings(compatibility_view_plan))
        cases = tuple(
            _test_case(
                consumer,
                matrix=matrix,
                compatibility_view_plan=compatibility_view_plan,
            )
            for consumer in _consumers(matrix)
        )
        payload: dict[str, Any] = {
            "schema_version": CONSUMER_TEST_KIT_SCHEMA,
            "status": "blocked" if blockers else "ready",
            "contract_id": matrix.get("contract_id"),
            "base_version": matrix.get("base_version"),
            "head_version": matrix.get("head_version"),
            "head_contract_version_id": matrix.get("head_contract_version_id"),
            "consumer_matrix_id": matrix.get("consumer_matrix_id"),
            "compatibility_view_plan_id": _optional(compatibility_view_plan, "compatibility_view_plan_id"),
            "required_bump": matrix.get("required_bump"),
            "test_cases": [dict(item) for item in cases],
            "summary": {
                "test_cases_count": len(cases),
                "required_cases_count": sum(1 for item in cases if item["required"]),
                "covered_by_compatibility_view": sum(1 for item in cases if item.get("compatibility_view")),
            },
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _kit_recommendations(blockers, warnings),
        }
        payload["test_kit_id"] = stable_fingerprint(payload)
        return payload


@dataclass(frozen=True, slots=True)
class SchemaConsumerCertificationEvaluator:
    """Converts consumer test execution results into release evidence."""

    def evaluate(self, *, test_kit: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
        blockers = list(_kit_blockers(test_kit))
        warnings = [str(item) for item in test_kit.get("warnings", []) if str(item)]
        required = tuple(item for item in _test_cases(test_kit) if item.get("required"))
        passed_ids = _case_ids(result.get("passed_cases"))
        failed_ids = _case_ids(result.get("failed_cases"))
        if str(result.get("status") or "").lower() == "passed" and not passed_ids and not failed_ids:
            passed_ids = {str(item["test_case_id"]) for item in required}
        blockers.extend(_result_blockers(required, passed_ids, failed_ids, result))
        failed_required = tuple(item for item in required if str(item["test_case_id"]) in failed_ids)
        passed_required = tuple(item for item in required if str(item["test_case_id"]) in passed_ids)
        status = "blocked" if blockers else "certified" if required else "warning"
        if not required:
            warnings.append("schema_contract_consumer_test.no_required_cases")
        payload: dict[str, Any] = {
            "schema_version": CONSUMER_CERTIFICATION_SCHEMA,
            "status": status,
            "contract_id": test_kit.get("contract_id"),
            "base_version": test_kit.get("base_version"),
            "head_version": test_kit.get("head_version"),
            "consumer_matrix_id": test_kit.get("consumer_matrix_id"),
            "test_kit_id": test_kit.get("test_kit_id"),
            "result_status": result.get("status"),
            "passed_cases": sorted(passed_ids),
            "failed_cases": sorted(failed_ids),
            "summary": {
                "required_cases": len(required),
                "passed_cases": len(passed_required),
                "failed_cases": len(failed_required),
            },
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _certification_recommendations(blockers, warnings),
        }
        payload["consumer_certification_id"] = stable_fingerprint(payload)
        return payload


@dataclass(frozen=True, slots=True)
class SchemaConsumerTestKitRenderer:
    """Renders consumer test kit artifacts for CI and owners."""

    def render(self, payload: Mapping[str, Any], output_format: str) -> str:
        if output_format == "pytest":
            return _render_pytest(payload)
        if output_format == "md":
            return _render_markdown(payload)
        if output_format == "table":
            return _render_table(payload)
        return _render_text(payload)


def _test_case(
    consumer: Mapping[str, Any],
    *,
    matrix: Mapping[str, Any],
    compatibility_view_plan: Mapping[str, Any] | None,
) -> dict[str, Any]:
    consumer_id = str(consumer.get("id") or "unknown")
    reads = sorted(str(item) for item in _mapping(consumer.get("reads")).get("columns", []) if str(item))
    required = bool(consumer.get("blockers")) or consumer.get("status") == "blocked"
    view = _matching_view(consumer, reads, compatibility_view_plan)
    assertions = [{"kind": "column_available", "column": column} for column in reads]
    constraint = str(consumer.get("version_constraint") or "")
    if constraint:
        assertions.append(
            {
                "kind": "version_constraint_allows_head",
                "constraint": constraint,
                "head_version": str(matrix.get("head_version") or ""),
            }
        )
    if view:
        assertions.append({"kind": "compatibility_view_available", "view": str(view.get("view"))})
    case: dict[str, Any] = {
        "test_case_id": stable_fingerprint(
            {
                "consumer_id": consumer_id,
                "consumer_matrix_id": matrix.get("consumer_matrix_id"),
                "reads": reads,
                "version_constraint": constraint,
            }
        ),
        "consumer_id": consumer_id,
        "consumer_type": consumer.get("type"),
        "owner": consumer.get("owner"),
        "source": consumer.get("source"),
        "confidence": consumer.get("confidence"),
        "version_constraint": consumer.get("version_constraint"),
        "required": required,
        "status": "requires_execution" if required else "advisory",
        "reads": {"columns": reads},
        "compatibility_view": view.get("view") if view else None,
        "assertions": assertions,
        "blockers": list(consumer.get("blockers", [])) if isinstance(consumer.get("blockers"), list) else [],
        "reviewer_action": _reviewer_action(required, view),
    }
    return case


def _matching_view(
    consumer: Mapping[str, Any],
    reads: Sequence[str],
    plan: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if not plan:
        return None
    constraint = str(consumer.get("version_constraint") or "")
    for view in plan.get("views", []):
        if not isinstance(view, Mapping) or view.get("status") == "blocked":
            continue
        if not _constraint_matches(constraint, str(view.get("version_constraint") or "")):
            continue
        projected = {str(item.get("column")) for item in view.get("projections", []) if isinstance(item, Mapping)}
        if set(reads).issubset(projected):
            return view
    return None


def _constraint_matches(consumer_constraint: str, view_constraint: str) -> bool:
    if not consumer_constraint or not view_constraint:
        return False
    if consumer_constraint == view_constraint:
        return True
    return bool(view_constraint.endswith(".x") and consumer_constraint.startswith(view_constraint[:-2]))


def _result_blockers(
    required: Sequence[Mapping[str, Any]],
    passed_ids: set[str],
    failed_ids: set[str],
    result: Mapping[str, Any],
) -> tuple[str, ...]:
    blockers: list[str] = []
    if str(result.get("status") or "").lower() == "failed" and not failed_ids:
        blockers.append("schema_contract_consumer_test.result_failed")
    for case in required:
        case_id = str(case.get("test_case_id"))
        consumer_id = str(case.get("consumer_id") or "unknown")
        if case_id in failed_ids:
            blockers.append(f"schema_contract_consumer_test.required_case_failed:{consumer_id}")
        elif passed_ids and case_id not in passed_ids:
            blockers.append(f"schema_contract_consumer_test.required_case_missing:{consumer_id}")
    return tuple(blockers)


def _render_pytest(payload: Mapping[str, Any]) -> str:
    lines = [
        '"""Generated by dpone schema contract consumers test-kit render."""',
        "",
        "import pytest",
        "",
    ]
    for case in _test_cases(payload):
        name = _safe_pytest_name(str(case.get("consumer_id") or "unknown"))
        marker = "" if case.get("required") else "@pytest.mark.xfail(reason='advisory consumer case')\n"
        lines.extend(
            [
                f"{marker}def test_consumer_contract_{name}():",
                f"    case = {case!r}",
                "    assert case['assertions'], 'consumer test case has no assertions'",
                "    # Replace this placeholder with the consumer owner's real query/API check.",
                "    assert case['status'] in {'requires_execution', 'advisory'}",
                "",
            ]
        )
    return "\n".join(lines)


def _render_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Schema Contract Consumer Test Kit",
        "",
        f"- status: {payload.get('status')}",
        f"- contract_id: {payload.get('contract_id')}",
        f"- head_version: {payload.get('head_version')}",
        "",
        "| Consumer | Required | Columns | Compatibility view | Owner |",
        "| --- | --- | --- | --- | --- |",
    ]
    for case in _test_cases(payload):
        columns = ", ".join(_mapping(case.get("reads")).get("columns", []))
        lines.append(
            f"| {case.get('consumer_id')} | {case.get('required')} | {columns} | "
            f"{case.get('compatibility_view') or ''} | {case.get('owner') or ''} |"
        )
    return "\n".join(lines) + "\n"


def _render_table(payload: Mapping[str, Any]) -> str:
    lines = ["consumer_id | required | columns | compatibility_view"]
    for case in _test_cases(payload):
        columns = ",".join(_mapping(case.get("reads")).get("columns", []))
        lines.append(
            f"{case.get('consumer_id')} | {case.get('required')} | {columns} | {case.get('compatibility_view') or ''}"
        )
    return "\n".join(lines) + "\n"


def _render_text(payload: Mapping[str, Any]) -> str:
    lines = [str(payload.get("schema_version", CONSUMER_TEST_KIT_SCHEMA))]
    for key in ("status", "contract_id", "head_version", "test_kit_id", "consumer_certification_id"):
        if payload.get(key):
            lines.append(f"- {key}: {payload.get(key)}")
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _matrix_blockers(matrix: Mapping[str, Any]) -> tuple[str, ...]:
    if matrix.get("schema_version") != "dpone.schema_contract_consumer_matrix.v1":
        return ("schema_contract_consumer_test.matrix_invalid_schema",)
    return ()


def _contract_blockers(
    matrix: Mapping[str, Any],
    contract_version: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    if not contract_version:
        return ()
    blockers: list[str] = []
    if matrix.get("contract_id") != contract_version.get("contract_id"):
        blockers.append("schema_contract_consumer_test.contract_id_mismatch")
    if matrix.get("head_version") != contract_version.get("version"):
        blockers.append("schema_contract_consumer_test.contract_version_mismatch")
    if (
        matrix.get("head_contract_version_id")
        and contract_version.get("contract_version_id")
        and matrix.get("head_contract_version_id") != contract_version.get("contract_version_id")
    ):
        blockers.append("schema_contract_consumer_test.contract_version_id_mismatch")
    return tuple(blockers)


def _view_warnings(plan: Mapping[str, Any] | None) -> tuple[str, ...]:
    if not plan:
        return ()
    return tuple(str(item) for item in plan.get("warnings", []) if str(item))


def _kit_blockers(test_kit: Mapping[str, Any]) -> tuple[str, ...]:
    blockers = [str(item) for item in test_kit.get("blockers", []) if str(item)]
    if test_kit.get("schema_version") != CONSUMER_TEST_KIT_SCHEMA:
        blockers.append("schema_contract_consumer_test.kit_invalid_schema")
    return tuple(blockers)


def _case_ids(raw: object) -> set[str]:
    return {str(item) for item in raw if str(item)} if isinstance(raw, list) else set()


def _consumers(matrix: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    return tuple(
        sorted(
            (item for item in matrix.get("consumers", []) if isinstance(item, Mapping)),
            key=lambda item: str(item.get("id")),
        )
    )


def _test_cases(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    return tuple(item for item in payload.get("test_cases", []) if isinstance(item, Mapping))


def _reviewer_action(required: bool, view: Mapping[str, Any] | None) -> str:
    if required and view:
        return "Run the consumer against the compatibility view and attach the result artifact."
    if required:
        return "Run the consumer against the head contract or add compatibility view coverage."
    return "Optional smoke test for non-blocking consumer evidence."


def _kit_recommendations(blockers: Sequence[str], warnings: Sequence[str]) -> list[str]:
    if blockers:
        return ["Fix the consumer matrix before generating owner test artifacts."]
    if warnings:
        return ["Review test kit warnings before using certification in a protected bundle gate."]
    return ["Render pytest or Markdown and route required cases to the listed consumer owners."]


def _certification_recommendations(blockers: Sequence[str], warnings: Sequence[str]) -> list[str]:
    if blockers:
        return ["Run or fix required consumer contract tests before promotion."]
    if warnings:
        return ["Review advisory consumer certification warnings before release closeout."]
    return ["Attach consumer certification to the schema migration bundle and registry record."]


def _safe_pytest_name(raw: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in raw).strip("_") or "unknown"


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _optional(payload: Mapping[str, Any] | None, key: str) -> Any:
    return payload.get(key) if payload else None


__all__ = [
    "CONSUMER_CERTIFICATION_SCHEMA",
    "CONSUMER_TEST_KIT_SCHEMA",
    "SchemaConsumerCertificationEvaluator",
    "SchemaConsumerTestKitBuilder",
    "SchemaConsumerTestKitRenderer",
]
