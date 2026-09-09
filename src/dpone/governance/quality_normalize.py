"""Pure adapter from manifest quality.checks dialect to executable quality.gates."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

_SUPPORTED_CHECK_TYPES = frozenset({"min_rows", "source_target_count"})
_QUALITY_MODES = frozenset({"fail", "warn"})
_SEVERITIES = frozenset({"error", "warning"})
_SIDES = frozenset({"source", "target"})
_QUALITY_FIELDS = frozenset({"mode", "acceptance", "checks", "gates"})
_CHECK_FIELDS = {
    "min_rows": frozenset({"id", "type", "mode", "severity", "side", "threshold", "value"}),
    "source_target_count": frozenset({"id", "type", "mode", "severity", "tolerance_pct"}),
}


def normalize_quality_config(raw: object) -> dict[str, Any]:
    """Normalize authored quality config into a gates-bearing mapping.

    Canonical runtime authoring is ``quality.gates``. Airflow and older
    source-sink examples use ``quality.checks`` plus top-level ``mode``.
    This adapter maps the compatibility dialect onto executable gates and
    fail-closes when authored checks cannot be executed under fail mode.
    """
    if raw is None:
        return {"gates": []}
    if not isinstance(raw, Mapping):
        raise ValueError("quality config must be a mapping")
    if not raw:
        raise ValueError("authored quality config must not be empty")

    values = dict(raw)
    _reject_unknown_fields(values, allowed=_QUALITY_FIELDS, field="quality")
    mode = str(values["mode"]).strip().lower() if "mode" in values else "fail"
    if mode not in _QUALITY_MODES:
        raise ValueError(f"quality mode must be one of: {', '.join(sorted(_QUALITY_MODES))}")
    default_severity = "warning" if mode == "warn" else "error"
    gates = _mapping_items(values.get("gates"), field="gates")
    checks = _mapping_items(values.get("checks"), field="checks")

    if gates:
        if checks and mode == "fail":
            raise ValueError(
                "leftover quality.checks while quality.gates are present under mode=fail; "
                "remove checks or migrate them into gates"
            )
        normalized_gates = [dict(item) for item in gates]
        normalized_gates.extend(
            _map_check_to_gate(check, default_severity=default_severity, index=index)
            for index, check in enumerate(checks)
        )
        values["gates"] = normalized_gates
        return values

    if not checks:
        if set(values).issubset({"mode", "checks", "gates"}):
            raise ValueError("authored quality config must define at least one check or gate")
        values["gates"] = []
        return values

    mapped = [
        _map_check_to_gate(check, default_severity=default_severity, index=index) for index, check in enumerate(checks)
    ]
    if not mapped and mode == "fail":
        raise ValueError("quality.checks produced no executable gates under mode=fail")
    values["gates"] = mapped
    return values


def _map_check_to_gate(check: Mapping[str, Any], *, default_severity: str, index: int) -> dict[str, Any]:
    check_type = str(check.get("type") or "").strip().lower()
    if not check_type:
        raise ValueError("quality check type is required")
    severity = _check_severity(check, default_severity=default_severity)
    if check_type not in _SUPPORTED_CHECK_TYPES:
        if severity == "warning":
            return {
                "id": str(check.get("id") or f"{check_type}_{index + 1}"),
                "type": check_type,
                "severity": "warning",
                "_compatibility_unknown_check": True,
            }
        supported = ", ".join(sorted(_SUPPORTED_CHECK_TYPES))
        raise ValueError(
            f"unsupported quality check type {check_type!r}; "
            f"supported mapped checks: {supported}; "
            "prefer canonical quality.gates for other gate types"
        )
    if check_type == "min_rows":
        if "threshold" not in check and "value" not in check:
            raise ValueError("quality check min_rows requires threshold or value")
        _reject_unknown_fields(
            check,
            allowed=_CHECK_FIELDS[check_type],
            field=f"quality check {check_type}",
        )
        if "threshold" in check and "value" in check and check["threshold"] != check["value"]:
            raise ValueError("quality check min_rows threshold and value must match when both are present")
        if "threshold" in check:
            threshold = check["threshold"]
        elif "value" in check:
            threshold = check["value"]
        side = str(check["side"]).strip().lower() if "side" in check else "target"
        if side not in _SIDES:
            raise ValueError(f"quality check side must be one of: {', '.join(sorted(_SIDES))}")
        return {
            "id": str(check.get("id") or f"min_rows_{index + 1}"),
            "type": "min_rows",
            "side": side,
            "threshold": _non_negative_integer(threshold, field="quality check threshold"),
            "severity": severity,
        }
    _reject_unknown_fields(
        check,
        allowed=_CHECK_FIELDS[check_type],
        field=f"quality check {check_type}",
    )
    tolerance_pct = _non_negative_finite_number(
        check.get("tolerance_pct", 0),
        field="quality check tolerance_pct",
    )
    return {
        "id": str(check.get("id") or f"source_target_count_{index + 1}"),
        "type": "row_count_reconciliation",
        "severity": severity,
        "tolerance": {"mode": "pct", "value": tolerance_pct},
    }


def _check_severity(check: Mapping[str, Any], *, default_severity: str) -> str:
    if "severity" in check:
        severity = str(check["severity"]).strip().lower()
        if severity not in _SEVERITIES:
            raise ValueError(f"quality check severity must be one of: {', '.join(sorted(_SEVERITIES))}")
        return severity
    if "mode" not in check:
        return default_severity
    check_mode = str(check["mode"]).strip().lower()
    if check_mode not in _QUALITY_MODES:
        raise ValueError(f"quality check mode must be one of: {', '.join(sorted(_QUALITY_MODES))}")
    if check_mode == "warn":
        return "warning"
    return "error"


def _mapping_items(raw: object, *, field: str) -> list[Mapping[str, Any]]:
    if raw is None:
        return []
    if not isinstance(raw, list | tuple):
        raise ValueError(f"quality.{field} must be a list")
    items: list[Mapping[str, Any]] = []
    for index, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise ValueError(f"quality.{field}[{index}] must be a mapping")
        items.append(item)
    return items


def _non_negative_integer(raw: object, *, field: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return raw


def _non_negative_finite_number(raw: object, *, field: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, int | float):
        raise ValueError(f"{field} must be a non-negative finite number")
    value = float(raw)
    if value < 0 or not math.isfinite(value):
        raise ValueError(f"{field} must be a non-negative finite number")
    return value


def _reject_unknown_fields(
    raw: Mapping[str, Any],
    *,
    allowed: frozenset[str],
    field: str,
) -> None:
    unknown = sorted(str(key) for key in raw if key not in allowed)
    if unknown:
        raise ValueError(f"{field}.{unknown[0]} is an unknown field")
