"""Resolve and validate canonical publish intent from already-resolved dbt meta."""

from __future__ import annotations

import re
from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from jsonschema import Draft202012Validator

from dpone.contracts.dbt_publish_models import DbtModelArtifact, DbtPublishIntent, DbtPublishIssue
from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts

_SECRET_KEY = re.compile(r"(?:password|secret|token|credential|connection_uri|vault_path|secret_name)", re.I)


class DbtPublishIntentResolver:
    """Maps ``node.config.meta.dpone.publish`` to the stable v1 intent contract."""

    def __init__(self, *, allow_legacy_physical_overrides: bool = False) -> None:
        self._allow_legacy_physical_overrides = allow_legacy_physical_overrides

    def resolve(self, model: DbtModelArtifact) -> tuple[DbtPublishIntent | None, tuple[DbtPublishIssue, ...]]:
        raw, shape_issues = _publish_mapping(model)
        if shape_issues or raw is None:
            return None, shape_issues
        validation_payload = deepcopy(raw)
        compatibility_issues = _legacy_physical_override_issues(
            validation_payload,
            model,
            allowed=self._allow_legacy_physical_overrides,
        )
        issues = [
            *_secret_issues(raw, model),
            *_schema_issues(validation_payload, model),
            *compatibility_issues,
        ]
        if any(item.severity == "error" for item in issues):
            return None, tuple(issues)
        enabled = raw["enabled"]
        if not enabled:
            return None, tuple(issues)
        profile = _text(raw.get("profile"))
        workflow = _text(raw.get("workflow"))
        strategy = _mapping(raw.get("strategy"))
        mode = _text(strategy.get("mode")) or "auto"
        target = _mapping(raw.get("target"))
        physical = _mapping(raw.get("physical_design"))
        execution = _mapping(raw.get("execution"))
        quality = _mapping(raw.get("quality"))
        lineage = _mapping(raw.get("lineage"))
        window_days = _positive_int(strategy.get("window_days"), model, issues, "strategy.window_days")
        max_parallelism = _positive_int(execution.get("max_parallelism"), model, issues, "execution.max_parallelism")
        lineage_enabled = _boolean(
            lineage.get("enabled", True),
            model,
            issues,
            "lineage.enabled",
        )
        quality_preset = _text(quality.get("preset")) or "standard"
        intent = DbtPublishIntent(
            enabled=True,
            profile=profile,
            workflow=workflow,
            target_schema=_optional_text(target.get("schema")),
            target_table=_optional_text(target.get("table")),
            strategy_mode=mode,
            unique_key=_string_tuple(strategy.get("unique_key")) or model.unique_key,
            partition_key=_optional_text(strategy.get("partition_key")),
            window_days=window_days,
            physical_profile=_optional_text(physical.get("profile")),
            engine=_optional_text(physical.get("engine")),
            order_by=_string_tuple(physical.get("order_by")),
            partition_by=_optional_text(physical.get("partition_by")),
            execution_profile=_optional_text(execution.get("profile")),
            max_parallelism=max_parallelism,
            quality_preset=quality_preset,
            lineage_enabled=lineage_enabled,
        )
        return (None if any(item.severity == "error" for item in issues) else intent), tuple(issues)


def _secret_issues(
    payload: Mapping[str, Any], model: DbtModelArtifact, prefix: str = "publish"
) -> tuple[DbtPublishIssue, ...]:
    issues = []
    for key, value in payload.items():
        path = f"{prefix}.{key}"
        if _SECRET_KEY.search(str(key)):
            issues.append(
                _issue(
                    model,
                    "DPONE_DBT_SECRET_FORBIDDEN",
                    f"Secret or runtime binding field is forbidden in dbt meta: {path}",
                )
            )
        if isinstance(value, Mapping):
            issues.extend(_secret_issues(value, model, path))
    return tuple(issues)


def _publish_mapping(
    model: DbtModelArtifact,
) -> tuple[dict[str, Any] | None, tuple[DbtPublishIssue, ...]]:
    dpone_value = model.meta.get("dpone")
    if dpone_value is None:
        return None, ()
    if not isinstance(dpone_value, Mapping):
        return None, (_invalid_scalar(model, "dpone", "a JSON object"),)
    publish_value = dpone_value.get("publish")
    if publish_value is None:
        return None, ()
    if not isinstance(publish_value, Mapping):
        return None, (_invalid_scalar(model, "publish", "a JSON object"),)
    return dict(publish_value), ()


def _schema_issues(
    payload: Mapping[str, Any],
    model: DbtModelArtifact,
) -> tuple[DbtPublishIssue, ...]:
    schema = dbt_schema_contracts()["dpone.dbt-publish-authoring.v1"]
    errors = sorted(
        Draft202012Validator(schema).iter_errors(payload),
        key=lambda error: tuple(str(item) for item in error.absolute_path),
    )
    return tuple(
        _invalid_scalar(
            model,
            ".".join(str(item) for item in error.absolute_path) or "publish",
            f"a valid value ({error.validator})",
        )
        for error in errors
    )


def _legacy_physical_override_issues(
    payload: dict[str, Any],
    model: DbtModelArtifact,
    *,
    allowed: bool,
) -> tuple[DbtPublishIssue, ...]:
    physical = payload.get("physical_design")
    if not isinstance(physical, dict):
        return ()
    aliases = tuple(name for name in ("engine", "partition_by") if name in physical)
    if not aliases:
        return ()
    for name in aliases:
        physical.pop(name)
    return tuple(
        DbtPublishIssue(
            code=(
                "DPONE_DBT_RAW_PHYSICAL_OVERRIDE_DEPRECATED" if allowed else "DPONE_DBT_RAW_PHYSICAL_OVERRIDE_FORBIDDEN"
            ),
            message=(
                f"physical_design.{name} is a deprecated preview-only input"
                if allowed
                else f"physical_design.{name} is forbidden in a certified release"
            ),
            path=f"{model.original_file_path}#config.meta.dpone.publish.physical_design.{name}",
            severity="warning" if allowed else "error",
            remediation="Use a platform-owned physical_design.profile before production publishing.",
        )
        for name in aliases
    )


def _positive_int(
    value: object,
    model: DbtModelArtifact,
    issues: list[DbtPublishIssue],
    field: str,
) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        issues.append(_invalid_scalar(model, field, "a positive JSON integer"))
        return None
    return value


def _boolean(
    value: object,
    model: DbtModelArtifact,
    issues: list[DbtPublishIssue],
    field: str,
) -> bool:
    if isinstance(value, bool):
        return value
    issues.append(_invalid_scalar(model, field, "a JSON boolean"))
    return False


def _invalid_scalar(model: DbtModelArtifact, field: str, expected: str) -> DbtPublishIssue:
    return _issue(model, "DPONE_DBT_INTENT_INVALID", f"{field} must be {expected}")


def _issue(model: DbtModelArtifact, code: str, message: str) -> DbtPublishIssue:
    return DbtPublishIssue(
        code=code,
        message=message,
        path=f"{model.original_file_path}#config.meta.dpone.publish",
        remediation="Fix the model meta and run `dpone dbt check` again.",
    )


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _optional_text(value: object) -> str | None:
    return _text(value) or None


def _string_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, list | tuple):
        return tuple(item.strip() for item in value if isinstance(item, str) and item.strip())
    return ()


__all__ = ["DbtPublishIntentResolver"]
