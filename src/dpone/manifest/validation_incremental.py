"""Universal manifest validation for incremental extraction contracts."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from copy import copy
from pathlib import Path

from dpone.config.postgres_xmin_execution import require_postgres_xmin_execution_route
from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.contracts.postgres_incremental_cursor import (
    PostgresIncrementalStrategy,
    normalize_postgres_incremental_strategy,
)
from dpone.contracts.target_max_incremental_cursor import (
    TargetMaxCursorPolicy,
    target_max_cursor_policy,
    target_max_mssql_cursor_is_unsafe,
)
from dpone.manifest.models import ProcessSpec
from dpone.manifest.validation_models import Severity, ValidationIssue
from dpone.manifest.validation_reconciliation import validate_key_snapshot_reconciliation


def validate_universal_process(
    spec: ProcessSpec,
    *,
    manifest_path: Path,
) -> Iterable[ValidationIssue]:
    """Validate rules that must run even without an optional profile."""

    issues: list[ValidationIssue] = []
    selector = spec.selector or spec.name
    load_cfg = getattr(spec.config, "load_config", None)
    if not load_cfg:
        return issues

    options = getattr(load_cfg, "options", {}) or {}
    issues.extend(
        validate_key_snapshot_reconciliation(
            spec,
            manifest_path=manifest_path,
            selector=selector,
            load_cfg=load_cfg,
            options=options,
        )
    )
    strategy_raw = options.get("incremental_strategy")
    incremental_column = options.get("incremental_column")
    source_type = extract_endpoint_type(spec, "source")
    sink_type = extract_endpoint_type(spec, "sink")
    load_strategy = _extract_load_strategy(spec, load_cfg)
    if "xmin_execution" in options:
        runtime_view = copy(load_cfg)
        runtime_view.options = {
            **options,
            "source_type": source_type,
            "sink_type": sink_type,
        }
        try:
            require_postgres_xmin_execution_route(runtime_view)
        except ValueError as exc:
            issues.append(
                ValidationIssue(
                    severity=Severity.ERROR,
                    code=_xmin_execution_issue_code(str(exc)),
                    message=(
                        f"Invalid PostgreSQL XMin initial/incremental handoff contract: {exc}. "
                        "Use a chunked initial backfill and a separate key-snapshot incremental manifest "
                        "with the same source.options.xmin_execution.handoff_id."
                    ),
                    manifest_path=manifest_path,
                    selector=selector,
                )
            )
    route_policy = target_max_cursor_policy(source_type)
    canonical_source_type = route_policy.source_type if route_policy is not None else source_type
    if (
        route_policy is not None
        and canonical_source_type in {"clickhouse", "mysql", "mssql"}
        and target_max_mssql_cursor_is_unsafe(
            source_type=source_type,
            sink_type=sink_type,
            load_strategy=load_strategy,
        )
    ):
        issues.append(_target_max_cursor_issue(route_policy, manifest_path=manifest_path, selector=selector))

    strategy = normalize_postgres_incremental_strategy(strategy_raw)
    if canonical_source_type != "postgres":
        if strategy is PostgresIncrementalStrategy.XMIN:
            issues.append(
                ValidationIssue(
                    severity=Severity.ERROR,
                    code="XMIN_REQUIRES_POSTGRES_SOURCE",
                    message=(
                        "source.options.incremental_strategy=xmin is supported only for source.type=postgres; "
                        f"got source.type={source_type or 'unknown'!r}."
                    ),
                    manifest_path=manifest_path,
                    selector=selector,
                )
            )
        return issues

    if strategy_raw is None and not incremental_column:
        return issues

    if strategy_raw is not None and strategy is None:
        issues.append(
            ValidationIssue(
                severity=Severity.ERROR,
                code="INCREMENTAL_STRATEGY_UNKNOWN",
                message=(
                    "source.options.incremental_strategy must be one of: xmin, postgres_xmin, column, column_cursor."
                ),
                manifest_path=manifest_path,
                selector=selector,
            )
        )
        return issues

    effective_strategy = strategy or PostgresIncrementalStrategy.COLUMN
    if effective_strategy is PostgresIncrementalStrategy.XMIN and incremental_column:
        issues.append(
            ValidationIssue(
                severity=Severity.ERROR,
                code="XMIN_CONFLICTS_WITH_INCREMENTAL_COLUMN",
                message=(
                    "source.options.incremental_strategy=xmin must not be combined with "
                    "source.options.incremental_column. Use incremental_strategy=column for column cursor extraction."
                ),
                manifest_path=manifest_path,
                selector=selector,
            )
        )
    if effective_strategy is PostgresIncrementalStrategy.COLUMN and not incremental_column:
        issues.append(
            ValidationIssue(
                severity=Severity.ERROR,
                code="COLUMN_CURSOR_REQUIRES_INCREMENTAL_COLUMN",
                message="source.options.incremental_strategy=column requires source.options.incremental_column.",
                manifest_path=manifest_path,
                selector=selector,
            )
        )
    if (
        effective_strategy is PostgresIncrementalStrategy.COLUMN
        and route_policy is not None
        and target_max_mssql_cursor_is_unsafe(
            source_type=source_type,
            sink_type=sink_type,
            load_strategy=load_strategy,
        )
    ):
        issues.append(_target_max_cursor_issue(route_policy, manifest_path=manifest_path, selector=selector))
    return issues


def _target_max_cursor_issue(
    policy: TargetMaxCursorPolicy,
    *,
    manifest_path: Path,
    selector: str,
) -> ValidationIssue:
    return ValidationIssue(
        severity=Severity.ERROR,
        code=policy.manifest_error_code,
        message=policy.guidance,
        manifest_path=manifest_path,
        selector=selector,
    )


def _xmin_execution_issue_code(error: str) -> str:
    suffix = error.rsplit(".", 1)[-1]
    return {
        "initial_contract_invalid": "POSTGRES_XMIN_INITIAL_CONTRACT_INVALID",
        "incremental_contract_invalid": "POSTGRES_XMIN_INCREMENTAL_CONTRACT_INVALID",
        "route_unsupported": "POSTGRES_XMIN_HANDOFF_ROUTE_UNSUPPORTED",
    }.get(suffix, "POSTGRES_XMIN_EXECUTION_INVALID")


def extract_endpoint_type(spec: ProcessSpec, endpoint: str) -> str | None:
    """Read one canonical lower-case endpoint type from raw manifest config."""

    raw = spec.raw_config or {}
    config = raw.get(endpoint) if isinstance(raw, Mapping) else None
    if isinstance(config, Mapping):
        endpoint_type = config.get("type")
        if isinstance(endpoint_type, str) and endpoint_type.strip():
            return canonical_endpoint_type(endpoint_type)
    return None


def extract_source_type(spec: ProcessSpec) -> str | None:
    """Compatibility helper for callers that need only the source dialect."""

    return extract_endpoint_type(spec, "source")


def _extract_load_strategy(spec: ProcessSpec, load_cfg: object) -> object | None:
    configured = getattr(load_cfg, "load_strategy", None)
    if configured is not None:
        return configured
    raw = spec.raw_config if isinstance(spec.raw_config, Mapping) else {}
    sink = raw.get("sink") if isinstance(raw, Mapping) else None
    strategy = sink.get("strategy") if isinstance(sink, Mapping) else None
    return strategy.get("mode") if isinstance(strategy, Mapping) else None


__all__ = ["extract_endpoint_type", "extract_source_type", "validate_universal_process"]
