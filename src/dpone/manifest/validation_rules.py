from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.contracts.postgres_mssql_type_policy import declared_postgres_mssql_contract_blockers
from dpone.contracts.technical_columns import TechnicalColumnsMode, resolve_technical_columns
from dpone.manifest.models import ProcessSpec
from dpone.manifest.validation_description import _validate_description_source_path
from dpone.manifest.validation_incremental import (
    extract_source_type as _extract_source_type,  # noqa: F401 - compatibility re-export
)
from dpone.manifest.validation_incremental import (
    validate_universal_process as _validate_incremental_process,
)
from dpone.manifest.validation_models import Severity, ValidationIssue
from dpone.manifest.validation_mssql_transaction import (
    validate_generic_mssql_transaction_state,
)


def _validate_universal_process(
    spec: ProcessSpec,
    *,
    manifest_path: Path,
) -> Iterable[ValidationIssue]:
    """Compose route-independent authoring gates behind the compatibility API."""

    issues = list(_validate_incremental_process(spec, manifest_path=manifest_path))
    issues.extend(
        validate_generic_mssql_transaction_state(
            spec,
            manifest_path=manifest_path,
        )
    )
    return issues


def _validate_process(
    spec: ProcessSpec,
    *,
    manifest_path: Path,
    dataset_re: re.Pattern[str] | None,
    table_re: re.Pattern[str] | None,
    required_labels: Sequence[str],
    require_table_description: bool,
    technical_columns_policy: TechnicalColumnsMode,
    technical_columns_severity: Severity,
    missing_labels_severity: Severity,
    missing_description_severity: Severity,
    label_patterns: Mapping[str, re.Pattern[str]],
    forbidden_label_values: Mapping[str, set[str]],
    invalid_label_value_severity: Severity,
    description_required_rx: Sequence[re.Pattern[str]],
    description_forbidden_rx: Sequence[re.Pattern[str]],
    require_source_path_in_description: bool,
    invalid_description_severity: Severity,
) -> Iterable[ValidationIssue]:
    issues: list[ValidationIssue] = []
    selector = spec.selector or spec.name

    load_cfg = getattr(spec.config, "load_config", None)
    if not load_cfg:
        return issues

    dataset = getattr(load_cfg, "target_schema", None)
    table = getattr(load_cfg, "target_table", None)

    if dataset_re and isinstance(dataset, str) and not dataset_re.match(dataset):
        issues.append(
            ValidationIssue(
                severity=Severity.ERROR,
                code="NAMING_DATASET_PATTERN",
                message=f"sink dataset '{dataset}' does not match pattern '{dataset_re.pattern}'",
                manifest_path=manifest_path,
                selector=selector,
            )
        )

    if table_re and isinstance(table, str) and not table_re.match(table):
        issues.append(
            ValidationIssue(
                severity=Severity.ERROR,
                code="NAMING_TABLE_PATTERN",
                message=f"sink table '{table}' does not match pattern '{table_re.pattern}'",
                manifest_path=manifest_path,
                selector=selector,
            )
        )

    opts = getattr(load_cfg, "options", {}) or {}
    table_labels = opts.get("table_labels") or {}

    # Technical columns (__dpone__loaded_at / __dpone__deleted_at)
    try:
        tech_res = resolve_technical_columns(opts)
        include_tech = tech_res.enabled

        # Report ambiguous/coerced values (UX)
        if tech_res.warning:
            if "conflicts" in tech_res.warning:
                issues.append(
                    ValidationIssue(
                        severity=Severity.ERROR,
                        code="TECHNICAL_COLUMNS_CONFLICT",
                        message=f"Conflicting technical columns settings: {tech_res.warning}",
                        manifest_path=manifest_path,
                        selector=selector,
                    )
                )
            elif "unknown string" in tech_res.warning:
                issues.append(
                    ValidationIssue(
                        severity=Severity.ERROR,
                        code="TECHNICAL_COLUMNS_BAD_VALUE",
                        message=(
                            "sink.options.include_technical_columns should be boolean (true/false) or use "
                            "sink.options.technical_columns=required|optional|forbidden; "
                            f"got {tech_res.include_flag_raw!r}"
                        ),
                        manifest_path=manifest_path,
                        selector=selector,
                    )
                )
    except ValueError as exc:
        include_tech = True
        issues.append(
            ValidationIssue(
                severity=Severity.ERROR,
                code="TECHNICAL_COLUMNS_BAD_MODE",
                message=str(exc),
                manifest_path=manifest_path,
                selector=selector,
            )
        )

    reconciliation_enabled = bool(getattr(load_cfg, "reconciliation", False))
    if reconciliation_enabled and not include_tech:
        issues.append(
            ValidationIssue(
                severity=Severity.ERROR,
                code="RECONCILIATION_REQUIRES_TECH_COLUMNS",
                message=(
                    "reconciliation включен, но технические колонки отключены "
                    "(sink.options.technical_columns=forbidden или sink.options.include_technical_columns=false). "
                    "Soft-delete reconciliation требует __dpone__loaded_at/__dpone__deleted_at."
                ),
                manifest_path=manifest_path,
                selector=selector,
            )
        )

    if technical_columns_policy == TechnicalColumnsMode.REQUIRED and not include_tech:
        issues.append(
            ValidationIssue(
                severity=technical_columns_severity,
                code="TECHNICAL_COLUMNS_DISABLED",
                message=(
                    "Технические колонки обязательны по профилю/конвенции, но они отключены "
                    "(sink.options.technical_columns=forbidden или sink.options.include_technical_columns=false). "
                    "Ожидаются __dpone__loaded_at и __dpone__deleted_at."
                ),
                manifest_path=manifest_path,
                selector=selector,
            )
        )

    if technical_columns_policy == TechnicalColumnsMode.FORBIDDEN and include_tech:
        issues.append(
            ValidationIssue(
                severity=technical_columns_severity,
                code="TECHNICAL_COLUMNS_FORBIDDEN",
                message=(
                    "Технические колонки запрещены по профилю/конвенции, но они включены. "
                    "Установите sink.options.technical_columns=forbidden (или include_technical_columns=false)."
                ),
                manifest_path=manifest_path,
                selector=selector,
            )
        )

    if required_labels:
        if not isinstance(table_labels, Mapping):
            issues.append(
                ValidationIssue(
                    severity=Severity.ERROR,
                    code="META_LABELS_TYPE",
                    message="sink.options.table_labels must be an object (dict)",
                    manifest_path=manifest_path,
                    selector=selector,
                )
            )
        else:
            missing = [k for k in required_labels if k not in table_labels]
            if missing:
                issues.append(
                    ValidationIssue(
                        severity=missing_labels_severity,
                        code="META_LABELS_MISSING",
                        message=f"missing required labels: {missing} (sink.options.table_labels)",
                        manifest_path=manifest_path,
                        selector=selector,
                    )
                )

    # Validate label values (patterns / forbidden values / non-empty)
    if isinstance(table_labels, Mapping):
        # 1) required labels must be non-empty
        for k in required_labels:
            if k not in table_labels:
                continue
            v = table_labels.get(k)
            if v is None or (isinstance(v, str) and not v.strip()):
                issues.append(
                    ValidationIssue(
                        severity=invalid_label_value_severity,
                        code="META_LABEL_VALUE_EMPTY",
                        message=f"label '{k}' must be non-empty (sink.options.table_labels)",
                        manifest_path=manifest_path,
                        selector=selector,
                    )
                )

        # 2) forbidden values
        for k, forbidden in forbidden_label_values.items():
            if not forbidden:
                continue
            if k not in table_labels:
                continue
            v = table_labels.get(k)
            if v is None:
                continue
            vv = str(v).strip().lower()
            if vv in forbidden:
                issues.append(
                    ValidationIssue(
                        severity=invalid_label_value_severity,
                        code="META_LABEL_VALUE_FORBIDDEN",
                        message=f"label '{k}' has forbidden value '{v}' (sink.options.table_labels)",
                        manifest_path=manifest_path,
                        selector=selector,
                    )
                )

        # 3) regex patterns
        for k, rx in label_patterns.items():
            if k not in table_labels:
                continue
            v = table_labels.get(k)
            if v is None:
                continue
            vv = str(v).strip()
            if not rx.match(vv):
                issues.append(
                    ValidationIssue(
                        severity=invalid_label_value_severity,
                        code="META_LABEL_VALUE_PATTERN",
                        message=f"label '{k}' value '{v}' does not match pattern '{rx.pattern}' (sink.options.table_labels)",
                        manifest_path=manifest_path,
                        selector=selector,
                    )
                )

    if require_table_description:
        desc = opts.get("table_description")
        if not desc:
            issues.append(
                ValidationIssue(
                    severity=missing_description_severity,
                    code="META_DESCRIPTION_MISSING",
                    message="missing table description (sink.options.table_description)",
                    manifest_path=manifest_path,
                    selector=selector,
                )
            )

    # Additional description checks (only if description exists)
    desc = opts.get("table_description")
    if isinstance(desc, str) and desc.strip():
        for rx in description_required_rx or ():
            if not rx.search(desc):
                issues.append(
                    ValidationIssue(
                        severity=invalid_description_severity,
                        code="META_DESCRIPTION_REQUIRED_REGEX",
                        message=f"table description must match regex: {rx.pattern!r}",
                        manifest_path=manifest_path,
                        selector=selector,
                    )
                )

        for rx in description_forbidden_rx or ():
            if rx.search(desc):
                issues.append(
                    ValidationIssue(
                        severity=invalid_description_severity,
                        code="META_DESCRIPTION_FORBIDDEN_REGEX",
                        message=f"table description matches forbidden regex: {rx.pattern!r}",
                        manifest_path=manifest_path,
                        selector=selector,
                    )
                )

        if require_source_path_in_description:
            issues.extend(
                _validate_description_source_path(
                    spec,
                    manifest_path=manifest_path,
                    selector=selector,
                    description=desc,
                    labels=table_labels if isinstance(table_labels, Mapping) else {},
                    severity=invalid_description_severity,
                )
            )

    if (
        canonical_endpoint_type(str(opts.get("source_type", ""))) == "postgres"
        and canonical_endpoint_type(str(opts.get("sink_type", ""))) == "mssql"
    ):
        source_options = opts.get("source_options")
        declared_columns = _declared_source_columns(source_options if isinstance(source_options, Mapping) else {})
        for blocker in declared_postgres_mssql_contract_blockers(declared_columns, opts) if declared_columns else ():
            issues.append(
                ValidationIssue(
                    severity=Severity.ERROR,
                    code="POSTGRES_MSSQL_TYPE_CONTRACT_BLOCKED",
                    message=(
                        f"{blocker}. Explicit PostgreSQL textual families require "
                        "schema_contract.columns.<column>.type=string; a physical override alone is insufficient."
                    ),
                    manifest_path=manifest_path,
                    selector=selector,
                )
            )

    return issues


def _declared_source_columns(options: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
    raw = options.get("columns")
    if isinstance(raw, Mapping):
        return tuple((str(name), str(dtype)) for name, dtype in raw.items())
    if isinstance(raw, Sequence) and not isinstance(raw, str | bytes):
        return tuple(
            (str(item["name"]), str(item.get("type", item.get("dtype", ""))))
            for item in raw
            if isinstance(item, Mapping) and item.get("name") and (item.get("type") or item.get("dtype"))
        )
    return ()


def has_errors(issues: Sequence[ValidationIssue]) -> bool:
    return any(i.severity == Severity.ERROR for i in issues)
