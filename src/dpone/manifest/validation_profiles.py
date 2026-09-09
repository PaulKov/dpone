from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.contracts.technical_columns import TechnicalColumnsMode
from dpone.manifest.validation_models import Severity, ValidationProfile


def get_profile(name: str) -> ValidationProfile:
    """Returns a built-in validation profile."""
    normalized = (name or "").strip().lower()
    if normalized in {"landing", "landing_raw", "landing_raw_v1"}:
        # See Experience-Landing naming convention.
        # In addition to naming patterns and required labels, we also enforce that
        # mandatory label values are not placeholders (e.g. host/type != 'unknown').
        return ValidationProfile(
            name="landing_raw_v1",
            dataset_pattern=r"^landing__[a-z][a-z0-9_]*__[a-z][a-z0-9_]*$",
            table_pattern=r"^[a-z][a-z0-9_]*__[a-z][a-z0-9_]*$",
            required_labels=("layer", "src", "db", "schema", "host", "type", "ingest"),
            require_table_description=True,
            technical_columns=TechnicalColumnsMode.REQUIRED,
            technical_columns_severity=Severity.ERROR,
            missing_labels_severity=Severity.ERROR,
            missing_description_severity=Severity.ERROR,
            label_value_patterns={
                # ASCII/snake_case for labels (a-z, 0-9, _)
                "layer": r"^[a-z0-9_]+$",
                "src": r"^[a-z0-9_]+$",
                "db": r"^[a-z0-9_]+$",
                "schema": r"^[a-z0-9_]+$",
                "host": r"^[a-z0-9_]+$",
                "type": r"^[a-z0-9_]+$",
                "ingest": r"^[a-z0-9_]+$",
            },
            forbidden_label_values={
                "host": ("unknown", "tbd", "todo", "unset", "none", "null", "n_a", "na"),
                "type": ("unknown", "tbd", "todo", "unset", "none", "null", "n_a", "na"),
            },
            invalid_label_value_severity=Severity.ERROR,
            # Description requirements (see Experience-Landing naming convention)
            # - must include original source path: {host}/{db}/{schema}/{table_original}
            # - must include owner/contact/SLA (and should not be placeholders)
            description_required_regex=(
                r"(?im)^Источник:\s*.+$",
                r"(?im)^Владелец:\s*.+$",
                r"(?im)^Контакт:\s*.+$",
                r"(?im)^SLA:\s*.+$",
            ),
            description_forbidden_regex=(
                # forbid placeholders for host/owner/contact/SLA
                r"(?im)^Источник:\s*(unknown|tbd|todo|unset|none|null|n_a|na)(/|\s|$)",
                r"(?im)^Владелец:\s*(unknown|tbd|todo|unset|none|null|n_a|na)\s*$",
                r"(?im)^Контакт:\s*(unknown|tbd|todo|unset|none|null|n_a|na)\s*$",
                r"(?im)^SLA:\s*(unknown|tbd|todo|unset|none|null|n_a|na)\s*$",
            ),
            require_source_path_in_description=True,
            invalid_description_severity=Severity.ERROR,
        )

    raise ValueError(f"Unknown validation profile '{name}'. Supported: landing_raw_v1")


def _profile_from_manifest(raw: Mapping[str, Any]) -> ValidationProfile | None:
    """Builds profile from manifest `validation:` block (if present)."""
    val = raw.get("validation")
    if not isinstance(val, Mapping):
        return None

    dataset_pattern = val.get("dataset_pattern")
    table_pattern = val.get("table_pattern")
    required_labels = val.get("required_labels")
    require_table_description = bool(val.get("require_table_description", False))
    # Backward compatible: old bool require_technical_columns
    require_technical_columns = bool(val.get("require_technical_columns", False))
    tech_policy_raw = val.get("technical_columns")

    def _parse_policy(x: Any) -> TechnicalColumnsMode:
        if x is None:
            return TechnicalColumnsMode.REQUIRED if require_technical_columns else TechnicalColumnsMode.OPTIONAL
        if isinstance(x, TechnicalColumnsMode):
            return x
        if isinstance(x, str):
            s = x.strip().lower()
            if s in {"required", "require", "req"}:
                return TechnicalColumnsMode.REQUIRED
            if s in {"optional", "opt", "default"}:
                return TechnicalColumnsMode.OPTIONAL
            if s in {"forbidden", "forbid", "off", "disabled"}:
                return TechnicalColumnsMode.FORBIDDEN
        if isinstance(x, bool):
            return TechnicalColumnsMode.REQUIRED if x else TechnicalColumnsMode.FORBIDDEN
        raise ValueError(
            "validation.technical_columns must be one of: required|optional|forbidden (or use legacy require_technical_columns=true)"
        )

    technical_columns = _parse_policy(tech_policy_raw)

    def _sev(x: Any, *, default: Severity) -> Severity:
        if isinstance(x, str):
            xx = x.strip().upper()
            if xx == "ERROR":
                return Severity.ERROR
            if xx in {"WARNING", "WARN"}:
                return Severity.WARNING
        return default

    missing_labels_severity = _sev(val.get("missing_labels_severity"), default=Severity.WARNING)
    missing_description_severity = _sev(val.get("missing_description_severity"), default=Severity.WARNING)
    invalid_label_value_severity = _sev(val.get("invalid_label_value_severity"), default=Severity.ERROR)
    invalid_description_severity = _sev(val.get("invalid_description_severity"), default=Severity.ERROR)
    technical_columns_severity = _sev(val.get("technical_columns_severity"), default=Severity.ERROR)

    if required_labels is None:
        labels_list: Sequence[str] = ()
    elif isinstance(required_labels, list) and all(isinstance(x, str) for x in required_labels):
        labels_list = tuple(required_labels)
    else:
        labels_list = ()

    label_value_patterns_raw = val.get("label_value_patterns")
    label_value_patterns: dict[str, str] = {}
    if isinstance(label_value_patterns_raw, Mapping):
        for k, v in label_value_patterns_raw.items():
            if isinstance(k, str) and isinstance(v, str) and k.strip() and v.strip():
                label_value_patterns[k.strip()] = v.strip()

    forbidden_raw = val.get("forbidden_label_values")
    forbidden: dict[str, Sequence[str]] = {}
    if isinstance(forbidden_raw, Mapping):
        for k, v in forbidden_raw.items():
            if not isinstance(k, str) or not k.strip():
                continue
            if isinstance(v, list) and all(isinstance(x, str) for x in v):
                forbidden[k.strip()] = tuple(v)
            elif isinstance(v, str) and v.strip():
                forbidden[k.strip()] = (v.strip(),)

    # Optional description regex checks
    desc_required_raw = val.get("description_required_regex")
    desc_forbidden_raw = val.get("description_forbidden_regex")

    def _as_str_list(x: Any) -> Sequence[str]:
        if x is None:
            return ()
        if isinstance(x, str) and x.strip():
            return (x.strip(),)
        if isinstance(x, list) and all(isinstance(i, str) and i.strip() for i in x):
            return tuple(i.strip() for i in x)
        return ()

    description_required_regex = _as_str_list(desc_required_raw)
    description_forbidden_regex = _as_str_list(desc_forbidden_raw)

    require_source_path_in_description = bool(val.get("require_source_path_in_description", False))

    return ValidationProfile(
        name="manifest",
        dataset_pattern=str(dataset_pattern) if dataset_pattern else None,
        table_pattern=str(table_pattern) if table_pattern else None,
        required_labels=labels_list,
        require_table_description=require_table_description,
        technical_columns=technical_columns,
        technical_columns_severity=technical_columns_severity,
        missing_labels_severity=missing_labels_severity,
        missing_description_severity=missing_description_severity,
        label_value_patterns=label_value_patterns,
        forbidden_label_values=forbidden,
        invalid_label_value_severity=invalid_label_value_severity,
        description_required_regex=description_required_regex,
        description_forbidden_regex=description_forbidden_regex,
        require_source_path_in_description=require_source_path_in_description,
        invalid_description_severity=invalid_description_severity,
    )
