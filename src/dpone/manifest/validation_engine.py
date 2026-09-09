from __future__ import annotations

import re

from dpone.manifest.models import LoadedManifest
from dpone.manifest.validation_models import Severity, ValidationIssue, ValidationProfile
from dpone.manifest.validation_profiles import _profile_from_manifest
from dpone.manifest.validation_rules import _validate_process, _validate_universal_process


def validate_manifest(
    manifest: LoadedManifest,
    *,
    profile: ValidationProfile | None = None,
) -> list[ValidationIssue]:
    """Validates a loaded manifest.

    Rules priority:
    1) If profile is passed explicitly -> use it.
    2) Else, if manifest has `validation:` block -> use it.
    3) Else -> no-op (returns empty list).
    """

    effective_profile = profile or _profile_from_manifest(manifest.raw)

    issues: list[ValidationIssue] = []
    for spec in manifest.processes:
        issues.extend(_validate_universal_process(spec, manifest_path=manifest.path))

    if effective_profile is None:
        return issues

    dataset_re = re.compile(effective_profile.dataset_pattern) if effective_profile.dataset_pattern else None
    table_re = re.compile(effective_profile.table_pattern) if effective_profile.table_pattern else None

    label_patterns: dict[str, re.Pattern[str]] = {}
    for k, pat in (effective_profile.label_value_patterns or {}).items():
        try:
            label_patterns[k] = re.compile(pat)
        except re.error:
            # Bad regex in profile/manifest - treat as configuration error.
            issues.append(
                ValidationIssue(
                    severity=Severity.ERROR,
                    code="VALIDATION_BAD_REGEX",
                    message=f"Invalid regex for label '{k}': {pat!r}",
                    manifest_path=manifest.path,
                    selector=None,
                )
            )

    forbidden_sets: dict[str, set[str]] = {}
    for k, vals in (effective_profile.forbidden_label_values or {}).items():
        if not vals:
            continue
        forbidden_sets[k] = {str(x).strip().lower() for x in vals if str(x).strip()}

    # Compile description regex checks (MULTILINE to support multi-line descriptions)
    desc_required_rx: list[re.Pattern[str]] = []
    for pat in effective_profile.description_required_regex or ():
        try:
            desc_required_rx.append(re.compile(pat, flags=re.MULTILINE))
        except re.error:
            issues.append(
                ValidationIssue(
                    severity=Severity.ERROR,
                    code="VALIDATION_BAD_REGEX",
                    message=f"Invalid regex for description_required_regex: {pat!r}",
                    manifest_path=manifest.path,
                    selector=None,
                )
            )

    desc_forbidden_rx: list[re.Pattern[str]] = []
    for pat in effective_profile.description_forbidden_regex or ():
        try:
            desc_forbidden_rx.append(re.compile(pat, flags=re.MULTILINE))
        except re.error:
            issues.append(
                ValidationIssue(
                    severity=Severity.ERROR,
                    code="VALIDATION_BAD_REGEX",
                    message=f"Invalid regex for description_forbidden_regex: {pat!r}",
                    manifest_path=manifest.path,
                    selector=None,
                )
            )

    for spec in manifest.processes:
        issues.extend(
            _validate_process(
                spec,
                manifest_path=manifest.path,
                dataset_re=dataset_re,
                table_re=table_re,
                required_labels=effective_profile.required_labels,
                require_table_description=effective_profile.require_table_description,
                technical_columns_policy=effective_profile.technical_columns,
                technical_columns_severity=effective_profile.technical_columns_severity,
                missing_labels_severity=effective_profile.missing_labels_severity,
                missing_description_severity=effective_profile.missing_description_severity,
                label_patterns=label_patterns,
                forbidden_label_values=forbidden_sets,
                invalid_label_value_severity=effective_profile.invalid_label_value_severity,
                description_required_rx=tuple(desc_required_rx),
                description_forbidden_rx=tuple(desc_forbidden_rx),
                require_source_path_in_description=effective_profile.require_source_path_in_description,
                invalid_description_severity=effective_profile.invalid_description_severity,
            )
        )

    return issues
