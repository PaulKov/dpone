from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from dpone._compat import StrEnum
from dpone.contracts.technical_columns import TechnicalColumnsMode


class Severity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    severity: Severity
    code: str
    message: str
    manifest_path: Path
    selector: str | None = None


@dataclass(frozen=True, slots=True)
class ValidationProfile:
    name: str
    dataset_pattern: str | None = None
    table_pattern: str | None = None
    required_labels: Sequence[str] = ()
    require_table_description: bool = False
    # Technical columns policy (__dpone__loaded_at/__dpone__deleted_at)
    # - required  : must be enabled
    # - optional  : no requirement (default)
    # - forbidden : must be disabled
    technical_columns: TechnicalColumnsMode = TechnicalColumnsMode.OPTIONAL
    technical_columns_severity: Severity = Severity.ERROR
    missing_labels_severity: Severity = Severity.WARNING
    missing_description_severity: Severity = Severity.WARNING

    # Optional additional checks for label values
    # - label_value_patterns: regex per label key
    # - forbidden_label_values: list of forbidden values per label key (case-insensitive)
    label_value_patterns: Mapping[str, str] = field(default_factory=dict)
    forbidden_label_values: Mapping[str, Sequence[str]] = field(default_factory=dict)
    invalid_label_value_severity: Severity = Severity.ERROR

    # Optional additional checks for table description (sink.options.table_description)
    # - description_required_regex: each regex must match description (re.MULTILINE)
    # - description_forbidden_regex: each regex must NOT match description (re.MULTILINE)
    # - require_source_path_in_description: enforce that description contains original source path
    description_required_regex: Sequence[str] = ()
    description_forbidden_regex: Sequence[str] = ()
    require_source_path_in_description: bool = False
    invalid_description_severity: Severity = Severity.ERROR
