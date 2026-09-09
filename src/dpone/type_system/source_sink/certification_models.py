"""Type-matrix certification models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any, Literal

DecisionCategory = Literal[
    "auto_inferred",
    "explicit_logical_contract",
    "explicit_physical_override",
    "compatible_widening",
    "incompatible_requires_policy",
    "quarantine_required",
    "variant_column_required",
]


@dataclass(frozen=True, slots=True)
class TypeCertificationCase:
    """One source-type certification row for a concrete source -> sink route."""

    name: str
    source: str
    sink: str
    source_type: str
    expected_canonical_type: str
    expected_target_type: str
    expected_transport: str
    nullable: bool = False
    sample_values: tuple[str, ...] = ()
    lossless: bool = True
    compatible: bool = True
    decision_category: DecisionCategory = "auto_inferred"
    decision_source: str = "source_metadata"
    type_fidelity: Mapping[str, Any] | None = None
    contract_override: Mapping[str, Any] | None = None
    physical_override: Mapping[str, Any] | None = None
    notes: str = ""

    @property
    def source_spec(self) -> str:
        suffix = " nullable" if self.nullable and "nullable" not in self.source_type.lower() else ""
        return f"{self.source_type}{suffix}"

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["source_spec"] = self.source_spec
        return payload


@dataclass(frozen=True, slots=True)
class TypeCertificationSuite:
    """A certified route matrix used by tests, docs, and release evidence."""

    source: str
    sink: str
    profile: str
    runbook: str
    cases: tuple[TypeCertificationCase, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "sink": self.sink,
            "profile": self.profile,
            "runbook": self.runbook,
            "cases": [case.to_dict() for case in self.cases],
        }


__all__ = ["DecisionCategory", "TypeCertificationCase", "TypeCertificationSuite"]
