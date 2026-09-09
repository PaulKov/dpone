"""Reviewed PostgreSQL→MSSQL route case contracts.

The release inventory contains only hashes and expected outcomes.  This module
is the human-reviewable authority behind those hashes: every case is created
from a canonical JSON parameter document, and the case identifier is included
in the digest so records cannot be relabelled without invalidating evidence.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

CASE_CONFIG_VERSION = "dpone.route_live.postgres_mssql.case_config.v1"


def canonical_json(value: object) -> str:
    """Serialize a reviewed value with a stable, locale-independent contract."""

    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def sha256_json(value: object) -> str:
    """Return the SHA-256 of :func:`canonical_json`."""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ReviewedCase:
    """One finite, immutable vendor-live expectation."""

    suite_id: str
    case_id: str
    config_json: str
    action_class: str
    outcome_class: str
    expected_mutation: bool

    @classmethod
    def create(
        cls,
        suite_id: str,
        case_id: str,
        *,
        parameters: Mapping[str, Any],
        action_class: str,
        outcome_class: str,
        expected_mutation: bool,
    ) -> ReviewedCase:
        """Create a case and freeze its exact semantic configuration."""

        for field, value in (
            ("suite_id", suite_id),
            ("case_id", case_id),
            ("action_class", action_class),
            ("outcome_class", outcome_class),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"reviewed_case.{field}_required")
        config = {
            "schema_version": CASE_CONFIG_VERSION,
            "suite_id": suite_id,
            "case_id": case_id,
            "parameters": dict(parameters),
        }
        encoded = canonical_json(config)
        # Round-tripping here rejects non-JSON values at authoring time and
        # ensures custom mapping implementations cannot mutate after creation.
        if not isinstance(json.loads(encoded), dict):  # pragma: no cover - construction invariant
            raise ValueError("reviewed_case.parameters_invalid")
        return cls(
            suite_id=suite_id,
            case_id=case_id,
            config_json=encoded,
            action_class=action_class,
            outcome_class=outcome_class,
            expected_mutation=expected_mutation,
        )

    @property
    def config_sha256(self) -> str:
        """Digest bound into inventory, evidence, and JUnit properties."""

        return hashlib.sha256(self.config_json.encode("utf-8")).hexdigest()

    def contract(self) -> dict[str, object]:
        """Return the strict inventory representation accepted by the gate."""

        return {
            "case_id": self.case_id,
            "config_sha256": self.config_sha256,
            "action_class": self.action_class,
            "outcome_class": self.outcome_class,
            "expected_mutation": self.expected_mutation,
        }


@dataclass(frozen=True, slots=True)
class ReviewedSuite:
    """Sorted, duplicate-free case authority for one evidence/JUnit pair."""

    suite_id: str
    schema_version: str
    cases: tuple[ReviewedCase, ...]

    @classmethod
    def create(
        cls,
        suite_id: str,
        schema_version: str,
        cases: tuple[ReviewedCase, ...] | list[ReviewedCase],
    ) -> ReviewedSuite:
        ordered = tuple(sorted(cases, key=lambda value: value.case_id))
        identifiers = tuple(value.case_id for value in ordered)
        if not suite_id or not schema_version or not ordered:
            raise ValueError("reviewed_suite.incomplete")
        if any(value.suite_id != suite_id for value in ordered):
            raise ValueError("reviewed_suite.case_scope_mismatch")
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("reviewed_suite.duplicate_case_id")
        return cls(suite_id=suite_id, schema_version=schema_version, cases=ordered)

    def inventory_entry(self) -> dict[str, object]:
        """Render the exact strict-gate inventory entry."""

        contracts = [value.contract() for value in self.cases]
        identifiers = [value.case_id for value in self.cases]
        return {
            "suite_id": self.suite_id,
            "evidence_file": f"{self.suite_id}.json",
            "junit_file": f"{self.suite_id}.xml",
            "schema_version": self.schema_version,
            "case_count": len(self.cases),
            "case_set_sha256": sha256_json(identifiers),
            "case_contract_sha256": sha256_json(contracts),
            "case_contracts": contracts,
        }


def case(
    suite_id: str,
    case_id: str,
    parameters: Mapping[str, Any],
    *,
    action: str,
    outcome: str,
    mutation: bool,
) -> ReviewedCase:
    """Concise reviewed-case constructor used by finite matrix modules."""

    return ReviewedCase.create(
        suite_id,
        case_id,
        parameters=parameters,
        action_class=action,
        outcome_class=outcome,
        expected_mutation=mutation,
    )


__all__ = [
    "CASE_CONFIG_VERSION",
    "ReviewedCase",
    "ReviewedSuite",
    "canonical_json",
    "case",
    "sha256_json",
]
