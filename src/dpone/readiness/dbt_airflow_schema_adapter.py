"""Readiness adapter for the public Airflow evidence-bundle schema."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.gitops.schema_validation import GitOpsSchemaValidator


def airflow_evidence_schema_is_valid(payload: Mapping[str, Any]) -> bool:
    """Return whether payload satisfies the canonical Airflow evidence schema."""

    return not GitOpsSchemaValidator().validate(
        payload,
        expected_kind="gitops.airflow_evidence_bundle",
    )


__all__ = ["airflow_evidence_schema_is_valid"]
