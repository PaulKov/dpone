"""Tests for self-service error catalog enrichment."""

from __future__ import annotations

import pytest

from dpone.readiness.airflow_self_service_models import Change
from dpone.readiness.self_service_error_catalog import (
    classify_manifest_validation,
    enrich_self_service_error,
    errors_from_workload_conflicts,
    invalid_reference_error,
    manifest_validation_error,
)


def test_classify_reserved_batch_var() -> None:
    message = "Переменная 'src_schema' зарезервирована и не может быть переопределена (/tmp/m.yaml)"
    assert classify_manifest_validation(message) == "DPONE_WORKLOAD_INIT_BATCH_VAR_RESERVED"


def test_enrich_self_service_error_includes_docs_and_fixes() -> None:
    payload = enrich_self_service_error(
        "DPONE_WORKLOAD_INIT_DOMAIN_CONFLICT",
        "workload already declared",
        stage="workload_init",
        path="dpone_workloads/gitops/domains/marketing.yaml",
    )
    assert payload["schema"] == "dpone.error.v1"
    assert payload["docs_url"] == "docs/errors/DPONE_WORKLOAD_INIT_DOMAIN_CONFLICT.md"
    assert payload["fixes"]
    assert payload["summary"]


def test_errors_from_workload_conflicts_maps_domain_and_scaffold() -> None:
    errors = errors_from_workload_conflicts(
        (
            Change("conflict", "workloads/marketing/dpone/manifests/app.yaml", "file exists with different content"),
            Change(
                "conflict",
                "dpone_workloads/gitops/domains/marketing.yaml",
                "workload 'app' already declared in domain catalog",
            ),
        ),
        domain="marketing",
        workload_id="app",
    )
    assert len(errors) == 2
    assert errors[0]["code"] == "DPONE_WORKLOAD_INIT_SCAFFOLD_CONFLICT"
    assert errors[1]["code"] == "DPONE_WORKLOAD_INIT_DOMAIN_CONFLICT"


def test_manifest_validation_error_entity() -> None:
    error = manifest_validation_error(
        message="invalid sink mode",
        path="workloads/marketing/dpone/manifests/app.yaml",
        domain="marketing",
        workload_id="app",
    )
    assert error["entity"] == {"kind": "workload", "id": "marketing/app"}
    assert error["stage"] == "validate"


def test_invalid_reference_error() -> None:
    error = invalid_reference_error("workload reference must be DOMAIN/WORKLOAD_ID")
    assert error["code"] == "DPONE_WORKLOAD_INIT_INVALID_REFERENCE"


@pytest.mark.parametrize(
    ("message", "code"),
    [
        ("schema mismatch", "DPONE_WORKLOAD_INIT_MANIFEST_INVALID"),
        ("Variable 'src_table' is reserved", "DPONE_WORKLOAD_INIT_BATCH_VAR_RESERVED"),
    ],
)
def test_classify_manifest_validation_fallback(message: str, code: str) -> None:
    assert classify_manifest_validation(message) == code
