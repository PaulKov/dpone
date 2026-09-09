"""Rendering tests for self-service init error catalog output."""

from __future__ import annotations

from dpone.commands.airflow_self_service_rendering import self_service_init_text
from dpone.readiness.self_service_error_catalog import enrich_self_service_error


def test_self_service_init_text_renders_docs_and_fixes() -> None:
    payload = {
        "passed": False,
        "changes": [],
        "errors": [
            enrich_self_service_error(
                "DPONE_WORKLOAD_INIT_DOMAIN_CONFLICT",
                "workload already declared",
                stage="workload_init",
                path="dpone_workloads/gitops/domains/marketing.yaml",
            )
        ],
    }
    text = self_service_init_text("dpone workload init", payload)
    assert "DPONE_WORKLOAD_INIT_DOMAIN_CONFLICT" in text
    assert "docs: https://paulkov.github.io/dpone/errors/DPONE_WORKLOAD_INIT_DOMAIN_CONFLICT/" in text
    assert "fix manual:" in text
