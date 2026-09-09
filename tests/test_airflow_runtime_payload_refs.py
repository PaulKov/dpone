"""Contract: workload runtime_payload_ids must resolve in the payload set."""

from __future__ import annotations

import pytest

from dpone.readiness.airflow_runtime_payload_refs import (
    RuntimePayloadRefError,
    dangling_runtime_payload_ids,
    require_runtime_payload_refs,
)


def test_dangling_runtime_payload_ids_detects_missing_refs() -> None:
    dangling = dangling_runtime_payload_ids(
        workload_packs=[
            {"id": "dbt__example_customer_mart_test", "runtime_payload_ids": ["dbt_project", "dbt_manifest"]},
            {"id": "orders"},
        ],
        runtime_payloads=[{"id": "dbt_project"}],
    )
    assert dangling == (("dbt__example_customer_mart_test", "dbt_manifest"),)


def test_require_runtime_payload_refs_passes_when_complete() -> None:
    require_runtime_payload_refs(
        workload_packs=[
            {
                "id": "dbt__demo",
                "runtime_payload_ids": ["dbt_manifest", "dbt_project", "dbt_selection_demo"],
            }
        ],
        runtime_payloads=[
            {"id": "dbt_manifest"},
            {"id": "dbt_project"},
            {"id": "dbt_selection_demo"},
        ],
    )


def test_require_runtime_payload_refs_fails_closed() -> None:
    with pytest.raises(RuntimePayloadRefError) as exc:
        require_runtime_payload_refs(
            workload_packs=[{"id": "dbt__demo", "runtime_payload_ids": ["dbt_project"]}],
            runtime_payloads=[],
            code="DPONE_RUNTIME_PAYLOAD_REFS_DANGLING",
        )
    assert exc.value.code == "DPONE_RUNTIME_PAYLOAD_REFS_DANGLING"
    assert "dbt_project" in str(exc.value)
