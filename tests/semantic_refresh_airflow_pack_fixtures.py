"""Package-only semantic-refresh fixtures for Airflow compatibility tests."""

from __future__ import annotations

import hashlib
import json

from dpone_airflow_pack.pack_identity import PACK_IDENTITY_SCHEMA, compute_pack_fingerprint
from dpone_airflow_pack.semantic_refresh_airflow import SemanticRefreshAirflowCallables
from dpone_airflow_pack.semantic_refresh_dag_projection import build_semantic_refresh_dag_projection


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _canonical_digest(value: dict[str, object]) -> str:
    payload = json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _topology() -> dict[str, object]:
    unsigned: dict[str, object] = {
        "activation": "POST_DEPLOYMENT_AUTHORITY_REQUIRED",
        "dag_id": "semantic_refresh_daily_marts",
        "dag_policy": {
            "catchup": False,
            "max_active_runs": 1,
            "max_active_tasks": 2,
            "owner": "data",
            "schedule": None,
            "start_date": "2026-01-01",
            "tags": ["dbt", "dpone", "semantic-refresh-v2"],
            "timezone": "UTC",
        },
        "dependencies": {
            "model.customers": [],
            "model.orders": ["model.customers"],
        },
        "logical_output_asset_uris": {
            "model.customers": "dpone://mart/customers",
            "model.orders": "dpone://mart/orders",
        },
        "model_unique_ids": ["model.customers", "model.orders"],
        "profile_sha256": _digest("8"),
        "project_config_overlay": {},
        "schema": "dpone.dbt-semantic-refresh-topology-template.v1",
        "workflow_name": "daily_marts",
    }
    return {**unsigned, "topology_sha256": _canonical_digest(unsigned)}


def _template_pack() -> dict[str, object]:
    payload: dict[str, object] = {
        "activation": "POST_DEPLOYMENT_AUTHORITY_REQUIRED",
        "dbt_execution_pack": {"schema": "test.semantic-refresh-dbt-execution-pack.v1"},
        "executable": False,
        "kind": "gitops.airflow_pack",
        "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
        "producer": "dpone dbt compile",
        "runtime_command": "",
        "runtime_payload_ids": [],
        "schema_version": "3",
        "semantic_refresh": {
            "mode": "semantic_refresh_v2_template",
            "package_artifacts_sha256": _digest("c"),
            "pre_release_bundle_sha256": _digest("d"),
            "topology": _topology(),
        },
        "workload": {
            "effective_config": {"authoring": {"mode": "semantic_refresh_v2_template"}},
            "workload_id": "semantic__daily_marts",
        },
    }
    payload["pack_fingerprint"] = compute_pack_fingerprint(payload)
    return payload


def _plan_bundle() -> dict[str, object]:
    unsigned: dict[str, object] = {
        "operation_plans": [
            {"model_unique_id": "model.customers", "operation_id": _digest("1")},
            {"model_unique_id": "model.orders", "operation_id": _digest("2")},
        ],
        "package_artifacts_sha256": _digest("c"),
        "pre_release_bundle_sha256": _digest("d"),
        "release_deployment_authority": {
            "deployment_id": _digest("3"),
            "release_id": _digest("5"),
        },
        "schema": "dpone.dbt-semantic-refresh-plan-bundle.v1",
        "targets": [],
        "workflow_plan": {"workflow_plan_sha256": _digest("f")},
    }
    return {**unsigned, "plan_bundle_sha256": _canonical_digest(unsigned)}


def semantic_refresh_dag_projection() -> dict[str, object]:
    """Build one valid run-neutral sidecar using only the Airflow pack."""

    return build_semantic_refresh_dag_projection(
        template_pack=_template_pack(),
        plan_bundle=_plan_bundle(),
    ).to_mapping()


def semantic_refresh_airflow_callables() -> SemanticRefreshAirflowCallables:
    """Return parse-inert callables suitable for package compatibility tests."""

    return SemanticRefreshAirflowCallables(
        dbt_build_test=lambda **_kwargs: None,
        resolve_execution_binding=lambda **_kwargs: _digest("e"),
        prepare=lambda **_kwargs: None,
        commit=lambda **_kwargs: None,
        read_publications=lambda **_kwargs: (),
        persist_summary=lambda payload: payload,
    )


__all__ = [
    "semantic_refresh_airflow_callables",
    "semantic_refresh_dag_projection",
]
