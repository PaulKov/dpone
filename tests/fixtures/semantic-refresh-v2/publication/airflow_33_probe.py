"""Airflow 3.3 serialization probe for a run-neutral semantic-refresh DAG."""

from __future__ import annotations

import hashlib
import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import airflow
from airflow.serialization.serialized_objects import DagSerialization
from dpone_airflow_pack.pack_identity import PACK_IDENTITY_SCHEMA, compute_pack_fingerprint
from dpone_airflow_pack.semantic_refresh_dag_authority import (
    LocalSemanticRefreshDagProjectionAuthority,
)
from dpone_airflow_pack.semantic_refresh_dag_projection import (
    build_semantic_refresh_dag_projection,
)
from dpone_airflow_pack.semantic_refresh_projection import DurableModelPublication

import dpone.app.semantic_refresh_worker_composition as worker_composition
from dpone.app.semantic_refresh_airflow_application import (
    build_semantic_refresh_airflow_application,
)
from dpone.ports.semantic_refresh_clickhouse_connection import (
    ClickHouseClusterConnectionAuthority,
    clickhouse_cluster_topology_sha256,
)
from dpone.readiness.airflow_self_service_templates import airflow_loader_template


def _digest(char: str) -> str:
    return "sha256:" + char * 64


def _canonical_digest(value: dict[str, object]) -> str:
    raw = json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _topology() -> dict[str, object]:
    unsigned: dict[str, object] = {
        "activation": "POST_DEPLOYMENT_AUTHORITY_REQUIRED",
        "dag_id": "semantic_refresh_airflow_33_probe",
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
        "dependencies": {"model.customers": [], "model.orders": ["model.customers"]},
        "logical_output_asset_uris": {
            "model.customers": "dpone://mart/customers",
            "model.orders": "dpone://mart/orders",
        },
        "model_unique_ids": ["model.customers", "model.orders"],
        "profile_sha256": _digest("8"),
        "project_config_overlay": {},
        "schema": "dpone.dbt-semantic-refresh-topology-template.v1",
        "workflow_name": "airflow_33_probe",
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
            "workload_id": "semantic__airflow_33_probe",
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


def _binding(**_kwargs: object) -> str:
    return _digest("e")


def _read_publications(**_kwargs: object) -> tuple[DurableModelPublication, ...]:
    return (_publication("1", "b", "c"), _publication("2", "d", "e"))


def _publication(operation: str, attempt: str, terminal: str) -> DurableModelPublication:
    return DurableModelPublication(
        operation_id=_digest(operation),
        operation_plan_sha256=_digest("9"),
        workflow_execution_binding_sha256=_binding(),
        attempt_binding_sha256=_digest(attempt),
        artifact_manifest_sha256=_digest("7"),
        clickhouse_terminal_receipt_sha256=_digest("8"),
        terminal_receipt_sha256=_digest(terminal),
        target_generation=2,
        scope_revision=1,
        status="COMPLETE",
    )


def _persist_summary(payload: dict[str, object]) -> dict[str, object]:
    return {
        "persisted": True,
        "status": "FULLY_COMPLETE",
        "terminal_summary_sha256": payload["terminal_summary_sha256"],
    }


def _exact_artifact(artifact_ref: str, digest: str) -> dict[str, str | int]:
    return {
        "artifact_ref": artifact_ref,
        "sha256": _digest(digest),
        "bytes": 128,
    }


def _write_verified_index(root: Path, projection) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / ".promotion.lock").touch(mode=0o660)
    deployment_dir = root / "deployments" / "prod" / projection.identity.deployment_id.replace(":", "-")
    deployment_dir.mkdir(parents=True)
    activation_dir = root / "activations" / "prod" / projection.identity.deployment_id.replace(":", "-")
    activation_dir.mkdir(parents=True)
    descriptor = projection.descriptor()
    filename = (
        f"semantic-refresh-{projection.identity.dag_projection_sha256.removeprefix('sha256:')}.dag-projection.json"
    )
    sidecar = deployment_dir / filename
    sidecar.write_bytes(projection.canonical_bytes())
    release_dir = root / "releases" / projection.identity.release_id.replace(":", "-")
    (release_dir / "packs").mkdir(parents=True)
    pack_bytes = b"{}\n"
    pack_path = release_dir / "packs" / "semantic-refresh-authority.json"
    pack_path.write_bytes(pack_bytes)
    pack_sha256 = "sha256:" + hashlib.sha256(pack_bytes).hexdigest()
    semantic_descriptor = {
        **descriptor,
        "artifact_ref": (f"cache://deployments/prod/{projection.identity.deployment_id.replace(':', '-')}/{filename}"),
        "authority": LocalSemanticRefreshDagProjectionAuthority.build(projection.identity).to_mapping(),
    }
    runtime_delivery = {
        "mode": "init_fetch",
        "trust_tier": "production",
        "artifact_registry_ref": "dpone-prod-artifacts",
        "identity": {
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
            "namespace": "data-platform",
        },
        "registry_config_ref": {
            "kind": "kubernetes_config_map",
            "name": "dpone-registry",
            "key": "registry.json",
            "sha256": _digest("4"),
        },
        "trust_policy_ref": {
            "kind": "kubernetes_config_map",
            "name": "dpone-trust-policy",
            "key": "policy.json",
            "sha256": _digest("5"),
        },
        "source": {"artifact_registry_ref": "dpone-prod-artifacts"},
        "verify": {"checksums": "required", "attestations": "required_for_prod"},
    }
    index = {
        "schema": "dpone.airflow-deployment-index.v2",
        "trust_tier": "production",
        "release_id": projection.identity.release_id,
        "deployment_id": projection.identity.deployment_id,
        "runtime_image_ref": f"registry.example/dpone/runtime@{_digest('6')}",
        "runtime_image_digest": _digest("6"),
        "binding_set_ref": _digest("7"),
        "connection_registry_ref": _digest("8"),
        "credential_runtime_ref": _digest("9"),
        "airflow_bundle_ref": None,
        "release": _exact_artifact(
            f"cache://releases/{projection.identity.release_id.replace(':', '-')}/release-set.json",
            "a",
        ),
        "deployment": _exact_artifact(
            f"cache://deployments/prod/{projection.identity.deployment_id.replace(':', '-')}/deployment.json",
            "b",
        ),
        "binding_set": _exact_artifact(
            f"cache://runtime-connection-contexts/{_digest('3').replace(':', '-')}/binding-set.json",
            "c",
        ),
        "connection_registry": _exact_artifact(
            f"cache://runtime-connection-contexts/{_digest('3').replace(':', '-')}/connection-registry.json",
            "d",
        ),
        "credential_runtime": _exact_artifact(
            f"cache://runtime-connection-contexts/{_digest('3').replace(':', '-')}/credential-runtime.json",
            "e",
        ),
        "dag_specs": [],
        "workload_packs": [
            {
                "id": "semantic-refresh-authority",
                "artifact_ref": (
                    f"cache://releases/{projection.identity.release_id.replace(':', '-')}/"
                    "packs/semantic-refresh-authority.json"
                ),
                "sha256": pack_sha256,
                "bytes": len(pack_bytes),
                "pack_fingerprint": _digest("0"),
            }
        ],
        "semantic_refresh_dag_projections": [semantic_descriptor],
        "runtime_artifact_delivery": runtime_delivery,
    }
    index_path = activation_dir / "airflow-index.json"
    index_path.write_text(json.dumps(index, sort_keys=True), encoding="utf-8")
    (root / "current").symlink_to(Path("activations") / "prod" / projection.identity.deployment_id.replace(":", "-"))
    (root / "current-pointer.json").write_text(
        json.dumps(
            {
                "activation_id": "11111111-1111-4111-8111-111111111111",
                "deployment_id": projection.identity.deployment_id,
                "environment": "prod",
                "promoted_at": "2026-08-09T00:00:00Z",
                "promoted_by": "semantic-refresh-airflow-3.3-probe",
                "release_id": projection.identity.release_id,
                "schema": "dpone.current-pointer.v1",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return index_path


class _Unused:
    endpoint_authority_id = "http://127.0.0.1:58124/"

    def verify(self, _subject: object) -> bool:
        return True

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"parse-time composition used {name}")


_WORKER_CALLS: list[dict[str, object]] = []


def _record_worker(**kwargs: object) -> str:
    _WORKER_CALLS.append(dict(kwargs))
    return "executed"


def main() -> None:
    projection = build_semantic_refresh_dag_projection(
        template_pack=_template_pack(),
        plan_bundle=_plan_bundle(),
    )
    connection_calls = 0

    def _connection() -> object:
        nonlocal connection_calls
        connection_calls += 1
        raise AssertionError("DAG parsing opened MSSQL")

    application = build_semantic_refresh_airflow_application(
        mssql_connection_factory=_connection,
        mssql_connection_authority_id="mssql://warehouse",
        authority_store_ref="mssql://control/semantic-refresh-authority",
        artifact_stores=_Unused(),
        seal_policy=_Unused(),
        clickhouse_http_client=_Unused(),
        clickhouse_connection_authority=ClickHouseClusterConnectionAuthority(
            clickhouse_cluster_authority_id="clickhouse://local-cluster",
            endpoint_authority_id="http://127.0.0.1:58124/",
            cluster_name="default",
            host_names=("localhost",),
            topology_sha256=clickhouse_cluster_topology_sha256((("localhost", 9000, 1, 1),)),
        ),
        run_admission_verifier=_Unused(),
        immutable_proof_rechecker=_Unused(),
        package_source_root=Path("/opt/dpone/runtime/dbt-dpone"),
        pod_identity_root=Path("/var/run/dpone/podinfo"),
        pod_uid_relative_path="uid",
        clock=lambda: datetime(
            2026,
            8,
            9,
            tzinfo=timezone.utc,  # noqa: UP017 - package supports Python 3.10.
        ),
    )
    worker_composition.execute_semantic_refresh_dbt_pack = _record_worker
    _write_verified_index(Path("/opt/airflow/.dpone-cache"), projection)
    Path("/opt/airflow/.dpone-ack").mkdir(parents=True, exist_ok=True)
    platform_runtime = types.ModuleType("platform_semantic_refresh_runtime")
    platform_runtime.application = application  # type: ignore[attr-defined]
    sys.modules[platform_runtime.__name__] = platform_runtime
    namespace: dict[str, object] = {}
    loader = airflow_loader_template()
    exec(compile(loader, "dpone_dags.py", "exec"), namespace)  # noqa: S102 - exact generated loader probe.
    loaded: Any = namespace["loaded"]
    report = loaded.report
    dag_id = str(projection.descriptor()["dag_id"])
    if getattr(report, "fatal", True) or getattr(report, "loaded", ()) != (dag_id,):
        raise AssertionError("generated loader did not install the verified semantic-refresh sidecar")
    dag: Any = namespace[dag_id]
    if connection_calls != 0:
        raise AssertionError("semantic-refresh application performed parse-time I/O")
    summary_id = "semantic_refresh__durable_workflow_summary"
    outlet_task_ids = sorted(task.task_id for task in dag.tasks if task.outlets)
    if outlet_task_ids != [summary_id]:
        raise AssertionError("semantic-refresh Assets must exist only on the durable terminal task")
    for task in dag.tasks:
        if task.do_xcom_push is not (task.task_id == summary_id):
            raise AssertionError("semantic-refresh XCom must exist only on the durable terminal task")
        if task.retries != 0:
            raise AssertionError("semantic-refresh tasks must disable automatic retries")
    encoded = json.dumps(DagSerialization.to_dict(dag), allow_nan=False, default=str, sort_keys=True)
    for expected in (summary_id, "dpone://mart/customers", "dpone://mart/orders", "{{ dag_run.run_id }}"):
        if expected not in encoded:
            raise AssertionError(f"serialized Airflow DAG omitted {expected}")
    if "scheduled__2026-08-08" in encoded or _binding() in encoded:
        raise AssertionError("serialized Airflow DAG contains a run-bound execution identity")
    result = application.worker.dbt_build_test(
        dbt_execution_pack={"schema": "test.semantic-refresh-dbt-execution-pack.v1"},
        plan_bundle=_plan_bundle(),
        profile_sha256=_digest("8"),
        projection_identity={
            "dag_projection_sha256": projection.identity.dag_projection_sha256,
            "deployment_id": projection.identity.deployment_id,
            "package_artifacts_sha256": projection.identity.package_artifacts_sha256,
            "plan_bundle_sha256": projection.identity.plan_bundle_sha256,
            "pre_release_bundle_sha256": projection.identity.pre_release_bundle_sha256,
            "release_id": projection.identity.release_id,
            "template_pack_fingerprint": projection.identity.template_pack_fingerprint,
            "topology_sha256": projection.identity.topology_sha256,
            "workflow_plan_sha256": projection.identity.workflow_plan_sha256,
        },
        project_config_overlay={},
        topology_sha256=projection.identity.topology_sha256,
        workflow_execution_id="scheduled__2026-08-09",
    )
    if result != "executed" or len(_WORKER_CALLS) != 1:
        raise AssertionError("concrete semantic-refresh worker callable did not execute once")
    if connection_calls != 0:
        raise AssertionError("mocked worker execution opened a protected client")
    print(
        json.dumps(
            {
                "airflow_version": airflow.__version__,
                "dag_id": dag.dag_id,
                "outlet_task_ids": outlet_task_ids,
                "serialized": True,
                "task_count": len(dag.tasks),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
