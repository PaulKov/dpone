from __future__ import annotations

import hashlib
import json
from pathlib import Path

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.readiness.airflow_operator_diagnostics import build_operator_diagnostics


def _init_fetch_delivery() -> dict[str, object]:
    return {
        "mode": "init_fetch",
        "artifact_registry_ref": "dpone-prod-artifacts",
        "identity": {"method": "kubernetes_workload_identity", "service_account": "dpone-runtime"},
        "source": {"artifact_registry_ref": "dpone-prod-artifacts"},
        "verify": {"checksums": "required", "attestations": "required_for_prod"},
    }


def test_operator_diagnostics_payload_validates_against_public_schema(tmp_path: Path) -> None:
    diagnostics = build_operator_diagnostics(tmp_path)

    assert diagnostics["kind"] == "dpone.airflow-operator-diagnostics.v1"
    assert (
        GitOpsSchemaValidator().validate(
            diagnostics,
            expected_kind="dpone.airflow-operator-diagnostics.v1",
        )
        == ()
    )


def test_operator_diagnostics_summarizes_missing_index(tmp_path: Path) -> None:
    diagnostics = build_operator_diagnostics(tmp_path)

    assert diagnostics["status"] == "planned"
    assert diagnostics["summary"] == {"passed": 1, "warning": 0, "failed": 0}
    assert diagnostics["next_actions"] == []


def test_operator_diagnostics_summarizes_invalid_index(tmp_path: Path) -> None:
    current = tmp_path / ".dpone-cache" / "current"
    current.mkdir(parents=True)
    (current / "airflow-index.json").write_text("{not-json", encoding="utf-8")

    diagnostics = build_operator_diagnostics(tmp_path)

    assert diagnostics["status"] == "invalid"
    assert diagnostics["summary"] == {"passed": 1, "warning": 0, "failed": 1}
    assert diagnostics["next_actions"] == [
        {
            "code": "DPONE_AIRFLOW_INDEX_INVALID",
            "action": "Regenerate the local Airflow preview/deployment index or resync the deployment cache.",
            "safety": "manual",
        }
    ]


def test_operator_diagnostics_rejects_oversized_index(tmp_path: Path) -> None:
    current = tmp_path / ".dpone-cache" / "current"
    current.mkdir(parents=True)
    (current / "airflow-index.json").write_bytes(b" " * (2 * 1024 * 1024 + 1))

    diagnostics = build_operator_diagnostics(tmp_path)

    assert diagnostics["status"] == "invalid"
    check = {item["code"]: item for item in diagnostics["checks"]}["DPONE_AIRFLOW_INDEX_TOO_LARGE"]
    assert check["message"] == "airflow-index.json exceeds the local diagnostics byte limit."


def test_operator_diagnostics_rejects_symlinked_index(tmp_path: Path) -> None:
    outside = tmp_path / "outside-index.json"
    outside.write_text("{}", encoding="utf-8")
    current = tmp_path / ".dpone-cache" / "current"
    current.mkdir(parents=True)
    (current / "airflow-index.json").symlink_to(outside)

    diagnostics = build_operator_diagnostics(tmp_path)

    assert diagnostics["status"] == "invalid"
    check = {item["code"]: item for item in diagnostics["checks"]}["DPONE_AIRFLOW_INDEX_UNSAFE"]
    assert check["message"] == "airflow-index.json is not a confined regular cache file."


def test_operator_diagnostics_reads_canonical_current_symlink(tmp_path: Path) -> None:
    cache_root = tmp_path / ".dpone-cache"
    deployment = cache_root / "deployments" / "local-preview" / ("sha256-" + "b" * 64)
    deployment.mkdir(parents=True)
    (deployment / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "b" * 64,
                "runtime_artifact_delivery": {"mode": "local_preview"},
                "workload_packs": [],
            }
        ),
        encoding="utf-8",
    )
    (cache_root / "current").symlink_to(
        Path("deployments") / "local-preview" / deployment.name,
        target_is_directory=True,
    )

    diagnostics = build_operator_diagnostics(tmp_path)

    assert diagnostics["status"] == "materialized"
    assert diagnostics["release_id"] == "sha256:" + "a" * 64
    assert diagnostics["deployment_id"] == "sha256:" + "b" * 64


def test_operator_diagnostics_rejects_current_symlink_escape(tmp_path: Path) -> None:
    cache_root = tmp_path / ".dpone-cache"
    cache_root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "airflow-index.json").write_text("{}", encoding="utf-8")
    (cache_root / "current").symlink_to(Path("..") / "outside", target_is_directory=True)

    diagnostics = build_operator_diagnostics(tmp_path)

    assert diagnostics["status"] == "invalid"
    check = {item["code"]: item for item in diagnostics["checks"]}["DPONE_AIRFLOW_INDEX_UNSAFE"]
    assert check["message"] == "airflow-index.json is not a confined regular cache file."


def test_operator_diagnostics_reports_invalid_deployment_identity_refs(tmp_path: Path) -> None:
    current = tmp_path / ".dpone-cache" / "current"
    current.mkdir(parents=True)
    (current / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "b" * 64,
                "binding_set_ref": "bindings-prod",
                "connection_registry_ref": "registry-prod",
                "credential_runtime_ref": "credential-runtime-prod",
                "runtime_image_digest": "dpone-runtime:latest",
                "runtime_artifact_delivery": _init_fetch_delivery(),
                "workload_packs": [],
            }
        ),
        encoding="utf-8",
    )

    diagnostics = build_operator_diagnostics(tmp_path)

    assert (
        GitOpsSchemaValidator().validate(
            diagnostics,
            expected_kind="dpone.airflow-operator-diagnostics.v1",
        )
        == ()
    )
    assert diagnostics["status"] == "operator_issues"
    assert diagnostics["binding_set_ref"] is None
    assert diagnostics["connection_registry_ref"] is None
    assert diagnostics["credential_runtime_ref"] is None
    assert diagnostics["runtime_image_digest"] is None
    check = {item["code"]: item for item in diagnostics["checks"]}["DPONE_OPERATOR_DEPLOYMENT_IDENTITY_INVALID"]
    assert check["status"] == "failed"
    assert check["message"] == (
        "Airflow deployment index contains non-digest deployment identity refs: "
        "binding_set_ref, connection_registry_ref, credential_runtime_ref, runtime_image_digest."
    )


def test_operator_diagnostics_reports_invalid_release_and_deployment_ids(tmp_path: Path) -> None:
    current = tmp_path / ".dpone-cache" / "current"
    current.mkdir(parents=True)
    (current / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": "release-prod",
                "deployment_id": "deployment-prod",
                "runtime_artifact_delivery": _init_fetch_delivery(),
                "workload_packs": [],
            }
        ),
        encoding="utf-8",
    )

    diagnostics = build_operator_diagnostics(tmp_path)

    assert (
        GitOpsSchemaValidator().validate(
            diagnostics,
            expected_kind="dpone.airflow-operator-diagnostics.v1",
        )
        == ()
    )
    assert diagnostics["status"] == "operator_issues"
    assert diagnostics["release_id"] is None
    assert diagnostics["deployment_id"] is None
    assert diagnostics["operator_pinning"] == "incomplete"
    checks = {item["code"]: item for item in diagnostics["checks"]}
    assert checks["DPONE_OPERATOR_DEPLOYMENT_IDENTITY_INVALID"]["status"] == "failed"
    assert checks["DPONE_OPERATOR_DEPLOYMENT_IDENTITY_INVALID"]["message"] == (
        "Airflow deployment index contains non-digest deployment identity refs: release_id, deployment_id."
    )
    assert checks["DPONE_OPERATOR_PINNING"]["status"] == "failed"


def test_operator_diagnostics_rejects_incomplete_init_fetch_delivery(tmp_path: Path) -> None:
    current = tmp_path / ".dpone-cache" / "current"
    current.mkdir(parents=True)
    (current / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "b" * 64,
                "runtime_artifact_delivery": {"mode": "init_fetch"},
                "workload_packs": [],
            }
        ),
        encoding="utf-8",
    )

    diagnostics = build_operator_diagnostics(tmp_path)

    assert diagnostics["status"] == "invalid"
    assert diagnostics["operator_pinning"] == "planned"
    check = {item["code"]: item for item in diagnostics["checks"]}["DPONE_AIRFLOW_INDEX_DELIVERY_INVALID"]
    assert check["status"] == "failed"
    assert "runtime_artifact_delivery.identity" in check["message"]
    assert diagnostics["next_actions"] == [
        {
            "code": "DPONE_AIRFLOW_INDEX_DELIVERY_INVALID",
            "action": "Regenerate the Airflow deployment index with a complete init_fetch delivery contract.",
            "safety": "manual",
        }
    ]


def test_operator_diagnostics_rejects_nested_incomplete_init_fetch_delivery(tmp_path: Path) -> None:
    current = tmp_path / ".dpone-cache" / "current"
    current.mkdir(parents=True)
    delivery = _init_fetch_delivery()
    delivery["identity"] = {"method": "kubernetes_workload_identity"}
    (current / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "b" * 64,
                "runtime_artifact_delivery": delivery,
                "workload_packs": [],
            }
        ),
        encoding="utf-8",
    )

    diagnostics = build_operator_diagnostics(tmp_path)

    assert diagnostics["status"] == "invalid"
    check = {item["code"]: item for item in diagnostics["checks"]}["DPONE_AIRFLOW_INDEX_DELIVERY_INVALID"]
    assert check["status"] == "failed"
    assert "runtime_artifact_delivery.identity.service_account" in check["message"]


def test_operator_diagnostics_reports_versioned_git_airflow_bundle(tmp_path: Path) -> None:
    current = tmp_path / ".dpone-cache" / "current"
    current.mkdir(parents=True)
    commit = "7ac31f2" + "0" * 33
    (current / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "b" * 64,
                "airflow_bundle_ref": f"git:{commit}",
                "runtime_artifact_delivery": _init_fetch_delivery(),
                "workload_packs": [],
            }
        ),
        encoding="utf-8",
    )

    diagnostics = build_operator_diagnostics(tmp_path)

    assert diagnostics["status"] == "materialized"
    assert diagnostics["airflow_bundle"] == {
        "backend": "git",
        "ref": f"git:{commit}",
        "versioned": True,
        "version": commit,
    }
    bundle_check = {check["code"]: check for check in diagnostics["checks"]}["DPONE_AIRFLOW_BUNDLE_VERSIONED"]
    assert bundle_check["status"] == "passed"


def test_operator_diagnostics_does_not_treat_git_branch_as_versioned(tmp_path: Path) -> None:
    current = tmp_path / ".dpone-cache" / "current"
    current.mkdir(parents=True)
    (current / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "b" * 64,
                "airflow_bundle_ref": "git:main",
                "runtime_artifact_delivery": _init_fetch_delivery(),
                "workload_packs": [],
            }
        ),
        encoding="utf-8",
    )

    diagnostics = build_operator_diagnostics(tmp_path)

    assert diagnostics["airflow_bundle"]["versioned"] is False
    checks = {item["code"]: item for item in diagnostics["checks"]}
    assert checks["DPONE_RERUN_NOT_REPRODUCIBLE"]["status"] == "warning"


def test_operator_diagnostics_warns_for_non_versioned_airflow_bundle(tmp_path: Path) -> None:
    current = tmp_path / ".dpone-cache" / "current"
    current.mkdir(parents=True)
    (current / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "b" * 64,
                "airflow_bundle_ref": "s3://dpone-dags/prod",
                "runtime_artifact_delivery": _init_fetch_delivery(),
                "workload_packs": [],
            }
        ),
        encoding="utf-8",
    )

    diagnostics = build_operator_diagnostics(tmp_path)

    assert diagnostics["status"] == "operator_warnings"
    assert diagnostics["airflow_bundle"] == {
        "backend": "s3",
        "ref": "s3://dpone-dags/prod",
        "versioned": False,
        "version": None,
    }
    bundle_check = {check["code"]: check for check in diagnostics["checks"]}["DPONE_RERUN_NOT_REPRODUCIBLE"]
    assert bundle_check["status"] == "warning"
    assert "content-addressed snapshot" in bundle_check["message"]


def test_operator_diagnostics_reports_airflow_connection_secret_volume_bridge(tmp_path: Path) -> None:
    cache_root = tmp_path / ".dpone-cache"
    release_dir = cache_root / "releases" / "sha256-release" / "packs"
    release_dir.mkdir(parents=True)
    pack = {
        "kind": "gitops.airflow_pack",
        "schema_version": "3",
        "workload": {"workload_id": "load_orders"},
        "kpo_kwargs": {
            "task_id": "orders__dpone_runtime",
            "name": "dpone-orders",
            "namespace": "airflow-example",
            "image": "registry.example/dpone:dev",
            "deferrable": True,
        },
        "connection_projection": {
            "mode": "kubernetes_secret_volume",
            "secret_name": "dpone-airflow-connection-bridge",
            "mount_path": "/run/secrets/dpone/airflow-connections",
            "payload_format": "airflow_connection_uri",
            "secret_values": False,
            "connections": [
                {
                    "connection_ref": "mssql_dev",
                    "registry_connection_ref": "mssql_prod",
                    "connection_id": "mssql_prod",
                    "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                    "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                    "fields": {"uri": "uri"},
                }
            ],
        },
    }
    pack_path = release_dir / "load_orders.airflow-pack.json"
    pack_bytes = json.dumps(pack, sort_keys=True).encode("utf-8")
    pack_path.write_bytes(pack_bytes)
    current = cache_root / "current"
    current.mkdir(parents=True)
    (current / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "b" * 64,
                "runtime_artifact_delivery": _init_fetch_delivery(),
                "workload_packs": [
                    {
                        "id": "load_orders",
                        "artifact_ref": "cache://releases/sha256-release/packs/load_orders.airflow-pack.json",
                        "sha256": "sha256:" + hashlib.sha256(pack_bytes).hexdigest(),
                        "bytes": len(pack_bytes),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    diagnostics = build_operator_diagnostics(tmp_path)

    assert diagnostics["status"] == "operator_issues"
    assert diagnostics["summary"] == {"passed": 4, "warning": 0, "failed": 1}
    assert diagnostics["next_actions"] == [
        {
            "code": "DPONE_AIRFLOW_CONNECTION_DEFERRABLE_CLEANUP_POLICY",
            "action": "Set connection_projection.cleanup_policy: retain for deferrable KPO tasks and configure platform-managed Secret cleanup.",
            "safety": "manual",
        }
    ]
    bridge = diagnostics["workload_operators"][0]["airflow_connection_bridge"]
    assert bridge["operator"] == "AirflowConnectionSecretVolumeKubernetesPodOperator"
    assert bridge["topology"] == "digest_only"
    assert bridge["secret_ref_fingerprint"] == canonical_fingerprint(
        {
            "secret_name": "dpone-airflow-connection-bridge",
            "mount_path": "/run/secrets/dpone/airflow-connections",
        }
    )
    assert bridge["cleanup_policy"] == "after_execute"
    assert bridge["deferrable"] is True
    assert bridge["connections"] == [
        {
            "connection_ref": "mssql_dev",
            "registry_connection_ref": "mssql_prod",
            "connection_id": "mssql_prod",
            "secret_ref_fingerprint": canonical_fingerprint(
                {
                    "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                    "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                }
            ),
            "fields": ["uri"],
        }
    ]
    rendered = json.dumps(diagnostics)
    assert "dpone-airflow-connection-bridge" not in rendered
    assert "AIRFLOW_CONN_MSSQL_PROD" not in rendered
    assert "/run/secrets/dpone/airflow-connections" not in rendered
    checks = {check["code"]: check for check in bridge["checks"]}
    assert checks["DPONE_AIRFLOW_CONNECTION_SECRET_VOLUME_OPERATOR"]["status"] == "passed"
    assert checks["DPONE_AIRFLOW_CONNECTION_DEFERRABLE_CLEANUP_POLICY"]["status"] == "failed"
    assert "cleanup_policy: retain" in checks["DPONE_AIRFLOW_CONNECTION_DEFERRABLE_CLEANUP_POLICY"]["message"]
    assert "mssql+pymssql://" not in json.dumps(diagnostics)
    assert diagnostics["parse_side_effects"]["airflow_connections"] is False
    assert diagnostics["parse_side_effects"]["kubernetes"] is False
    assert diagnostics["parse_side_effects"]["vault"] is False
