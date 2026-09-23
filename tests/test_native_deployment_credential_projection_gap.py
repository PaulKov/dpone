"""Synthetic regressions originating as red credential-delivery gap tests.

These tests exposed failures before implementation. They
exercise existing producers and the real pure Pod composer, without patching
provider behavior, reading credentials, or contacting Airflow/Kubernetes/SQL.
They are not live certification or proof that external integration is complete.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from typing import Any

import pytest
from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError, init_fetch_context_from_payload
from dpone_airflow_pack.init_fetch_pod import compose_init_fetch_operator_kwargs
from dpone_airflow_pack.pack_task_runtime import runtime_operator_kwargs

from dpone.readiness.airflow_connection_bridge_report import airflow_connection_bridge_report
from dpone.readiness.airflow_connection_runtime_registry import runtime_connection_snapshots
from dpone.readiness.dbt_airflow_execution_pack import DbtAirflowExecutionPackBuilder
from tests.test_airflow_provider_init_fetch_execution import XCOM_IMAGE, _v2_payload
from tests.test_dbt_runtime_execution import _v2_pack

_CONTROL_REF = "workspace_control"
_TARGET_REGISTRY_REF = "warehouse_runtime"
_CONTROL_REGISTRY_REF = "control_runtime"


def _registry(*, shared_connection: bool = False) -> dict[str, Any]:
    """Use aliases distinct from both source connection IDs and logical refs."""

    return {
        "schema": "dpone.connection-registry.v1",
        "environment": "prod",
        "connections": {
            _TARGET_REGISTRY_REF: _entry("sql_target_login", "warehouse"),
            _CONTROL_REGISTRY_REF: _entry("sql_target_login" if shared_connection else "sql_control_login", "control"),
        },
    }


def _entry(connection_id: str, database: str) -> dict[str, Any]:
    return {
        "type": "mssql",
        "connection": {"database": database},
        "credentials": {
            "resolver": "airflow_connection",
            "connection_id": connection_id,
            "execution_mode": "operator_bridge",
        },
    }


def _snapshots(registry: dict[str, Any]) -> dict[str, bytes]:
    return runtime_connection_snapshots(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {
                "warehouse": {"connection_ref": _TARGET_REGISTRY_REF},
                _CONTROL_REF: {"connection_ref": _CONTROL_REGISTRY_REF},
            },
        },
        connection_registry=registry,
        credential_runtime={
            "schema": "dpone.credential-runtime.v1",
            "environment": "prod",
            "runtime": {"mode": "airflow_dev"},
        },
    )


def _snapshot_subject(snapshots: dict[str, bytes]) -> dict[str, str]:
    return {name: "sha256:" + hashlib.sha256(payload).hexdigest() for name, payload in snapshots.items()}


@pytest.mark.parametrize("registry_ref", [_TARGET_REGISTRY_REF, _CONTROL_REGISTRY_REF])
def test_source_connection_key_change_changes_published_runtime_authority(registry_ref: str) -> None:
    """A new Secret source must never retain the old credential-context subject."""

    original = _registry()
    changed = deepcopy(original)
    changed["connections"][registry_ref]["credentials"]["connection_id"] = "replacement_login"

    from dpone_airflow_pack.credential_projection_contract import canonical_projection_bytes

    from dpone.readiness.airflow_credential_projection import build_credential_projection
    from tests.test_airflow_credential_projection import projection_case

    def subject(registry: dict[str, Any]) -> dict[str, str]:
        snapshots = _snapshots(registry)
        inputs = projection_case()
        inputs.update(source_registry=registry, snapshots=snapshots)
        projection = build_credential_projection(**inputs)
        assert projection is not None
        return _snapshot_subject(
            {**snapshots, "credential_projection": canonical_projection_bytes(projection.to_dict())}
        )

    assert subject(original) != subject(changed), (
        "Published runtime authority loses source connection_id and therefore exact Secret key provenance"
    )


@pytest.mark.parametrize("shared_connection", [False, True])
def test_bridge_source_mapping_preserves_aliases_and_shared_secret_key(shared_connection: bool) -> None:
    """The existing source planner already knows the mapping that publication loses."""

    registry = _registry(shared_connection=shared_connection)
    source = deepcopy(registry)
    report = airflow_connection_bridge_report(registry, {ref: ref for ref in registry["connections"]})
    entries = {item["registry_connection_ref"]: item for item in report["projection"]["connections"]}

    assert entries[_TARGET_REGISTRY_REF]["secret_key"] == "AIRFLOW_CONN_SQL_TARGET_LOGIN"
    expected_control_key = "AIRFLOW_CONN_SQL_TARGET_LOGIN" if shared_connection else "AIRFLOW_CONN_SQL_CONTROL_LOGIN"
    assert entries[_CONTROL_REGISTRY_REF]["secret_key"] == expected_control_key
    assert entries[_TARGET_REGISTRY_REF]["mount_path"] != entries[_CONTROL_REGISTRY_REF]["mount_path"]
    assert registry == source
    assert "secret_values" in report["projection"] and report["projection"]["secret_values"] is False
    assert all("password" not in json.dumps(payload) for payload in (source, report))


def _native_case() -> tuple[dict[str, Any], dict[str, Any], Any]:
    """Compose genuine native v2 pack bytes with exact synthetic snapshot descriptors."""

    execution = _v2_pack()
    pack = DbtAirflowExecutionPackBuilder().build(
        workflow_id=execution.workflow_id,
        execution_pack=execution,
        runtime_payload_ids=(),
        xcom_sidecar_image=XCOM_IMAGE,
        pool="dbt",
    )
    workload_id = pack["workload"]["workload_id"]
    payload = _v2_payload()
    snapshots = _snapshots(_registry())
    for name, content in snapshots.items():
        payload[name]["sha256"] = "sha256:" + hashlib.sha256(content).hexdigest()
        payload[name]["bytes"] = len(content)
    pack_bytes = json.dumps(pack, sort_keys=True, separators=(",", ":")).encode()
    artifact = payload["workload_packs"][0]
    artifact.update(
        id=workload_id,
        artifact_ref=artifact["artifact_ref"].replace("orders.json", f"{workload_id}.json"),
        sha256="sha256:" + hashlib.sha256(pack_bytes).hexdigest(),
        bytes=len(pack_bytes),
        pack_fingerprint=pack["pack_fingerprint"],
    )
    kwargs = runtime_operator_kwargs(pack, strict_provider_execution=True)
    kwargs["env_vars"]["DPONE_DBT_WORKSPACE_AUTHORITY_CONNECTION_REF"] = _CONTROL_REF
    return pack, kwargs, init_fetch_context_from_payload(payload)


def test_native_pack_does_not_embed_secret_transport() -> None:
    """Authoring stays portable while credentials belong to the deployment."""

    pack, kwargs, _context = _native_case()
    assert pack["connection_projection"] == {}
    assert not any(name.startswith("AIRFLOW_CONN_") for name in kwargs["env_vars"])
    assert set(pack["provider_execution"]["pod_spec"]["spec"]["containers"][0]) == {"name"}


def test_native_workspace_requires_deployment_credential_closure_before_pod_creation() -> None:
    """A projected control ref without authenticated mount authority must fail closed.

    Today composition returns a Pod with neither target nor control credential
    files. Expecting failure deliberately avoids inventing Secret keys from an
    alias or injecting a proposed future schema into this regression test.
    """

    pack, kwargs, context = _native_case()
    with pytest.raises(InitFetchProviderError, match="credential.*projection|projection.*credential"):
        compose_init_fetch_operator_kwargs(
            pack=pack,
            kwargs=kwargs,
            context=context,
            workload_id=pack["workload"]["workload_id"],
            execution_kind="runtime",
            execution_scope="workload",
            hook_execution="externalized",
        )
