from __future__ import annotations

import re
from collections.abc import Mapping
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import dpone_airflow_pack.pack_tasks as pack_tasks
import pytest
from dpone_airflow_pack.init_fetch_contract import (
    InitFetchProviderError,
    init_fetch_context_from_payload,
)
from dpone_airflow_pack.init_fetch_pod import compose_init_fetch_operator_kwargs
from dpone_airflow_pack.operators import (
    AirflowConnectionSecretVolumeKubernetesPodOperator,
    PinnedXComSidecarKubernetesPodOperator,
)
from dpone_airflow_pack.pack_identity import compute_pack_fingerprint
from dpone_airflow_pack.pack_task_helpers import safe_overrides
from dpone_airflow_pack.pack_task_runtime import operator_class, runtime_operator_kwargs
from dpone_airflow_pack.provider_execution import require_provider_execution
from dpone_airflow_pack.provider_execution_contract import (
    MAX_EXECUTION_TIMEOUT_SECONDS,
    WORKLOAD_ID_METADATA_KEY,
    kubernetes_label_value,
    provider_execution_json_schema,
)
from dpone_airflow_pack.xcom_sidecar import (
    XCOM_SIDECAR_CONTAINER_NAME,
    XComSidecarRuntimeConfig,
)
from jsonschema import Draft202012Validator

from dpone.gitops.airflow_compact_runtime import (
    compact_kpo_kwargs,
    compact_provider_execution,
)
from dpone.gitops.schema_contracts import get_gitops_schema_contract
from dpone.readiness.dbt_airflow_execution_pack import DbtAirflowExecutionPackBuilder
from tests.test_airflow_provider_init_fetch_execution import (
    IMAGE_REF,
    PACK_FINGERPRINT,
    PACK_SHA256,
    XCOM_IMAGE,
    _mapping_plan,
    _owned_hook_step,
    _v2_payload,
)
from tests.test_airflow_provider_init_fetch_execution import (
    _strict_pack as baseline_strict_pack,
)


def _strict_pack() -> dict[str, Any]:
    pack = baseline_strict_pack()
    pack["xcom"] = {"sidecar_image": XCOM_IMAGE}
    return pack


def _context() -> Any:
    return init_fetch_context_from_payload(_v2_payload())


def test_strict_preflight_requires_independently_verified_pack_identity() -> None:
    with pytest.raises(InitFetchProviderError) as exc_info:
        pack_tasks._preflight_delivery_pack(
            _strict_pack(),
            provenance={"pack_sha256": PACK_SHA256},
            delivery_context=_context(),
            expected_workload_id="orders",
        )

    assert exc_info.value.code == "DPONE_INIT_FETCH_PACK_MIGRATION_REQUIRED"


def test_strict_preflight_rejects_pack_mutated_after_identity_verification() -> None:
    pack = _strict_pack()
    pack["steps"] = [{"name": "unexpected"}]

    with pytest.raises(InitFetchProviderError) as exc_info:
        pack_tasks._preflight_delivery_pack(
            pack,
            provenance={
                "pack_sha256": PACK_SHA256,
                "verified_pack_fingerprint": PACK_FINGERPRINT,
            },
            delivery_context=_context(),
            expected_workload_id="orders",
        )

    assert exc_info.value.code == "DPONE_CACHE_CHECKSUM_MISMATCH"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("task_executor", "KubernetesExecutor"),
        ("deferrable", True),
        ("on_finish_action", "keep_pod"),
        ("get_logs", False),
        ("logging_interval_seconds", 30),
    ],
)
def test_strict_execution_rejects_non_asset_static_knobs(field: str, value: object) -> None:
    pack = _strict_pack()
    pack["airflow"] = {"execution": {field: value}}

    with pytest.raises(InitFetchProviderError) as exc_info:
        runtime_operator_kwargs(pack, strict_provider_execution=True)

    assert exc_info.value.code == "DPONE_INIT_FETCH_RESERVED_COLLISION"


def test_strict_execution_keeps_assets_bounded_and_fixes_task_policy() -> None:
    pack = _strict_pack()
    pack["airflow"] = {
        "execution": {
            "inlets": [{"uri": "dpone://source/orders"}],
            "outlets": [{"uri": "dpone://sink/orders"}],
        }
    }

    kwargs = compose_init_fetch_operator_kwargs(
        pack=pack,
        kwargs=runtime_operator_kwargs(pack, strict_provider_execution=True),
        context=_context(),
        workload_id="orders",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )

    assert kwargs["retries"] == 0
    assert kwargs["trigger_rule"] == "all_success"
    assert kwargs["do_xcom_push"] is True
    assert kwargs["get_logs"] is True
    assert kwargs["on_finish_action"] == "delete_succeeded_pod"
    assert kwargs["annotations"][WORKLOAD_ID_METADATA_KEY] == "orders"
    assert kwargs["full_pod_spec"]["metadata"]["annotations"][WORKLOAD_ID_METADATA_KEY] == "orders"
    for field in ("executor", "deferrable", "logging_interval"):
        assert field not in kwargs


def test_strict_execution_uses_certified_dag_default_retries() -> None:
    pack = _strict_pack()
    pack["provider_execution"]["retry_authority"] = {
        "schema": "dpone.airflow-retry-authority.v1",
        "mode": "postgres_xmin_initial_mssql_target_atomic_v1",
        "max_task_retries": 3,
    }
    dag = SimpleNamespace(default_args={"retries": 2})

    kwargs = compose_init_fetch_operator_kwargs(
        pack=pack,
        kwargs=runtime_operator_kwargs(
            pack,
            dag=dag,
            strict_provider_execution=True,
        ),
        context=_context(),
        workload_id="orders",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )

    assert kwargs["retries"] == 2


def test_strict_scheduler_overrides_admit_only_pool_and_retry_count() -> None:
    assert safe_overrides(
        {"pool": "dpone_crm_archive_source", "retries": 2},
        strict=True,
    ) == {"pool": "dpone_crm_archive_source", "retries": 2}


@pytest.mark.parametrize("pool", ["", " ", True, "x" * 257])
def test_strict_scheduler_override_rejects_invalid_pool(pool: object) -> None:
    with pytest.raises(InitFetchProviderError) as exc_info:
        safe_overrides({"pool": pool}, strict=True)

    assert exc_info.value.code == "DPONE_INIT_FETCH_RESERVED_COLLISION"


def test_provider_execution_schema_has_one_package_authority() -> None:
    root_schema = get_gitops_schema_contract("gitops.airflow_pack").schema

    assert root_schema["properties"]["provider_execution"] == provider_execution_json_schema()


def test_provider_execution_preserves_only_valid_airflow_pool() -> None:
    pack = _strict_pack()
    pack["provider_execution"]["kpo_kwargs"]["pool"] = "dpone_dbt__pricing__runtime"

    projection = require_provider_execution(pack)

    assert projection.kpo_kwargs["pool"] == "dpone_dbt__pricing__runtime"
    invalid = deepcopy(pack)
    invalid["provider_execution"]["kpo_kwargs"]["pool"] = " "
    assert tuple(Draft202012Validator(provider_execution_json_schema()).iter_errors(invalid["provider_execution"]))
    with pytest.raises(InitFetchProviderError):
        require_provider_execution(invalid)


@pytest.mark.parametrize("seconds", [1, MAX_EXECUTION_TIMEOUT_SECONDS])
def test_provider_execution_timeout_is_bounded_on_wire_and_materialized_as_timedelta(
    seconds: int,
) -> None:
    pack = _strict_pack()
    pack["provider_execution"]["kpo_kwargs"]["execution_timeout_seconds"] = seconds

    assert not tuple(Draft202012Validator(provider_execution_json_schema()).iter_errors(pack["provider_execution"]))
    projection = require_provider_execution(pack)
    kwargs = runtime_operator_kwargs(pack, strict_provider_execution=True)

    assert projection.kpo_kwargs["execution_timeout_seconds"] == seconds
    assert "execution_timeout" not in kwargs
    assert "execution_timeout_seconds" not in kwargs


@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        MAX_EXECUTION_TIMEOUT_SECONDS + 1,
        True,
        1.5,
        float("nan"),
        float("inf"),
        float("-inf"),
        "900",
    ],
)
def test_provider_execution_schema_and_runtime_reject_invalid_timeout(value: object) -> None:
    pack = _strict_pack()
    pack["provider_execution"]["kpo_kwargs"]["execution_timeout_seconds"] = value

    assert tuple(Draft202012Validator(provider_execution_json_schema()).iter_errors(pack["provider_execution"]))
    with pytest.raises(InitFetchProviderError) as exc_info:
        require_provider_execution(pack)

    assert exc_info.value.code == "DPONE_INIT_FETCH_PROVIDER_EXECUTION_INVALID"


def test_provider_execution_normalizes_json_schema_integer_float() -> None:
    pack = _strict_pack()
    pack["provider_execution"]["kpo_kwargs"]["execution_timeout_seconds"] = 1.0

    assert not tuple(Draft202012Validator(provider_execution_json_schema()).iter_errors(pack["provider_execution"]))
    projection = require_provider_execution(pack)

    assert projection.kpo_kwargs["execution_timeout_seconds"] == 1
    assert isinstance(projection.kpo_kwargs["execution_timeout_seconds"], int)


def test_provider_execution_timeout_reaches_final_strict_operator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecordingOperator:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

        def __rshift__(self, other: Any) -> Any:
            return other

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda *_args, **_kwargs: RecordingOperator)
    pack = _strict_pack()
    pack["provider_execution"]["kpo_kwargs"]["execution_timeout_seconds"] = 900

    tasks = pack_tasks._build_tasks_from_loaded_pack(
        pack,
        dag=SimpleNamespace(),
        operator_overrides=None,
        node=None,
        task_group=None,
        delivery_context=_context(),
    )

    assert tasks["dpone_runtime"].kwargs["execution_timeout"] == timedelta(seconds=900)
    assert "execution_timeout_seconds" not in tasks["dpone_runtime"].kwargs


def test_provider_execution_accepts_bounded_task_executor() -> None:
    pack = _strict_pack()
    pack["provider_execution"]["kpo_kwargs"]["executor"] = "KubernetesExecutor"

    assert not tuple(Draft202012Validator(provider_execution_json_schema()).iter_errors(pack["provider_execution"]))
    projection = require_provider_execution(pack)

    assert projection.kpo_kwargs["executor"] == "KubernetesExecutor"


@pytest.mark.parametrize("value", ["", "  ", 7, None, "Kubernetes\nExecutor"])
def test_provider_execution_rejects_invalid_executor(value: object) -> None:
    pack = _strict_pack()
    pack["provider_execution"]["kpo_kwargs"]["executor"] = value

    with pytest.raises(InitFetchProviderError) as exc_info:
        require_provider_execution(pack)

    assert exc_info.value.code == "DPONE_INIT_FETCH_PROVIDER_EXECUTION_INVALID"


def test_provider_executor_reaches_final_strict_operator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecordingOperator:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

        def __rshift__(self, other: Any) -> Any:
            return other

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda *_args, **_kwargs: RecordingOperator)
    pack = _strict_pack()
    pack["provider_execution"]["kpo_kwargs"]["executor"] = "KubernetesExecutor"

    tasks = pack_tasks._build_tasks_from_loaded_pack(
        pack,
        dag=SimpleNamespace(),
        operator_overrides=None,
        node=None,
        task_group=None,
        delivery_context=_context(),
    )

    assert tasks["dpone_runtime"].kwargs["executor"] == "KubernetesExecutor"


def test_compact_kpo_kwargs_project_workload_task_executor() -> None:
    workload = SimpleNamespace(
        workload_id="orders",
        effective_config={
            "airflow": {"execution": {"task_executor": "KubernetesExecutor"}},
        },
    )
    runtime_manifest = SimpleNamespace(path="runtime/manifest.json")

    kpo_kwargs = compact_kpo_kwargs(workload, runtime_manifest=runtime_manifest)
    provider_execution = compact_provider_execution(kpo_kwargs=kpo_kwargs, pod_spec=None)

    assert kpo_kwargs["executor"] == "KubernetesExecutor"
    assert provider_execution["kpo_kwargs"]["executor"] == "KubernetesExecutor"


def test_compact_kpo_kwargs_without_task_executor_stay_executor_free() -> None:
    workload = SimpleNamespace(workload_id="orders", effective_config={})
    runtime_manifest = SimpleNamespace(path="runtime/manifest.json")

    kpo_kwargs = compact_kpo_kwargs(workload, runtime_manifest=runtime_manifest)
    provider_execution = compact_provider_execution(kpo_kwargs=kpo_kwargs, pod_spec=None)

    assert "executor" not in kpo_kwargs
    assert "executor" not in provider_execution["kpo_kwargs"]


def test_provider_execution_executor_length_boundary() -> None:
    accepted = _strict_pack()
    accepted["provider_execution"]["kpo_kwargs"]["executor"] = "e" * 253
    assert require_provider_execution(accepted).kpo_kwargs["executor"] == "e" * 253

    rejected = _strict_pack()
    rejected["provider_execution"]["kpo_kwargs"]["executor"] = "e" * 254
    with pytest.raises(InitFetchProviderError) as exc_info:
        require_provider_execution(rejected)
    assert exc_info.value.code == "DPONE_INIT_FETCH_PROVIDER_EXECUTION_INVALID"


def test_provider_executor_reaches_pre_hook_and_mapped_operators(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pack = _strict_pack()
    pack["provider_execution"]["kpo_kwargs"]["executor"] = "KubernetesExecutor"
    pack["steps"] = [
        _owned_hook_step(
            name="prepare_orders",
            hook_id="refresh_orders",
            selector="dbo.orders",
        )
    ]
    pack["mapping_plan"] = _mapping_plan()
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    deployment_payload = _v2_payload()
    deployment_payload["workload_packs"][0]["pack_fingerprint"] = pack["pack_fingerprint"]
    context = init_fetch_context_from_payload(deployment_payload)

    class RecordingOperator:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

        def __rshift__(self, other: Any) -> Any:
            return other

        @classmethod
        def partial(cls, **kwargs: Any) -> Any:
            return SimpleNamespace(expand=lambda **expanded: cls(**kwargs, expanded_env_vars=expanded["env_vars"]))

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda *_args, **_kwargs: RecordingOperator)
    tasks = pack_tasks._build_dpone_gitops_task_group_from_loaded_pack(
        pack,
        provenance={
            "pack_sha256": PACK_SHA256,
            "verified_pack_fingerprint": pack["pack_fingerprint"],
        },
        dag=SimpleNamespace(),
        operator_overrides=None,
        node=None,
        task_group=None,
        delivery_context=context,
    )

    assert tasks["prepare_orders"].kwargs["executor"] == "KubernetesExecutor"
    assert tasks["dpone_runtime"].kwargs["executor"] == "KubernetesExecutor"


def test_dbt_execution_pack_projects_declared_executor() -> None:
    execution_pack = SimpleNamespace(
        to_dict=lambda: {},
        timeout_seconds=3600,
        adapter_runtime=SimpleNamespace(airflow_execution_timeout_seconds=lambda timeout: timeout),
    )

    pack = DbtAirflowExecutionPackBuilder().build(
        workflow_id="daily",
        execution_pack=execution_pack,  # type: ignore[arg-type]
        runtime_payload_ids=("dbt_project",),
        xcom_sidecar_image=XCOM_IMAGE,
        pool="dpone_dbt",
        executor="KubernetesExecutor",
    )

    assert pack["provider_execution"]["kpo_kwargs"]["executor"] == "KubernetesExecutor"
    assert require_provider_execution(pack).kpo_kwargs["executor"] == "KubernetesExecutor"


def test_dbt_execution_pack_without_executor_stays_executor_free() -> None:
    execution_pack = SimpleNamespace(
        to_dict=lambda: {},
        timeout_seconds=3600,
        adapter_runtime=SimpleNamespace(airflow_execution_timeout_seconds=lambda timeout: timeout),
    )

    pack = DbtAirflowExecutionPackBuilder().build(
        workflow_id="daily",
        execution_pack=execution_pack,  # type: ignore[arg-type]
        runtime_payload_ids=("dbt_project",),
        xcom_sidecar_image=XCOM_IMAGE,
        pool="dpone_dbt",
    )

    assert "executor" not in pack["provider_execution"]["kpo_kwargs"]


def test_compact_provider_timeout_remains_bound_to_pack_fingerprint() -> None:
    workload = SimpleNamespace(workload_id="orders", effective_config={})
    runtime_manifest = SimpleNamespace(path="runtime/manifest.json")
    kpo_kwargs = compact_kpo_kwargs(workload, runtime_manifest=runtime_manifest)
    kpo_kwargs["execution_timeout_seconds"] = 900
    provider_execution = compact_provider_execution(kpo_kwargs=kpo_kwargs, pod_spec=None)
    pack = _strict_pack()
    pack["provider_execution"] = provider_execution
    changed = deepcopy(pack)
    changed["provider_execution"]["kpo_kwargs"]["execution_timeout_seconds"] = 901

    assert provider_execution["kpo_kwargs"]["execution_timeout_seconds"] == 900
    assert compute_pack_fingerprint(pack) != compute_pack_fingerprint(changed)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_labels",
        "too_many_node_selectors",
        "unknown_toleration_field",
        "nested_resource_value",
    ],
)
def test_provider_execution_schema_and_runtime_reject_same_structural_boundaries(
    mutation: str,
) -> None:
    pack = _strict_pack()
    projection = pack["provider_execution"]
    spec = projection["pod_spec"]["spec"]
    if mutation == "missing_labels":
        projection["kpo_kwargs"].pop("labels")
    elif mutation == "too_many_node_selectors":
        spec["nodeSelector"] = {f"node-{index}": "worker" for index in range(65)}
    elif mutation == "unknown_toleration_field":
        spec["tolerations"] = [
            {
                "key": "dedicated",
                "operator": "Equal",
                "value": "warehouse",
                "effect": "NoSchedule",
                "command": "forbidden",
            }
        ]
    else:
        spec["containers"][0]["resources"] = {"requests": {"cpu": {"value": "1"}}}

    assert tuple(Draft202012Validator(provider_execution_json_schema()).iter_errors(projection))
    with pytest.raises(InitFetchProviderError):
        require_provider_execution(pack)


@pytest.mark.parametrize(
    ("workload_id", "preserved"),
    [
        ("a" * 63, True),
        ("a" * 64, False),
        ("a" * 128, False),
        ("orders_", False),
        ("orders-", False),
    ],
)
def test_logical_workload_id_has_deterministic_kubernetes_label_projection(
    workload_id: str,
    preserved: bool,
) -> None:
    projected = kubernetes_label_value(workload_id)

    assert (projected == workload_id) is preserved
    assert len(projected) <= 63
    assert re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?", projected)
    assert projected == kubernetes_label_value(workload_id)


def test_long_workload_ids_with_same_prefix_have_distinct_provider_projections() -> None:
    first_id = ("orders" * 20)[:127] + "a"
    second_id = ("orders" * 20)[:127] + "b"
    first = kubernetes_label_value(first_id)
    second = kubernetes_label_value(second_id)

    assert first != second
    for workload_id, expected_label in ((first_id, first), (second_id, second)):
        workload = SimpleNamespace(workload_id=workload_id, effective_config={})
        runtime_manifest = SimpleNamespace(path="runtime/manifest.json")
        kpo_kwargs = compact_kpo_kwargs(workload, runtime_manifest=runtime_manifest)
        provider_execution = compact_provider_execution(kpo_kwargs=kpo_kwargs, pod_spec=None)
        pack = _strict_pack()
        pack["workload"]["workload_id"] = workload_id
        pack["provider_execution"] = provider_execution

        projection = require_provider_execution(pack, expected_workload_id=workload_id)

        assert projection.workload_id == workload_id
        assert projection.kpo_kwargs["labels"][WORKLOAD_ID_METADATA_KEY] == expected_label


@pytest.mark.parametrize(
    "field",
    [
        "task_id",
        "name",
        "labels",
        "env_vars",
        "namespace",
        "service_account_name",
        "node_selector",
        "affinity",
        "tolerations",
        "topology_spread_constraints",
        "container_resources",
        "do_xcom_push",
        "image",
        "cmds",
        "arguments",
        "full_pod_spec",
        "trigger_rule",
        "xcom_sidecar",
        "airflow_connection_projection",
        "secrets",
        "env_from",
        "security_context",
        "container_security_context",
        "image_pull_secrets",
        "volumes",
        "volume_mounts",
        "init_containers",
    ],
)
def test_strict_operator_overrides_reject_every_static_authority(
    field: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    constructed: list[dict[str, Any]] = []

    class RecordingOperator:
        def __init__(self, **kwargs: Any) -> None:
            constructed.append(kwargs)

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda *_args, **_kwargs: RecordingOperator)

    with pytest.raises(InitFetchProviderError) as exc_info:
        pack_tasks._build_tasks_from_loaded_pack(
            _strict_pack(),
            dag=SimpleNamespace(),
            operator_overrides={field: object()},
            node=None,
            task_group=None,
            delivery_context=_context(),
        )

    assert exc_info.value.code == "DPONE_INIT_FETCH_RESERVED_COLLISION"
    assert constructed == []


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_workload_id",
        "expected_workload_mismatch",
        "task_id_mismatch",
        "name_mismatch",
        "missing_workload_label",
        "workload_label_mismatch",
    ],
)
def test_strict_identity_is_exact_and_has_no_legacy_fallback(mutation: str) -> None:
    pack = _strict_pack()
    expected_workload_id = "orders"
    projection = pack["provider_execution"]["kpo_kwargs"]
    if mutation == "missing_workload_id":
        pack["workload"] = {"id": "orders"}
    elif mutation == "expected_workload_mismatch":
        expected_workload_id = "customers"
    elif mutation == "task_id_mismatch":
        projection["task_id"] = "customers__dpone_runtime"
    elif mutation == "name_mismatch":
        projection["name"] = "dpone-customers"
    elif mutation == "missing_workload_label":
        projection["labels"].pop("dpone.dev/workload-id")
    else:
        projection["labels"]["dpone.dev/workload-id"] = "customers"

    with pytest.raises(InitFetchProviderError) as exc_info:
        require_provider_execution(pack, expected_workload_id=expected_workload_id)

    assert exc_info.value.code == "DPONE_INIT_FETCH_PROVIDER_EXECUTION_INVALID"


@pytest.mark.parametrize(
    "field,value",
    [
        ("dpone.dev/managed-by", "manual"),
        ("dpone.dev/runtime-contract", "legacy"),
    ],
)
def test_provider_execution_accepts_legacy_runtime_authority_labels_for_provider_override(
    field: str,
    value: str,
) -> None:
    pack = _strict_pack()
    pack["provider_execution"]["kpo_kwargs"]["labels"][field] = value

    projection = require_provider_execution(pack, expected_workload_id="orders")

    assert projection.kpo_kwargs["labels"][field] == value


def test_strict_connection_projection_rejects_incomplete_bridge_before_operator_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pack = _strict_pack()
    pack["connection_projection"] = {
        "mode": "kubernetes_secret_volume",
        "connections": [{"connection_id": "warehouse"}],
    }
    constructed: list[dict[str, Any]] = []

    class RecordingOperator:
        def __init__(self, **kwargs: Any) -> None:
            constructed.append(kwargs)

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda *_args, **_kwargs: RecordingOperator)

    with pytest.raises(InitFetchProviderError) as exc_info:
        pack_tasks._build_tasks_from_loaded_pack(
            pack,
            dag=SimpleNamespace(),
            operator_overrides=None,
            node=None,
            task_group=None,
            delivery_context=_context(),
        )

    assert exc_info.value.code == "DPONE_INIT_FETCH_CONNECTION_BRIDGE_INVALID"
    assert constructed == []


def test_strict_unsafe_airflow_env_projection_remains_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pack = _strict_pack()
    pack["connection_projection"] = {
        "mode": "unsafe_airflow_env",
        "connection_ids": ["warehouse"],
    }
    constructed: list[dict[str, Any]] = []

    class RecordingOperator:
        def __init__(self, **kwargs: Any) -> None:
            constructed.append(kwargs)

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda *_args, **_kwargs: RecordingOperator)

    with pytest.raises(InitFetchProviderError) as exc_info:
        pack_tasks._build_tasks_from_loaded_pack(
            pack,
            dag=SimpleNamespace(),
            operator_overrides=None,
            node=None,
            task_group=None,
            delivery_context=_context(),
        )

    assert exc_info.value.code == "DPONE_INIT_FETCH_CONNECTION_BRIDGE_INVALID"
    assert constructed == []


def test_strict_closed_connection_bridge_selects_secret_volume_operator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pack = _strict_pack()
    projection = {
        "mode": "kubernetes_secret_volume",
        "secret_name": "dpone-airflow-connection-bridge",
        "mount_path": "/run/secrets/dpone/airflow-connections",
        "payload_format": "airflow_connection_uri",
        "secret_values": False,
        "cleanup_policy": "after_execute",
        "connections": [
            {
                "connection_ref": "warehouse",
                "registry_connection_ref": "warehouse",
                "connection_id": "warehouse",
                "secret_key": "AIRFLOW_CONN_WAREHOUSE",
                "mount_path": "/run/secrets/dpone/airflow-connections/warehouse",
                "fields": {"uri": "uri"},
            }
        ],
    }
    pack["connection_projection"] = projection
    selected: list[type[Any]] = []
    captured: list[dict[str, Any]] = []

    class RecordingOperator:
        def __init__(self, **kwargs: Any) -> None:
            captured.append(kwargs)

    def _capture(pack_surface: Mapping[str, Any]) -> type[Any]:
        chosen = operator_class(pack_surface)
        selected.append(chosen)
        return RecordingOperator

    monkeypatch.setattr(pack_tasks, "_operator_class", _capture)

    tasks = pack_tasks._build_tasks_from_loaded_pack(
        pack,
        dag=SimpleNamespace(),
        operator_overrides=None,
        node=None,
        task_group=None,
        delivery_context=_context(),
    )

    assert selected == [AirflowConnectionSecretVolumeKubernetesPodOperator]
    assert captured[0]["airflow_connection_projection"] == projection
    assert "dpone_runtime" in tasks


@pytest.mark.parametrize(
    ("xcom", "code"),
    [
        ({}, "DPONE_INIT_FETCH_PACK_MIGRATION_REQUIRED"),
        ({"sidecar_image": "alpine:3.23.4"}, "DPONE_INIT_FETCH_RESERVED_COLLISION"),
        (
            {"sidecar_image": "registry.example/airflow/xcom:3.23.4"},
            "DPONE_INIT_FETCH_RESERVED_COLLISION",
        ),
    ],
)
def test_strict_xcom_sidecar_must_be_present_and_digest_pinned_before_operator_creation(
    xcom: dict[str, object],
    code: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pack = _strict_pack()
    pack["xcom"] = xcom
    constructed: list[dict[str, Any]] = []

    class RecordingOperator:
        def __init__(self, **kwargs: Any) -> None:
            constructed.append(kwargs)

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda *_args, **_kwargs: RecordingOperator)

    with pytest.raises(InitFetchProviderError) as exc_info:
        pack_tasks._build_tasks_from_loaded_pack(
            pack,
            dag=SimpleNamespace(),
            operator_overrides=None,
            node=None,
            task_group=None,
            delivery_context=_context(),
        )

    assert exc_info.value.code == code
    assert constructed == []


def test_strict_xcom_sidecar_is_passed_to_the_runtime_operator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecordingOperator:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

        def __rshift__(self, other: Any) -> Any:
            return other

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda *_args, **_kwargs: RecordingOperator)

    tasks = pack_tasks._build_tasks_from_loaded_pack(
        _strict_pack(),
        dag=SimpleNamespace(),
        operator_overrides=None,
        node=None,
        task_group=None,
        delivery_context=_context(),
    )

    runtime = tasks["dpone_runtime"]
    assert runtime.kwargs["xcom_sidecar"] == XComSidecarRuntimeConfig(
        image=XCOM_IMAGE,
        strict_runtime_image=IMAGE_REF,
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_sidecar",
        "duplicate_sidecar",
        "extra_base_container",
        "wrong_base_image",
        "missing_init",
        "extra_init",
        "wrong_init_image",
    ],
)
def test_final_strict_pod_rejects_unapproved_container_topology(
    mutation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pod = {
        "spec": {
            "containers": [
                {"name": "base", "image": IMAGE_REF},
                {"name": XCOM_SIDECAR_CONTAINER_NAME, "image": "mutable-before-provider-pin"},
            ],
            "initContainers": [{"name": "dpone-runtime-init-fetch", "image": IMAGE_REF}],
        }
    }
    if mutation == "missing_sidecar":
        pod["spec"]["containers"].pop()
    elif mutation == "duplicate_sidecar":
        pod["spec"]["containers"].append({"name": XCOM_SIDECAR_CONTAINER_NAME, "image": XCOM_IMAGE})
    elif mutation == "extra_base_container":
        pod["spec"]["containers"].append({"name": "debug", "image": IMAGE_REF})
    elif mutation == "wrong_base_image":
        pod["spec"]["containers"][0]["image"] = "registry.example/attacker@sha256:" + "0" * 64
    elif mutation == "missing_init":
        pod["spec"]["initContainers"] = []
    elif mutation == "extra_init":
        pod["spec"]["initContainers"].append({"name": "debug", "image": IMAGE_REF})
    else:
        pod["spec"]["initContainers"][0]["image"] = "registry.example/attacker@sha256:" + "0" * 64

    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    monkeypatch.setattr(
        base_operator,
        "build_pod_request_obj",
        lambda _self, context=None: deepcopy(pod),
    )
    operator = object.__new__(PinnedXComSidecarKubernetesPodOperator)
    operator.do_xcom_push = True
    operator.env_vars = None
    operator.xcom_sidecar = XComSidecarRuntimeConfig(
        image=XCOM_IMAGE,
        strict_runtime_image=IMAGE_REF,
    )

    with pytest.raises(InitFetchProviderError) as exc_info:
        operator.build_pod_request_obj(context={})

    assert exc_info.value.code == "DPONE_INIT_FETCH_RESERVED_COLLISION"


def test_final_strict_pod_pins_and_accepts_exact_provider_topology(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pod = {
        "spec": {
            "containers": [
                {"name": "base", "image": IMAGE_REF},
                {"name": XCOM_SIDECAR_CONTAINER_NAME, "image": "mutable-before-provider-pin"},
            ],
            "initContainers": [{"name": "dpone-runtime-init-fetch", "image": IMAGE_REF}],
        }
    }
    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    monkeypatch.setattr(
        base_operator,
        "build_pod_request_obj",
        lambda _self, context=None: deepcopy(pod),
    )
    operator = object.__new__(PinnedXComSidecarKubernetesPodOperator)
    operator.do_xcom_push = True
    operator.env_vars = None
    operator.xcom_sidecar = XComSidecarRuntimeConfig(
        image=XCOM_IMAGE,
        strict_runtime_image=IMAGE_REF,
    )

    built = operator.build_pod_request_obj(context={})

    assert built["spec"]["containers"][1]["image"] == XCOM_IMAGE


@pytest.mark.parametrize(
    "field",
    [
        "affinity",
        "topologySpreadConstraints",
        "schedulerName",
        "priorityClassName",
        "unknownNestedField",
    ],
)
def test_provider_execution_v1_rejects_speculative_pod_fields(field: str) -> None:
    pack = _strict_pack()
    pack["provider_execution"]["pod_spec"]["spec"][field] = {} if field != "schedulerName" else "custom"

    with pytest.raises(InitFetchProviderError) as exc_info:
        require_provider_execution(pack)

    assert exc_info.value.code == "DPONE_INIT_FETCH_RESERVED_COLLISION"


@pytest.mark.parametrize(
    "mutation",
    ["unknown_toleration_field", "too_many_tolerations", "invalid_effect", "nested_resource", "unknown_resource"],
)
def test_provider_execution_v1_rejects_unbounded_nested_values(mutation: str) -> None:
    pack = _strict_pack()
    spec = pack["provider_execution"]["pod_spec"]["spec"]
    toleration = {
        "key": "dedicated",
        "operator": "Equal",
        "value": "datawarehouse",
        "effect": "NoSchedule",
    }
    if mutation == "unknown_toleration_field":
        spec["tolerations"] = [{**toleration, "command": "attacker"}]
    elif mutation == "too_many_tolerations":
        spec["tolerations"] = [toleration] * 33
    elif mutation == "invalid_effect":
        spec["tolerations"] = [{**toleration, "effect": "Execute"}]
    elif mutation == "nested_resource":
        spec["containers"][0]["resources"] = {"requests": {"cpu": {"value": "1"}}}
    else:
        spec["containers"][0]["resources"] = {"claims": [{"name": "attacker"}]}

    with pytest.raises(InitFetchProviderError):
        require_provider_execution(pack)


def test_provider_execution_v1_accepts_only_concrete_produced_extensions() -> None:
    pack = _strict_pack()
    spec = pack["provider_execution"]["pod_spec"]["spec"]
    spec["tolerations"] = [
        {
            "key": "dedicated",
            "operator": "Equal",
            "value": "datawarehouse",
            "effect": "NoSchedule",
        }
    ]
    spec["containers"][0]["resources"] = {
        "requests": {"cpu": "1", "memory": "1Gi"},
        "limits": {"cpu": "2", "memory": "2Gi"},
    }

    projection = require_provider_execution(pack)

    assert projection.workload_id == "orders"
    assert projection.pod_spec["spec"] == spec


def test_strict_init_fetch_preserves_image_pull_secrets_from_provider_execution() -> None:
    """Private Harbor pulls need pod imagePullSecrets; dropping them yields ErrImagePull."""

    pack = _strict_pack()
    pack["provider_execution"]["pod_spec"]["spec"]["imagePullSecrets"] = [{"name": "regsecret"}]
    workload = SimpleNamespace(workload_id="orders", effective_config={"image_pull_secret": "regsecret"})
    runtime_manifest = SimpleNamespace(path="runtime/manifest.json")
    kpo_kwargs = compact_kpo_kwargs(workload, runtime_manifest=runtime_manifest)
    projected = compact_provider_execution(
        kpo_kwargs=kpo_kwargs,
        pod_spec={
            "spec": {
                "imagePullSecrets": [{"name": "regsecret"}],
                "containers": [{"name": "base", "image": "ignored"}],
            }
        },
    )
    assert projected["pod_spec"]["spec"]["imagePullSecrets"] == [{"name": "regsecret"}]

    kwargs = compose_init_fetch_operator_kwargs(
        pack=pack,
        kwargs=pack["provider_execution"]["kpo_kwargs"],
        context=_context(),
        workload_id="orders",
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )

    assert kwargs["full_pod_spec"]["spec"]["imagePullSecrets"] == [{"name": "regsecret"}]


def test_xcom_push_without_explicit_sidecar_fails_closed_not_mutable_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pod = {
        "spec": {
            "containers": [
                {"name": "base", "image": IMAGE_REF},
                {"name": XCOM_SIDECAR_CONTAINER_NAME, "image": "alpine:3.23.4"},
            ]
        }
    }
    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    monkeypatch.setattr(
        base_operator,
        "build_pod_request_obj",
        lambda _self, context=None: deepcopy(pod),
    )
    operator = object.__new__(PinnedXComSidecarKubernetesPodOperator)
    operator.do_xcom_push = True
    operator.env_vars = None
    operator.xcom_sidecar = None

    with pytest.raises(RuntimeError, match="DPONE_AIRFLOW_XCOM_SIDECAR_IMAGE_REQUIRED"):
        operator.build_pod_request_obj(context={})


def test_pin_xcom_sidecar_image_rejects_omitted_image() -> None:
    from dpone_airflow_pack.xcom_sidecar import pin_xcom_sidecar_image

    with pytest.raises(RuntimeError, match="DPONE_AIRFLOW_XCOM_SIDECAR_IMAGE_REQUIRED"):
        pin_xcom_sidecar_image({"spec": {"containers": []}}, "")
