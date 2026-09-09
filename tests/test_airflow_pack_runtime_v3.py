from __future__ import annotations

import base64
import hashlib
import io
import json
import sys
import tarfile
import types
from dataclasses import replace
from pathlib import Path

import pytest
from dpone_airflow_pack import build_dpone_gitops_task_group_from_pack
from dpone_airflow_pack.connection_secret_lifecycle import (
    ATTEMPT_REF_ANNOTATION,
    CLEANUP_POLICY_LABEL,
    LIFECYCLE_VERSION_LABEL,
    MANAGED_BY_LABEL,
    RESOURCE_KIND_LABEL,
    SECRET_REF_ANNOTATION,
    SECRET_RESOURCE_KIND,
)
from dpone_airflow_pack.operators import (
    AirflowConnectionSecretVolumeKubernetesPodOperator,
    PinnedXComSidecarKubernetesPodOperator,
    UnsafeAirflowConnectionEnvKubernetesPodOperator,
    stringify_env_vars,
)
from dpone_airflow_pack.pack_identity import verify_pack_fingerprint

from dpone.gitops.airflow_compact_pack_bootstrap import (
    InlineWorkloadConfigurationError,
    runtime_workload_bootstrap,
)


@pytest.fixture(autouse=True)
def _stub_dependency_light_kpo_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    monkeypatch.setattr(
        base_operator,
        "execute",
        lambda self, context: {
            "kind": "test.kpo_execute",
            "task_id": getattr(self, "kwargs", {}).get("task_id"),
        },
    )


def _install_fake_airflow(monkeypatch: pytest.MonkeyPatch) -> None:
    airflow = types.ModuleType("airflow")
    providers = types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    standard_operators = types.ModuleType("airflow.providers.standard.operators")
    python = types.ModuleType("airflow.providers.standard.operators.python")
    cncf = types.ModuleType("airflow.providers.cncf")
    kubernetes = types.ModuleType("airflow.providers.cncf.kubernetes")
    operators = types.ModuleType("airflow.providers.cncf.kubernetes.operators")
    pod = types.ModuleType("airflow.providers.cncf.kubernetes.operators.pod")

    class KubernetesPodOperator:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

    class PythonOperator:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

    pod.KubernetesPodOperator = KubernetesPodOperator
    python.PythonOperator = PythonOperator
    for name, module in {
        "airflow": airflow,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": standard_operators,
        "airflow.providers.standard.operators.python": python,
        "airflow.providers.cncf": cncf,
        "airflow.providers.cncf.kubernetes": kubernetes,
        "airflow.providers.cncf.kubernetes.operators": operators,
        "airflow.providers.cncf.kubernetes.operators.pod": pod,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


def _install_fake_kubernetes(monkeypatch: pytest.MonkeyPatch) -> None:
    kubernetes = types.ModuleType("kubernetes")
    client = types.ModuleType("kubernetes.client")
    models = types.ModuleType("kubernetes.client.models")

    class V1Pod:
        pass

    class ApiClient:
        def _ApiClient__deserialize_model(self, data: object, klass: type[object]) -> object:
            def convert(value: object) -> object:
                if isinstance(value, dict):
                    return types.SimpleNamespace(**{key: convert(item) for key, item in value.items()})
                if isinstance(value, list):
                    return [convert(item) for item in value]
                return value

            pod = klass()
            for key, value in dict(data).items():
                setattr(pod, key, convert(value))
            return pod

    client.ApiClient = ApiClient
    models.V1Pod = V1Pod
    client.models = models
    kubernetes.client = client
    monkeypatch.setitem(sys.modules, "kubernetes", kubernetes)
    monkeypatch.setitem(sys.modules, "kubernetes.client", client)
    monkeypatch.setitem(sys.modules, "kubernetes.client.models", models)


def _airflow_task_context() -> dict[str, object]:
    return {
        "task_instance": types.SimpleNamespace(
            dag_id="orders_daily",
            task_id="orders__dpone_runtime",
            run_id="manual__2026-07-16T00:00:00+00:00",
            try_number=1,
            map_index=-1,
        )
    }


def test_structured_runtime_bootstrap_contains_argv_not_scheduler_shell() -> None:
    bootstrap = runtime_workload_bootstrap(
        runtime_manifest_path=".dpone/runtime/orders/manifest.json",
        workload_id="orders",
        process_selectors=("orders_daily",),
    )

    assert bootstrap == {
        "schema": "dpone.airflow-runtime-bootstrap.v1",
        "commands": {
            "orders": {
                "argv": [
                    "dpone",
                    "run",
                    ".dpone/runtime/orders/manifest.json",
                    "--format",
                    "json",
                    "--selector",
                    "orders",
                ],
                "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
            },
            "__workload__": {
                "argv": [
                    "dpone",
                    "run",
                    ".dpone/runtime/orders/manifest.json",
                    "--format",
                    "json",
                ],
                "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
            },
            "orders_daily": {
                "argv": [
                    "dpone",
                    "run",
                    ".dpone/runtime/orders/manifest.json",
                    "--format",
                    "json",
                    "--selector",
                    "orders_daily",
                ],
                "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
            },
        },
    }
    assert "/bin/sh" not in json.dumps(bootstrap)


def test_structured_runtime_bootstrap_keys_single_process_by_workload_id() -> None:
    bootstrap = runtime_workload_bootstrap(
        runtime_manifest_path="dpone_workloads/manifests/mssql/sale_plan_type1_smoke.yaml",
        workload_id="mssql_sale_plan_type1_smoke",
        process_selectors=(None,),
    )

    assert bootstrap == {
        "schema": "dpone.airflow-runtime-bootstrap.v1",
        "commands": {
            "mssql_sale_plan_type1_smoke": {
                "argv": [
                    "dpone",
                    "run",
                    "dpone_workloads/manifests/mssql/sale_plan_type1_smoke.yaml",
                    "--format",
                    "json",
                    "--selector",
                    "mssql_sale_plan_type1_smoke",
                ],
                "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
            },
            "__workload__": {
                "argv": [
                    "dpone",
                    "run",
                    "dpone_workloads/manifests/mssql/sale_plan_type1_smoke.yaml",
                    "--format",
                    "json",
                ],
                "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
            },
            "__default_process__": {
                "argv": [
                    "dpone",
                    "run",
                    "dpone_workloads/manifests/mssql/sale_plan_type1_smoke.yaml",
                    "--format",
                    "json",
                ],
                "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
            },
        },
    }


@pytest.mark.parametrize(
    ("workload_id", "process_selectors"),
    (
        ("__workload__", ()),
        ("__default_process__", ()),
        ("orders", ("__workload__",)),
        ("orders", ("__default_process__",)),
    ),
)
def test_structured_runtime_bootstrap_reserves_whole_workload_key(
    workload_id: str,
    process_selectors: tuple[str | None, ...],
) -> None:
    with pytest.raises(
        InlineWorkloadConfigurationError,
        match="runtime_bootstrap_selector_reserved",
    ):
        runtime_workload_bootstrap(
            runtime_manifest_path="runtime/orders.yaml",
            workload_id=workload_id,
            process_selectors=process_selectors,
        )


def test_structured_runtime_bootstrap_keeps_default_workload_id_distinct_from_default_process() -> None:
    bootstrap = runtime_workload_bootstrap(
        runtime_manifest_path="runtime/orders.yaml",
        workload_id="__default__",
        process_selectors=(None,),
    )

    commands = bootstrap["commands"]
    assert isinstance(commands, dict)
    assert commands["__default__"]["argv"][-2:] == ["--selector", "__default__"]
    assert commands["__default_process__"]["argv"] == [
        "dpone",
        "run",
        "runtime/orders.yaml",
        "--format",
        "json",
    ]


def test_runnable_pack_reuses_one_digest_pinned_archive_for_structured_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
    from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
    from dpone.runtime.runtime_payload_archive import runtime_payload_archive
    from tests.airflow_dag_spec_repo import write_manifest

    manifest = write_manifest(tmp_path, "orders")
    workload = GitOpsWorkloadDefinition(
        workload_id="orders",
        manifest=manifest,
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={"image": "registry.example/dpone:dev", "airflow": {}},
        provenance={},
    )
    reads: list[str] = []
    from dpone.gitops import airflow_compact_pack_bootstrap as bootstrap_module

    real_read = bootstrap_module.read_confined_file

    def counted_read(root: Path, relative: str, *, max_bytes: int) -> bytes:
        reads.append(relative)
        return real_read(root, relative, max_bytes=max_bytes)

    monkeypatch.setattr(bootstrap_module, "read_confined_file", counted_read)

    report = AirflowCompactPackBuilder().build(
        workload=workload,
        output_path="packs/orders.airflow-pack.json",
        repo_root=tmp_path,
    )
    payload = report.to_jsonable()
    runtime_payload = payload["runtime_payload"]
    archive = runtime_payload["archive"]
    compressed = base64.b64decode(archive["data"], validate=True)

    assert runtime_payload["schema"] == "dpone.airflow-runtime-payload.v1"
    assert payload["pack_identity"] == {"schema": "dpone.airflow-pack-identity.v1"}
    assert verify_pack_fingerprint(payload) == payload["pack_fingerprint"]
    assert archive["encoding"] == "base64"
    assert archive["format"] == "tar+gzip"
    assert archive["bytes"] == len(compressed) > 0
    assert archive["sha256"] == "sha256:" + hashlib.sha256(compressed).hexdigest()
    assert base64.b64encode(compressed).decode("ascii") == archive["data"]
    decoded_by_runtime = runtime_payload_archive(payload)
    assert decoded_by_runtime.data == compressed
    assert decoded_by_runtime.sha256 == archive["sha256"]
    assert decoded_by_runtime.declared_bytes == archive["bytes"]
    assert reads.count(manifest) == 1
    init_script = payload["pod_spec"]["spec"]["initContainers"][0]["args"][0]
    assert archive["data"] in init_script
    assert "/bin/sh" not in json.dumps(payload["runtime_bootstrap"])

    with tarfile.open(fileobj=io.BytesIO(compressed), mode="r:gz") as unpacked:
        members = unpacked.getmembers()
    member_names = [member.name for member in members]
    assert members
    assert len(member_names) == len(set(member_names))
    assert all(member.isfile() for member in members)
    assert all(not name.startswith("/") and ".." not in Path(name).parts for name in member_names)

    schema = json.loads(Path("docs/schemas/gitops/airflow-pack.schema.json").read_text(encoding="utf-8"))
    jsonschema.validate(payload, schema)
    for invalid_payload in (
        {
            **runtime_payload,
            "secret": "must-not-enter-the-pack",
        },
        {
            **runtime_payload,
            "archive": {**archive, "bytes": 0},
        },
        {
            **runtime_payload,
            "archive": {**archive, "sha256": "sha256:" + "A" * 64},
        },
        {
            **runtime_payload,
            "archive": {**archive, "data": archive["data"] + "\\n"},
        },
    ):
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate({**payload, "runtime_payload": invalid_payload}, schema)

    without_payload = replace(report, runtime_payload=None).to_jsonable()
    assert without_payload["pack_fingerprint"] != payload["pack_fingerprint"]
    assert report.runtime_payload is not None
    changed_descriptor = replace(
        report,
        runtime_payload=replace(
            report.runtime_payload,
            bytes=report.runtime_payload.bytes + 1,
        ),
    ).to_jsonable()
    assert changed_descriptor["pack_fingerprint"] != payload["pack_fingerprint"]


def test_pack_tasks_deserializes_json_pod_spec_when_kubernetes_client_is_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pack_path = tmp_path / ".dpone/gitops/airflow/orders/airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "schema_version": "3",
                "workload": {"workload_id": "orders"},
                "pod_spec": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "dpone-orders"},
                    "spec": {"containers": [{"name": "base", "image": "registry.example/dpone:dev"}]},
                },
                "kpo_kwargs": {
                    "task_id": "orders__dpone_runtime",
                    "name": "dpone-orders",
                    "namespace": "airflow-dev",
                    "image": "registry.example/dpone:dev",
                },
            }
        ),
        encoding="utf-8",
    )
    _install_fake_airflow(monkeypatch)
    _install_fake_kubernetes(monkeypatch)

    runtime = build_dpone_gitops_task_group_from_pack(pack_path, dag=object())["dpone_runtime"]

    assert runtime.kwargs["full_pod_spec"].metadata.name == "dpone-orders"
    assert runtime.kwargs["full_pod_spec"].spec.containers[0].image == "registry.example/dpone:dev"


def test_pack_tasks_selects_airflow_connection_secret_volume_operator(tmp_path: Path) -> None:
    pack_path = tmp_path / ".dpone/gitops/airflow/orders/airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    projection = {
        "mode": "kubernetes_secret_volume",
        "secret_name": "dpone-airflow-connection-bridge",
        "mount_path": "/run/secrets/dpone/airflow-connections",
        "payload_format": "airflow_connection_uri",
        "secret_values": False,
        "connections": [
            {
                "connection_ref": "mssql_dev",
                "registry_connection_ref": "mssql_dev",
                "connection_id": "mssql_prod",
                "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                "fields": {"uri": "uri"},
            }
        ],
    }
    pack_path.write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "schema_version": "3",
                "workload": {"workload_id": "orders"},
                "connection_projection": projection,
                "pod_spec": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "dpone-orders"},
                    "spec": {"containers": [{"name": "base", "image": "registry.example/dpone:dev"}]},
                },
                "kpo_kwargs": {
                    "task_id": "orders__dpone_runtime",
                    "name": "dpone-orders",
                    "namespace": "airflow-dev",
                    "image": "registry.example/dpone:dev",
                },
            }
        ),
        encoding="utf-8",
    )

    runtime = build_dpone_gitops_task_group_from_pack(pack_path, dag=object())["dpone_runtime"]

    assert isinstance(runtime, AirflowConnectionSecretVolumeKubernetesPodOperator)
    assert runtime.airflow_connection_projection == projection


def test_unsafe_airflow_connection_env_operator_injects_connection_overrides_at_execute_time() -> None:
    class Reader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return {
                "mssql_prod": "mssql+pymssql://user:pwd@mssql.example/analytics_reporting",
                "ClickHouse": "http://user:pwd@clickhouse.example/default?secure=false",
            }[connection_id]

    reader = Reader()
    operator = UnsafeAirflowConnectionEnvKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        image="registry.example/dpone:dev",
        namespace="airflow-dev",
        unsafe_airflow_connection_ids=("mssql_prod", "ClickHouse"),
        unsafe_connection_reader=reader,
        unsafe_runtime_database_overrides={"ClickHouse": "DWH_Raw"},
        unsafe_runtime_scheme_overrides={"ClickHouse": "clickhouse"},
        unsafe_runtime_query_overrides={"mssql_prod": {"trust_server_certificate": "yes"}},
        env_vars={"KEEP_ME": "1"},
    )

    operator.execute(context={})

    assert reader.connection_ids == ["mssql_prod", "ClickHouse"]
    assert operator.env_vars == {
        "KEEP_ME": "1",
        "AIRFLOW_CONN_MSSQL_PROD": (
            "mssql+pymssql://user:pwd@mssql.example/analytics_reporting?trust_server_certificate=yes"
        ),
        "AIRFLOW_CONN_CLICKHOUSE": "clickhouse://user:pwd@clickhouse.example/DWH_Raw?secure=false",
    }


def test_airflow_connection_secret_volume_operator_projects_uri_files_without_env_leak() -> None:
    class Reader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return {
                "mssql_prod": "mssql+pymssql://user:pwd@mssql.example/analytics_reporting",
                "clickhouse_prod": "clickhouse://user:pwd@clickhouse.example/default?secure=false",
            }[connection_id]

    class Projector:
        def __init__(self) -> None:
            self.events: list[tuple[str, str, object]] = []

        def upsert(self, *, namespace: str, secret: object) -> None:
            self.events.append(("upsert", namespace, secret))

        def delete(self, *, namespace: str, name: str) -> None:
            self.events.append(("delete", namespace, name))

    reader = Reader()
    projector = Projector()
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "dpone-orders"},
        "spec": {
            "containers": [
                {
                    "name": "base",
                    "image": "registry.example/dpone:dev",
                    "env": [{"name": "KEEP_ME", "value": "1"}],
                }
            ]
        },
    }
    operator = AirflowConnectionSecretVolumeKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        namespace="airflow-example",
        full_pod_spec=pod,
        airflow_connection_projection={
            "mode": "kubernetes_secret_volume",
            "secret_name": "dpone-airflow-connection-bridge",
            "mount_path": "/run/secrets/dpone/airflow-connections",
            "payload_format": "airflow_connection_uri",
            "secret_values": False,
            "connections": [
                {
                    "connection_ref": "mssql_dev",
                    "registry_connection_ref": "mssql_dev",
                    "connection_id": "mssql_prod",
                    "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                    "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                    "fields": {"uri": "uri"},
                },
                {
                    "connection_ref": "clickhouse_dev",
                    "registry_connection_ref": "clickhouse_dev",
                    "connection_id": "clickhouse_prod",
                    "secret_key": "AIRFLOW_CONN_CLICKHOUSE_PROD",
                    "mount_path": "/run/secrets/dpone/airflow-connections/clickhouse_dev",
                    "fields": {"uri": "uri"},
                },
            ],
        },
        airflow_connection_projection_reader=reader,
        airflow_connection_secret_projector=projector,
        env_vars={"DPONE_TRY_NUMBER": 1},
    )

    operator.execute(context=_airflow_task_context())

    assert reader.connection_ids == ["mssql_prod", "clickhouse_prod"]
    assert operator.env_vars == {"DPONE_TRY_NUMBER": 1}
    container = operator.full_pod_spec["spec"]["containers"][0]
    assert container["env"] == [{"name": "KEEP_ME", "value": "1"}]
    published_secret = projector.events[0][2]
    assert isinstance(published_secret, dict)
    published_name = published_secret["metadata"]["name"]
    assert published_name.startswith("dpone-airflow-connection-bridge-")
    assert container["volumeMounts"] == [
        {
            "name": "dpone-airflow-connection-bridge",
            "mountPath": "/run/secrets/dpone/airflow-connections",
            "readOnly": True,
        }
    ]
    assert operator.full_pod_spec["spec"]["volumes"] == [
        {
            "name": "dpone-airflow-connection-bridge",
            "secret": {
                "secretName": published_name,
                "items": [
                    {
                        "key": "AIRFLOW_CONN_MSSQL_PROD",
                        "path": "mssql_dev/uri",
                    },
                    {
                        "key": "AIRFLOW_CONN_CLICKHOUSE_PROD",
                        "path": "clickhouse_dev/uri",
                    },
                ],
            },
        }
    ]
    assert published_secret["apiVersion"] == "v1"
    assert published_secret["kind"] == "Secret"
    assert published_secret["type"] == "Opaque"
    assert published_secret["immutable"] is True
    assert published_secret["metadata"]["labels"] == {
        MANAGED_BY_LABEL: "dpone",
        RESOURCE_KIND_LABEL: SECRET_RESOURCE_KIND,
        LIFECYCLE_VERSION_LABEL: "v1",
        CLEANUP_POLICY_LABEL: "after_execute",
    }
    annotations = published_secret["metadata"]["annotations"]
    assert annotations[SECRET_REF_ANNOTATION] == operator.airflow_connection_projected_secret_ref["secret_ref"]
    assert annotations[ATTEMPT_REF_ANNOTATION].startswith("sha256:")
    assert published_secret["stringData"] == {
        "AIRFLOW_CONN_MSSQL_PROD": "mssql+pymssql://user:pwd@mssql.example/analytics_reporting",
        "AIRFLOW_CONN_CLICKHOUSE_PROD": "clickhouse://user:pwd@clickhouse.example/default?secure=false",
    }
    assert operator.airflow_connection_projected_secret == operator.airflow_connection_projected_secret_ref
    assert operator.airflow_connection_projected_secret is not None
    assert set(operator.airflow_connection_projected_secret) == {
        "secret_ref",
        "attempt_ref",
        "cleanup_policy",
    }
    assert projector.events == [("upsert", "airflow-example", published_secret), ("delete", "airflow-example", published_name)]
    assert "mssql+pymssql://" not in json.dumps(operator.full_pod_spec)
    assert "clickhouse://user:pwd" not in json.dumps(operator.full_pod_spec)
    built = operator.build_pod_request_obj(context=_airflow_task_context())
    assert built["spec"]["volumes"][0]["secret"]["items"][0]["path"] == "mssql_dev/uri"


def test_airflow_connection_secret_volume_operator_rejects_uri_connection_id_before_reading() -> None:
    class Reader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return "postgres://etl:secret@pg.internal:5432/dwh"

    class Projector:
        def __init__(self) -> None:
            self.events: list[tuple[str, str, object]] = []

        def upsert(self, *, namespace: str, secret: object) -> None:
            self.events.append(("upsert", namespace, secret))

        def delete(self, *, namespace: str, name: str) -> None:
            self.events.append(("delete", namespace, name))

    reader = Reader()
    projector = Projector()
    operator = AirflowConnectionSecretVolumeKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        namespace="airflow-example",
        airflow_connection_projection={
            "mode": "kubernetes_secret_volume",
            "secret_name": "dpone-airflow-connection-bridge",
            "mount_path": "/run/secrets/dpone/airflow-connections",
            "payload_format": "airflow_connection_uri",
            "secret_values": False,
            "connections": [
                {
                    "connection_ref": "mssql_dev",
                    "registry_connection_ref": "mssql_dev",
                    "connection_id": "postgres://etl:secret@pg.internal:5432/dwh",
                    "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                    "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                    "fields": {"uri": "uri"},
                }
            ],
        },
        airflow_connection_projection_reader=reader,
        airflow_connection_secret_projector=projector,
    )

    with pytest.raises(ValueError) as exc_info:
        operator.execute(context=_airflow_task_context())

    assert "logical Airflow Connection id" in str(exc_info.value)
    assert "etl:secret" not in str(exc_info.value)
    assert reader.connection_ids == []
    assert projector.events == []


def test_airflow_connection_secret_volume_operator_rejects_invalid_secret_key_before_reading() -> None:
    class Reader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return "postgres://etl:secret@pg.internal:5432/dwh"

    class Projector:
        def __init__(self) -> None:
            self.events: list[tuple[str, str, object]] = []

        def upsert(self, *, namespace: str, secret: object) -> None:
            self.events.append(("upsert", namespace, secret))

        def delete(self, *, namespace: str, name: str) -> None:
            self.events.append(("delete", namespace, name))

    reader = Reader()
    projector = Projector()
    operator = AirflowConnectionSecretVolumeKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        namespace="airflow-example",
        airflow_connection_projection={
            "mode": "kubernetes_secret_volume",
            "secret_name": "dpone-airflow-connection-bridge",
            "mount_path": "/run/secrets/dpone/airflow-connections",
            "payload_format": "airflow_connection_uri",
            "secret_values": False,
            "connections": [
                {
                    "connection_ref": "mssql_dev",
                    "registry_connection_ref": "mssql_dev",
                    "connection_id": "mssql_prod",
                    "secret_key": "AIRFLOW_CONN_MSSQL_PROD\nPASSWORD=secret",
                    "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                    "fields": {"uri": "uri"},
                }
            ],
        },
        airflow_connection_projection_reader=reader,
        airflow_connection_secret_projector=projector,
    )

    with pytest.raises(ValueError) as exc_info:
        operator.execute(context=_airflow_task_context())

    assert "AIRFLOW_CONN_*" in str(exc_info.value)
    assert "PASSWORD=secret" not in str(exc_info.value)
    assert reader.connection_ids == []
    assert projector.events == []


def test_airflow_connection_secret_volume_operator_rejects_invalid_secret_name_before_reading() -> None:
    class Reader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return "postgres://etl:secret@pg.internal:5432/dwh"

    class Projector:
        def __init__(self) -> None:
            self.events: list[tuple[str, str, object]] = []

        def upsert(self, *, namespace: str, secret: object) -> None:
            self.events.append(("upsert", namespace, secret))

        def delete(self, *, namespace: str, name: str) -> None:
            self.events.append(("delete", namespace, name))

    reader = Reader()
    projector = Projector()
    operator = AirflowConnectionSecretVolumeKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        namespace="airflow-example",
        airflow_connection_projection={
            "mode": "kubernetes_secret_volume",
            "secret_name": "dpone-airflow-connection-bridge\nPASSWORD=secret",
            "mount_path": "/run/secrets/dpone/airflow-connections",
            "payload_format": "airflow_connection_uri",
            "secret_values": False,
            "connections": [
                {
                    "connection_ref": "mssql_dev",
                    "registry_connection_ref": "mssql_dev",
                    "connection_id": "mssql_prod",
                    "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                    "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                    "fields": {"uri": "uri"},
                }
            ],
        },
        airflow_connection_projection_reader=reader,
        airflow_connection_secret_projector=projector,
    )

    with pytest.raises(ValueError) as exc_info:
        operator.execute(context=_airflow_task_context())

    assert "Kubernetes Secret name" in str(exc_info.value)
    assert "PASSWORD=secret" not in str(exc_info.value)
    assert reader.connection_ids == []
    assert projector.events == []


def test_airflow_connection_secret_volume_operator_rejects_deferrable_after_execute_cleanup() -> None:
    class Reader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return "mssql+pymssql://user:pwd@mssql.example/analytics_reporting"

    class Projector:
        def __init__(self) -> None:
            self.events: list[tuple[str, str, object]] = []

        def upsert(self, *, namespace: str, secret: object) -> None:
            self.events.append(("upsert", namespace, secret))

        def delete(self, *, namespace: str, name: str) -> None:
            self.events.append(("delete", namespace, name))

    reader = Reader()
    projector = Projector()
    operator = AirflowConnectionSecretVolumeKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        namespace="airflow-example",
        deferrable=True,
        airflow_connection_projection={
            "mode": "kubernetes_secret_volume",
            "secret_name": "dpone-airflow-connection-bridge",
            "mount_path": "/run/secrets/dpone/airflow-connections",
            "payload_format": "airflow_connection_uri",
            "secret_values": False,
            "connections": [
                {
                    "connection_ref": "mssql_dev",
                    "registry_connection_ref": "mssql_dev",
                    "connection_id": "mssql_prod",
                    "secret_key": "AIRFLOW_CONN_MSSQL_PROD",
                    "mount_path": "/run/secrets/dpone/airflow-connections/mssql_dev",
                    "fields": {"uri": "uri"},
                }
            ],
        },
        airflow_connection_projection_reader=reader,
        airflow_connection_secret_projector=projector,
    )

    with pytest.raises(ValueError, match="cleanup_policy: retain"):
        operator.execute(context={})

    assert reader.connection_ids == []
    assert projector.events == []


def test_stringify_env_vars_coerces_numeric_values_for_kubernetes() -> None:
    assert stringify_env_vars({"DPONE_TRY_NUMBER": 1, "DPONE_DAG_ID": "orders"}) == {
        "DPONE_TRY_NUMBER": "1",
        "DPONE_DAG_ID": "orders",
    }


def test_unsafe_airflow_connection_env_operator_stringifies_rendered_env_vars() -> None:
    class Reader:
        def read_uri(self, connection_id: str) -> str:
            return "mssql+pymssql://user:pwd@mssql.example/analytics_reporting"

    operator = UnsafeAirflowConnectionEnvKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        image="registry.example/dpone:dev",
        namespace="airflow-dev",
        unsafe_airflow_connection_ids=("mssql_prod",),
        unsafe_connection_reader=Reader(),
        env_vars={"DPONE_TRY_NUMBER": 1},
    )

    operator.execute(context={})

    assert operator.env_vars["DPONE_TRY_NUMBER"] == "1"
    assert operator.env_vars["AIRFLOW_CONN_MSSQL_PROD"].startswith("mssql+pymssql://")


def test_unsafe_airflow_connection_env_operator_rejects_uri_connection_id_before_reading() -> None:
    class Reader:
        def __init__(self) -> None:
            self.connection_ids: list[str] = []

        def read_uri(self, connection_id: str) -> str:
            self.connection_ids.append(connection_id)
            return "postgres://etl:secret@pg.internal:5432/dwh"

    reader = Reader()
    operator = UnsafeAirflowConnectionEnvKubernetesPodOperator(
        task_id="orders__dpone_runtime",
        name="dpone-orders",
        image="registry.example/dpone:dev",
        namespace="airflow-dev",
        unsafe_airflow_connection_ids=("postgres://etl:secret@pg.internal:5432/dwh",),
        unsafe_connection_reader=reader,
    )

    with pytest.raises(ValueError) as exc_info:
        operator.execute(context={})

    assert "logical Airflow Connection id" in str(exc_info.value)
    assert "etl:secret" not in str(exc_info.value)
    assert reader.connection_ids == []


def test_unsafe_airflow_connection_env_operator_patches_full_pod_spec_env() -> None:
    from dpone_airflow_pack.operators import patch_pod_spec_env_vars

    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "spec": {"containers": [{"name": "base", "image": "registry.example/dpone:dev"}]},
    }
    patched = patch_pod_spec_env_vars(
        pod,
        {
            "KEEP_ME": "1",
            "AIRFLOW_CONN_MSSQL_PROD": "mssql+pymssql://user:pwd@mssql.example/analytics_reporting",
        },
    )
    assert patched["spec"]["containers"][0]["env"] == [
        {"name": "KEEP_ME", "value": "1"},
        {
            "name": "AIRFLOW_CONN_MSSQL_PROD",
            "value": "mssql+pymssql://user:pwd@mssql.example/analytics_reporting",
        },
    ]


def test_patch_pod_spec_env_vars_updates_init_fetch_init_container() -> None:
    from dpone_airflow_pack.operators import patch_pod_spec_env_vars

    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "spec": {
            "initContainers": [
                {
                    "name": "dpone-runtime-init-fetch",
                    "image": "registry.example/dpone@sha256:" + "a" * 64,
                    "env": [
                        {
                            "name": "AWS_ACCESS_KEY_ID",
                            "value": "{{ conn.s3_dpone_artifacts_reader.login }}",
                        }
                    ],
                }
            ],
            "containers": [
                {
                    "name": "base",
                    "image": "registry.example/dpone@sha256:" + "a" * 64,
                    "env": [
                        {
                            "name": "AWS_ACCESS_KEY_ID",
                            "value": "{{ conn.s3_dpone_artifacts_reader.login }}",
                        }
                    ],
                }
            ],
        },
    }
    patched = patch_pod_spec_env_vars(
        pod,
        {
            "AWS_ACCESS_KEY_ID": "rendered-access-key",
            "AWS_SECRET_ACCESS_KEY": "rendered-secret",
            "DPONE_INIT_FETCH_PLAN_SHA256": "sha256:" + "b" * 64,
        },
    )
    expected = [
        {"name": "AWS_ACCESS_KEY_ID", "value": "rendered-access-key"},
        {"name": "AWS_SECRET_ACCESS_KEY", "value": "rendered-secret"},
        {"name": "DPONE_INIT_FETCH_PLAN_SHA256", "value": "sha256:" + "b" * 64},
    ]
    assert patched["spec"]["containers"][0]["env"] == expected
    assert patched["spec"]["initContainers"][0]["env"] == expected


def test_unsafe_airflow_connection_env_operator_materializes_full_pod_spec_before_execute() -> None:
    class Reader:
        def read_uri(self, connection_id: str) -> str:
            return {
                "clickhouse_example": "clickhouse://user:pwd@clickhouse.example/default?secure=true",
                "mssql_example": "mssql+pymssql://user:pwd@mssql.example/analytics_reporting",
            }[connection_id]

    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "dpone-marketing"},
        "spec": {
            "containers": [
                {
                    "name": "base",
                    "image": "registry.example/dpone:dev",
                    "command": ["/bin/sh"],
                    "args": ["echo hi"],
                }
            ]
        },
    }
    operator = UnsafeAirflowConnectionEnvKubernetesPodOperator(
        task_id="marketing__dpone_runtime",
        name="dpone-marketing",
        namespace="airflow-example",
        full_pod_spec=pod,
        unsafe_airflow_connection_ids=("clickhouse_example", "mssql_example"),
        unsafe_connection_reader=Reader(),
        env_vars={"DPONE_TRY_NUMBER": "1"},
    )

    operator.execute(context={})

    container_env = operator.full_pod_spec["spec"]["containers"][0]["env"]
    env_names = {item["name"] for item in container_env}
    assert "AIRFLOW_CONN_CLICKHOUSE_EXAMPLE" in env_names
    assert "AIRFLOW_CONN_MSSQL_EXAMPLE" in env_names
    assert "DPONE_TRY_NUMBER" in env_names
    built = operator.build_pod_request_obj(context={})
    if isinstance(built, dict):
        built_env = built["spec"]["containers"][0]["env"]
    else:
        built_env = built.spec.containers[0].env or []
        built_env = [{"name": item.name, "value": item.value} for item in built_env]
    assert {item["name"] for item in built_env} >= env_names
