from __future__ import annotations

import base64
import io
import tarfile
from pathlib import Path
from types import SimpleNamespace

from dpone.adapters.fs_memory import InMemoryFileSystem
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.airflow_compact_runtime import compact_provider_execution
from dpone.gitops.airflow_dag_spec_artifacts import AirflowDagSpecArtifactWriter
from dpone.gitops.airflow_runner_contract import resolve_airflow_runner_contract, runner_workspace_path
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogReport, GitOpsWorkloadDefinition
from dpone.services.gitops.airflow_compact_pack_service import GitOpsAirflowCompactPackService


def test_runner_contract_nested_placement_and_legacy_flat_keys() -> None:
    airflow = {
        "runner": {
            "placement": {
                "node_selector": {"dedicated": "datawarehouse"},
                "tolerations": [
                    {"key": "dedicated", "value": "datawarehouse", "effect": "NoSchedule"},
                ],
            }
        },
        "node_selector": {"ignored": "legacy"},
    }
    contract = resolve_airflow_runner_contract(airflow)
    assert contract.placement.node_selector == {"dedicated": "datawarehouse"}
    assert contract.placement.tolerations[0].effect == "NoSchedule"


def test_missing_workload_report_preserves_runtime_manifest_contract(tmp_path: Path) -> None:
    placeholder = GitOpsWorkloadDefinition(
        workload_id="existing",
        manifest="pipelines/existing/pipeline.yaml",
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={"image": "registry.example/dpone:dev", "airflow": {}},
        provenance={},
    )
    catalog = GitOpsWorkloadCatalogReport(workload_set="workloads.yaml", env="dev", workloads=(placeholder,))
    context = SimpleNamespace(settings=SimpleNamespace(repo_root=tmp_path), fs=InMemoryFileSystem())
    service = GitOpsAirflowCompactPackService(
        ctx=context,
        dag_spec_writer=AirflowDagSpecArtifactWriter(repo_root=tmp_path),
    )
    service._resolve_catalog = lambda args: catalog  # type: ignore[method-assign]

    report = service._build_report(
        args=SimpleNamespace(output_dir=".dpone/gitops/airflow"),
        workload_id="missing",
    )

    assert report.blockers[0].code == "workload_not_found"
    assert report.runtime_manifest.path == "pipelines/existing/pipeline.yaml"
    assert report.runtime_manifest.sha256 is None


def test_runner_contract_embed_asset_bind_projects_ca_cert_into_query_overrides() -> None:
    airflow = {
        "connection_projection": {"mode": "unsafe_airflow_env"},
        "runner": {
            "embed_assets": [
                {
                    "path": "certs/RootCA.pem",
                    "bind": {"connection_id": "clickhouse_example", "query_key": "ca_cert"},
                }
            ]
        },
    }
    contract = resolve_airflow_runner_contract(airflow)
    assert contract.embed_paths == ("certs/RootCA.pem",)
    assert contract.connection_projection["query_overrides"]["clickhouse_example"]["ca_cert"] == (
        runner_workspace_path("certs/RootCA.pem")
    )


def test_runner_contract_blocks_when_embed_asset_file_is_missing(tmp_path: Path) -> None:
    contract = resolve_airflow_runner_contract(
        {"runner": {"embed_paths": ["certs/RootCA.pem"]}},
        repo_root=tmp_path,
        workload_id="marketing_sample_web_sync",
    )
    assert not contract.passed
    assert contract.blockers[0].code == "runner_embed_asset_missing"


def test_compact_pack_materializes_runner_contract_into_pod_spec_and_projection(tmp_path: Path) -> None:
    manifest = tmp_path / "workloads" / "marketing" / "dpone" / "manifests" / "sample_web_sync.yaml"
    cert = tmp_path / "certs" / "RootCA.pem"
    manifest.parent.mkdir(parents=True)
    cert.parent.mkdir(parents=True)
    # Non-MSSQL sink: this test covers runner placement/embed projection, not
    # AIP-60 authority. Incomplete mssql sinks now fail-closed on pack.
    manifest.write_text(
        "name: marketing_sample_web_sync\n"
        "source:\n  type: clickhouse\n  connection_ref: ClickHouse\n"
        "  table: {schema: src, name: t}\n"
        "sink:\n  type: postgres\n  connection_id: pg\n"
        "  table: {schema: dst, name: t}\n  mode: append\n",
        encoding="utf-8",
    )
    cert.write_text("TEST-CA\n", encoding="utf-8")

    workload = GitOpsWorkloadDefinition(
        workload_id="marketing_sample_web_sync",
        manifest="workloads/marketing/dpone/manifests/sample_web_sync.yaml",
        domain="marketing",
        catalog_path="dpone_workloads/gitops/domains/marketing.yaml",
        effective_config={
            "image": "harbor.example/dpone:master",
            "namespace": "airflow-example",
            "airflow": {
                "service_account_name": "airflow-sa",
                "runner": {
                    "placement": {
                        "node_selector": {"dedicated": "datawarehouse"},
                        "tolerations": [
                            {"key": "dedicated", "value": "datawarehouse", "effect": "NoSchedule"},
                        ],
                    },
                    "embed_assets": [
                        {
                            "path": "certs/RootCA.pem",
                            "bind": {
                                "connection_id": "clickhouse_example",
                                "query_key": "ca_cert",
                            },
                        }
                    ],
                },
                "connection_projection": {
                    "mode": "unsafe_airflow_env",
                    "connection_ids": ["clickhouse_example"],
                },
            },
        },
        provenance={},
    )

    report = AirflowCompactPackBuilder().build(
        workload=workload,
        output_path=".dpone/gitops/airflow/marketing_sample_web_sync/airflow-pack.json",
        repo_root=tmp_path,
    )
    payload = report.to_jsonable()

    assert report.passed
    pod_spec = payload["pod_spec"]["spec"]
    assert pod_spec["nodeSelector"] == {"dedicated": "datawarehouse"}
    assert payload["provider_execution"] == {
        "schema": "dpone.airflow-provider-execution.v1",
        "kpo_kwargs": {
            "task_id": "marketing_sample_web_sync__dpone_runtime",
            "name": "dpone-marketing-sample-web-sync",
            "labels": {
                "app.kubernetes.io/component": "dpone-runner",
                "dpone.dev/workload-id": "marketing_sample_web_sync",
            },
            "env_vars": payload["kpo_kwargs"]["env_vars"],
        },
        "pod_spec": {
            "spec": {
                "nodeSelector": {"dedicated": "datawarehouse"},
                "tolerations": [
                    {
                        "key": "dedicated",
                        "value": "datawarehouse",
                        "effect": "NoSchedule",
                        "operator": "Equal",
                    }
                ],
                "containers": [{"name": "base"}],
            }
        },
    }
    assert (
        payload["connection_projection"]["query_overrides"]["clickhouse_example"]["ca_cert"]
        == "/workspace/repo/certs/RootCA.pem"
    )

    init_script = pod_spec["initContainers"][0]["args"][0]
    archive_b64 = init_script.split("<<'DPONE_INLINE_WORKLOAD_TAR_EOF' | tar -xz -C /workspace/repo\n", 1)[1].strip()
    with tarfile.open(fileobj=io.BytesIO(base64.b64decode(archive_b64)), mode="r:gz") as archive:
        assert sorted(archive.getnames()) == [
            "certs/RootCA.pem",
            "workloads/marketing/dpone/manifests/sample_web_sync.yaml",
        ]


def test_compact_pack_emits_retry_authority_for_exact_initial_route() -> None:
    repo_root = Path(__file__).parents[1]
    workload = GitOpsWorkloadDefinition(
        workload_id="orders_xmin_initial",
        manifest="examples/batch/postgres-xmin-initial-to-mssql.batch.yaml",
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={
            "image": "registry.example/dpone:dev",
            "airflow": {"xcom_sidecar_image": "registry.example/alpine:3.20"},
        },
        provenance={},
    )

    report = AirflowCompactPackBuilder().build(
        workload=workload,
        output_path=".dpone/gitops/airflow/orders/airflow-pack.json",
        repo_root=repo_root,
    )

    assert report.provider_execution["retry_authority"] == {
        "schema": "dpone.airflow-retry-authority.v1",
        "mode": "postgres_xmin_initial_mssql_target_atomic_v1",
        "max_task_retries": 3,
    }


def test_compact_pack_emits_retry_authority_for_exact_shadow_initial_route(
    tmp_path: Path,
) -> None:
    source = Path(__file__).parents[1] / "examples/batch/postgres-xmin-initial-to-mssql.batch.yaml"
    manifest = tmp_path / "postgres-xmin-shadow-initial-to-mssql.batch.yaml"
    manifest.write_text(
        source.read_text(encoding="utf-8").replace(
            "      unique_key: [id]\n      backfill:\n        inner_mode: incremental_merge\n",
            "      unique_key: [id]\n"
            "      only_new_rows: false\n"
            "      backfill:\n"
            "        inner_mode: incremental_append\n"
            "        publication: {mode: shadow_swap, retain_backup: true}\n",
        ),
        encoding="utf-8",
    )
    workload = GitOpsWorkloadDefinition(
        workload_id="orders_xmin_shadow_initial",
        manifest=manifest.name,
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={
            "image": "registry.example/dpone:dev",
            "airflow": {"xcom_sidecar_image": "registry.example/alpine:3.20"},
        },
        provenance={},
    )

    report = AirflowCompactPackBuilder().build(
        workload=workload,
        output_path=".dpone/gitops/airflow/orders/airflow-pack.json",
        repo_root=tmp_path,
    )

    assert report.provider_execution["retry_authority"] == {
        "schema": "dpone.airflow-retry-authority.v1",
        "mode": "postgres_xmin_initial_mssql_target_atomic_v1",
        "max_task_retries": 3,
    }


def test_compact_pack_fingerprint_binds_provider_execution_projection(tmp_path: Path) -> None:
    manifest = tmp_path / "pipeline.yaml"
    manifest.write_text("name: orders\nsource:\n  type: mssql\nsink:\n  type: clickhouse\n", encoding="utf-8")

    def build(node: str) -> dict[str, object]:
        workload = GitOpsWorkloadDefinition(
            workload_id="orders",
            manifest="pipeline.yaml",
            domain="sales",
            catalog_path="domains/sales.yaml",
            effective_config={
                "image": "registry.example/dpone:dev",
                "airflow": {"runner": {"placement": {"node_selector": {"node": node}}}},
            },
            provenance={},
        )
        return (
            AirflowCompactPackBuilder()
            .build(
                workload=workload,
                output_path=".dpone/gitops/airflow/orders/airflow-pack.json",
                repo_root=tmp_path,
            )
            .to_jsonable()
        )

    first = build("a")
    second = build("b")

    assert first["provider_execution"] != second["provider_execution"]
    assert first["pack_fingerprint"] != second["pack_fingerprint"]


def test_provider_execution_projects_only_provider_certified_pod_fields() -> None:
    projection = compact_provider_execution(
        kpo_kwargs={
            "task_id": "orders__dpone_runtime",
            "name": "dpone-orders",
            "labels": {"dpone.dev/workload-id": "orders"},
            "env_vars": {},
        },
        pod_spec={
            "spec": {
                "nodeSelector": {"dedicated": "datawarehouse"},
                "tolerations": [],
                "affinity": {"nodeAffinity": {}},
                "topologySpreadConstraints": [],
                "schedulerName": "custom",
                "priorityClassName": "batch",
                "containers": [{"name": "base", "resources": {"requests": {"cpu": "1"}}}],
            }
        },
    )

    assert projection["pod_spec"]["spec"] == {
        "nodeSelector": {"dedicated": "datawarehouse"},
        "tolerations": [],
        "containers": [{"name": "base", "resources": {"requests": {"cpu": "1"}}}],
    }


def test_compact_pack_fails_closed_when_runner_asset_missing(tmp_path: Path) -> None:
    manifest = tmp_path / "workloads" / "marketing" / "dpone" / "manifests" / "sample_web_sync.yaml"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("name: x\nsource:\n  type: clickhouse\nsink:\n  type: mssql\n", encoding="utf-8")
    workload = GitOpsWorkloadDefinition(
        workload_id="marketing_sample_web_sync",
        manifest="workloads/marketing/dpone/manifests/sample_web_sync.yaml",
        domain="marketing",
        catalog_path="x",
        effective_config={
            "image": "i:master",
            "airflow": {"runner": {"embed_paths": ["certs/RootCA.pem"]}},
        },
        provenance={},
    )

    report = AirflowCompactPackBuilder().build(
        workload=workload,
        output_path="pack.json",
        repo_root=tmp_path,
    )

    assert not report.passed
    assert report.blockers[0].code == "runner_embed_asset_missing"


def test_legacy_runner_embed_paths_remain_supported() -> None:
    contract = resolve_airflow_runner_contract(
        {"runner_embed_paths": ["certs/RootCA.pem", "certs/RootCA.pem", "../escape.pem"]}
    )
    assert contract.embed_paths == ("certs/RootCA.pem",)
