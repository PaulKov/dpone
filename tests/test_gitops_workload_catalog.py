from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from dpone.gitops.workload_catalog import WorkloadCatalogResolver
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue
from dpone.gitops.workload_impact import AffectedWorkloadResolver


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(text).strip() + "\n", encoding="utf-8")
    return path


def _workload_tree(tmp_path: Path) -> Path:
    root = tmp_path / "dpone_workloads"
    _write(
        root / "gitops.yaml",
        """
        gitops:
          version: 1
          defaults:
            runner: airflow
            resources_profile: safe_worker
            outcome_mode: xcom_then_gate
            airflow:
              service_account_name: airflow-sa
              image_pull_secret: regsecret
              connection_projection:
                mode: unsafe_airflow_env
                connection_ids: [mssql_prod, ClickHouse]
          environments:
            dev:
              namespace: airflow-dev
              runner_policy: advisory
            prod:
              namespace: airflow-prod
              runner_policy: certified_only
          source_types:
            mssql:
              resources_profile: throughput
          sources:
            mssql_reporting:
              source_type: mssql
              snapshot_profile: weak_worker_chunks
          includes:
            - path: gitops/domains/**/*.yaml
            - manifest_glob: manifests/**/*.yaml
              infer_workload: true
        """,
    )
    _write(
        root / "gitops" / "domains" / "interchange.yaml",
        """
        domain: interchange
        defaults:
          owner: data-office
          labels: [interchange]
          resources_profile: balanced
          airflow:
            xcom_sidecar_image: registry.example/alpine:3.23.4
            connection_projection:
              database_overrides:
                ClickHouse: DWH_Raw
        workloads:
          inter_ch_example_customer_directory:
            manifest: ../../manifests/mssql/interchange/example_customer_directory.yaml
            source: mssql_reporting
            schedule: "0 7 * * *"
            resources_profile: safe_worker
        """,
    )
    _write(
        root / "manifests" / "mssql" / "interchange" / "example_customer_directory.yaml",
        """
        source: {}
        sink: {}
        processes:
          - name: load
            sql_file: ../../../sql/interchange/example_customer_directory.sql
        """,
    )
    _write(
        root / "sql" / "interchange" / "example_customer_directory.sql",
        "SELECT 1 AS id\n",
    )
    _write(root / "manifests" / "mssql" / "orders.yaml", "source: {}\nsink: {}\n")
    return root


def test_workload_catalog_resolves_includes_precedence_and_provenance(tmp_path: Path) -> None:
    root = _workload_tree(tmp_path)

    report = WorkloadCatalogResolver(repo_root=tmp_path).resolve(root / "gitops.yaml", env="dev")

    assert report.passed
    assert [item.workload_id for item in report.workloads] == [
        "inter_ch_example_customer_directory",
        "mssql__interchange__example_customer_directory",
        "mssql__orders",
    ]
    workload = report.by_id("inter_ch_example_customer_directory")
    assert workload.manifest == "dpone_workloads/manifests/mssql/interchange/example_customer_directory.yaml"
    assert workload.effective_config["runner"] == "airflow"
    assert workload.effective_config["namespace"] == "airflow-dev"
    assert workload.effective_config["runner_policy"] == "advisory"
    assert workload.effective_config["owner"] == "data-office"
    assert workload.effective_config["source_type"] == "mssql"
    assert workload.effective_config["snapshot_profile"] == "weak_worker_chunks"
    assert workload.effective_config["resources_profile"] == "safe_worker"
    assert workload.effective_config["airflow"] == {
        "service_account_name": "airflow-sa",
        "image_pull_secret": "regsecret",
        "xcom_sidecar_image": "registry.example/alpine:3.23.4",
        "connection_projection": {
            "mode": "unsafe_airflow_env",
            "connection_ids": ["mssql_prod", "ClickHouse"],
            "database_overrides": {"ClickHouse": "DWH_Raw"},
        },
    }
    assert workload.provenance["resources_profile"].scope == "workload"
    assert workload.provenance["namespace"].scope == "environment"
    assert workload.provenance["airflow.image_pull_secret"].scope == "global"
    assert workload.provenance["airflow.xcom_sidecar_image"].scope == "domain"
    assert workload.provenance["airflow.connection_projection.mode"].scope == "global"
    assert workload.provenance["airflow.connection_projection.database_overrides.ClickHouse"].scope == "domain"
    assert str(tmp_path) not in report.to_json()


def test_workload_catalog_blocks_duplicate_ids(tmp_path: Path) -> None:
    root = _workload_tree(tmp_path)
    _write(
        root / "gitops" / "domains" / "duplicate.yaml",
        """
        domain: duplicate
        workloads:
          inter_ch_example_customer_directory:
            manifest: ../../manifests/mssql/orders.yaml
        """,
    )

    report = WorkloadCatalogResolver(repo_root=tmp_path).resolve(root / "gitops.yaml", env="dev")

    assert not report.passed
    assert [issue.code for issue in report.blockers] == ["workload_id_duplicate"]
    assert isinstance(report.blockers[0], GitOpsWorkloadCatalogIssue)


def test_affected_workload_resolver_maps_catalog_manifest_and_global_changes(tmp_path: Path) -> None:
    root = _workload_tree(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(root / "gitops.yaml", env="dev")

    impact = AffectedWorkloadResolver(repo_root=tmp_path).resolve(
        catalog,
        changed_files=[
            "dpone_workloads/gitops/domains/interchange.yaml",
            "dpone_workloads/manifests/mssql/orders.yaml",
        ],
    )

    assert impact.passed
    assert [item.workload_id for item in impact.affected_workloads] == [
        "inter_ch_example_customer_directory",
        "mssql__interchange__example_customer_directory",
        "mssql__orders",
    ]
    assert impact.affected_workloads[0].reasons[0].reason == "catalog_changed"
    assert str(tmp_path) not in impact.to_json()


def test_affected_workload_resolver_maps_sql_only_dependency_closure(tmp_path: Path) -> None:
    root = _workload_tree(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(root / "gitops.yaml", env="dev")

    impact = AffectedWorkloadResolver(repo_root=tmp_path).resolve(
        catalog,
        changed_files=["dpone_workloads/sql/interchange/example_customer_directory.sql"],
    )

    assert impact.passed
    assert [item.workload_id for item in impact.affected_workloads] == [
        "inter_ch_example_customer_directory",
        "mssql__interchange__example_customer_directory",
    ]
    assert {reason.reason for reason in impact.affected_workloads[0].reasons} == {"sql_file_changed"}


def test_affected_workload_resolver_maps_embed_asset_dependency_closure(tmp_path: Path) -> None:
    root = _workload_tree(tmp_path)
    cert = tmp_path / "certs" / "RootCA.pem"
    cert.parent.mkdir(parents=True)
    cert.write_text("pem\n", encoding="utf-8")
    _write(
        root / "gitops" / "domains" / "interchange.yaml",
        """
        domain: interchange
        defaults:
          owner: data-office
          labels: [interchange]
          resources_profile: balanced
          airflow:
            xcom_sidecar_image: registry.example/alpine:3.23.4
            connection_projection:
              database_overrides:
                ClickHouse: DWH_Raw
        workloads:
          inter_ch_example_customer_directory:
            manifest: ../../manifests/mssql/interchange/example_customer_directory.yaml
            source: mssql_reporting
            schedule: "0 7 * * *"
            resources_profile: safe_worker
            airflow:
              runner:
                embed_assets:
                  - path: certs/RootCA.pem
        """,
    )
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(root / "gitops.yaml", env="dev")
    workload = catalog.by_id("inter_ch_example_customer_directory")
    assert "runner" in workload.effective_config["airflow"]

    impact = AffectedWorkloadResolver(repo_root=tmp_path).resolve(
        catalog,
        changed_files=["certs/RootCA.pem"],
    )

    assert impact.passed
    assert [item.workload_id for item in impact.affected_workloads] == [
        "inter_ch_example_customer_directory",
    ]
    assert {reason.reason for reason in impact.affected_workloads[0].reasons} == {"embed_asset_changed"}


def test_affected_workload_resolver_maps_authoring_fragment_dependency_closure(
    tmp_path: Path,
) -> None:
    import yaml

    # Mirror folder-authoring layout used by WorkloadDependencyResolver contract tests:
    # pipeline lives at repo-root pipelines/... (not under dpone_workloads/).
    root = tmp_path / "dpone_workloads"
    pipeline = {
        "kind": "dpone.flow.v1",
        "authoring": {"mode": "folder", "source": "pipelines/orders_daily/pipeline.yaml"},
        "metadata": {"id": "orders_daily", "domain": "sales", "tags": ["dpone"]},
        "fragments": ["steps/load.yaml"],
    }
    fragment = {
        "kind": "dpone.flow-fragment.v1",
        "processes": [
            {
                "name": "orders_daily",
                "source": {
                    "type": "mssql",
                    "connection_ref": "mssql_dev",
                    "table": {"schema": "dbo", "name": "orders"},
                },
                "sink": {
                    "type": "clickhouse",
                    "connection_ref": "clickhouse_dev",
                    "table": {"schema": "analytics", "name": "orders"},
                    "strategy": {"mode": "replace"},
                },
            }
        ],
    }
    _write(
        root / "gitops.yaml",
        """
        gitops:
          version: 1
          defaults:
            runner: airflow
            resources_profile: safe_worker
            outcome_mode: xcom_then_gate
          environments:
            dev:
              namespace: airflow-dev
              runner_policy: advisory
          includes:
            - path: gitops/domains/**/*.yaml
        """,
    )
    _write(
        root / "gitops" / "domains" / "orders.yaml",
        """
        domain: orders
        workloads:
          orders_daily:
            manifest: ../../../pipelines/orders_daily/pipeline.yaml
        """,
    )
    pipeline_path = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    pipeline_path.parent.mkdir(parents=True)
    pipeline_path.write_text(yaml.safe_dump(pipeline, sort_keys=False), encoding="utf-8")
    fragment_path = pipeline_path.parent / "steps" / "load.yaml"
    fragment_path.parent.mkdir(parents=True)
    fragment_path.write_text(yaml.safe_dump(fragment, sort_keys=False), encoding="utf-8")

    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(root / "gitops.yaml", env="dev")
    impact = AffectedWorkloadResolver(repo_root=tmp_path).resolve(
        catalog,
        changed_files=["pipelines/orders_daily/steps/load.yaml"],
    )

    assert impact.passed
    assert [item.workload_id for item in impact.affected_workloads] == ["orders_daily"]
    assert {reason.reason for reason in impact.affected_workloads[0].reasons} == {"authoring_fragment_changed"}
