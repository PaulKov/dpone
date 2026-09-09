"""Tests for build-time Airflow asset graph inference."""

from __future__ import annotations

from pathlib import Path

import yaml

from dpone.gitops.airflow_asset_graph import build_asset_graph
from dpone.gitops.workload_catalog import WorkloadCatalogResolver
from tests.airflow_dag_spec_repo import (
    manifest_ref,
    write_domain,
    write_flow_manifest,
    write_manifest,
    write_workload_set,
)


def test_build_asset_graph_infers_producer_consumer_edge(tmp_path: Path) -> None:
    app = write_manifest(
        tmp_path,
        "app",
        outlets=["postgres://dst/app"],
        sink_schema="dst",
        sink_table="app",
    )
    web = write_manifest(
        tmp_path,
        "web",
        source_schema="dst",
        source_table="app",
        inlets=["postgres://dst/app"],
    )
    write_domain(
        tmp_path,
        workloads={"marketing_app": manifest_ref(app), "marketing_web": manifest_ref(web)},
    )
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    report = build_asset_graph(catalog.workloads, repo_root=tmp_path)

    assert len(report.edges) == 1
    edge = report.edges[0]
    assert edge.uri == "postgres://dst/app"
    assert edge.producer_node_id == "marketing_app"
    assert edge.consumer_node_id == "marketing_web"


def test_single_process_flow_asset_node_keeps_workload_identity(tmp_path: Path) -> None:
    flow = write_flow_manifest(tmp_path, "orders")
    write_domain(tmp_path, workloads={"sales_orders": manifest_ref(flow)})
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    report = build_asset_graph(catalog.workloads, repo_root=tmp_path)

    assert [node.node_id for node in report.nodes] == ["sales_orders"]


def test_single_process_explicit_batch_asset_node_uses_process_identity(tmp_path: Path) -> None:
    path = tmp_path / "workloads/marketing/dpone/manifests/orders.batch.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "kind": "dpone.batch.v1",
                "defaults": {
                    "source": {
                        "type": "postgres",
                        "connection_id": "pg_src",
                        "table": {"schema": "{{ src_schema }}", "name": "{{ src_table }}"},
                    },
                    "sink": {
                        "type": "postgres",
                        "connection_id": "pg_dst",
                        "table": {"schema": "dst", "name": "{{ src_table }}"},
                        "mode": "append",
                    },
                },
                "naming": {"process_name": "{{ src_schema }}__{{ src_table }}"},
                "schemas": {"public": {"tables": [{"table": "orders"}]}},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    write_domain(
        tmp_path,
        workloads={"sales_orders": manifest_ref(path.relative_to(tmp_path).as_posix())},
    )
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    report = build_asset_graph(catalog.workloads, repo_root=tmp_path)

    assert [node.node_id for node in report.nodes] == ["sales_orders__public__orders"]


def test_build_asset_graph_blocks_ambiguous_producers(tmp_path: Path) -> None:
    first = write_manifest(tmp_path, "w1", outlets=["postgres://dst/shared"], sink_table="shared")
    second = write_manifest(tmp_path, "w2", outlets=["postgres://dst/shared"], sink_table="shared")
    consumer = write_manifest(tmp_path, "w3", source_schema="dst", source_table="shared")
    write_domain(
        tmp_path,
        workloads={
            "marketing_w1": manifest_ref(first),
            "marketing_w2": manifest_ref(second),
            "marketing_w3": manifest_ref(consumer),
        },
    )
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    report = build_asset_graph(catalog.workloads, repo_root=tmp_path)

    assert report.edges == ()
    assert any(blocker.code == "asset_graph_uri_ambiguous" for blocker in report.blockers)
    assert not report.warnings
