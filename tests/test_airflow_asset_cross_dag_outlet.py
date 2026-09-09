"""Tests for cross-DAG inferred outlet and schedule behavior."""

from __future__ import annotations

from pathlib import Path

from dpone.gitops.airflow_asset_cross_dag import cross_dag_recommendations
from dpone.gitops.airflow_asset_graph import AssetGraphEdge, AssetGraphReport, build_asset_graph
from dpone.gitops.airflow_dag_spec_builder import AirflowDagSpecBuilder
from dpone.gitops.workload_catalog import WorkloadCatalogResolver
from tests.airflow_dag_spec_repo import dag_declaration, manifest_ref, write_domain, write_manifest, write_workload_set


def test_cross_dag_build_does_not_request_manual_outlet_for_inferable_sink(tmp_path: Path) -> None:
    producer = write_manifest(
        tmp_path,
        "producer",
        sink_schema="dst",
        sink_table="shared",
    )
    consumer = write_manifest(
        tmp_path,
        "consumer",
        source_schema="dst",
        source_table="shared",
        inlets=["postgres://dst/shared"],
    )
    write_domain(
        tmp_path,
        domain="marketing",
        workloads={"marketing_producer": manifest_ref(producer)},
        dags={"DAG__producer": dag_declaration(workloads=["marketing_producer"], wiring={"mode": "assets"})},
    )
    write_domain(
        tmp_path,
        domain="sales",
        workloads={"sales_consumer": manifest_ref(consumer)},
        dags={"DAG__consumer": dag_declaration(workloads=["sales_consumer"], wiring={"mode": "assets"})},
    )
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )
    dag_report = AirflowDagSpecBuilder(repo_root=tmp_path).build(
        workload_set=workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )
    recommendations = cross_dag_recommendations(
        asset_graph=build_asset_graph(catalog.workloads, repo_root=tmp_path),
        specs=dag_report.specs,
        catalog=catalog,
    )

    assert recommendations == ()
    assert any(
        warning.code == "asset_graph_cross_dag_schedule_inferred"
        for warning in dag_report.by_dag_id("DAG__consumer").warnings
    )
    assert not any(
        item.message and "add 'postgres://dst/shared' to airflow.execution.outlets" in item.message
        for item in dag_report.warnings
    )


def test_cross_dag_requires_outlet_for_non_inferred_edge(tmp_path: Path) -> None:
    producer = write_manifest(
        tmp_path,
        "producer",
        sink_schema="dst",
        sink_table="shared",
    )
    consumer = write_manifest(
        tmp_path,
        "consumer",
        source_schema="dst",
        source_table="shared",
    )
    curated_uri = "postgres://dst/shared"
    write_domain(
        tmp_path,
        domain="marketing",
        workloads={"marketing_producer": manifest_ref(producer)},
        dags={"DAG__producer": dag_declaration(workloads=["marketing_producer"], wiring={"mode": "assets"})},
    )
    write_domain(
        tmp_path,
        domain="sales",
        workloads={"sales_consumer": manifest_ref(consumer)},
        dags={
            "DAG__consumer": dag_declaration(
                workloads=["sales_consumer"],
                schedule={"assets": [curated_uri]},
                wiring={"mode": "assets"},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )
    dag_report = AirflowDagSpecBuilder(repo_root=tmp_path).build(
        workload_set=workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )
    asset_graph = build_asset_graph(catalog.workloads, repo_root=tmp_path)
    producer_node_id = next(node.node_id for node in asset_graph.nodes if node.workload_id == "marketing_producer")
    consumer_node_id = next(node.node_id for node in asset_graph.nodes if node.workload_id == "sales_consumer")
    curated_graph = AssetGraphReport(
        nodes=asset_graph.nodes,
        edges=(
            AssetGraphEdge(
                uri=curated_uri,
                producer_node_id=producer_node_id,
                consumer_node_id=consumer_node_id,
                reason="curated",
            ),
        ),
    )

    recommendations = cross_dag_recommendations(
        asset_graph=curated_graph,
        specs=dag_report.specs,
        catalog=catalog,
    )

    assert len(recommendations) == 1
    assert recommendations[0].missing_outlet is True
    assert recommendations[0].missing_schedule_asset is False
