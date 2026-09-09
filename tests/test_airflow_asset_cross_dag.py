"""Tests for cross-DAG asset schedule recommendations."""

from __future__ import annotations

from pathlib import Path

from dpone.gitops.airflow_asset_cross_dag import cross_dag_recommendations
from dpone.gitops.airflow_asset_graph import build_asset_graph
from dpone.gitops.airflow_dag_spec import DagScheduleAssets
from dpone.gitops.airflow_dag_spec_builder import AirflowDagSpecBuilder
from dpone.gitops.workload_catalog import WorkloadCatalogResolver
from tests.airflow_dag_spec_repo import dag_declaration, manifest_ref, write_domain, write_manifest, write_workload_set


def test_cross_dag_build_infers_consumer_schedule_assets(tmp_path: Path) -> None:
    producer = write_manifest(
        tmp_path,
        "producer",
        outlets=["postgres://dst/shared"],
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
    builder = AirflowDagSpecBuilder(repo_root=tmp_path)
    asset_graph = build_asset_graph(catalog.workloads, repo_root=tmp_path)
    dag_report = builder.build(workload_set=workload_set.relative_to(tmp_path).as_posix(), env="dev")

    consumer_spec = dag_report.by_dag_id("DAG__consumer")
    schedule = consumer_spec.declaration.schedule
    assert isinstance(schedule, DagScheduleAssets)
    assert any(asset.uri == "postgres://dst/shared" for asset in schedule.assets)
    assert any(warning.code == "asset_graph_cross_dag_schedule_inferred" for warning in consumer_spec.warnings)
    assert not any(warning.code == "asset_graph_cross_dag_wiring" for warning in dag_report.warnings)

    recommendations = cross_dag_recommendations(
        asset_graph=asset_graph,
        specs=dag_report.specs,
        catalog=catalog,
    )
    assert not recommendations


def test_cross_dag_keeps_cron_schedule_warning(tmp_path: Path) -> None:
    producer = write_manifest(
        tmp_path,
        "producer",
        outlets=["postgres://dst/shared"],
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
        dags={
            "DAG__consumer": dag_declaration(
                workloads=["sales_consumer"],
                schedule="0 6 * * *",
                wiring={"mode": "assets"},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)
    builder = AirflowDagSpecBuilder(repo_root=tmp_path)
    dag_report = builder.build(workload_set=workload_set.relative_to(tmp_path).as_posix(), env="dev")

    consumer_spec = dag_report.by_dag_id("DAG__consumer")
    assert consumer_spec.declaration.schedule == "0 6 * * *"
    assert any(warning.code == "asset_graph_cross_dag_wiring" for warning in dag_report.warnings)


def test_cross_dag_build_satisfies_declared_outlet_and_inferred_schedule(tmp_path: Path) -> None:
    producer = write_manifest(
        tmp_path,
        "producer",
        outlets=["postgres://dst/shared"],
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
    builder = AirflowDagSpecBuilder(repo_root=tmp_path)
    asset_graph = build_asset_graph(catalog.workloads, repo_root=tmp_path)
    dag_report = builder.build(workload_set=workload_set.relative_to(tmp_path).as_posix(), env="dev")

    recommendations = cross_dag_recommendations(
        asset_graph=asset_graph,
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
