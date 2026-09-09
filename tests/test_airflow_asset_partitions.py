from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator

from dpone.gitops.airflow_asset_graph import build_asset_graph
from dpone.gitops.airflow_asset_partition import (
    AssetPartitionContractError,
    parse_asset_partition,
)
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.airflow_dag_spec import DagScheduleAssets
from dpone.gitops.airflow_dag_spec_builder import AirflowDagSpecBuilder
from dpone.gitops.workload_catalog import WorkloadCatalogResolver
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
from dpone.services.interval_context import IntervalContextService
from tests.airflow_dag_spec_repo import dag_declaration, manifest_ref, write_domain, write_manifest, write_workload_set


def _daily_partition(dimension: str = "business_date") -> dict[str, object]:
    return {
        "dimensions": {
            dimension: {
                "type": "temporal",
                "granularity": "day",
                "timezone": "UTC",
            }
        }
    }


def _asset(uri: str, *, partition: dict[str, object] | None = None) -> dict[str, object]:
    payload: dict[str, object] = {"uri": uri}
    if partition is not None:
        payload["partition"] = partition
    return payload


def _write_provider_pack(
    tmp_path: Path,
    *,
    execution: dict[str, object],
) -> Path:
    path = tmp_path / "airflow-pack.json"
    path.write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "schema_version": "3",
                "producer": "test",
                "kpo_kwargs": {
                    "task_id": "load_orders",
                    "name": "load-orders",
                    "namespace": "airflow",
                    "image": "dpone-runtime:test",
                },
                "airflow": {"execution": execution},
                "steps": [],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_partition_contract_normalizes_one_temporal_dimension() -> None:
    partition = parse_asset_partition(_daily_partition())

    assert partition is not None
    assert partition.dimension == "business_date"
    assert partition.granularity == "day"
    assert partition.timezone == "UTC"
    assert partition.key_format == "%Y-%m-%d"
    assert partition.to_jsonable()["dimensions"]["business_date"]["source"] == "dag_schedule"


def test_partition_contract_rejects_multiple_dimensions_in_v1() -> None:
    raw = _daily_partition()
    raw["dimensions"]["tenant"] = {"type": "categorical"}  # type: ignore[index]

    with pytest.raises(AssetPartitionContractError) as raised:
        parse_asset_partition(raw)

    assert raised.value.code == "asset_partition_dimension_count_invalid"


def test_partition_contract_rejects_unknown_timezone() -> None:
    raw = _daily_partition()
    raw["dimensions"]["business_date"]["timezone"] = "Mars/Olympus"  # type: ignore[index]

    with pytest.raises(AssetPartitionContractError, match="unknown asset partition timezone"):
        parse_asset_partition(raw)


def test_asset_graph_inherits_producer_partition_for_consumer(tmp_path: Path) -> None:
    uri = "postgres://dst/orders"
    producer = write_manifest(
        tmp_path,
        "producer",
        outlets=[_asset(uri, partition=_daily_partition())],
        sink_table="orders",
    )
    consumer = write_manifest(
        tmp_path,
        "consumer",
        inlets=[uri],
        source_schema="dst",
        source_table="orders",
    )
    write_domain(
        tmp_path,
        workloads={"producer": manifest_ref(producer), "consumer": manifest_ref(consumer)},
    )
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    report = build_asset_graph(catalog.workloads, repo_root=tmp_path)

    assert not report.blockers
    assert len(report.edges) == 1
    assert report.edges[0].partition is not None
    assert report.edges[0].partition.dimension == "business_date"
    assert report.edges[0].partition_provenance == "inherited:producer_outlet"


def test_asset_graph_blocks_partition_contract_mismatch(tmp_path: Path) -> None:
    uri = "postgres://dst/orders"
    producer = write_manifest(
        tmp_path,
        "producer",
        outlets=[_asset(uri, partition=_daily_partition("business_date"))],
        sink_table="orders",
    )
    consumer = write_manifest(
        tmp_path,
        "consumer",
        inlets=[_asset(uri, partition=_daily_partition("loaded_date"))],
        source_schema="dst",
        source_table="orders",
    )
    write_domain(
        tmp_path,
        workloads={"producer": manifest_ref(producer), "consumer": manifest_ref(consumer)},
    )
    workload_set = write_workload_set(tmp_path)
    catalog = WorkloadCatalogResolver(repo_root=tmp_path).resolve(
        workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    report = build_asset_graph(catalog.workloads, repo_root=tmp_path)

    assert report.edges == ()
    assert any(item.code == "asset_partition_contract_mismatch" for item in report.blockers)


def test_cross_dag_builder_emits_partitioned_producer_and_consumer_plans(tmp_path: Path) -> None:
    uri = "postgres://dst/orders"
    producer = write_manifest(
        tmp_path,
        "producer",
        outlets=[_asset(uri, partition=_daily_partition())],
        sink_table="orders",
    )
    consumer = write_manifest(
        tmp_path,
        "consumer",
        inlets=[uri],
        source_schema="dst",
        source_table="orders",
    )
    write_domain(
        tmp_path,
        domain="producer_domain",
        workloads={"producer": manifest_ref(producer)},
        dags={
            "orders_producer": dag_declaration(
                workloads=["producer"],
                schedule="0 2 * * *",
                timezone="UTC",
                wiring={"mode": "assets"},
            )
        },
    )
    write_domain(
        tmp_path,
        domain="consumer_domain",
        workloads={"consumer": manifest_ref(consumer)},
        dags={
            "orders_consumer": dag_declaration(
                workloads=["consumer"],
                schedule=None,
                timezone="UTC",
                wiring={"mode": "assets"},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)

    report = AirflowDagSpecBuilder(repo_root=tmp_path).build(
        workload_set=workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    assert report.passed
    producer_spec = report.by_dag_id("orders_producer")
    consumer_spec = report.by_dag_id("orders_consumer")
    assert producer_spec.partition_plan is not None
    assert producer_spec.partition_plan.mode == "cron_producer"
    assert consumer_spec.partition_plan is not None
    assert consumer_spec.partition_plan.mode == "asset_consumer"
    assert isinstance(consumer_spec.declaration.schedule, DagScheduleAssets)
    assert consumer_spec.declaration.schedule.assets[0].partition is not None
    assert consumer_spec.to_jsonable()["partition_plan"]["dimension"] == "business_date"


def test_partitioned_producer_without_schedule_fails_closed(tmp_path: Path) -> None:
    producer = write_manifest(
        tmp_path,
        "producer",
        outlets=[_asset("dpone://orders", partition=_daily_partition())],
        sink_table="orders",
    )
    write_domain(
        tmp_path,
        workloads={"producer": manifest_ref(producer)},
        dags={
            "orders_producer": dag_declaration(
                workloads=["producer"],
                schedule=None,
                timezone="UTC",
                wiring={"mode": "assets"},
            )
        },
    )

    report = AirflowDagSpecBuilder(repo_root=tmp_path).build(
        workload_set=write_workload_set(tmp_path).relative_to(tmp_path).as_posix(),
        env="dev",
    )

    assert not report.passed
    assert any(item.code == "asset_partition_producer_schedule_required" for item in report.blockers)


def test_producer_dag_with_mixed_partition_contracts_fails_closed(tmp_path: Path) -> None:
    producer = write_manifest(
        tmp_path,
        "producer",
        outlets=[
            _asset("dpone://orders", partition=_daily_partition("business_date")),
            _asset("dpone://customers", partition=_daily_partition("loaded_date")),
        ],
        sink_table="orders",
    )
    write_domain(
        tmp_path,
        workloads={"producer": manifest_ref(producer)},
        dags={
            "mixed_producer": dag_declaration(
                workloads=["producer"],
                schedule="0 2 * * *",
                timezone="UTC",
                wiring={"mode": "assets"},
            )
        },
    )

    report = AirflowDagSpecBuilder(repo_root=tmp_path).build(
        workload_set=write_workload_set(tmp_path).relative_to(tmp_path).as_posix(),
        env="dev",
    )

    assert not report.passed
    assert any(item.code == "asset_partition_producer_mixed_contracts" for item in report.blockers)


def test_partitioned_producer_rejects_unpartitioned_asset_schedule(tmp_path: Path) -> None:
    producer = write_manifest(
        tmp_path,
        "producer",
        outlets=[_asset("dpone://orders", partition=_daily_partition())],
        sink_table="orders",
    )
    write_domain(
        tmp_path,
        workloads={"producer": manifest_ref(producer)},
        dags={
            "orders_producer": dag_declaration(
                workloads=["producer"],
                schedule={"assets": [{"uri": "dpone://external/calendar", "external": True}]},
                timezone="UTC",
                wiring={"mode": "assets"},
            )
        },
    )

    report = AirflowDagSpecBuilder(repo_root=tmp_path).build(
        workload_set=write_workload_set(tmp_path).relative_to(tmp_path).as_posix(),
        env="dev",
    )

    assert not report.passed
    assert any(item.code == "asset_partition_contract_mismatch" for item in report.blockers)


def test_provider_negotiates_native_and_degraded_partition_schedules() -> None:
    from dpone_airflow_pack.asset_partitions import (
        AirflowPartitionCapabilities,
        materialize_partition_schedule,
    )

    class CronPartitionTimetable:
        def __init__(self, cron: str, *, timezone: str, key_format: str) -> None:
            self.cron = cron
            self.timezone = timezone
            self.key_format = key_format

    class IdentityMapper:
        pass

    class PartitionedAssetTimetable:
        def __init__(self, *, assets: object, default_partition_mapper: object) -> None:
            self.assets = assets
            self.default_partition_mapper = default_partition_mapper

    capabilities = AirflowPartitionCapabilities(
        cron_partition_timetable=CronPartitionTimetable,
        partitioned_asset_timetable=PartitionedAssetTimetable,
        identity_mapper=IdentityMapper,
    )
    plan = {
        "mode": "cron_producer",
        "dimension": "business_date",
        "granularity": "day",
        "timezone": "UTC",
        "key_format": "%Y-%m-%d",
        "mapper": "identity",
    }

    native = materialize_partition_schedule(
        schedule="0 2 * * *",
        partition_plan=plan,
        assets=(),
        capabilities=capabilities,
    )
    degraded = materialize_partition_schedule(
        schedule="0 2 * * *",
        partition_plan=plan,
        assets=(),
        capabilities=replace(
            capabilities,
            cron_partition_timetable=None,
            partitioned_asset_timetable=None,
            identity_mapper=None,
        ),
    )

    assert native.handled is True
    assert native.mode == "native"
    assert native.value.key_format == "%Y-%m-%d"
    assert degraded.handled is False
    assert degraded.mode == "degraded_unpartitioned"


def test_provider_rejects_partially_available_partition_sdk() -> None:
    from dpone_airflow_pack.asset_partitions import (
        AirflowPartitionCapabilities,
        materialize_partition_schedule,
    )

    capabilities = AirflowPartitionCapabilities(
        cron_partition_timetable=type("CronPartitionTimetable", (), {}),
        partitioned_asset_timetable=None,
        identity_mapper=None,
    )

    with pytest.raises(ValueError, match="DPONE_AIRFLOW_PARTITION_CAPABILITY_INVALID"):
        materialize_partition_schedule(
            schedule="0 2 * * *",
            partition_plan={"mode": "cron_producer"},
            assets=(),
            capabilities=capabilities,
        )


def test_provider_materializes_identity_mapped_asset_consumer() -> None:
    from dpone_airflow_pack.asset_partitions import (
        AirflowPartitionCapabilities,
        materialize_partition_schedule,
    )

    class Asset:
        def __init__(self, name: str) -> None:
            self.names = (name,)

        def __and__(self, other: Asset) -> Asset:
            combined = Asset(self.names[0])
            combined.names = (*self.names, *other.names)
            return combined

    class IdentityMapper:
        pass

    class PartitionedAssetTimetable:
        def __init__(self, *, assets: Asset, default_partition_mapper: object) -> None:
            self.assets = assets
            self.default_partition_mapper = default_partition_mapper

    capabilities = AirflowPartitionCapabilities(
        cron_partition_timetable=type("CronPartitionTimetable", (), {}),
        partitioned_asset_timetable=PartitionedAssetTimetable,
        identity_mapper=IdentityMapper,
    )
    decision = materialize_partition_schedule(
        schedule={"assets": [{"uri": "dpone://orders"}]},
        partition_plan={
            "mode": "asset_consumer",
            "dimension": "business_date",
            "timezone": "UTC",
            "key_format": "%Y-%m-%d",
        },
        assets=(Asset("orders"), Asset("customers")),
        capabilities=capabilities,
    )

    assert decision.handled is True
    assert decision.mode == "native"
    assert decision.value.assets.names == ("orders", "customers")
    assert isinstance(decision.value.default_partition_mapper, IdentityMapper)


def test_compact_pack_normalizes_partition_defaults() -> None:
    workload = GitOpsWorkloadDefinition(
        workload_id="orders_daily",
        manifest="orders.yaml",
        domain="sales",
        catalog_path="domain.yaml",
        effective_config={
            "image": "dpone-runtime@sha256:abc",
            "namespace": "airflow",
            "airflow": {
                "execution": {
                    "outlets": [_asset("dpone://orders", partition=_daily_partition())],
                }
            },
        },
        provenance={},
    )

    report = AirflowCompactPackBuilder().build(
        workload=workload,
        output_path="packs/orders/airflow-pack.json",
    )

    partition = report.to_jsonable()["airflow"]["execution"]["outlets"][0]["partition"]
    dimension = partition["dimensions"]["business_date"]
    assert dimension["key_format"] == "%Y-%m-%d"
    assert dimension["source"] == "dag_schedule"


def test_partitioned_dag_spec_validates_against_public_schema(tmp_path: Path) -> None:
    uri = "postgres://dst/orders"
    producer = write_manifest(
        tmp_path,
        "producer",
        outlets=[_asset(uri, partition=_daily_partition())],
        sink_table="orders",
    )
    write_domain(
        tmp_path,
        workloads={"producer": manifest_ref(producer)},
        dags={
            "orders_producer": dag_declaration(
                workloads=["producer"],
                schedule="0 2 * * *",
                timezone="UTC",
                wiring={"mode": "assets"},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)
    report = AirflowDagSpecBuilder(repo_root=tmp_path).build(
        workload_set=workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )
    schema_path = Path(__file__).parents[1] / "docs/schemas/gitops/airflow-dag-spec.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    Draft202012Validator(schema).validate(report.by_dag_id("orders_producer").to_jsonable())


def test_provider_partition_env_is_bounded_and_uses_scheduler_key() -> None:
    import dpone_airflow_pack.asset_partitions as provider_partitions

    pack = {
        "airflow": {
            "execution": {
                "outlets": [_asset("dpone://orders", partition=parse_asset_partition(_daily_partition()).to_jsonable())]
            }
        }
    }

    env = provider_partitions.partition_env_vars_from_pack(pack, materialization_mode="native")

    assert env["DPONE_PARTITION_DIMENSION"] == "business_date"
    assert env["DPONE_PARTITION_MODE"] == "native"
    assert "dag_run.partition_key" in env["DPONE_PARTITION_KEY"]


def test_provider_rejects_malformed_pack_partition() -> None:
    from dpone_airflow_pack.asset_partitions import partition_env_vars_from_pack

    pack = {
        "airflow": {
            "execution": {
                "outlets": [{"uri": "dpone://orders", "partition": {}}],
            }
        }
    }

    with pytest.raises(ValueError, match="requires exactly one dimension"):
        partition_env_vars_from_pack(pack)


def test_custom_dag_partitioned_pack_reports_degraded_mode(tmp_path: Path) -> None:
    from dpone_airflow_pack import build_dpone_gitops_task_group_from_pack

    pack_path = _write_provider_pack(
        tmp_path,
        execution={
            "outlets": [_asset("dpone://orders", partition=parse_asset_partition(_daily_partition()).to_jsonable())]
        },
    )

    runtime = build_dpone_gitops_task_group_from_pack(pack_path, dag=object())["dpone_runtime"]

    assert runtime.env_vars["DPONE_PARTITION_MODE"] == "degraded_unpartitioned"


def test_consumer_runtime_context_uses_attached_dag_partition_plan(tmp_path: Path) -> None:
    from dpone_airflow_pack import build_dpone_gitops_task_group_from_pack

    plan = {
        "mode": "asset_consumer",
        "dimension": "business_date",
        "type": "temporal",
        "granularity": "day",
        "timezone": "UTC",
        "key_format": "%Y-%m-%d",
        "source": "dag_schedule",
    }
    dag = SimpleNamespace(
        _dpone_partition_mode="native",
        _dpone_partition_plan=plan,
    )
    pack_path = _write_provider_pack(tmp_path, execution={"inlets": ["dpone://orders"]})

    runtime = build_dpone_gitops_task_group_from_pack(pack_path, dag=dag)["dpone_runtime"]

    assert runtime.env_vars["DPONE_PARTITION_DIMENSION"] == "business_date"
    assert runtime.env_vars["DPONE_PARTITION_MODE"] == "native"
    assert "dag_run.partition_key" in runtime.env_vars["DPONE_PARTITION_KEY"]


def test_runtime_context_rejects_dag_and_pack_partition_mismatch(tmp_path: Path) -> None:
    from dpone_airflow_pack import build_dpone_gitops_task_group_from_pack

    dag = SimpleNamespace(
        _dpone_partition_mode="native",
        _dpone_partition_plan={
            "mode": "asset_consumer",
            "dimension": "loaded_date",
            "type": "temporal",
            "granularity": "day",
            "timezone": "UTC",
            "key_format": "%Y-%m-%d",
            "source": "dag_schedule",
        },
    )
    pack_path = _write_provider_pack(
        tmp_path,
        execution={
            "inlets": [_asset("dpone://orders", partition=parse_asset_partition(_daily_partition()).to_jsonable())]
        },
    )

    with pytest.raises(ValueError, match="mixed partition contracts"):
        build_dpone_gitops_task_group_from_pack(pack_path, dag=dag)


def test_native_partition_context_requires_actual_partition_key() -> None:
    from dpone.contracts.run_interval import RunInterval

    interval = RunInterval(
        partition_dimension="business_date",
        partition_mode="native",
        partition_key=None,
    )

    with pytest.raises(ValueError, match="DPONE_AIRFLOW_PARTITION_KEY_MISSING"):
        IntervalContextService(interval)


def test_partition_runtime_context_rejects_unbounded_scheduler_key() -> None:
    from dpone.contracts.run_interval import RunInterval

    interval = RunInterval(
        partition_dimension="business_date",
        partition_mode="native",
        partition_key="x" * 257,
    )

    with pytest.raises(ValueError, match="DPONE_AIRFLOW_PARTITION_KEY_INVALID"):
        IntervalContextService(interval)


def test_partition_runtime_context_rejects_partial_metadata() -> None:
    from dpone.contracts.run_interval import RunInterval

    interval = RunInterval(partition_key="2026-07-16")

    with pytest.raises(ValueError, match="DPONE_AIRFLOW_PARTITION_MODE_MISSING"):
        IntervalContextService(interval)
