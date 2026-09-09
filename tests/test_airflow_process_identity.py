from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from dpone_airflow_pack.node_materialization import PackNodeMaterialization, materialize_process_plan

from dpone.gitops.airflow_compact_process_plans import CompactProcessPlanError, build_compact_process_plans
from dpone.gitops.airflow_dag_spec_builder import AirflowDagSpecBuilder
from dpone.gitops.airflow_process_identity import (
    requires_selector_scoped_execution,
    resolve_airflow_process_identities,
)
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
from dpone.manifest.models import LoadedManifest, ProcessSpec
from tests.airflow_dag_spec_repo import (
    dag_declaration,
    manifest_ref,
    standard_repo,
    write_batch_manifest,
    write_domain,
    write_manifest,
    write_workload_set,
)


def test_single_flow_keeps_workload_node_but_requires_selector() -> None:
    manifest = _manifest(source_kind="dpone.flow.v1", processes=(_process("load_orders", "dbo.orders"),))

    identities = resolve_airflow_process_identities(manifest, workload_id="orders")

    assert [(item.node_id, item.process.selector, item.selector_required) for item in identities] == [
        ("orders", "dbo.orders", True)
    ]
    assert requires_selector_scoped_execution(manifest)


def test_single_classic_authoring_keeps_workload_node_but_requires_selector() -> None:
    manifest = _manifest(
        kind="dpone.batch.v1",
        raw={"authoring": {"mode": "classic"}},
        processes=(_process("load_orders", "dbo.orders"),),
    )

    identities = resolve_airflow_process_identities(manifest, workload_id="orders")

    assert [(item.node_id, item.process.selector, item.selector_required) for item in identities] == [
        ("orders", "dbo.orders", True)
    ]


def test_multi_process_flow_uses_process_node_identity() -> None:
    manifest = _manifest(
        source_kind="dpone.flow.v1",
        processes=(
            _process("load_orders", "dbo.orders"),
            _process("load_customers", "dbo.customers"),
        ),
    )

    identities = resolve_airflow_process_identities(manifest, workload_id="sales")

    assert [item.node_id for item in identities] == ["sales__load_orders", "sales__load_customers"]
    assert all(item.selector_required for item in identities)


def test_explicit_batch_keeps_process_identity_even_with_one_process() -> None:
    manifest = _manifest(
        kind="dpone.batch.v1",
        source_kind="dpone.batch.v1",
        processes=(_process("load_orders", "dbo.orders"),),
    )

    identities = resolve_airflow_process_identities(manifest, workload_id="orders")

    assert [(item.node_id, item.selector_required) for item in identities] == [("orders__load_orders", True)]


def test_legacy_selectorless_manifest_keeps_existing_workload_identity() -> None:
    manifest = _manifest(processes=(_process("orders", None),))

    identities = resolve_airflow_process_identities(manifest, workload_id="orders")

    assert [(item.node_id, item.selector_required) for item in identities] == [("orders", False)]


def test_process_plan_node_identity_mismatch_is_rejected() -> None:
    pack = {
        "runtime_selection": {"mode": "process_plan"},
        "process_plans": {
            "dbo.orders": {
                "selector": "dbo.orders",
                "dag_node": {"node_id": "orders__other"},
                "runtime_commands": {
                    "inline": "dpone run runtime/orders.yaml --selector dbo.orders",
                    "expanded": "dpone run runtime/orders.yaml --selector dbo.orders",
                },
                "steps": [],
            }
        },
    }
    node = PackNodeMaterialization(
        node_id="orders__load_orders",
        workload_id="orders",
        selector="dbo.orders",
        visibility="inline",
        task_group=None,
    )

    with pytest.raises(ValueError, match="process plan node_id does not match"):
        materialize_process_plan(pack, node)


def test_explicit_batch_uses_same_process_identity_for_assets_and_dag(tmp_path: Path) -> None:
    producer = write_batch_manifest(tmp_path, "producer")
    producer_path = tmp_path / producer
    producer_payload = yaml.safe_load(producer_path.read_text(encoding="utf-8"))
    producer_payload["schemas"]["public"]["tables"] = [{"table": "t1"}]
    producer_path.write_text(yaml.safe_dump(producer_payload, sort_keys=False), encoding="utf-8")
    consumer = write_manifest(tmp_path, "consumer", source_schema="dst", source_table="public__t1")
    write_domain(
        tmp_path,
        workloads={"producer": manifest_ref(producer), "consumer": manifest_ref(consumer)},
        dags={
            "DAG__assets": dag_declaration(
                workloads=["producer", "consumer"],
                wiring={"mode": "assets"},
            )
        },
    )
    workload_set = write_workload_set(tmp_path)

    report = AirflowDagSpecBuilder(repo_root=tmp_path).build(
        workload_set=workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    assert report.passed, [blocker.to_jsonable() for blocker in report.blockers]
    spec = report.by_dag_id("DAG__assets")
    assert [(node.node_id, node.selector) for node in spec.nodes] == [
        ("producer__public__t1", "public.t1"),
        ("consumer", None),
    ]
    assert [(edge.upstream, edge.downstream, edge.reason) for edge in spec.edges] == [
        ("producer__public__t1", "consumer", "inferred")
    ]


def test_explicit_wiring_unknown_node_blocks_instead_of_dropping_edge(tmp_path: Path) -> None:
    workload_set = standard_repo(
        tmp_path,
        dags={
            "DAG__explicit": dag_declaration(
                wiring={"mode": "explicit", "dependencies": {"marketing_web": ["missing_node"]}}
            )
        },
    )

    report = AirflowDagSpecBuilder(repo_root=tmp_path).build(
        workload_set=workload_set.relative_to(tmp_path).as_posix(),
        env="dev",
    )

    assert not report.passed
    assert report.specs == ()
    assert [blocker.code for blocker in report.blockers] == ["dag_spec_wiring_reference_unknown"]
    assert report.warnings == ()


@pytest.mark.parametrize(
    ("selectors", "expected_code"),
    [
        ((None,), "DPONE_AIRFLOW_NODE_SELECTOR_MISSING"),
        (("orders", "orders"), "DPONE_AIRFLOW_PROCESS_PLAN_SELECTOR_DUPLICATE"),
    ],
)
def test_flow_plan_rejects_missing_or_duplicate_selectors_before_command_build(
    tmp_path: Path,
    selectors: tuple[str | None, ...],
    expected_code: str,
) -> None:
    manifest_path = tmp_path / "pipeline.yaml"
    manifest_path.write_text("kind: dpone.flow.v1\n", encoding="utf-8")
    processes = tuple(
        ProcessSpec(
            name=f"orders_{index}",
            config_path=manifest_path,
            config=SimpleNamespace(),
            raw_config={},
            selector=selector,
        )
        for index, selector in enumerate(selectors)
    )
    loaded = LoadedManifest(
        path=manifest_path,
        kind="dpone.batch.v1",
        raw={"kind": "dpone.flow.v1", "authoring": {"mode": "flow"}},
        processes=processes,
        source_kind="dpone.flow.v1",
    )

    class StaticLoader:
        def load(self, *_args: object, **_kwargs: object) -> LoadedManifest:
            return loaded

    workload = GitOpsWorkloadDefinition(
        workload_id="orders",
        manifest="pipeline.yaml",
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={"image": "dpone:test", "airflow": {}},
        provenance={},
    )

    with pytest.raises(CompactProcessPlanError) as exc_info:
        build_compact_process_plans(
            workload=workload,
            runtime_manifest_path="runtime/orders.yaml",
            repo_root=tmp_path,
            output_path=".dpone/gitops/airflow/orders/airflow-pack.json",
            manifest_loader=StaticLoader(),  # type: ignore[arg-type]
        )

    assert exc_info.value.code == expected_code


def _manifest(
    *,
    kind: str = "etl_manifest",
    source_kind: str | None = None,
    raw: dict[str, object] | None = None,
    processes: tuple[ProcessSpec, ...],
) -> LoadedManifest:
    return LoadedManifest(
        path=Path("pipeline.yaml"),
        kind=kind,
        raw=raw or {},
        processes=processes,
        source_kind=source_kind,
    )


def _process(name: str, selector: str | None) -> ProcessSpec:
    return ProcessSpec(
        name=name,
        config_path=Path("pipeline.yaml"),
        config=SimpleNamespace(task_group=None),
        raw_config={},
        selector=selector,
    )
