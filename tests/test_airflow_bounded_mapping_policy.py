from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

import dpone.backfill.mapping as mapping_module
from dpone.backfill.mapping import (
    AIRFLOW_MAPPING_ITEM_ENV,
    AirflowBackfillMappingViolation,
    build_airflow_mapping_plan,
    mapping_policy_from_mapping,
    parse_airflow_mapping_item_json,
    serialize_airflow_mapping_item,
)
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.config.mssql_strategy_contract import MSSQLStrategyContractError
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.airflow_compact_process_plans import CompactProcessPlanError, build_compact_process_plans
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition


def _config(*, days: int = 10, sink_type: str = "postgres", parallel_workers: int = 1) -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        load_strategy=LoadStrategy.BACKFILL,
        partition={"column": "business_date", "values_from_staging": True},
        options={
            "source_type": "mssql",
            "sink_type": sink_type,
            "backfill": {
                "inner_mode": "partition_replace",
                "parallel_workers": parallel_workers,
                "chunk": {
                    "column": "business_date",
                    "from": "2025-01-01",
                    "to": f"2025-01-{days:02d}",
                    "step": "1d",
                },
                "state": {
                    "backend": "audit_schema",
                    "require_distributed_lock": True,
                },
            },
        },
    )


def test_mapping_policy_defaults_to_internal_without_changing_runtime() -> None:
    policy = mapping_policy_from_mapping(None)

    assert policy.mode == "internal"
    assert policy.max_items == 200
    assert policy.max_active == 16
    assert policy.pool == "dpone_backfill"

    config = _config()
    config.load_strategy = LoadStrategy.FULL_REFRESH
    config.options.pop("backfill")
    plan = build_airflow_mapping_plan(config, None)

    assert plan.mode == "internal"
    assert plan.items == ()
    assert plan.chunks_total == 0
    assert plan.to_jsonable()["schema"] == "dpone.airflow-mapping-plan.v1"


@pytest.mark.parametrize(
    ("mapping", "code"),
    [
        ({"mode": "everything"}, "DPONE_AIRFLOW_MAPPING_POLICY_INVALID"),
        ({"mode": "visible", "max_items": 0, "pool": "pool"}, "DPONE_AIRFLOW_MAPPING_POLICY_INVALID"),
        ({"mode": "visible", "max_items": 201, "pool": "pool"}, "DPONE_AIRFLOW_MAPPING_POLICY_INVALID"),
        (
            {"mode": "visible", "max_items": 5, "max_active": 6, "pool": "pool"},
            "DPONE_AIRFLOW_MAPPING_POLICY_INVALID",
        ),
        ({"mode": "visible", "pool": ""}, "DPONE_AIRFLOW_MAPPING_POOL_REQUIRED"),
    ],
)
def test_mapping_policy_fails_closed_on_invalid_limits(mapping: dict[str, object], code: str) -> None:
    with pytest.raises(AirflowBackfillMappingViolation) as raised:
        mapping_policy_from_mapping(mapping)

    assert raised.value.code == code


def test_visible_plan_maps_one_item_per_chunk_and_is_deterministic() -> None:
    mapping = {"mode": "visible", "max_items": 10, "max_active": 3, "pool": "history"}

    left = build_airflow_mapping_plan(_config(), mapping)
    right = build_airflow_mapping_plan(_config(), mapping)

    assert left == right
    assert left.items_total == 10
    assert [(item.first_chunk_index, item.last_chunk_index) for item in left.items] == [
        (index, index) for index in range(1, 11)
    ]
    assert left.plan_fingerprint.startswith("sha256:")
    assert left.backfill_plan_hash is not None
    assert left.backfill_plan_hash.startswith("sha256:")


def test_visible_plan_blocks_task_explosion() -> None:
    with pytest.raises(AirflowBackfillMappingViolation) as raised:
        build_airflow_mapping_plan(
            _config(),
            {"mode": "visible", "max_items": 9, "max_active": 3, "pool": "history"},
        )

    assert raised.value.code == "DPONE_AIRFLOW_MAPPING_TASK_LIMIT_EXCEEDED"


def test_summary_plan_builds_balanced_contiguous_ranges() -> None:
    plan = build_airflow_mapping_plan(
        _config(days=10),
        {"mode": "summary", "max_items": 3, "max_active": 2, "pool": "history"},
    )

    assert [(item.first_chunk_index, item.last_chunk_index, item.chunks_count) for item in plan.items] == [
        (1, 4, 4),
        (5, 7, 3),
        (8, 10, 3),
    ]
    assert [index for item in plan.items for index in item.chunk_indexes] == list(range(1, 11))


@pytest.mark.parametrize(
    "sink_type",
    (
        "mssql",
        "MSSQL",
        "microsoft mssql",
        "microsoft_mssql",
        "odbc",
        "sqlserver",
        "sql_server",
        "sql-server",
    ),
)
def test_mapped_plan_accepts_every_canonical_mssql_alias(sink_type: str) -> None:
    plan = build_airflow_mapping_plan(
        _config(sink_type=sink_type),
        {"mode": "summary", "max_items": 3, "max_active": 2, "pool": "history"},
    )

    assert plan.chunks_total == 10
    assert plan.items_total == 3


@pytest.mark.parametrize(
    "mapping",
    (
        None,
        {"mode": "summary", "max_items": 3, "max_active": 2, "pool": "history"},
    ),
    ids=("internal", "summary"),
)
def test_public_mapping_plan_rejects_invalid_mssql_campaign_before_chunk_planning(
    monkeypatch: pytest.MonkeyPatch,
    mapping: dict[str, object] | None,
) -> None:
    config = _config(sink_type="mssql")
    config.options["backfill"]["inner_mode"] = "replace"
    config.only_new_rows = True
    monkeypatch.setattr(
        mapping_module,
        "plan_chunks",
        lambda *_args, **_kwargs: pytest.fail("invalid MSSQL campaign reached chunk planning"),
    )

    with pytest.raises(MSSQLStrategyContractError) as raised:
        build_airflow_mapping_plan(config, mapping)

    assert raised.value.blocker == "mssql.strategy.backfill.irrelevant_only_new_rows"


def test_public_mapping_plan_rejects_authored_chunk_scope_before_chunk_planning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(sink_type="odbc")
    config.options["source_type"] = "postgresql"
    config.options["backfill"]["inner_mode"] = "replace"
    config.portable_scope = {
        "version": 1,
        "kind": "equality",
        "column": "business_date",
        "value": {"type": "date", "value": "2026-08-23"},
    }
    monkeypatch.setattr(
        mapping_module,
        "plan_chunks",
        lambda *_args, **_kwargs: pytest.fail("authored chunk scope reached chunk planning"),
    )

    with pytest.raises(MSSQLStrategyContractError) as raised:
        build_airflow_mapping_plan(config, None)

    assert raised.value.blocker == "backfill.portable_scope_is_runtime_owned"


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (
            lambda config: setattr(config, "load_strategy", LoadStrategy.FULL_REFRESH),
            "DPONE_AIRFLOW_MAPPING_REQUIRES_CHUNKED_BACKFILL",
        ),
        (
            lambda config: config.options["backfill"].pop("chunk"),
            "DPONE_AIRFLOW_MAPPING_REQUIRES_CHUNKED_BACKFILL",
        ),
        (
            lambda config: config.options["backfill"].update(parallel_workers=2),
            "DPONE_AIRFLOW_MAPPING_PARALLELISM_MULTIPLIER",
        ),
        (
            lambda config: config.options["backfill"].update(state={"backend": "local_file"}),
            "DPONE_AIRFLOW_MAPPING_STATE_UNSUPPORTED",
        ),
        (
            lambda config: config.options.update(sink_type="clickhouse"),
            "DPONE_AIRFLOW_MAPPING_STATE_UNSUPPORTED",
        ),
    ],
)
def test_mapped_plan_requires_certified_chunked_backfill(mutate, code: str) -> None:
    config = _config()
    mutate(config)

    with pytest.raises(AirflowBackfillMappingViolation) as raised:
        build_airflow_mapping_plan(
            config,
            {"mode": "summary", "max_items": 3, "max_active": 2, "pool": "history"},
        )

    assert raised.value.code == code


def test_mapping_item_round_trips_without_predicates_or_credentials() -> None:
    plan = build_airflow_mapping_plan(
        _config(),
        {"mode": "summary", "max_items": 3, "max_active": 2, "pool": "history"},
    )

    encoded = serialize_airflow_mapping_item(plan, plan.items[1])
    selection = parse_airflow_mapping_item_json(encoded)

    assert selection.item_index == 1
    assert selection.chunk_indexes == (5, 6, 7)
    assert selection.mapping_plan_fingerprint == plan.plan_fingerprint
    assert "predicate" not in encoded
    assert "connection" not in encoded
    assert AIRFLOW_MAPPING_ITEM_ENV == "DPONE_AIRFLOW_MAPPING_ITEM"


def test_mapping_item_rejects_unknown_or_oversized_input() -> None:
    payload = {
        "schema": "dpone.airflow-mapping-item.v1",
        "mode": "summary",
        "mapping_plan_fingerprint": "sha256:" + "1" * 64,
        "backfill_plan_hash": "sha256:" + "2" * 64,
        "item_index": 0,
        "first_chunk_index": 1,
        "last_chunk_index": 1,
        "chunks_count": 1,
        "predicate": "secret = 'value'",
    }

    with pytest.raises(AirflowBackfillMappingViolation) as unknown:
        parse_airflow_mapping_item_json(json.dumps(payload))
    assert unknown.value.code == "DPONE_AIRFLOW_MAPPING_ITEM_INVALID"

    with pytest.raises(AirflowBackfillMappingViolation) as oversized:
        parse_airflow_mapping_item_json(" " * 8193)
    assert oversized.value.code == "DPONE_AIRFLOW_MAPPING_ITEM_INVALID"


def test_compact_process_plan_compiles_workload_mapping_without_runtime_io(tmp_path: Path) -> None:
    manifest = _write_backfill_manifest(tmp_path)
    workload = GitOpsWorkloadDefinition(
        workload_id="orders",
        manifest=manifest,
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={
            "image": "dpone:test",
            "airflow": {"mapping": {"mode": "summary", "max_items": 2, "max_active": 2, "pool": "history"}},
        },
        provenance={},
    )

    plans = build_compact_process_plans(
        workload=workload,
        runtime_manifest_path="runtime/orders.yaml",
        repo_root=tmp_path,
        output_path=".dpone/gitops/airflow/orders/airflow-pack.json",
    )

    assert len(plans) == 1
    assert plans[0].mapping_plan["mode"] == "summary"
    assert plans[0].mapping_plan["items_total"] == 2
    assert plans[0].to_jsonable()["mapping_plan"] == plans[0].mapping_plan

    pack = (
        AirflowCompactPackBuilder()
        .build(
            workload=workload,
            output_path=".dpone/gitops/airflow/orders/airflow-pack.json",
            repo_root=tmp_path,
        )
        .to_jsonable()
    )
    assert "mapped_kpo_kwargs" in pack
    assert "kpo_kwargs" not in pack


def test_compact_process_plan_reports_mapping_errors_with_stable_code(tmp_path: Path) -> None:
    manifest = _write_backfill_manifest(tmp_path)
    workload = GitOpsWorkloadDefinition(
        workload_id="orders",
        manifest=manifest,
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={"image": "dpone:test", "airflow": {"mapping": {"mode": "visible", "pool": ""}}},
        provenance={},
    )

    with pytest.raises(CompactProcessPlanError) as raised:
        build_compact_process_plans(
            workload=workload,
            runtime_manifest_path="runtime/orders.yaml",
            repo_root=tmp_path,
            output_path=".dpone/gitops/airflow/orders/airflow-pack.json",
        )

    assert raised.value.code == "DPONE_AIRFLOW_MAPPING_POOL_REQUIRED"


def _write_backfill_manifest(root: Path) -> str:
    path = root / "pipelines/orders.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "name": "orders",
                "source": {
                    "type": "mssql",
                    "connection_id": "src",
                    "table": {"schema": "dbo", "name": "orders"},
                },
                "sink": {
                    "type": "postgres",
                    "connection_id": "tgt",
                    "table": {"schema": "analytics", "name": "orders"},
                    "strategy": {
                        "mode": "backfill",
                        "backfill": {
                            "inner_mode": "partition_replace",
                            "parallel_workers": 1,
                            "chunk": {
                                "column": "business_date",
                                "from": "2025-01-01",
                                "to": "2025-01-04",
                                "step": "1d",
                            },
                            "state": {"backend": "audit_schema", "require_distributed_lock": True},
                        },
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path.relative_to(root).as_posix()
