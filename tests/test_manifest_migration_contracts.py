from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.contracts.errors import ETLConfigurationError
from dpone.manifest.migrate import (
    BatchPlan,
    GroupKey,
    LegacyProcess,
    MigrationConfig,
    ProcessRef,
    _ensure_required_defaults,
    _group_processes,
    _infer_dataset_vars_and_naming,
    _ratio,
    _to_identifier,
    build_batch_manifest,
    deep_common_dict,
    deep_diff,
    normalize_depends_on,
    relative_posix,
    rewrite_depends_on,
)


def _legacy_process(
    tmp_path: Path,
    *,
    table: str,
    target_dataset: str = "landing__demo__db",
    task_group: str = "load",
    depends_on: list[Any] | None = None,
    strategy_mode: str = "incremental_merge",
) -> LegacyProcess:
    path = tmp_path / "legacy" / f"{table}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("kind: dpone.v1\n", encoding="utf-8")

    raw: dict[str, Any] = {
        "name": f"public_{table}__{strategy_mode}",
        "task_group": task_group,
        "source": {
            "type": "postgres",
            "table": {"schema": "public", "name": table},
        },
        "sink": {
            "type": "bigquery",
            "table": {"schema": target_dataset, "name": f"public__{table}"},
            "strategy": {"mode": strategy_mode, "unique_key": "id"},
        },
    }
    if depends_on is not None:
        raw["depends_on"] = depends_on

    load_config = SimpleNamespace(
        source_schema="public",
        source_table=table,
        target_schema=target_dataset,
        target_table=f"public__{table}",
    )
    config = SimpleNamespace(
        load_config=load_config,
        task_group=task_group,
        name=f"public_{table}__{strategy_mode}",
        load_strategy=strategy_mode,
    )
    return LegacyProcess(path=path.resolve(), spec=SimpleNamespace(raw_config=raw, config=config))


def test_migration_config_and_group_key_contracts(tmp_path: Path) -> None:
    cfg = MigrationConfig(src_path=tmp_path / "legacy", out_dir=tmp_path / "batch")

    assert cfg.recursive is True
    assert cfg.group_by == "dataset"
    assert cfg.overwrite is False
    assert cfg.infer_naming is True
    assert GroupKey("landing__demo__db", "").to_filename() == "landing_demo_db.batch.yaml"
    assert GroupKey("", "load-api").to_filename() == "load_api.batch.yaml"
    assert GroupKey("landing", "load").to_filename() == "landing_load.batch.yaml"
    assert _to_identifier(" Landing API / Demo ") == "landing_api_demo"


def test_group_processes_by_dataset_task_group_and_rejects_unknown_mode(tmp_path: Path) -> None:
    orders = _legacy_process(tmp_path, table="orders", target_dataset="landing__demo__db", task_group="load")
    users = _legacy_process(tmp_path, table="users", target_dataset="raw__demo__db", task_group="sync")

    by_dataset = _group_processes([orders, users], group_by="dataset")
    by_task_group = _group_processes([orders, users], group_by="task_group")
    by_both = _group_processes([orders, users], group_by="dataset_task_group")

    assert set(by_dataset) == {GroupKey("landing__demo__db", ""), GroupKey("raw__demo__db", "")}
    assert set(by_task_group) == {GroupKey("", "load"), GroupKey("", "sync")}
    assert set(by_both) == {GroupKey("landing__demo__db", "load"), GroupKey("raw__demo__db", "sync")}

    with pytest.raises(ETLConfigurationError, match="group-by"):
        _group_processes([orders], group_by="unknown")


def test_depends_on_normalization_and_rewrite_contracts(tmp_path: Path) -> None:
    source_dir = tmp_path / "legacy"
    current_batch = tmp_path / "out" / "current.batch.yaml"
    other_batch = tmp_path / "out" / "other.batch.yaml"
    orders = source_dir / "orders.yaml"
    users = source_dir / "users.yaml"

    deps = normalize_depends_on(
        [
            "orders.yaml",
            {"path": "users.yaml", "condition": "success"},
            {"group": "bootstrap"},
            {"path": "#already.selector"},
            {"path": "missing.yaml"},
            {"note": "unchanged"},
        ]
    )

    rewritten = rewrite_depends_on(
        deps,
        current_batch_path=current_batch,
        mapping={
            orders.resolve(): ProcessRef(current_batch.resolve(), "public.orders"),
            users.resolve(): ProcessRef(other_batch.resolve(), "public.users"),
        },
        source_manifest_dir=source_dir,
    )

    assert rewritten == [
        {"path": "#public.orders"},
        {"path": "other.batch.yaml#public.users", "condition": "success"},
        {"group": "bootstrap"},
        {"path": "#already.selector"},
        {"path": "missing.yaml"},
        {"note": "unchanged"},
    ]
    assert normalize_depends_on(None) == []
    assert normalize_depends_on(["orders.yaml"]) == [{"path": "orders.yaml"}]

    with pytest.raises(ETLConfigurationError, match="depends_on должен быть массивом"):
        normalize_depends_on("orders.yaml")
    with pytest.raises(ETLConfigurationError, match="depends_on элементы"):
        normalize_depends_on([object()])


def test_deep_common_diff_and_required_defaults_contracts() -> None:
    first = {
        "source": {"type": "postgres", "table": {"schema": "public", "name": "orders"}},
        "sink": {"type": "bigquery", "strategy": {"mode": "incremental_merge", "unique_key": "id"}},
        "tags": ["daily"],
        "owner": "data",
    }
    second = {
        "source": {"type": "postgres", "table": {"schema": "public", "name": "users"}},
        "sink": {"type": "bigquery", "strategy": {"mode": "incremental_merge", "unique_key": "id"}},
        "tags": ["daily"],
        "owner": "data",
    }

    common = deep_common_dict([first, second])

    assert common == {
        "owner": "data",
        "sink": {"strategy": {"mode": "incremental_merge", "unique_key": "id"}, "type": "bigquery"},
        "source": {"table": {"schema": "public"}, "type": "postgres"},
        "tags": ["daily"],
    }
    assert deep_diff(first, common) == {"source": {"table": {"name": "orders"}}}
    assert deep_diff(common, common) == {}
    assert _ensure_required_defaults({}, first, manifest_name="demo")["sink"] == first["sink"]

    with pytest.raises(ETLConfigurationError, match="отсутствует sink"):
        _ensure_required_defaults({}, {"source": {}}, manifest_name="broken")
    with pytest.raises(ETLConfigurationError, match="отсутствует source"):
        _ensure_required_defaults({"sink": {}}, {"sink": {}}, manifest_name="broken")


def test_batch_manifest_builder_infers_naming_and_rewrites_internal_dependencies(tmp_path: Path) -> None:
    orders = _legacy_process(tmp_path, table="orders")
    order_items = _legacy_process(tmp_path, table="order_items", depends_on=["orders.yaml"])
    out_path = tmp_path / "out" / "landing_demo_db.batch.yaml"
    batch = BatchPlan(GroupKey("landing__demo__db", ""), out_path, (orders, order_items))
    mapping = {
        orders.path.resolve(): ProcessRef(out_path.resolve(), "public.orders"),
        order_items.path.resolve(): ProcessRef(out_path.resolve(), "public.order_items"),
    }

    manifest = build_batch_manifest(
        batch,
        mapping,
        infer_naming=True,
        naming_threshold=0.8,
        convention="public",
        registry_paths=(tmp_path / "registry" / "sources.yaml",),
    )

    assert manifest["kind"] == "dpone.batch.v1"
    assert manifest["convention"] == "public"
    assert manifest["registry"] == "../registry/sources.yaml"
    assert manifest["vars"] == {"layer": "landing", "src_system": "demo", "src_database": "db"}
    assert manifest["naming"] == {
        "sink_dataset": "{{ layer }}__{{ src_system }}__{{ src_database }}",
        "sink_table": "{{ src_schema }}__{{ src_table }}",
        "process_name": "{{ src_schema }}_{{ src_table }}__{{ sink.strategy.mode }}",
    }
    assert manifest["defaults"]["sink"]["strategy"] == {"mode": "incremental_merge", "unique_key": "id"}
    tables = {
        item if isinstance(item, str) else item["table"]: item for item in manifest["schemas"]["public"]["tables"]
    }
    assert set(tables) == {"orders", "order_items"}
    assert tables["order_items"]["depends_on"] == [{"path": "#public.orders"}]


def test_batch_manifest_builder_preserves_overrides_when_naming_is_disabled(tmp_path: Path) -> None:
    orders = _legacy_process(tmp_path, table="orders")
    batch = BatchPlan(GroupKey("custom", ""), tmp_path / "out" / "custom.batch.yaml", (orders,))

    manifest = build_batch_manifest(
        batch,
        {orders.path.resolve(): ProcessRef(batch.out_path.resolve(), "public.orders")},
        infer_naming=False,
        naming_threshold=1.0,
    )

    table = manifest["schemas"]["public"]["tables"][0]

    assert "naming" not in manifest
    assert table == "orders"


def test_small_migration_helpers_contracts(tmp_path: Path) -> None:
    assert _infer_dataset_vars_and_naming("landing__demo__db") == (
        {"layer": "landing", "src_system": "demo", "src_database": "db"},
        {"sink_dataset": "{{ layer }}__{{ src_system }}__{{ src_database }}"},
    )
    assert _infer_dataset_vars_and_naming("custom_dataset") == ({}, {})
    assert _ratio([True, False, True]) == pytest.approx(2 / 3)
    assert _ratio([]) == 0.0
    assert relative_posix(tmp_path / "out" / "batch.yaml", start=tmp_path) == "out/batch.yaml"

    with pytest.raises(ETLConfigurationError, match="Пустая группа"):
        build_batch_manifest(
            BatchPlan(GroupKey("empty", ""), tmp_path / "empty.batch.yaml", ()),
            {},
            infer_naming=True,
            naming_threshold=0.8,
        )
