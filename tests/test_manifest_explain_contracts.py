from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.contracts.errors import ETLConfigurationError
from dpone.manifest.explain_io import read_yaml, select_process
from dpone.manifest.explain_models import ExplainChange, ExplainLayer, ExplainResult
from dpone.manifest.explain_trace import build_naming_origin, build_vars_origin, extract_table_parts, find_table_spec
from dpone.manifest.explain_utils import (
    canonicalize_path,
    collect_template_vars,
    fill_missing_origins,
    get_by_path,
    iter_leaf_paths,
    origin_for_path,
    parent_path,
    path_relevant,
    safe_equal,
    split_path,
)
from dpone.manifest.explain_why import explain_why, make_dot_patch, naming_why, suggest_patches


def explain_result() -> ExplainResult:
    return ExplainResult(
        manifest_path=Path("/repo/manifests/batch.yaml"),
        kind="dpone.batch.v1",
        selector="public.orders",
        process_name="load_orders",
        task_group="loaders",
        source="postgres",
        sink="bigquery",
        conventions=(),
        registry=None,
        vars={"src_schema": "public", "src_table": "orders", "src": "demo"},
        vars_origin={"src_schema": "implicit", "src_table": "implicit", "src": "manifest:vars"},
        naming={
            "sink_table": "{{ src_schema }}__{{ src_table }}",
            "labels": {"src": "{{ src }}", "schema": "{{ src_schema }}"},
        },
        naming_origin={"sink_table": "manifest:naming", "labels": "convention:landing"},
        derived_fields={
            "sink.table.name": "sink_table",
            "sink.options.table_labels": "labels",
        },
        config_layers=(
            ExplainLayer(id="defaults", title="defaults"),
            ExplainLayer(id="naming:public.orders", title="apply naming"),
        ),
        config_changes={
            "defaults": [
                ExplainChange(path="sink.table.schema", kind="add", before=None, after="landing"),
                ExplainChange(path="depends_on[+0]", kind="append", before=None, after={"path": "extract.yaml"}),
            ],
            "naming:public.orders": [
                ExplainChange(path="sink.table.name", kind="change", before="{{ src_table }}", after="public__orders"),
                ExplainChange(path="sink.options.table_labels.src", kind="add", before=None, after="demo"),
            ],
        },
        config_patches={},
        config_patch_removes={},
        config_origin={
            "sink": "defaults",
            "sink.table": "defaults",
            "sink.table.schema": "defaults",
            "sink.table.name": "naming:public.orders",
            "sink.options.table_labels.src": "naming:public.orders",
            "depends_on": "table.depends_on:public.orders",
            "depends_on[0]": "table.depends_on:public.orders",
        },
        final_config={
            "name": "load_orders",
            "sink": {
                "table": {"schema": "landing", "name": "public__orders"},
                "options": {"table_labels": {"src": "demo"}},
            },
            "depends_on": [{"path": "extract.yaml"}],
        },
        matches_compiler=False,
        config_snapshots={
            "defaults": {"sink": {"table": {"schema": "landing", "name": "{{ src_table }}"}}},
            "naming:public.orders": {
                "sink": {
                    "table": {"schema": "landing", "name": "public__orders"},
                    "options": {"table_labels": {"src": "demo"}},
                },
                "depends_on": [{"path": "extract.yaml"}],
            },
        },
        warnings=(),
    )


def test_explain_utils_parse_paths_and_resolve_values() -> None:
    obj = {"sink": {"tables": [{"name": "orders"}]}, "depends_on": [{"path": "extract"}]}

    assert canonicalize_path("depends_on[+0].path") == "depends_on[0].path"
    assert split_path("sink.tables[0].name") == ["sink", "tables", 0, "name"]
    assert get_by_path(obj, "sink.tables[0].name") == (True, "orders")
    assert get_by_path(obj, "sink.tables[2].name") == (False, None)
    assert parent_path("sink.tables[0].name") == "sink.tables[0]"
    assert parent_path("depends_on[0]") == "depends_on"
    assert path_relevant("sink.table", "sink.table.name") is True
    assert path_relevant("sink.table.name", "sink.table") is True
    assert path_relevant("source.table", "sink.table") is False

    with pytest.raises(ETLConfigurationError, match="нет закрывающей скобки"):
        split_path("depends_on[0")
    with pytest.raises(ETLConfigurationError, match="Некорректный индекс"):
        split_path("depends_on[x]")


def test_explain_utils_collect_templates_leaf_paths_origins_and_safe_equal() -> None:
    config = {
        "sink": {"table": {"name": "{{ src_schema }}__{{ src_table }}"}},
        "labels": ["{{ src }}", "static"],
    }
    origins = {"sink.table": "defaults", "labels": "manifest"}

    assert collect_template_vars(config) == ["src", "src_schema", "src_table"]
    assert list(iter_leaf_paths(config)) == ["sink.table.name", "labels[0]", "labels[1]"]
    assert fill_missing_origins(config, origins) == {
        "sink.table": "defaults",
        "labels": "manifest",
        "sink.table.name": "defaults",
        "labels[0]": "manifest",
        "labels[1]": "manifest",
    }
    assert origin_for_path(origins, "sink.table.name") == "defaults"

    class ExplosiveEqual:
        def __eq__(self, other: object) -> bool:
            raise RuntimeError("boom")

    assert safe_equal(ExplosiveEqual(), object()) is False


def test_explain_io_reads_yaml_and_selects_processes(tmp_path: Path) -> None:
    path = tmp_path / "manifest.yaml"
    path.write_text("kind: dpone.batch.v1\n", encoding="utf-8")
    processes = [
        SimpleNamespace(selector="public.orders", name="orders"),
        SimpleNamespace(selector="public.users", name="users"),
    ]

    assert read_yaml(path) == {"kind": "dpone.batch.v1"}
    assert select_process(processes, "users") is processes[1]
    assert select_process([processes[0]], None) is processes[0]

    with pytest.raises(ETLConfigurationError, match="не найдена"):
        read_yaml(tmp_path / "missing.yaml")
    with pytest.raises(ETLConfigurationError, match="Selector 'missing' не найден"):
        select_process(processes, "missing")
    with pytest.raises(ETLConfigurationError, match="Manifest содержит 2 процессов"):
        select_process(processes, None)


def test_explain_why_returns_timeline_naming_items_parent_and_patches() -> None:
    result = explain_result()

    why = explain_why(result, "sink.table.name")
    labels = explain_why(result, "sink.options.table_labels")
    dep = explain_why(result, "depends_on[+0].path")
    missing = explain_why(result, "sink.table.missing")

    assert why.exists is True
    assert why.final_value == "public__orders"
    assert why.final_origin == "naming:public.orders"
    assert [event.layer_id for event in why.events] == ["naming:public.orders"]
    assert why.naming == {
        "derived_from_path": "sink.table.name",
        "naming_key": "sink_table",
        "template": "{{ src_schema }}__{{ src_table }}",
        "template_origin": "manifest:naming",
        "referenced_vars": [
            {"var": "src_schema", "value": "public", "origin": "implicit"},
            {"var": "src_table", "value": "orders", "origin": "implicit"},
        ],
    }
    assert labels.map_items == ({"key": "src", "value": "demo", "origin": "naming:public.orders"},)
    assert dep.list_items is None
    assert dep.parent == {
        "path": "depends_on[0]",
        "value": {"path": "extract.yaml"},
        "origin": "table.depends_on:public.orders",
    }
    assert dep.suggested_patches == (
        {
            "scope": "table-spec:depends_on",
            "title": "Добавить элемент в depends_on на уровне table spec (append)",
            "snippet": {"depends_on": [{"path": "extract.yaml"}]},
        },
    )
    assert why.timeline[-1] == {
        "layer_id": "final:compiler",
        "layer_title": "final (compiler output)",
        "exists": True,
        "value": "public__orders",
    }
    assert missing.exists is False
    assert missing.warnings == ("Путь не найден в финальном конфиге",)


def test_explain_why_rejects_empty_query_and_builds_dot_patches() -> None:
    result = explain_result()

    assert make_dot_patch("sink.table.name", "orders") == {"sink": {"table": {"name": "orders"}}}
    assert make_dot_patch("depends_on[0]", {"path": "x"}) == {}
    assert suggest_patches(result, "sink.table.name", "orders")[0]["snippet"] == {"sink": {"table": {"name": "orders"}}}
    assert naming_why(result, "sink.options.table_labels.src")["template"] == "{{ src }}"

    with pytest.raises(ETLConfigurationError, match="--why требует непустой путь"):
        explain_why(result, "")


def test_explain_trace_finds_table_specs_and_extracts_table_parts() -> None:
    manifest = {
        "schemas": {
            "public": {
                "defaults": {"source": {"type": "postgres"}},
                "tables": [
                    "orders",
                    {
                        "id": "custom-users",
                        "table": "users",
                        "vars": {"owner": "data"},
                        "naming": {"sink_table": "users_v2"},
                        "overrides": {"name": "load_users"},
                        "depends_on": [{"path": "orders"}],
                    },
                ],
            }
        }
    }

    schema_name, table_name, schema_block, table_spec = find_table_spec(manifest, "public.orders")
    assert (schema_name, table_name) == ("public", "orders")
    assert schema_block["defaults"] == {"source": {"type": "postgres"}}
    assert table_spec == "orders"
    assert find_table_spec(manifest, "custom-users")[1] == "users"
    assert extract_table_parts(table_spec) == ({}, {}, {}, [])
    assert extract_table_parts(find_table_spec(manifest, "custom-users")[3]) == (
        {"owner": "data"},
        {"sink_table": "users_v2"},
        {"name": "load_users"},
        [{"path": "orders"}],
    )

    with pytest.raises(ETLConfigurationError, match="schemas должен быть непустым"):
        find_table_spec({}, "public.orders")
    with pytest.raises(ETLConfigurationError, match="Не удалось найти table spec"):
        find_table_spec(manifest, "missing")


def test_explain_trace_builds_vars_and_naming_origin_precedence(tmp_path: Path) -> None:
    registry = SimpleNamespace(entry_source=tmp_path / "sources.yaml", vars={"src": "registry", "host": "db"})
    conventions = [
        SimpleNamespace(
            name="landing",
            patch={
                "vars": {"src": "convention", "layer": "landing"},
                "naming": {"sink_table": "conv_{{ src_table }}"},
            },
        )
    ]
    raw_manifest = {
        "vars": {"src": "manifest"},
        "naming": {"sink_table": "manifest_{{ src_table }}", "labels": {"src": "{{ src }}"}},
    }
    schema_block = {
        "vars": {"src_schema": "public"},
        "naming": {"sink_schema": "landing"},
    }
    table_spec = {
        "table": "orders",
        "vars": {"src_table": "orders"},
        "naming": {"sink_table": "table_{{ src_table }}"},
    }

    vars_origin = build_vars_origin(
        raw_manifest=raw_manifest,
        conventions=conventions,
        registry=registry,
        schema_block=schema_block,
        table_spec=table_spec,
    )
    naming_origin = build_naming_origin(
        raw_manifest=raw_manifest,
        conventions=conventions,
        schema_block=schema_block,
        table_spec=table_spec,
    )

    assert vars_origin["src"] == "manifest:vars"
    assert vars_origin["host"] == "registry:sources.yaml"
    assert vars_origin["layer"] == "convention:landing"
    assert vars_origin["src_schema"] == "schema:vars"
    assert vars_origin["src_table"] == "table:vars"
    assert vars_origin["env_code"] == "built-in"
    assert naming_origin == {
        "sink_table": "table:naming",
        "labels": "manifest:naming",
        "sink_schema": "schema:naming",
    }
