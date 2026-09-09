from __future__ import annotations

import json
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.technical_columns import TechnicalColumnCatalog, TechnicalColumnRole
from dpone.runtime.artifacts import (
    BatchedFileExportArtifact,
    FileExportArtifact,
    InMemoryRowsArtifact,
    PartitionedFileExportArtifact,
)
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.normalization import NestedNormalizationOptions, NestedNormalizationService
from dpone.runtime.sinks.base import LoadResult
from dpone.runtime.sources.base import ExtractResult

_RUN_ID = "01JZ0000000000000000000000"
_LOAD_ID = "01JZ0000000000000000000001"


def test_nested_normalizer_splits_root_objects_arrays_and_scalar_lists_with_stable_hierarchy() -> None:
    service = NestedNormalizationService()
    result = service.normalize_rows(
        [
            {
                "order_id": 42,
                "status": "paid",
                "customer": {"customer_id": "c-1", "name": "Ada"},
                "items": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 1}],
                "tags": ["vip", "new"],
            }
        ],
        schema=[("order_id", "bigint"), ("status", "text"), ("customer", "json"), ("items", "json"), ("tags", "json")],
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
        run_id=_RUN_ID,
        load_id=_LOAD_ID,
        source_type="api",
        source_schema="api",
        source_table="orders",
        unique_key=["order_id"],
    )

    assert [table.name for table in result.tables] == ["orders", "orders__customer", "orders__items", "orders__tags"]

    catalog = TechnicalColumnCatalog()
    row_id = catalog.name(TechnicalColumnRole.ROW_ID)
    parent_id = catalog.name(TechnicalColumnRole.PARENT_ROW_ID)
    root_id = catalog.name(TechnicalColumnRole.ROOT_ROW_ID)
    list_index = catalog.name(TechnicalColumnRole.LIST_INDEX)

    root = result.table("orders").rows[0]
    customer = result.table("orders__customer").rows[0]
    items = result.table("orders__items").rows
    tags = result.table("orders__tags").rows

    assert root == {
        "order_id": 42,
        "status": "paid",
        "__dpone__load_id": _LOAD_ID,
        "__dpone__loaded_at": root["__dpone__loaded_at"],
        "__dpone__row_id": root[row_id],
        "__dpone__extracted_at": root["__dpone__extracted_at"],
        "__dpone__parent_row_id": None,
        "__dpone__root_row_id": root[row_id],
        "__dpone__list_index": 0,
    }
    assert customer["customer_id"] == "c-1"
    assert customer[parent_id] == root[row_id]
    assert customer[root_id] == root[row_id]
    assert customer[list_index] is None
    assert [item["sku"] for item in items] == ["A", "B"]
    assert [item[parent_id] for item in items] == [root[row_id], root[row_id]]
    assert [item[list_index] for item in items] == [0, 1]
    assert [tag["value"] for tag in tags] == ["vip", "new"]
    assert [tag[list_index] for tag in tags] == [0, 1]

    repeated = service.normalize_rows(
        [
            {
                "tags": ["vip", "new"],
                "items": [{"qty": 2, "sku": "A"}, {"qty": 1, "sku": "B"}],
                "customer": {"name": "Ada", "customer_id": "c-1"},
                "status": "paid",
                "order_id": 42,
            }
        ],
        schema=[],
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
        run_id=_RUN_ID,
        load_id=_LOAD_ID,
        source_type="api",
        source_schema="api",
        source_table="orders",
        unique_key=["order_id"],
    )

    assert repeated.table("orders").rows[0][row_id] == root[row_id]
    assert [row[row_id] for row in repeated.table("orders__items").rows] == [row[row_id] for row in items]


def test_nested_normalizer_rejects_disabled_lineage_because_child_tables_need_identity() -> None:
    service = NestedNormalizationService()

    try:
        service.normalize_rows(
            [{"id": 1, "items": [{"sku": "A"}]}],
            schema=[],
            root_table="orders",
            options=NestedNormalizationOptions(enabled=True),
            run_id=_RUN_ID,
            load_id=_LOAD_ID,
            source_type="api",
            source_schema="api",
            source_table="orders",
            unique_key=["id"],
            lineage_enabled=False,
        )
    except ValueError as exc:
        assert "nested normalization requires lineage" in str(exc)
    else:  # pragma: no cover - keeps the assertion message clearer than pytest.raises for this contract.
        raise AssertionError("disabled lineage must fail for nested normalization")


def test_nested_normalizer_supports_dlt_like_nested_level_limit() -> None:
    service = NestedNormalizationService()

    try:
        service.normalize_rows(
            [{"id": 1, "items": [{"sku": "A", "discounts": [{"code": "VIP"}]}]}],
            schema=[],
            root_table="orders",
            options=NestedNormalizationOptions.from_config({"enabled": True, "nested_level": 1}),
            run_id=_RUN_ID,
            load_id=_LOAD_ID,
            source_type="api",
            source_schema="api",
            source_table="orders",
            unique_key=["id"],
        )
    except ValueError as exc:
        assert "nested_level=1" in str(exc)
    else:
        raise AssertionError("nested_level must reject deeper child tables")


def test_nested_normalizer_reads_local_file_export_artifact_with_json_cells(tmp_path) -> None:
    path = tmp_path / "orders.csv"
    path.write_text(
        'order_id,customer,items\n42,"{""customer_id"": ""c-1""}","[{""sku"": ""A""}, {""sku"": ""B""}]"\n',
        encoding="utf-8",
    )
    payload = SimpleNamespace(
        artifact=FileExportArtifact(str(path), ["order_id", "customer", "items"], format="csv"),
        schema=[("order_id", "bigint"), ("customer", "json"), ("items", "json")],
    )

    result = NestedNormalizationService().normalize_payload(
        payload,
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
        run_id=_RUN_ID,
        load_id=_LOAD_ID,
        source_type="postgres",
        source_schema="public",
        source_table="orders",
        unique_key=["order_id"],
    )

    assert result.table("orders").rows[0]["order_id"] == "42"
    assert result.table("orders__customer").rows[0]["customer_id"] == "c-1"
    assert [row["sku"] for row in result.table("orders__items").rows] == ["A", "B"]
    assert not path.exists()


def test_nested_normalizer_reads_partitioned_file_export_artifacts(tmp_path) -> None:
    first = tmp_path / "orders_1.tsv"
    second = tmp_path / "orders_2.tsv"
    first.write_text('1\t[{"sku": "A"}]\n', encoding="utf-8")
    second.write_text('2\t[{"sku": "B"}]\n', encoding="utf-8")
    artifact = PartitionedFileExportArtifact(
        [
            FileExportArtifact(str(first), ["order_id", "items"], format="tsv"),
            FileExportArtifact(str(second), ["order_id", "items"], format="tsv"),
        ],
        ["order_id", "items"],
        estimated_rows=2,
    )
    payload = SimpleNamespace(artifact=artifact, schema=[("order_id", "bigint"), ("items", "json")])

    result = NestedNormalizationService().normalize_payload(
        payload,
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
        run_id=_RUN_ID,
        load_id=_LOAD_ID,
        source_type="postgres",
        source_schema="public",
        source_table="orders",
        unique_key=["order_id"],
    )

    assert [row["order_id"] for row in result.table("orders").rows] == ["1", "2"]
    assert [row["sku"] for row in result.table("orders__items").rows] == ["A", "B"]
    assert not first.exists()
    assert not second.exists()


def test_nested_normalizer_reads_batched_file_export_artifact(tmp_path) -> None:
    files = []
    for index, sku in enumerate(["A", "B"], start=1):
        path = tmp_path / f"orders_{index}.jsonl"
        path.write_text(f'{{"order_id": {index}, "items": [{{"sku": "{sku}"}}]}}\n', encoding="utf-8")
        files.append(path)

    def batches():
        for path in files:
            yield FileExportArtifact(str(path), ["order_id", "items"], format="jsonl")

    payload = SimpleNamespace(
        artifact=BatchedFileExportArtifact(batches, ["order_id", "items"], batch_size=1, format="jsonl"),
        schema=[("order_id", "bigint"), ("items", "json")],
    )

    result = NestedNormalizationService().normalize_payload(
        payload,
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
        run_id=_RUN_ID,
        load_id=_LOAD_ID,
        source_type="api",
        source_schema="api",
        source_table="orders",
        unique_key=["order_id"],
    )

    assert [row["order_id"] for row in result.table("orders").rows] == [1, 2]
    assert [row["sku"] for row in result.table("orders__items").rows] == ["A", "B"]
    assert all(not path.exists() for path in files)


@dataclass
class _NestedSource:
    events: list[str]

    def get_incremental_state(self, load_config):
        self.events.append("get_state")
        return None

    def extract(self, load_config, last_state):
        self.events.append("extract")
        return ExtractResult(
            artifact=InMemoryRowsArtifact(
                [
                    {
                        "order_id": 1,
                        "customer": {"customer_id": "c-1"},
                        "items": [{"sku": "A"}, {"sku": "B"}],
                    }
                ]
            ),
            schema=[("order_id", "bigint"), ("customer", "json"), ("items", "json")],
            state=SimpleNamespace(cursor="next"),
        )


@dataclass
class _NestedSink:
    events: list[str]
    loads: list[tuple[str, list[dict], list[tuple[str, str]]]]

    def stage_payload(self, load_config, payload):
        self.events.append(f"stage:{load_config.target_table}")
        rows = list(payload.artifact._rows)
        return SimpleNamespace(
            staged_rows=len(rows),
            target_table=load_config.target_table,
            rows=rows,
            schema=list(payload.schema),
            payload_schema=tuple(payload.schema or ()),
        )

    def finalize_staged_load(self, load_config, handle):
        self.events.append(f"load:{load_config.target_table}")
        rows = list(handle.rows)
        self.loads.append((load_config.target_table, rows, list(handle.schema)))
        return LoadResult(inserted_rows=len(rows), updated_rows=0, total_rows=len(rows), staging_rows=len(rows))

    def abort_staged_load(self, handle) -> None:
        self.events.append(f"abort:{handle.target_table}")

    def cleanup_staged_load(self, handle) -> None:
        del handle

    def load(self, load_config, payload):
        handle = self.stage_payload(load_config, payload)
        try:
            return self.finalize_staged_load(load_config, handle)
        except Exception:
            self.abort_staged_load(handle)
            raise


def test_etl_processor_loads_normalized_nested_tables_before_state_commit() -> None:
    events: list[str] = []
    source = _NestedSource(events)
    sink = _NestedSink(events, [])
    cfg = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="api",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key=["order_id"],
        options={
            "schema_evolution": {"enabled": False},
            "lineage": {"preset": "hierarchical"},
            "normalization": {"nested": {"enabled": True}},
        },
    )

    result = ETLProcessor(source, sink).run(cfg)

    assert result["status"] == "success"
    assert result["loaded_rows"] == 4
    assert result["reconciliation_metrics"]["nested_normalization"]["tables"] == {
        "orders": 1,
        "orders__customer": 1,
        "orders__items": 2,
    }
    assert result["reconciliation_metrics"]["quality_gates"]["passed"] is True
    assert [name for name, _, _ in sink.loads] == ["orders__customer", "orders__items", "orders"]
    assert events == [
        "get_state",
        "extract",
        "stage:orders__customer",
        "stage:orders__items",
        "stage:orders",
        "load:orders__customer",
        "load:orders__items",
        "load:orders",
    ]
    rows_by_table = {name: rows for name, rows, _schema in sink.loads}
    child_rows = rows_by_table["orders__items"]
    assert child_rows[0]["__dpone__parent_row_id"] == rows_by_table["orders"][0]["__dpone__row_id"]
    assert [row["__dpone__list_index"] for row in child_rows] == [0, 1]


def test_nested_finalize_failure_retains_staging_and_blocks_automatic_retry() -> None:
    events: list[str] = []
    source = _NestedSource(events)

    class _MutateThenFailChildSink(_NestedSink):
        def finalize_staged_load(self, load_config, handle):
            self.events.append(f"load:{load_config.target_table}")
            rows = list(handle.rows)
            self.loads.append((load_config.target_table, rows, list(handle.schema)))
            if load_config.target_table != "orders":
                raise RuntimeError("child target outcome unknown")
            return LoadResult(inserted_rows=len(rows), updated_rows=0, total_rows=len(rows), staging_rows=len(rows))

    sink = _MutateThenFailChildSink(events, [])
    cfg = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="api",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key=["order_id"],
        options={
            "schema_evolution": {"enabled": False},
            "lineage": {"preset": "hierarchical"},
            "normalization": {"nested": {"enabled": True}},
        },
    )
    with pytest.raises(RuntimeError, match="nested_package_partial_finalize") as raised:
        ETLProcessor(source, sink).run(cfg)

    assert raised.value.details["target_outcome"] == "commit_unknown"
    assert raised.value.details["cleanup_status"] == "retained_for_reconciliation"
    assert raised.value.details["safe_to_retry"] is False
    assert raised.value.details["retained_members"][0]["target"] == "landing.orders__customer"
    assert [name for name, _rows, _schema in sink.loads] == ["orders__customer"]
    assert not any(event.startswith("abort:") for event in events)


def test_manifest_json_schemas_expose_nested_normalization_options() -> None:
    config_schema = json.loads(open("src/dpone/schema/etl-config.schema.json", encoding="utf-8").read())
    batch_schema = json.loads(open("src/dpone/schema/etl-batch-manifest.schema.json", encoding="utf-8").read())

    for schema in (config_schema, batch_schema):
        sink_options = (
            schema["properties"]["sink"]["properties"]["options"]["properties"]
            if "sink" in schema.get("properties", {})
            else schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]["options"]["properties"]
        )
        nested = sink_options["normalization"]["properties"]["nested"]["properties"]
        assert nested["enabled"]["default"] is False
        assert nested["table_separator"]["default"] == "__"
        assert nested["scalar_list_value_column"]["default"] == "value"
        assert nested["nested_level"]["default"] == 32
        assert nested["max_depth"]["default"] == 32


def test_nested_path_policies_preserve_ignore_and_quarantine_paths() -> None:
    from dpone.runtime.normalization.normalizer import NestedNormalizationService
    from dpone.runtime.normalization.options import NestedNormalizationOptions

    options = NestedNormalizationOptions.from_config(
        {
            "enabled": True,
            "paths": {
                "customer": "preserve_json",
                "debug_blob": "ignore",
                "internal": "quarantine",
            },
        }
    )

    result = NestedNormalizationService().normalize_rows(
        [{"order_id": 1, "customer": {"id": 7}, "debug_blob": {"trace": "x"}, "internal": {"token": "secret"}}],
        root_table="orders",
        options=options,
    )

    root_row = result.table("orders").rows[0]
    assert root_row["customer"] == {"id": 7}
    assert "debug_blob" not in root_row
    assert "orders__customer" not in result.as_mapping()
    assert "orders__debug_blob" not in result.as_mapping()
    assert result.table("orders__quarantine").rows[0]["path"] == "internal"
    assert result.table("orders__quarantine").rows[0]["reason"] == "path_policy_quarantine"


def test_nested_split_path_can_override_child_table_name() -> None:
    from dpone.runtime.normalization.normalizer import NestedNormalizationService
    from dpone.runtime.normalization.options import NestedNormalizationOptions

    options = NestedNormalizationOptions.from_config(
        {
            "enabled": True,
            "split_paths": [{"path": "$.items[*]", "table": "order_lines", "policy": "child_table"}],
        }
    )

    result = NestedNormalizationService().normalize_rows(
        [{"order_id": 1, "items": [{"sku": "A"}, {"sku": "B"}]}],
        root_table="orders",
        options=options,
    )

    assert "order_lines" in result.as_mapping()
    assert "orders__items" not in result.as_mapping()
    assert [row["sku"] for row in result.table("order_lines").rows] == ["A", "B"]


def test_nested_explosion_guardrails_fail_fast() -> None:
    import pytest

    from dpone.runtime.normalization.normalizer import NestedNormalizationService
    from dpone.runtime.normalization.options import NestedNormalizationOptions

    options = NestedNormalizationOptions.from_config(
        {"enabled": True, "guardrails": {"max_array_length": 1, "max_rows_per_root": 4}}
    )

    with pytest.raises(ValueError, match="max_array_length"):
        NestedNormalizationService().normalize_rows(
            [{"order_id": 1, "items": [{"sku": "A"}, {"sku": "B"}]}],
            root_table="orders",
            options=options,
        )


def test_nested_raw_landing_adds_raw_table() -> None:
    from dpone.runtime.normalization.normalizer import NestedNormalizationService
    from dpone.runtime.normalization.options import NestedNormalizationOptions

    options = NestedNormalizationOptions.from_config(
        {
            "enabled": True,
            "raw_landing": {"enabled": True, "table_suffix": "__raw", "payload_column": "payload"},
        }
    )
    source_row = {"order_id": 1, "items": [{"sku": "A"}]}

    result = NestedNormalizationService().normalize_rows([source_row], root_table="orders", options=options)

    raw_row = result.table("orders__raw").rows[0]
    assert raw_row["payload"] == source_row
    assert raw_row["__dpone__parent_row_id"] is None


def test_hierarchy_contract_validates_required_tables_and_columns() -> None:
    import pytest

    from dpone.runtime.normalization.contracts import HierarchyContract
    from dpone.runtime.normalization.normalizer import NestedNormalizationService
    from dpone.runtime.normalization.options import NestedNormalizationOptions

    result = NestedNormalizationService().normalize_rows(
        [{"order_id": 1, "items": [{"sku": "A"}]}],
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
    )

    HierarchyContract.from_config(
        {
            "tables": {
                "orders": {"required_columns": ["order_id"]},
                "orders__items": {"parent": "orders", "required_columns": ["sku"]},
            }
        }
    ).validate(result)

    with pytest.raises(ValueError, match="required column"):
        HierarchyContract.from_config({"tables": {"orders__items": {"required_columns": ["missing"]}}}).validate(result)


def test_hierarchy_builder_rebuilds_nested_payload() -> None:
    from dpone.runtime.normalization.hierarchy_builder import HierarchyBuilderService
    from dpone.runtime.normalization.normalizer import NestedNormalizationService
    from dpone.runtime.normalization.options import NestedNormalizationOptions

    result = NestedNormalizationService().normalize_rows(
        [{"order_id": 1, "customer": {"id": 7}, "items": [{"sku": "A"}, {"sku": "B"}], "tags": ["new"]}],
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
    )

    rebuilt = HierarchyBuilderService().rebuild(result, root_table="orders")

    assert rebuilt == [
        {
            "order_id": 1,
            "customer": {"id": 7},
            "items": [{"sku": "A"}, {"sku": "B"}],
            "tags": ["new"],
        }
    ]


def test_normalization_preview_reports_tables_and_columns() -> None:
    from dpone.runtime.normalization.options import NestedNormalizationOptions
    from dpone.runtime.normalization.preview import NormalizationPreviewService

    payload = NormalizationPreviewService().preview_rows(
        [{"order_id": 1, "items": [{"sku": "A"}]}],
        root_table="orders",
        options=NestedNormalizationOptions(enabled=True),
    )

    assert payload["root_table"] == "orders"
    assert payload["tables"]["orders"]["row_count"] == 1
    assert payload["tables"]["orders__items"]["columns"][:1] == ["sku"]


def test_normalize_preview_command_writes_json_file(tmp_path) -> None:
    import argparse
    import json
    import logging

    from dpone.commands.normalize_cmd import cmd_normalize_preview

    sample = tmp_path / "orders.jsonl"
    sample.write_text('{"order_id": 1, "items": [{"sku": "A"}]}\n', encoding="utf-8")
    output = tmp_path / "preview.json"
    args = argparse.Namespace(
        sample=str(sample),
        root_table="orders",
        config=None,
        nested_level=None,
        limit=100,
        format="json",
        output=str(output),
    )

    assert cmd_normalize_preview(args, ctx=object(), logger=logging.getLogger(__name__)) == 0

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["tables"]["orders"]["row_count"] == 1
    assert payload["tables"]["orders__items"]["row_count"] == 1
