from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.cli.parser import build_parser
from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.schema_contracts import SchemaContract
from dpone.readiness.target_type_resolvers import TargetTypeResolver
from dpone.runtime.support.temporal_fidelity import TemporalFidelityPolicy
from dpone.type_system import SampleTypeProfiler, TypeInferenceOptions
from dpone.type_system.models import InferredColumn


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def test_type_profiler_preserves_null_empty_string_and_low_cardinality_signals() -> None:
    rows = [
        {"id": 1, "status": "new", "comment": "", "amount": "10.50"},
        {"id": 2, "status": "paid", "comment": None, "amount": "20.00"},
        {"id": 3, "status": "new", "comment": "contains text", "amount": "30.25"},
    ]

    profiles = SampleTypeProfiler(TypeInferenceOptions(empty_string_is_null=False)).profile_rows(rows)

    assert profiles["comment"].null_count == 1
    assert profiles["comment"].empty_string_count == 1
    assert profiles["status"].distinct_count == 2
    assert profiles["status"].distinct_ratio == pytest.approx(2 / 3)
    assert profiles["amount"].observed_types == ("decimal",)


def test_schema_contract_overrides_inference_and_target_type_overrides_win() -> None:
    contract = SchemaContract.from_config(
        {
            "enforcement": "strict",
            "columns": {
                "amount": {"type": "decimal", "precision": 18, "scale": 4, "nullable": False},
                "payload": {"type": "json", "nullable": True},
            },
        }
    )
    physical = PhysicalDesignOptions.from_config(
        {
            "columns": {
                "amount": {
                    "target_type": {
                        "mssql": "decimal(18,4)",
                        "postgres": "numeric(18,4)",
                        "clickhouse": "Decimal(18,4)",
                        "bigquery": "NUMERIC",
                    }
                }
            }
        }
    )

    plan = PhysicalDesignPlanner().plan(
        sink_type="mssql",
        table="landing.orders",
        source_schema=[("amount", "text"), ("payload", "text")],
        schema_contract=contract,
        options=physical,
    )

    assert plan.columns["amount"].target_type == "decimal(18,4)"
    assert plan.columns["amount"].decision_source == "physical_override"
    assert plan.columns["payload"].target_type == "nvarchar(max)"
    assert plan.contract.enforcement == "strict"


def test_clickhouse_low_cardinality_modes_are_governed_by_profile_and_config() -> None:
    rows = [{"status": "new", "email": f"user_{idx}@example.com"} for idx in range(100)]
    rows.extend({"status": "paid", "email": f"other_{idx}@example.com"} for idx in range(100))
    profiles = SampleTypeProfiler().profile_rows(rows)

    auto_plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders",
        source_schema=[("status", "text"), ("email", "text")],
        profiles=profiles,
        options=PhysicalDesignOptions.from_config(
            {"storage": {"clickhouse": {"low_cardinality": {"mode": "auto", "max_distinct_ratio": 0.05}}}}
        ),
    )
    off_plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders",
        source_schema=[("status", "text")],
        profiles=profiles,
        options=PhysicalDesignOptions.from_config({"storage": {"clickhouse": {"low_cardinality": {"mode": "off"}}}}),
    )
    explicit_plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders",
        source_schema=[("status", "text")],
        profiles=profiles,
        options=PhysicalDesignOptions.from_config(
            {"storage": {"clickhouse": {"low_cardinality": {"mode": "explicit", "columns": ["status"]}}}}
        ),
    )

    assert auto_plan.columns["status"].target_type == "LowCardinality(String)"
    assert auto_plan.columns["email"].target_type == "String"
    assert "low cardinality profile" in auto_plan.columns["status"].reason
    assert off_plan.columns["status"].target_type == "String"
    assert explicit_plan.columns["status"].target_type == "LowCardinality(String)"


def test_clickhouse_physical_plan_preserves_nullable_by_default_and_can_force_non_nullable() -> None:
    preserve_plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders",
        source_schema=[("amount", "int nullable")],
        options=PhysicalDesignOptions(),
    )
    non_nullable_plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders",
        source_schema=[("amount", "int nullable"), ("comment", "nvarchar(510) nullable")],
        options=PhysicalDesignOptions.from_config(
            {
                "storage": {
                    "clickhouse": {
                        "nullability": {
                            "mode": "non_nullable_by_default",
                            "columns": {
                                "comment": {"mode": "preserve_source"},
                            },
                        }
                    }
                }
            }
        ),
    )

    assert preserve_plan.columns["amount"].target_type == "Nullable(Int64)"
    assert non_nullable_plan.columns["amount"].target_type == "Int64"
    assert non_nullable_plan.columns["amount"].decision_source == "physical_design"
    assert non_nullable_plan.columns["comment"].target_type == "Nullable(String)"


def test_target_type_resolver_applies_temporal_fidelity_policy_for_offset_timestamps() -> None:
    column = InferredColumn(
        name="occurred_at",
        logical_type="timestamp",
        nullable=True,
        confidence=0.99,
        decision_source="source_metadata",
        reason="source metadata type timestamptz",
        timezone=True,
    )
    resolver = TargetTypeResolver(
        temporal_policy=TemporalFidelityPolicy.from_config(
            {"temporal": {"offset_timestamp": {"mode": "fixed_timezone", "timezone": "Europe/Moscow"}}}
        )
    )
    text_resolver = TargetTypeResolver(
        temporal_policy=TemporalFidelityPolicy.from_config(
            {"temporal": {"offset_timestamp": {"mode": "preserve_text"}}}
        )
    )

    clickhouse = resolver.resolve(
        sink_type="clickhouse",
        column=column,
        options=PhysicalDesignOptions(),
    )
    mssql_text = text_resolver.resolve(
        sink_type="mssql",
        column=column,
        options=PhysicalDesignOptions(),
    )

    assert clickhouse.target_type == "DateTime64(6, 'Europe/Moscow')"
    assert clickhouse.reason == "temporal fidelity fixed_timezone"
    assert mssql_text.target_type == "nvarchar(max)"


def test_physical_design_planner_accepts_manifest_temporal_fidelity_config() -> None:
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.events",
        source_schema=[("occurred_at", "timestamptz")],
        options=PhysicalDesignOptions(),
        type_fidelity={"temporal": {"offset_timestamp": {"mode": "fixed_timezone", "timezone": "Europe/Moscow"}}},
    )

    assert plan.columns["occurred_at"].target_type == "DateTime64(6, 'Europe/Moscow')"


def test_physical_design_planner_applies_per_column_temporal_policy() -> None:
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.events",
        source_schema=[("created_at", "timestamptz"), ("raw_at", "iso8601_offset_timestamp")],
        options=PhysicalDesignOptions(),
        type_fidelity={
            "temporal": {
                "offset_timestamp": {
                    "mode": "utc_instant",
                    "columns": {
                        "raw_at": {"mode": "preserve_text"},
                    },
                }
            }
        },
    )

    assert plan.columns["created_at"].target_type == "DateTime64(6, 'UTC')"
    assert plan.columns["raw_at"].target_type == "String"


def test_physical_design_planner_uses_string_for_malformed_preserve_text() -> None:
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.events",
        source_schema=[("occurred_at", "iso8601_offset_timestamp")],
        options=PhysicalDesignOptions(),
        type_fidelity={"temporal": {"offset_timestamp": {"mode": "utc_instant", "malformed": "preserve_text"}}},
    )

    assert plan.columns["occurred_at"].target_type == "String"


def test_physical_design_renders_target_specific_ddl() -> None:
    source_schema = [("id", "bigint"), ("amount", "numeric(18,2)"), ("status", "text")]

    mssql = PhysicalDesignPlanner().plan(
        sink_type="mssql",
        table="landing.orders",
        source_schema=source_schema,
        options=PhysicalDesignOptions.from_config(
            {
                "indexes": {"primary_key": ["id"]},
                "storage": {"mssql": {"compression": "page", "clustered_columnstore": False}},
            }
        ),
    )
    clickhouse = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders",
        source_schema=source_schema,
        options=PhysicalDesignOptions.from_config(
            {
                "storage": {
                    "clickhouse": {
                        "engine": "MergeTree",
                        "partition_by": "toYYYYMM(created_at)",
                        "order_by": ["id"],
                    }
                }
            }
        ),
    )
    bigquery = PhysicalDesignPlanner().plan(
        sink_type="bigquery",
        table="project.landing.orders",
        source_schema=source_schema,
        options=PhysicalDesignOptions.from_config(
            {"storage": {"bigquery": {"partition_by": "created_at", "clustering": ["id", "status"]}}}
        ),
    )

    assert "CREATE TABLE [landing].[orders]" in "\n".join(mssql.ddl)
    assert "DATA_COMPRESSION = PAGE" in "\n".join(mssql.ddl)
    assert "CREATE TABLE `landing`.`orders`" in "\n".join(clickhouse.ddl)
    assert "ENGINE = MergeTree" in "\n".join(clickhouse.ddl)
    assert "PARTITION BY toYYYYMM(created_at)" in "\n".join(clickhouse.ddl)
    assert "CREATE TABLE `project.landing.orders`" in "\n".join(bigquery.ddl)
    assert "CLUSTER BY id, status" in "\n".join(bigquery.ddl)


def test_schema_cli_registers_infer_and_physical_plan() -> None:
    parser = build_parser()

    assert parser.parse_args(["schema", "infer"]).schema_cmd == "infer"
    assert parser.parse_args(["schema", "physical-plan"]).schema_cmd == "physical-plan"
    assert parser.parse_args(["schema", "physical-diff", "--actual", "actual.json"]).schema_cmd == "physical-diff"


def test_schema_infer_and_physical_plan_cli_render_manifest_contracts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _patch_context(monkeypatch)
    manifest = tmp_path / "orders.batch.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "source": {
                    "type": "api",
                    "options": {
                        "columns": [
                            {"name": "amount", "type": "text"},
                            {"name": "status", "type": "text"},
                            {"name": "occurred_at", "type": "timestamptz"},
                        ],
                        "type_fidelity": {
                            "temporal": {
                                "offset_timestamp": {
                                    "mode": "fixed_timezone",
                                    "timezone": "Europe/Moscow",
                                }
                            }
                        },
                    },
                },
                "sink": {
                    "type": "clickhouse",
                    "table": {"schema": "landing", "name": "orders"},
                    "options": {
                        "schema_contract": {
                            "enforcement": "strict",
                            "columns": {"amount": {"type": "decimal", "precision": 18, "scale": 4}},
                        },
                        "physical_design": {
                            "storage": {
                                "clickhouse": {
                                    "order_by": ["amount"],
                                    "low_cardinality": {"mode": "explicit", "columns": ["status"]},
                                }
                            }
                        },
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as infer_exit:
        cli_main.main(["schema", "infer", "--manifest", str(manifest), "--format", "json"])

    assert infer_exit.value.code == 0
    infer_payload = json.loads(capsys.readouterr().out)
    assert infer_payload["columns"]["amount"]["logical_type"] == "decimal"
    assert infer_payload["schema_contract"]["enforcement"] == "strict"

    with pytest.raises(SystemExit) as physical_exit:
        cli_main.main(["schema", "physical-plan", "--manifest", str(manifest), "--format", "json"])

    assert physical_exit.value.code == 0
    physical_payload = json.loads(capsys.readouterr().out)
    assert physical_payload["columns"]["status"]["target_type"] == "LowCardinality(String)"
    assert physical_payload["columns"]["occurred_at"]["target_type"] == "DateTime64(6, 'Europe/Moscow')"
    assert "ORDER BY (`amount`)" in "\n".join(physical_payload["ddl"])


def test_manifest_schemas_expose_type_contract_and_physical_design_sections() -> None:
    config_schema = json.loads(Path("src/dpone/schema/etl-config.schema.json").read_text(encoding="utf-8"))
    batch_schema = json.loads(Path("src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))

    for schema in (config_schema, batch_schema):
        sink_options = (
            schema["properties"]["sink"]["properties"]["options"]["properties"]
            if "sink" in schema.get("properties", {})
            else schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]["options"]["properties"]
        )
        source_options = (
            schema["properties"]["source"]["properties"]["options"]["properties"]
            if "source" in schema.get("properties", {})
            else schema["definitions"]["process_fragment"]["properties"]["source"]["properties"]["options"][
                "properties"
            ]
        )

        assert schema["properties"]["schema_contract"]["properties"]["enforcement"]["enum"] == [
            "strict",
            "coerce",
            "quarantine",
            "warn",
        ]
        assert sink_options["schema_contract"]["properties"]["columns"]["additionalProperties"]["properties"]["type"]
        assert sink_options["type_inference"]["properties"]["conflict_policy"]["enum"] == [
            "fail",
            "variant_column",
            "quarantine",
        ]
        assert sink_options["physical_design"]["properties"]["apply"]["enum"] == [
            "online",
            "safe_window",
            "plan_only",
            "manual_approval",
        ]
        assert sink_options["physical_design"]["properties"]["reconciliation"]["properties"]["mode"]["enum"] == [
            "block",
            "auto_safe",
            "plan_only",
            "safe_window",
        ]
        assert sink_options["physical_design"]["properties"]["migration"]["properties"]["strategy"]["enum"] == [
            "block",
            "online_safe",
            "shadow",
        ]
        assert sink_options["physical_design"]["properties"]["storage"]["properties"]["clickhouse"]["properties"][
            "low_cardinality"
        ]["properties"]["mode"]["enum"] == ["off", "auto", "explicit", "force", "preserve"]
        assert sink_options["physical_design"]["properties"]["storage"]["properties"]["clickhouse"]["properties"][
            "table_settings"
        ]["additionalProperties"]["type"] == ["string", "number", "integer", "boolean"]
        nullability = sink_options["physical_design"]["properties"]["storage"]["properties"]["clickhouse"][
            "properties"
        ]["nullability"]["properties"]
        assert nullability["mode"]["enum"] == ["preserve_source", "non_nullable_by_default"]
        assert nullability["null_handling"]["enum"] == ["default", "fail_fast"]
        for options in (source_options, sink_options):
            temporal = options["type_fidelity"]["properties"]["temporal"]["properties"]["offset_timestamp"][
                "properties"
            ]
            naive = options["type_fidelity"]["properties"]["temporal"]["properties"]["naive_timestamp"]["properties"]
            assert temporal["mode"]["enum"] == ["utc_instant", "fixed_timezone", "preserve_offset", "preserve_text"]
            assert temporal["malformed"]["enum"] == ["fail", "preserve_text"]
            assert temporal["columns"]["additionalProperties"]["properties"]["malformed"]["enum"] == [
                "fail",
                "preserve_text",
            ]
            assert naive["mode"]["enum"] == ["datetime64"]
            assert naive["transfer_encoding"]["enum"] == ["auto", "text", "epoch"]
