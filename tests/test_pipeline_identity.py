from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.manifest.pipeline_identity import PIPELINE_ID_PATTERN, PipelineId, PipelineIdError
from dpone.manifest.pipeline_source_reference import (
    PipelineSourceReferenceError,
    resolve_pipeline_source_relative,
)
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service


@pytest.mark.parametrize(
    "value",
    (
        "orders_daily",
        "orders-daily",
        "current",
        "o1",
        "a" + "b" * 127,
    ),
)
def test_pipeline_id_accepts_only_canonical_schema_identity(value: str) -> None:
    assert str(PipelineId.parse(value)) == value


def test_pipeline_id_direct_constructor_cannot_bypass_validation() -> None:
    with pytest.raises(PipelineIdError) as exc:
        PipelineId("Orders Daily")

    assert exc.value.suggested_id == "orders_daily"


@pytest.mark.parametrize(
    "value",
    (
        "",
        "a",
        "OrdersDaily",
        "orders.daily",
        "_orders",
        "-orders",
        "orders daily",
        "orders/daily",
        "orders\ndaily",
        "orders\x00daily",
        "a" * 129,
        "\u0437\u0430\u043a\u0430\u0437\u044b",
    ),
)
def test_pipeline_id_rejects_noncanonical_values_with_safe_suggestion(value: str) -> None:
    with pytest.raises(PipelineIdError) as exc:
        PipelineId.parse(value)

    suggestion = exc.value.suggested_id
    assert str(PipelineId.parse(suggestion)) == suggestion
    assert str(exc.value) == "Pipeline id must match ^[a-z0-9][a-z0-9_-]{1,127}$."


def test_pipeline_id_pattern_matches_public_flow_schema() -> None:
    schema = json.loads(
        (Path(__file__).parents[1] / "src/dpone/schema/etl-flow-manifest.schema.json").read_text(encoding="utf-8")
    )

    assert schema["definitions"]["metadata"]["properties"]["id"]["pattern"] == PIPELINE_ID_PATTERN


def test_bare_pipeline_reference_uses_canonical_pipeline_identity(tmp_path: Path) -> None:
    with pytest.raises(PipelineSourceReferenceError):
        resolve_pipeline_source_relative(tmp_path, "OrdersDaily")


def test_init_pipeline_rejects_noncanonical_id_without_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "init",
                "pipeline",
                "Orders Daily",
                "--recipe",
                "mssql-to-clickhouse-incremental",
                "--format",
                "json",
            ]
        )

    captured = capsys.readouterr()
    assert exc.value.code == 2
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_PIPELINE_ID_INVALID"
    assert payload["errors"][0]["stage"] == "init_pipeline"
    assert payload["errors"][0]["suggested_id"] == "orders_daily"
    assert payload["errors"][0]["fixes"] == [
        {
            "id": "use_suggested_pipeline_id",
            "safety": "safe",
            "command": ("dpone init pipeline orders_daily --recipe mssql-to-clickhouse-incremental --authoring flow"),
        }
    ]
    assert "Orders Daily" not in captured.out
    assert list(tmp_path.iterdir()) == []


def test_bare_pipeline_reference_must_match_compiled_metadata_id(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    initialized = service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=False,
    )
    assert initialized.passed is True
    source = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["metadata"]["id"] = "customers_daily"
    source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    checked = service.check("orders_daily")

    assert checked.passed is False
    assert checked.errors[0]["code"] == "DPONE_PIPELINE_ID_MISMATCH"
    assert checked.errors[0]["entity"] == {"kind": "pipeline", "id": "orders_daily"}


def test_pipeline_id_is_projected_to_json_string_on_scaffold_failure(tmp_path: Path) -> None:
    result = build_airflow_self_service_service(root=tmp_path).init_pipeline(
        pipeline_id="orders_daily",
        recipe="missing-recipe",
        airflow=True,
    )

    assert result.errors[0]["entity"] == {"kind": "pipeline", "id": "orders_daily"}
    json.dumps(result.to_dict())
