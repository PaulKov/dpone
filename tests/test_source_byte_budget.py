from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft7Validator

from dpone.config.load_strategy import MAX_SOURCE_BYTE_BUDGET, SOURCE_BYTE_BUDGET_OPTION, LoadStrategy
from dpone.contracts.source_byte_budget_admission import source_byte_budget_rejection
from dpone.dag.errors import DagConfigurationError
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.runtime.sinks.clickhouse_staged_evidence import SourceByteBudgetError, enforce_source_byte_budget


def _config(maximum: object, *, strategy: LoadStrategy = LoadStrategy.FULL_REFRESH) -> SimpleNamespace:
    return SimpleNamespace(load_strategy=strategy, options={SOURCE_BYTE_BUDGET_OPTION: maximum})


def _enforce(config: SimpleNamespace, payload: SimpleNamespace):  # noqa: ANN202
    return enforce_source_byte_budget(
        payload,
        maximum_bytes=config.options[SOURCE_BYTE_BUDGET_OPTION],
        full_refresh=config.load_strategy is LoadStrategy.FULL_REFRESH,
    )


def test_partition_evidence_is_aggregated_once_per_slice() -> None:
    artifact = SimpleNamespace(
        source_byte_measurement_complete=True,
        slice_evidence=[
            {"partition_index": 0, "slice_index": 0, "bytes": 7, "sha256": "a"},
            {"partition_index": 0, "slice_index": 0, "bytes": 7, "sha256": "a"},
            {"partition_index": 0, "slice_index": 1, "bytes": 11, "sha256": "b"},
        ],
    )

    evidence = _enforce(_config(18), SimpleNamespace(artifact=artifact))

    assert evidence is not None
    assert evidence.observed_bytes == 18
    assert evidence.unique_parts == 2


def test_budget_excess_is_rejected_before_publication() -> None:
    artifact = SimpleNamespace(
        source_byte_measurement_complete=True,
        slice_evidence=[{"partition_index": 0, "slice_index": 0, "bytes": 19, "sha256": "a"}],
    )

    with pytest.raises(SourceByteBudgetError, match="DPONE_SOURCE_BYTE_BUDGET_EXCEEDED") as caught:
        _enforce(_config(18), SimpleNamespace(artifact=artifact))

    assert caught.value.observed_bytes == 19
    assert caught.value.maximum_bytes == 18


def test_unmeasurable_payload_fails_closed() -> None:
    with pytest.raises(SourceByteBudgetError, match="DPONE_SOURCE_BYTE_BUDGET_UNMEASURABLE"):
        _enforce(_config(18), SimpleNamespace(artifact=object()))


def test_public_contract_wrapper_is_unwrapped_without_private_field_access() -> None:
    inner = SimpleNamespace(
        source_byte_measurement_complete=True,
        slice_evidence=[{"partition_index": 0, "slice_index": 0, "bytes": 9, "sha256": "a"}],
    )

    class Wrapper:
        @property
        def validated_file_contract_artifact(self) -> object:
            return inner

    evidence = _enforce(_config(9), SimpleNamespace(artifact=Wrapper()))

    assert evidence is not None
    assert evidence.observed_bytes == 9


def test_conflicting_replay_identity_fails_closed() -> None:
    artifact = SimpleNamespace(
        source_byte_measurement_complete=True,
        slice_evidence=[
            {"partition_index": 0, "slice_index": 0, "bytes": 7, "sha256": "a"},
            {"partition_index": 0, "slice_index": 0, "bytes": 7, "sha256": "b"},
        ],
    )

    with pytest.raises(SourceByteBudgetError, match="DPONE_SOURCE_BYTE_BUDGET_UNMEASURABLE"):
        _enforce(_config(18), SimpleNamespace(artifact=artifact))


def test_empty_lazy_plan_requires_explicit_completion() -> None:
    with pytest.raises(SourceByteBudgetError, match="DPONE_SOURCE_BYTE_BUDGET_UNMEASURABLE"):
        _enforce(
            _config(18),
            SimpleNamespace(artifact=SimpleNamespace(source_byte_measurement_complete=False, slice_evidence=[])),
        )

    evidence = _enforce(
        _config(18),
        SimpleNamespace(artifact=SimpleNamespace(source_byte_measurement_complete=True, slice_evidence=[])),
    )
    assert evidence is not None
    assert evidence.observed_bytes == 0


def test_completed_empty_mssql_bcp_stream_is_measurable() -> None:
    artifact = SimpleNamespace(
        source_export_provider="mssql_bcp_pipe",
        stats=SimpleNamespace(
            chunks=0,
            size_bytes=0,
            sha256="sha256:" + "0" * 64,
        ),
        extraction_lifecycle=SimpleNamespace(receipt=SimpleNamespace(complete=True)),
    )

    evidence = _enforce(_config(18), SimpleNamespace(artifact=artifact))

    assert evidence is not None
    assert evidence.observed_bytes == 0
    assert evidence.unique_parts == 1


@pytest.mark.parametrize("maximum", [0, -1, True, "18", MAX_SOURCE_BYTE_BUDGET + 1])
def test_budget_must_be_a_positive_integer(maximum: object) -> None:
    with pytest.raises(ValueError, match="max_source_bytes_out_of_range"):
        _enforce(_config(maximum), SimpleNamespace(artifact=object()))


def test_budget_is_rejected_for_non_full_refresh() -> None:
    with pytest.raises(SourceByteBudgetError, match="DPONE_SOURCE_BYTE_BUDGET_STRATEGY_INVALID"):
        _enforce(
            _config(18, strategy=LoadStrategy.INCREMENTAL_APPEND),
            SimpleNamespace(artifact=object()),
        )


def _process(
    *,
    budget: object,
    source_options: dict[str, object] | None = None,
    sink_type: str = "clickhouse",
) -> dict[str, object]:
    return {
        "source": {
            "type": "mssql",
            "connection_id": "source",
            "table": {"schema": "dbo", "name": "source_model"},
            "options": source_options or {},
        },
        "sink": {
            "type": sink_type,
            "connection_id": "target",
            "table": {"schema": "analytics", "name": "published_model"},
            "strategy": {"mode": "full_refresh", "max_source_bytes": budget},
        },
    }


def test_builder_preserves_the_strategy_budget_in_reserved_runtime_options() -> None:
    config = LoadConfigBuilder().build(_process(budget=18))

    assert config.options[SOURCE_BYTE_BUDGET_OPTION] == 18
    assert "max_source_bytes" not in config.options


@pytest.mark.parametrize("budget", [0, -1, True, "18", MAX_SOURCE_BYTE_BUDGET + 1])
def test_builder_rejects_invalid_budget_before_runtime_io(budget: object) -> None:
    with pytest.raises(DagConfigurationError, match="integer between"):
        LoadConfigBuilder().build(_process(budget=budget))


def test_endpoint_options_cannot_override_strategy_budget() -> None:
    with pytest.raises(DagConfigurationError, match="misplaced"):
        LoadConfigBuilder().build(_process(budget=18, source_options={"max_source_bytes": 1000}))


@pytest.mark.parametrize("sink_type", ["postgres", "mssql", "bigquery"])
def test_builder_rejects_budget_for_sink_without_enforcement(sink_type: str) -> None:
    with pytest.raises(DagConfigurationError, match=f"unsupported for sink.type={sink_type}"):
        LoadConfigBuilder().build(_process(budget=18, sink_type=sink_type))


def test_legacy_admission_predicate_remains_fail_closed() -> None:
    assert source_byte_budget_rejection({}) is None
    assert source_byte_budget_rejection({"max_source_bytes": 18}) is not None


def test_public_batch_schema_accepts_full_refresh_source_byte_budget() -> None:
    schema = json.loads(Path("src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))
    strategy = schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]["strategy"]
    validator = Draft7Validator({**strategy, "definitions": schema.get("definitions", {})})

    assert list(validator.iter_errors({"mode": "full_refresh", "max_source_bytes": 18})) == []
