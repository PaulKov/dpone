from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION, LoadStrategy
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.sinks.clickhouse_staged_evidence import SourceByteBudgetError
from dpone.runtime.sinks.clickhouse_staged_load import ClickHouseStagedLoadService


class _Decoder:
    def prepare(self, load_config, staging_config, payload):  # noqa: ANN001
        del load_config, payload
        return staging_config, None


class _Sink:
    def __init__(self) -> None:
        self._staging_decoder = _Decoder()
        self.dropped: list[str] = []
        self.swapped = False
        self.publication_cleanup: Any | None = None
        self.publication_receipt: Any | None = None

    def _create_payload_staging_table(self, load_config, payload):  # noqa: ANN001
        del payload
        return SimpleNamespace(target_schema="technical", target_table="stage", options=load_config.options)

    def _insert_payload(self, staging_config, payload):  # noqa: ANN001
        del staging_config, payload
        return 1

    def _drop_table(self, table, config):  # noqa: ANN001
        del config
        self.dropped.append(table)

    @staticmethod
    def _table(config):  # noqa: ANN001
        return f"{config.target_schema}.{config.target_table}"

    def _swap_table_into_target(self, load_config, replacement):  # noqa: ANN001
        del load_config, replacement
        self.swapped = True
        return self.publication_receipt

    @staticmethod
    def _count(load_config):  # noqa: ANN001
        del load_config
        return 1

    def _cleanup_full_refresh_publication(self, receipt):  # noqa: ANN001
        self.publication_cleanup = receipt


def _config(maximum: int) -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="dbo",
        source_table="source_model",
        target_schema="analytics",
        target_table="published_model",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={SOURCE_BYTE_BUDGET_OPTION: maximum},
    )


def _payload(byte_count: int) -> SimpleNamespace:
    artifact = SimpleNamespace(
        source_byte_measurement_complete=True,
        slice_evidence=[{"partition_index": 0, "slice_index": 0, "bytes": byte_count, "sha256": "digest"}],
    )
    return SimpleNamespace(artifact=artifact, schema=(("id", "bigint"),))


@pytest.mark.parametrize("payload", [_payload(11), SimpleNamespace(artifact=object(), schema=())])
def test_budget_failure_drops_staging_without_publishing(payload: SimpleNamespace) -> None:
    sink = _Sink()

    with pytest.raises(SourceByteBudgetError):
        ClickHouseStagedLoadService(sink).load(_config(10), payload)

    assert sink.dropped == ["technical.stage"]
    assert sink.swapped is False


def test_exact_budget_is_admitted_and_receipted() -> None:
    sink = _Sink()
    service = ClickHouseStagedLoadService(sink)

    handle = service.stage(_config(10), _payload(10))

    assert handle.metadata["source_byte_budget"] == {
        "maximum_bytes": 10,
        "observed_bytes": 10,
        "unique_parts": 1,
        "measurement": "source_emitted_bytes",
        "schema_version": "dpone.runtime.source-byte-budget.v1",
    }


def test_full_refresh_cleanup_is_delegated_to_uuid_bound_publication_receipt() -> None:
    publication = {
        "marker": {"candidate": "stage"},
        "marker_table": "published_model__dpone_full_refresh_publication",
        "recovered_after_error": False,
    }
    receipt = SimpleNamespace(
        marker=SimpleNamespace(operation_id="operation"),
        to_dict=lambda: publication,
    )
    sink = _Sink()
    sink.publication_receipt = receipt
    service = ClickHouseStagedLoadService(sink)
    staging = SimpleNamespace(target_schema="technical", target_table="stage")
    handle = StagedLoadHandle(staging_config=staging, payload_schema=(), staged_rows=1, metadata={})

    result = service._full_refresh(_config(10), handle)
    service.cleanup(handle)

    assert result.commit_receipt_id == "operation"
    assert handle.metadata["full_refresh_publication"] == publication
    assert sink.dropped == []
    assert sink.publication_cleanup == publication
