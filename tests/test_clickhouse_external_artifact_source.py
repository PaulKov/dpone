from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION
from dpone.contracts.clickhouse_external_replication import ExternalContractError
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sinks.clickhouse_external_artifact_source import ClickHouseExternalArtifactSource
from dpone.runtime.sinks.load_payload import LoadPayload


class _Sink:
    _payload_ingestion = SimpleNamespace(
        _clickhouse_schema=lambda config, schema: tuple((name, "Int64") for name, _ in schema)
    )


def _payload(rows: list[dict[str, object]]) -> LoadPayload:
    return LoadPayload(artifact=InMemoryRowsArtifact(rows), schema=(("id", "bigint"),))


def test_sealed_source_is_order_independent_replayable_and_path_free() -> None:
    first = ClickHouseExternalArtifactSource(
        sink=_Sink(), load_config=object(), payload=_payload([{"id": 2}, {"id": 1}]), maximum_rows=2
    )
    second = ClickHouseExternalArtifactSource(
        sink=_Sink(), load_config=object(), payload=_payload([{"id": 1}, {"id": 2}]), maximum_rows=2
    )

    assert first.identity.wire_digest == second.identity.wire_digest
    assert first.open_replay().artifact.estimated_rows == 2
    assert "path" not in repr(first).lower()


def test_revalidation_detects_mutation_and_budget_is_fail_closed() -> None:
    payload = _payload([{"id": 1}])
    source = ClickHouseExternalArtifactSource(sink=_Sink(), load_config=object(), payload=payload, maximum_rows=1)
    payload.artifact._rows[0]["id"] = 2  # type: ignore[attr-defined]

    with pytest.raises(ExternalContractError, match="ARTIFACT_CHANGED"):
        source.revalidate(source.identity)
    with pytest.raises(ExternalContractError, match="CONTENT_BUDGET_EXCEEDED"):
        ClickHouseExternalArtifactSource(
            sink=_Sink(), load_config=object(), payload=_payload([{"id": 1}, {"id": 2}]), maximum_rows=1
        )


def test_sealed_source_enforces_exact_source_byte_budget_before_staging() -> None:
    payload = _payload([{"id": 1}])
    baseline = ClickHouseExternalArtifactSource(
        sink=_Sink(),
        load_config=SimpleNamespace(options={SOURCE_BYTE_BUDGET_OPTION: 10_000}),
        payload=payload,
        maximum_rows=1,
    )
    size = baseline.identity.byte_size

    exact = ClickHouseExternalArtifactSource(
        sink=_Sink(),
        load_config=SimpleNamespace(options={SOURCE_BYTE_BUDGET_OPTION: size}),
        payload=_payload([{"id": 1}]),
        maximum_rows=1,
    )
    assert exact.identity.byte_size == size

    with pytest.raises(ExternalContractError, match="CONTENT_BUDGET_EXCEEDED"):
        ClickHouseExternalArtifactSource(
            sink=_Sink(),
            load_config=SimpleNamespace(options={SOURCE_BYTE_BUDGET_OPTION: size - 1}),
            payload=_payload([{"id": 1}]),
            maximum_rows=1,
        )


def test_retained_artifact_reopens_after_source_instance_is_gone(tmp_path: Path) -> None:
    source = ClickHouseExternalArtifactSource(
        sink=_Sink(),
        load_config=object(),
        payload=_payload([{"id": 1}, {"id": 2}]),
        maximum_rows=2,
    )
    source.persist(tmp_path)

    reopened = ClickHouseExternalArtifactSource.reopen(
        root=tmp_path,
        binding_id=source.binding_id,
        expected=source.identity,
    )

    assert reopened.identity == source.identity
    assert reopened.open_replay().artifact._rows == [{"id": 1}, {"id": 2}]  # type: ignore[attr-defined]
    assert str(tmp_path) not in repr(reopened)
    reopened.release(tmp_path)
    assert list(tmp_path.iterdir()) == []
