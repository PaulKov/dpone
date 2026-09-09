from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.native_transfer_execution import NativeTransferResourcePolicy
from dpone.runtime.native_transfer_slicing import TransferSlice
from dpone.runtime.sinks.clickhouse_payload_ingestion import ClickHousePayloadIngestionService
from dpone.runtime.sinks.load_payload import LoadPayload


def test_clickhouse_rejects_mssql_native_file_without_native_wire_contract(tmp_path) -> None:
    payload = tmp_path / "payload.bcp"
    payload.write_bytes(b"\xfe\xff\x00\x01")
    artifact = FileExportArtifact(
        str(payload),
        columns=["id"],
        compressed=False,
        format="mssql-native",
    )
    service = ClickHousePayloadIngestionService(_FakeSink(), sink_factory=lambda _connector: _FakeSink())

    with pytest.raises(ValueError, match="clickhouse_mssql_native_requires_native_wire_contract"):
        service.insert_file(_load_config(), artifact, [("id", "int")])


def test_partitioned_stream_load_reports_slice_delta_instead_of_cumulative_staging_count() -> None:
    sink = _CumulativeStreamSink()
    service = ClickHousePayloadIngestionService(sink, sink_factory=lambda _connector: sink)
    artifact = PartitionedTransferPlanArtifact(
        slices=(
            TransferSlice(0, 0, 0, 2, estimated_rows=2),
            TransferSlice(0, 1, 2, 5, estimated_rows=3),
        ),
        columns=["id"],
        exporter=lambda _item: pytest.fail("file exporter must not run for stream transport"),
        stream_exporter=_native_stream,
        transport_plan=SimpleNamespace(transport="stream"),
        resource_policy=NativeTransferResourcePolicy(),
    )

    rows = service.insert_payload(_load_config(), LoadPayload(artifact=artifact, schema=[("id", "int")]))

    assert rows == 5
    assert [item["rows_loaded"] for item in artifact.slice_evidence] == [2, 3]


def test_partitioned_file_load_reports_slice_delta_instead_of_cumulative_staging_count(tmp_path) -> None:
    sink = _CumulativeFileSink()
    service = ClickHousePayloadIngestionService(sink, sink_factory=lambda _connector: sink)
    artifact = PartitionedTransferPlanArtifact(
        slices=(
            TransferSlice(0, 0, 0, 2, estimated_rows=2),
            TransferSlice(0, 1, 2, 5, estimated_rows=3),
        ),
        columns=["id"],
        exporter=lambda item: _native_file(tmp_path, item),
        resource_policy=NativeTransferResourcePolicy(),
    )

    rows = service.insert_payload(_load_config(), LoadPayload(artifact=artifact, schema=[("id", "int")]))

    assert rows == 5
    assert [item["rows_loaded"] for item in artifact.slice_evidence] == [2, 3]


class _FakeSink:
    connector = SimpleNamespace(connection=SimpleNamespace(execute=lambda *args, **kwargs: None))

    def _should_use_http_bulk(self, *_args, **_kwargs) -> bool:
        return False

    def _should_use_client_bulk(self, *_args, **_kwargs) -> bool:
        return False


class _CumulativeStreamSink(_FakeSink):
    def __init__(self) -> None:
        self._count_value = 0

    def _count(self, _load_config: LoadConfig) -> int:
        return self._count_value

    def _should_use_client_stream(self, *_args, **_kwargs) -> bool:
        return True

    def _should_use_http_stream(self, *_args, **_kwargs) -> bool:
        return False

    def _insert_stream_with_client(
        self,
        _load_config: LoadConfig,
        artifact: ByteStreamArtifact,
        _schema: list[tuple[str, str]],
    ) -> int:
        self._count_value += int(artifact.estimated_rows or 0)
        return self._count_value


class _CumulativeFileSink(_FakeSink):
    def __init__(self) -> None:
        self._count_value = 0

    def _count(self, _load_config: LoadConfig) -> int:
        return self._count_value

    def _should_use_client_bulk(self, *_args, **_kwargs) -> bool:
        return True

    def _insert_file_with_client(
        self,
        _load_config: LoadConfig,
        artifact: FileExportArtifact,
        _schema: list[tuple[str, str]],
    ) -> int:
        self._count_value += int(artifact.estimated_rows or 0)
        return self._count_value


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
    )


def _native_stream(item: TransferSlice) -> ByteStreamArtifact:
    return ByteStreamArtifact(
        lambda: iter((b"",)),
        columns=("id",),
        format="mssql-delimited",
        estimated_rows=item.estimated_rows,
    )


def _native_file(tmp_path, item: TransferSlice) -> FileExportArtifact:
    path = tmp_path / f"slice_{item.slice_index}.tsv"
    path.write_text("\n".join(str(index) for index in range(int(item.estimated_rows or 0))) + "\n", encoding="utf-8")
    return FileExportArtifact(
        str(path),
        ["id"],
        format="mssql-delimited",
        estimated_rows=item.estimated_rows,
    )
