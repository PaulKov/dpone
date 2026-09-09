from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.sinks.clickhouse_columnar_direct_push import ClickHouseColumnarDirectPushLoader


def test_clickhouse_columnar_direct_push_loader_loads_chunked_artifact_in_order(tmp_path: Path) -> None:
    first = tmp_path / "chunk-00000.parquet"
    second = tmp_path / "chunk-00001.parquet"
    first.write_bytes(b"PAR1 first")
    second.write_bytes(b"PAR1 second")
    runner = _Runner()
    artifact = _ChunkedArtifact([first, second])
    loader = ClickHouseColumnarDirectPushLoader(
        table_name=lambda _: "raw.orders__staging",
        count_rows=lambda _: 2,
        client_runner_factory=lambda *_args, **_kwargs: runner,
        http_runner_factory=lambda *_args, **_kwargs: runner,
    )

    inserted = loader.load_chunked(_cfg(), artifact, [("id", "int")])

    assert inserted == 2
    assert runner.calls == [
        ("raw.orders__staging", ("id",), str(first)),
        ("raw.orders__staging", ("id",), str(second)),
    ]


def _cfg() -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="raw",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"clickhouse_bulk": {"mode": "client"}},
    )


class _ChunkedArtifact:
    format = "parquet"

    def __init__(self, paths: list[Path]) -> None:
        self._paths = paths

    def iter_chunks(self):
        for index, path in enumerate(self._paths):
            yield SimpleNamespace(index=index, path=path)


class _Runner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[str, ...], str]] = []

    def insert_file(self, table: str, columns: list[str], path: str) -> None:
        self.calls.append((table, tuple(columns), path))
