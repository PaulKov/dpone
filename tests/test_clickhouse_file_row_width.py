"""Malformed CSV must fail through the public parser/processor/export route.

These synthetic real-row tests exercise production dispatch and a real COPY
child, not a live database. Extra fields are injected as malformed external
COPY responses; ordinary PostgreSQL SELECT is not claimed to emit them.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
import yaml

from dpone.manifest.loader import ManifestLoaderRouter
from dpone.runtime.etl.owned_payload_scope import OwnedPayloadScope
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.process_logging import create_etl_logger
from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink
from dpone.runtime.sources.postgres import PostgresSource
from tests.helpers.clickhouse_csv_width_peers import (
    ClickHousePeer,
    LocalClickHouseConnector,
    LocalPostgresConnector,
    PostgresPeer,
)

pytestmark = pytest.mark.integration_matrix_mock


@contextmanager
def observe_source_ownership(scopes: list[OwnedPayloadScope]) -> Iterator[None]:
    """Read the public ownership result without replacing runtime behavior."""
    code = OwnedPayloadScope.from_extract_result.__func__.__code__
    previous = sys.getprofile()

    def observe(frame: Any, event: str, result: Any) -> None:
        if event == "return" and frame.f_code is code and isinstance(result, OwnedPayloadScope):
            scopes.append(result)
        if previous is not None:
            previous(frame, event, result)

    sys.setprofile(observe)
    try:
        yield
    finally:
        sys.setprofile(previous)


def run_public_csv(
    tmp_path: Path, wire: bytes, source: PostgresPeer, target: ClickHousePeer, scopes: list[OwnedPayloadScope]
) -> dict[str, Any]:
    """Load real YAML and compose only external I/O dependencies explicitly."""
    source.wire_path.write_bytes(wire)
    manifest = {
        "name": "public_csv_width",
        "source": {
            "type": "postgres",
            "connection_id": "synthetic_postgres",
            "table": {"schema": "public", "name": "width_case"},
            "options": {
                "export_format": "csv",
                "compress_export": False,
                "batch_commit_mode": "whole",
                "batch_size": 1,
            },
        },
        "sink": {
            "type": "clickhouse",
            "connection_id": "synthetic_clickhouse",
            "table": {"schema": "analytics", "name": "width_case"},
            "strategy": {"mode": "full_refresh"},
            "options": {"clickhouse_bulk": {"mode": "python"}, "lineage": False},
        },
        "runtime": {"storage": {"work_dir": str(tmp_path / "work")}},
    }
    manifest_path = tmp_path / "manifest.yaml"
    manifest_path.write_text(yaml.safe_dump(manifest), encoding="utf-8")
    config = ManifestLoaderRouter().load(manifest_path, metadata_only=True).processes[0].config.load_config
    logger = create_etl_logger()
    connector = LocalClickHouseConnector(target)
    processor = ETLProcessor(
        source=PostgresSource(LocalPostgresConnector(source), None, logger, sink_connector=connector),
        sink=ClickHouseSink(connector, logger=logger),
        etl_logger=logger,
    )
    with observe_source_ownership(scopes):
        return processor.run(config)


@pytest.mark.parametrize(
    ("bad_record", "actual"),
    [(b'"2","SECRET_VALUE","EXTRA_SENTINEL"\n', 3), (b'"SECRET_VALUE"\n', 1)],
    ids=["extra-field", "missing-field-before-conversion"],
)
@pytest.mark.parametrize("prefix", [b"", b'"1","kept"\n'], ids=["first-row", "after-batch"])
def test_public_csv_rejects_wrong_width_without_finalization(
    tmp_path: Path, bad_record: bytes, actual: int, prefix: bytes
) -> None:
    source = PostgresPeer(tmp_path / "external-copy.csv")
    target = ClickHousePeer()
    scopes: list[OwnedPayloadScope] = []

    with pytest.raises(
        ValueError, match=rf"^clickhouse_file_row_width_mismatch: expected=2, actual={actual}$"
    ) as error:
        run_public_csv(tmp_path, prefix + bad_record, source, target, scopes)

    assert "SECRET_VALUE" not in str(error.value)
    assert "EXTRA_SENTINEL" not in str(error.value)
    assert target.inserts == ([[(1, "kept")]] if prefix else [])
    assert not any(query.startswith("RENAME TABLE") for query in target.queries)
    assert "committed" not in target.load_statuses
    assert scopes and scopes[0].terminal_receipt is not None
    assert scopes[0].terminal_receipt.outcome.value == "abort"
    assert any(query.startswith("DROP TABLE") and "__dpone_staging_" in query for query in target.queries)
    assert source.child_exit_codes == [0]
    assert source.copy_statements == [
        'COPY (SELECT "id", "value" FROM "public"."width_case") TO STDOUT WITH (FORMAT CSV, FORCE_QUOTE *)'
    ]


def test_public_csv_exact_width_keeps_conversion_and_commits(tmp_path: Path) -> None:
    source = PostgresPeer(tmp_path / "external-copy.csv")
    target = ClickHousePeer()
    scopes: list[OwnedPayloadScope] = []

    result = run_public_csv(tmp_path, '"1","café, quoted ""text"""\n'.encode(), source, target, scopes)

    assert result["status"] == "success"
    assert target.tables["`analytics`.`width_case`"] == [(1, 'café, quoted "text"')]
    assert "committed" in target.load_statuses
    assert scopes and scopes[0].terminal_receipt is not None
    assert scopes[0].terminal_receipt.outcome.value == "success"
    assert source.child_exit_codes == [0]
