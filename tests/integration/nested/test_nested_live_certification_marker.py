from __future__ import annotations

import json
import os
import socket
from pathlib import Path

import pytest

from dpone.readiness.nested_live_certification import NestedLiveCertificationCase, NestedLiveCertificationRunner


def _port_available(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


@pytest.mark.integration_nested_live
def test_nested_live_readiness_records_connectivity_without_certifying_routes(tmp_path: Path) -> None:
    if os.getenv("DPONE_ENABLE_NESTED_LIVE", "0") != "1":
        pytest.skip("set DPONE_ENABLE_NESTED_LIVE=1 to run nested live certification harness")

    artifact = NestedLiveCertificationRunner(
        cases=(
            NestedLiveCertificationCase(
                sink_type="postgres",
                available=lambda: _port_available("localhost", int(os.getenv("DPONE_POSTGRES_PORT", "5432"))),
                check=lambda: {"native_route": "copy", "state_backend": "postgres"},
            ),
            NestedLiveCertificationCase(
                sink_type="mssql",
                available=lambda: _port_available("localhost", int(os.getenv("DPONE_MSSQL_PORT", "1433"))),
                check=lambda: {"native_route": "bcp", "state_backend": "mssql"},
            ),
            NestedLiveCertificationCase(
                sink_type="clickhouse",
                available=lambda: _port_available("localhost", int(os.getenv("DPONE_CLICKHOUSE_PORT", "8123"))),
                check=lambda: {"native_route": "json_each_row", "delete_finalizer": "lightweight_delete"},
            ),
            NestedLiveCertificationCase(
                sink_type="kafka",
                available=lambda: _port_available("localhost", int(os.getenv("DPONE_KAFKA_PORT", "9092"))),
                check=lambda: {"native_route": "file_stream", "delete_finalizer": "keyed_delete_event"},
            ),
        )
    ).run(output_dir=tmp_path)

    assert artifact.json_path.exists()
    assert artifact.markdown_path.exists()
    payload = json.loads(artifact.json_path.read_text(encoding="utf-8"))
    assert payload["status"] == "unverified"
    assert all(item["status"] in {"unverified", "skipped"} for item in payload["sinks"].values())
