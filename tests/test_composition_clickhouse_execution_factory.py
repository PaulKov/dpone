"""Offline ClickHouse pack-exec compose and sealed snapshot reopen."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from dpone.app.composition_clickhouse_execution_factory import (
    compose_clickhouse_pack_dependencies,
    load_sealed_clickhouse_snapshot,
)
from dpone.app.composition_clickhouse_publication import (
    clickhouse_http_endpoint,
    snapshot_limits_from_manifest,
)
from dpone.app.composition_clickhouse_source import clickhouse_type_for_mssql
from dpone.contracts.composition_activation import CompositionAdmissionError
from tests.composition_snapshot_helpers import digest

_RELEASE = "sha256:" + "a" * 64
_GENERATION_UUID = str(UUID(int=13))


def _snapshot_payload() -> dict[str, object]:
    return {
        "generation_ref": digest("generation record"),
        "generation_uuid": _GENERATION_UUID,
        "columns": [{"name": "id", "type_name": "Int32"}, {"name": "name", "type_name": "Nullable(String)"}],
    }


def _write_snapshot(cache: Path, workload_id: str = "a_native") -> Path:
    root = cache / "releases" / _RELEASE.replace(":", "-") / "composition-snapshots"
    root.mkdir(parents=True)
    path = root / f"{workload_id}.json"
    path.write_text(json.dumps(_snapshot_payload()), encoding="utf-8")
    return path


def test_load_sealed_clickhouse_snapshot_reopens_columns(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    _write_snapshot(cache)
    snapshot = load_sealed_clickhouse_snapshot(cache, _RELEASE, "a_native")
    assert snapshot["generation_ref"] == digest("generation record")
    assert snapshot["generation_uuid"] == _GENERATION_UUID
    assert [column.name for column in snapshot["columns"]] == ["id", "name"]
    assert snapshot["generation"] is None


def test_load_sealed_clickhouse_snapshot_rejects_missing_file(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    with pytest.raises(CompositionAdmissionError, match="clickhouse_source_payload"):
        load_sealed_clickhouse_snapshot(cache, _RELEASE, "a_native")


def test_clickhouse_http_endpoint_requires_literal_ip() -> None:
    resolved = SimpleNamespace(
        credentials=SimpleNamespace(
            host="127.0.0.1",
            port=8123,
            username="admin",
            password="secret",
            secure=False,
            additional_params={},
        )
    )
    endpoint, credentials, ca_file = clickhouse_http_endpoint(resolved)
    assert endpoint == "http://127.0.0.1:8123"
    assert credentials.username == "admin"
    assert ca_file is None
    resolved.credentials.host = "clickhouse.internal"
    with pytest.raises(CompositionAdmissionError, match="clickhouse_transport_endpoint"):
        clickhouse_http_endpoint(resolved)


def test_snapshot_limits_reuse_source_ceiling() -> None:
    limits = snapshot_limits_from_manifest({"sink": {"strategy": {"mode": "full_refresh", "max_source_bytes": 1000}}})
    assert limits.max_source_bytes == 1000
    assert limits.max_total_transient_bytes == 3000


def test_mssql_type_map_rejects_unknown_types() -> None:
    assert clickhouse_type_for_mssql("int", nullable=False, precision=None, scale=None) == "Int32"
    assert clickhouse_type_for_mssql("nvarchar", nullable=True, precision=None, scale=None) == "Nullable(String)"
    assert clickhouse_type_for_mssql("decimal", nullable=False, precision=18, scale=4) == "Decimal(18, 4)"
    with pytest.raises(CompositionAdmissionError, match="clickhouse_source_payload"):
        clickhouse_type_for_mssql("xml", nullable=False, precision=None, scale=None)


def test_compose_clickhouse_pack_dependencies_fail_closes_without_parent() -> None:
    assert (
        compose_clickhouse_pack_dependencies(parent={}, manifest={}, environment={}, plan=SimpleNamespace(writes=()))
        is None
    )
