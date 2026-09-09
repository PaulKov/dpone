from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.manifest.loader import ManifestLoaderRouter
from dpone.readiness import native_transfer_route_guard as route_guard_module
from dpone.readiness.native_transfer_route_guard import NativeTransferRouteRunGuard
from dpone.services.manifest import ManifestCommandContext
from dpone.services.run_manifest import RunManifestService

_MSSQL_ALIASES = (
    "mssql",
    "MSSQL",
    "microsoft mssql",
    "microsoft_mssql",
    "odbc",
    "sqlserver",
    "sql_server",
    "sql-server",
)


@pytest.mark.parametrize("source_type", ("postgres", "postgresql", "PostgreSQL"))
@pytest.mark.parametrize("sink_type", _MSSQL_ALIASES)
def test_route_guard_canonicalizes_endpoint_aliases_before_planning(
    monkeypatch: pytest.MonkeyPatch,
    source_type: str,
    sink_type: str,
) -> None:
    captured: dict[str, Any] = {}

    class _Planner:
        def plan(self, **kwargs: Any) -> SimpleNamespace:
            captured.update(kwargs)
            return SimpleNamespace(blockers=())

    monkeypatch.setattr(route_guard_module, "NativeTransferRoutePlanner", _Planner)

    blockers = NativeTransferRouteRunGuard().blockers(
        raw_config={
            "source": {"type": source_type, "options": {}},
            "sink": {"type": sink_type, "strategy": {"mode": "full_refresh"}, "options": {}},
        },
        manifest_dir=Path("."),
    )

    assert blockers == ()
    assert captured["source_type"] == "postgres"
    assert captured["sink_type"] == "mssql"


def test_run_manifest_blocks_certified_only_route_before_process_io(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        """
name: orders
source:
  type: postgres
  connection_id: postgres
  table:
    schema: public
    name: orders
  options:
    extract_mode: copy_to_stdout
    native_transfer:
      execution:
        certification:
          mode: certified_only
          artifact: .dpone/certification/native_transfer_route_certification.json
sink:
  type: clickhouse
  connection_id: clickhouse
  table:
    schema: analytics
    name: orders
  strategy:
    mode: full_refresh
  options:
    clickhouse_bulk:
      mode: http
""".strip()
        + "\n",
        encoding="utf-8",
    )

    def fail_factory(**_kwargs):
        raise AssertionError("process factory must not run when route certification blocks")

    result = RunManifestService(process_factory=fail_factory).run(
        path=manifest,
        manifest_ctx=ManifestCommandContext(registry_paths=tuple(), loader=ManifestLoaderRouter()),
    )

    assert result.passed is False
    assert result.result.status == "error"
    assert "native_transfer_route_certification.missing" in result.result.errors
