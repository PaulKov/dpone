from __future__ import annotations

from types import SimpleNamespace

from dpone.backfill.diagnostic_connector import BackfillDiagnosticConnectorResolver
from dpone.config.load_config import LoadConfig


def test_backfill_diagnostic_connector_skips_local_file_state() -> None:
    resolver = BackfillDiagnosticConnectorResolver(sink_factory=_SinkFactory())

    assert resolver.resolve(_load_config(), {"state": {"backend": "local_file"}}) is None


def test_backfill_diagnostic_connector_builds_sink_connector_for_audit_schema() -> None:
    sink_factory = _SinkFactory()
    resolver = BackfillDiagnosticConnectorResolver(sink_factory=sink_factory)

    connector = resolver.resolve(_load_config(), {"state": {"backend": "audit_schema"}})

    assert connector == "connector:clickhouse_dwh"
    assert sink_factory.calls == [
        {
            "connection_id": "clickhouse_dwh",
            "connection_type": "clickhouse",
            "credentials_source": "env",
            "mount_point": "",
            "path": "",
            "proxy_enable": False,
            "state_storage": None,
        }
    ]


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql_oltp",
        target_conn_id="clickhouse_dwh",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        options={
            "sink_type": "clickhouse",
            "sink_options": {"connection_type": "env"},
        },
    )


class _SinkFactory:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(connector=f"connector:{kwargs['connection_id']}")
