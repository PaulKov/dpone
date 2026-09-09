"""Unit coverage for isolated XMin → MSSQL live-matrix route construction."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

from tests.integration.postgres import postgres_xmin_mssql_snapshot_live_matrix_support as support


@dataclass(frozen=True)
class _LoadConfig:
    """Minimal replaceable load configuration used by the route-fork contract."""

    options: dict[str, str]


class _Connector:
    """Closable connector double sufficient for route-fork lifecycle coverage."""

    def close(self) -> None:
        """Mirror the connector cleanup protocol."""


def test_fork_snapshot_route_exposes_its_checkpoint_storage(monkeypatch, tmp_path) -> None:
    """A fork keeps the checkpoint object used by its source and sink available."""

    postgres = _Connector()
    target = _Connector()
    state = _Connector()
    checkpoint_storage = object()
    route = SimpleNamespace(
        target=target,
        state=state,
        master=_Connector(),
        load_config=_LoadConfig(options={}),
        source_schema="source",
        target_database="target",
        state_database="state",
    )

    monkeypatch.setattr(support, "postgres_connector", lambda: postgres)
    monkeypatch.setattr(
        support, "_mssql_connector", lambda *_args, **_kwargs: target if _args[0] is route.target else state
    )
    monkeypatch.setattr(support, "MSSQLXMinStateStorage", lambda *_args, **_kwargs: checkpoint_storage)
    monkeypatch.setattr(support, "MSSQLSink", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(support, "PostgresSource", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(support, "MSSQLRunStateStorage", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(support, "MSSQLLoadAuditStorage", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(support, "LoadIdentityService", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(support, "ETLProcessor", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(support, "bind_factual_mssql_database_authority", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(support, "bind_factual_postgres_source_authority", lambda *_args, **_kwargs: None)

    with support.fork_snapshot_route(route, tmp_path) as forked:
        assert forked.checkpoint_storage is checkpoint_storage
