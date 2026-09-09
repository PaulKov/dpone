"""Sink connector resolver for read-only backfill diagnostics."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class BackfillDiagnosticConnectorResolver:
    """Resolve the sink connector needed to read audit-schema backfill state."""

    def __init__(self, *, sink_factory: Any | None = None) -> None:
        self._sink_factory = sink_factory

    def resolve(self, load_config: Any, backfill_options: Mapping[str, Any]) -> Any | None:
        if _state_backend(backfill_options) != "audit_schema":
            return None
        sink_type = _sink_type(load_config)
        if not sink_type:
            raise ValueError("backfill.state.backend=audit_schema requires sink.type in manifest options")
        connection_id = str(getattr(load_config, "target_conn_id", "") or "")
        if not connection_id:
            raise ValueError("backfill.state.backend=audit_schema requires sink.connection_id")
        sink = self._factory().create(
            connection_id=connection_id,
            connection_type=sink_type,
            credentials_source=_credentials_source(load_config),
            mount_point=_sink_option(load_config, "vault_mount_point"),
            path=_sink_option(load_config, "vault_path"),
            state_storage=None,
            proxy_enable=bool(_sink_option(load_config, "proxy_enable", False)),
        )
        return getattr(sink, "connector", sink)

    def _factory(self) -> Any:
        if self._sink_factory is not None:
            return self._sink_factory
        from dpone.runtime.credentials.factory import SinkFactory

        return SinkFactory


def _state_backend(backfill_options: Mapping[str, Any]) -> str:
    state = backfill_options.get("state") if isinstance(backfill_options.get("state"), Mapping) else {}
    return str((state or {}).get("backend") or "local_file")


def _sink_type(load_config: Any) -> str:
    options = getattr(load_config, "options", None)
    if not isinstance(options, Mapping):
        return ""
    return str(options.get("sink_type") or "").strip().lower()


def _credentials_source(load_config: Any) -> str:
    return str(_sink_option(load_config, "connection_type", "airflow") or "airflow")


def _sink_option(load_config: Any, key: str, default: Any = "") -> Any:
    options = getattr(load_config, "options", None)
    sink_options = options.get("sink_options") if isinstance(options, Mapping) else None
    return sink_options.get(key, default) if isinstance(sink_options, Mapping) else default


__all__ = ["BackfillDiagnosticConnectorResolver"]
