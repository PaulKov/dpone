"""BigQuery storage construction helpers for StateFactory."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def create_bigquery_run_state_storage(
    *,
    bigquery_connector: Any,
    connector_factory: Callable[..., Any],
    connection_id: str | None,
    credentials_source: str,
    state_table: str,
    schema: str,
    mount_point: str | None,
    path: str | None,
    proxy_enable: bool,
) -> Any:
    from dpone.runtime.state.run_state import RunStateStorage

    connector = bigquery_connector or connector_factory(
        connection_id=connection_id,
        credentials_source=credentials_source,
        mount_point=mount_point,
        path=path,
        proxy_enable=proxy_enable,
    )
    return RunStateStorage(bigquery_connector=connector, dataset=schema, table=state_table)


def create_bigquery_xmin_state_storage(
    *,
    bigquery_connector: Any,
    connector_factory: Callable[..., Any],
    connection_id: str | None,
    credentials_source: str,
    state_table: str,
    schema: str,
    mount_point: str | None,
    path: str | None,
    proxy_enable: bool,
) -> Any:
    from dpone.runtime.state.xmin_storage import XMinStateStorage

    connector = bigquery_connector or connector_factory(
        connection_id=connection_id,
        credentials_source=credentials_source,
        mount_point=mount_point,
        path=path,
        proxy_enable=proxy_enable,
    )
    return XMinStateStorage(bigquery_connector=connector, dataset=schema, table=state_table)


__all__ = ["create_bigquery_run_state_storage", "create_bigquery_xmin_state_storage"]
