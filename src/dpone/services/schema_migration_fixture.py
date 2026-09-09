"""File-IO facade for schema migration rehearsal fixture commands."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml

from dpone.readiness.schema_migration_data_profile import MigrationDataProfileAnalyzer
from dpone.readiness.schema_migration_fixture import (
    ArtifactSampleFixtureProvider,
    MigrationFixtureBuilder,
    MigrationFixturePlanner,
    SyntheticMigrationFixtureProvider,
)


class MigrationRehearsalDataFacade:
    """Application facade for fixture plan, build and profile artifacts."""

    def plan(
        self,
        *,
        pack_path: str,
        manifest_path: str,
        policy_path: str | None = None,
    ) -> dict[str, Any]:
        return MigrationFixturePlanner().plan(
            pack=_read_mapping(pack_path),
            manifest=_read_mapping(manifest_path),
            policy=_read_mapping(policy_path) if policy_path else {},
        )

    def build(
        self,
        *,
        plan_path: str,
        target_connection_path: str,
        execute: bool = False,
    ) -> dict[str, Any]:
        plan = _read_mapping(plan_path)
        rows = _provider_rows(plan)
        seeder = _target_seeder(_read_mapping(target_connection_path)) if execute else None
        return MigrationFixtureBuilder().build(plan=plan, rows=rows, execute=execute, seeder=seeder)

    def profile(
        self,
        *,
        fixture_build_path: str,
        target_connection_path: str,
        stage: str,
    ) -> dict[str, Any]:
        fixture_build = _read_mapping(fixture_build_path)
        rows = _profile_rows(fixture_build=fixture_build, connection=_read_mapping(target_connection_path))
        return MigrationDataProfileAnalyzer().profile(
            rows=rows,
            pack_id=str(fixture_build.get("pack_id", "")),
            fixture_build_id=str(fixture_build.get("fixture_build_id", "")),
            stage=stage,
            key_columns=_strings(fixture_build.get("key_columns", [])),
            hierarchy=_mapping(fixture_build.get("hierarchy")),
            checks=_mapping(fixture_build.get("checks")),
        )


class _ClickHouseFixtureSeeder:
    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def seed(self, *, target: Mapping[str, Any], columns: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> None:
        if not rows:
            return
        table = _clickhouse_table(target=target, database=getattr(self._connector, "database", "default"))
        quoted_columns = ", ".join(f"`{column}`" for column in columns)
        data = [tuple(row.get(column) for column in columns) for row in rows]
        self._connector.connection.execute(f"INSERT INTO {table} ({quoted_columns}) VALUES", data)


class _ClickHouseRowsReader:
    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def read(self, *, target: Mapping[str, Any], columns: Sequence[str]) -> list[dict[str, Any]]:
        if not columns:
            return []
        table = _clickhouse_table(target=target, database=getattr(self._connector, "database", "default"))
        quoted_columns = ", ".join(f"`{column}`" for column in columns)
        return self._connector.get_records(f"SELECT {quoted_columns} FROM {table}", as_dict=True)


def _provider_rows(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    mode = str(plan.get("mode", "none"))
    if mode == "synthetic":
        return SyntheticMigrationFixtureProvider().rows(plan)
    if mode in {"artifact_sample", "masked_sample"}:
        return ArtifactSampleFixtureProvider().rows(plan)
    return []


def _target_seeder(connection: Mapping[str, Any]) -> _ClickHouseFixtureSeeder | None:
    if _connection_type(connection) != "clickhouse":
        return None
    return _ClickHouseFixtureSeeder(_clickhouse_connector(connection))


def _profile_rows(*, fixture_build: Mapping[str, Any], connection: Mapping[str, Any]) -> list[dict[str, Any]]:
    embedded = fixture_build.get("rows", [])
    if isinstance(embedded, list) and embedded:
        return [dict(row) for row in embedded if isinstance(row, Mapping)]
    if fixture_build.get("status") != "built" or _connection_type(connection) != "clickhouse":
        return []
    return _ClickHouseRowsReader(_clickhouse_connector(connection)).read(
        target=_mapping(fixture_build.get("target")),
        columns=_strings(fixture_build.get("columns", [])),
    )


def _clickhouse_connector(connection: Mapping[str, Any]) -> Any:
    module = import_module("dpone.runtime.connectors.clickhouse")
    connector_cls = module.ClickHouseConnector
    return connector_cls(
        host=str(connection.get("host", "127.0.0.1")),
        port=int(connection.get("port", 9000)),
        database=str(connection.get("database", "default")),
        user=str(connection.get("user", connection.get("username", "default"))),
        password=str(connection.get("password", "")),
        secure=bool(connection.get("secure", False)),
        compression=bool(connection.get("compression", True)),
    )


def _clickhouse_table(*, target: Mapping[str, Any], database: str) -> str:
    raw = str(target.get("table", "")).strip()
    if "." in raw:
        schema, table = raw.rsplit(".", 1)
    else:
        schema, table = database, raw
    return f"`{schema}`.`{table}`"


def _connection_type(connection: Mapping[str, Any]) -> str:
    return str(connection.get("type", connection.get("sink_type", ""))).strip().lower()


def _read_mapping(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    raw_text = Path(path).read_text(encoding="utf-8")
    raw = json.loads(raw_text) if str(path).lower().endswith(".json") else yaml.safe_load(raw_text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _strings(raw: object) -> tuple[str, ...]:
    return tuple(str(item) for item in raw if str(item)) if isinstance(raw, list | tuple) else ()


__all__ = ["MigrationRehearsalDataFacade"]
