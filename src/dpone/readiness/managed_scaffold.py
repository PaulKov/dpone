"""Connector scaffold and Studio bootstrap payload services."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import yaml

from dpone.contracts.credential_env import connection_env_name
from dpone.readiness.managed_models import InitBundleResult
from dpone.readiness.managed_templates import (
    _connector_doc_template,
    _connector_template,
    _connector_test_template,
    _sink_template,
    _source_template,
)
from dpone.readiness.managed_utils import _safe_identifier
from dpone.readiness.studio_ui_assets import (
    StudioUiAssetsStatus,
    probe_studio_ui_assets,
)


class ConnectorScaffoldService:
    """Generates self-service manifests and connector SDK scaffolds."""

    def generate_init_bundle(
        self,
        *,
        output_path: str | Path,
        source_type: str,
        sink_type: str,
        source_connection: str,
        sink_connection: str,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        strategy: str,
        unique_key: str | None = None,
    ) -> InitBundleResult:
        out = self._resolve_init_manifest_path(Path(output_path))
        out.parent.mkdir(parents=True, exist_ok=True)
        manifest = self._manifest_payload(
            source_type=source_type,
            sink_type=sink_type,
            source_connection=source_connection,
            sink_connection=sink_connection,
            source_schema=source_schema,
            source_table=source_table,
            target_schema=target_schema,
            target_table=target_table,
            strategy=strategy,
            unique_key=unique_key,
        )
        out.write_text(yaml.safe_dump(manifest, sort_keys=False, allow_unicode=True), encoding="utf-8")
        env_path = out.with_suffix(".env.example")
        env_path.write_text(self._env_example(source_connection, sink_connection), encoding="utf-8")
        smoke = f"dpone plan {out} --selector {source_schema}.{source_table} --format text"
        return InitBundleResult(manifest_path=out, env_example_path=env_path, smoke_command=smoke)

    def manifest_payload(
        self,
        *,
        source_type: str,
        sink_type: str,
        source_connection: str,
        sink_connection: str,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        strategy: str,
        unique_key: str | None = None,
    ) -> dict[str, Any]:
        """Build the canonical scaffold payload without writing project files."""

        return self._manifest_payload(
            source_type=source_type,
            sink_type=sink_type,
            source_connection=source_connection,
            sink_connection=sink_connection,
            source_schema=source_schema,
            source_table=source_table,
            target_schema=target_schema,
            target_table=target_table,
            strategy=strategy,
            unique_key=unique_key,
        )

    def _resolve_init_manifest_path(self, output_path: Path) -> Path:
        if output_path.exists() and output_path.is_dir():
            return output_path / "manifest.yaml"
        if str(output_path).endswith(("/", "\\")):
            return output_path / "manifest.yaml"
        return output_path

    def scaffold_connector(self, name: str, *, root: str | Path = ".") -> dict[str, Any]:
        normalized = _safe_identifier(name)
        base = Path(root)
        package = base / "src" / "dpone_ext" / normalized
        tests = base / "tests"
        docs = base / "docs"
        package.mkdir(parents=True, exist_ok=True)
        tests.mkdir(parents=True, exist_ok=True)
        docs.mkdir(parents=True, exist_ok=True)
        (package / "__init__.py").write_text(f'"""{normalized} community connector."""\n', encoding="utf-8")
        (package / "connector.py").write_text(_connector_template(normalized), encoding="utf-8")
        (package / "source.py").write_text(_source_template(normalized), encoding="utf-8")
        (package / "sink.py").write_text(_sink_template(normalized), encoding="utf-8")
        (tests / f"test_{normalized}_contracts.py").write_text(_connector_test_template(normalized), encoding="utf-8")
        (docs / f"{normalized.upper()}_CONNECTOR.md").write_text(_connector_doc_template(normalized), encoding="utf-8")
        return {
            "connector": normalized,
            "package_dir": str(package),
            "test_path": str(tests / f"test_{normalized}_contracts.py"),
            "docs_path": str(docs / f"{normalized.upper()}_CONNECTOR.md"),
        }

    def studio_payload(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 8765,
        ui_assets: StudioUiAssetsStatus | None = None,
        endpoints: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Return truthful compatibility metadata for the local API adapter."""

        if ui_assets is None:
            ui_assets = probe_studio_ui_assets()
        if endpoints is None:
            from dpone.readiness.studio_api_routes import studio_routes

            endpoints = tuple(route.path for route in studio_routes() if not route.deprecated)
        return {
            "url": f"http://{host}:{int(port)}",
            "auth": "local_only",
            "mode": "local_development_adapter",
            **ui_assets.to_dict(),
            "rbac": "not_implemented",
            "openapi": "/openapi.json",
            "endpoints": list(endpoints),
        }

    def _manifest_payload(self, **kwargs: Any) -> dict[str, Any]:
        source_type = str(kwargs["source_type"])
        sink_type = str(kwargs["sink_type"])
        source_schema = str(kwargs["source_schema"])
        source_table = str(kwargs["source_table"])
        target_schema = str(kwargs["target_schema"])
        target_table = str(kwargs["target_table"])
        strategy = str(kwargs["strategy"])
        unique_key = kwargs.get("unique_key")
        source: dict[str, Any] = {
            "type": source_type,
            "connection_id": kwargs["source_connection"],
            "table": {"schema": source_schema, "name": source_table},
            "options": {"batch_size": 50000},
        }
        sink: dict[str, Any] = {
            "type": sink_type,
            "connection_id": kwargs["sink_connection"],
            "table": {"schema": target_schema, "name": target_table},
            "strategy": {"mode": strategy},
            "options": {
                "schema_evolution": {
                    "enabled": True,
                    "mode": "widening",
                    "apply_safe": True,
                    "on_breaking": "fail",
                    "on_type_change": "fail",
                    "new_column_prefix": "__dpone__nc__",
                }
            },
        }
        effective_unique_key = _effective_scaffold_unique_key(strategy, unique_key)
        if effective_unique_key:
            sink["strategy"]["unique_key"] = effective_unique_key
        defaults: dict[str, Any] = {"source": source, "sink": sink}
        if sink_type == "mssql":
            defaults["state"] = _mssql_target_atomic_state()
        return {
            "kind": "dpone.batch.v1",
            "vars": {"layer": target_schema, "src_system": source_type, "src_database": "demo"},
            "naming": {
                "process_name": "{{ src_schema }}.{{ src_table }}",
                "task_group": "{{ src_system }}",
                "sink_dataset": target_schema,
                "sink_table": target_table,
                "description": "Generated dpone pipeline for {{ src_schema }}.{{ src_table }}",
            },
            "defaults": defaults,
            "quality": {
                "gates": [
                    {
                        "id": "target_min_rows",
                        "type": "min_rows",
                        "side": "target",
                        "threshold": 1,
                        "severity": "warning",
                    }
                ]
            },
            "observability": {"artifacts": {"enabled": True, "formats": ["json", "md", "html"]}},
            "performance": {"advisor": {"enabled": True}},
            "certification": {"connector_status": "experimental"},
            "schemas": {source_schema: {"tables": [{"table": source_table}]}},
        }

    def _env_example(self, source_connection: str, sink_connection: str) -> str:
        return (
            f"# dpone generated connection placeholders\n"
            f"{connection_env_name(source_connection, 'host')}=example.com\n"
            f"{connection_env_name(source_connection, 'username')}=etl\n"
            f"{connection_env_name(source_connection, 'password')}=change-me\n"
            f"{connection_env_name(sink_connection, 'host')}=example.com\n"
            f"{connection_env_name(sink_connection, 'username')}=etl\n"
            f"{connection_env_name(sink_connection, 'password')}=change-me\n"
        )


def _mssql_target_atomic_state() -> dict[str, Any]:
    """Return an executable generic MSSQL governance scaffold without new credentials."""

    return {
        "type": "mssql",
        "reuse": "sink",
        "atomicity": "target_atomic",
        "provisioning": "external",
        "table": {"name": "dpone_load_attempt"},
    }


def _effective_scaffold_unique_key(strategy: str, unique_key: object) -> str | None:
    """Keep the minimal Studio request executable for key-based strategies."""

    authored = str(unique_key or "").strip()
    if authored:
        return authored
    if strategy.strip().lower() in {"incremental_merge", "snapshot_diff", "scd2", "cdc_apply"}:
        return "id"
    return None


__all__ = ["ConnectorScaffoldService"]
