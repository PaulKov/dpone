"""Application services for readiness-oriented CLI commands."""

from __future__ import annotations

import json
from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from typing import Any, Literal, cast

from dpone.readiness.capability_discovery_protocols import CapabilitySnapshotProtocol
from dpone.readiness.capability_legacy_projection import (
    legacy_certification_markdown,
    legacy_certification_payload,
)
from dpone.readiness.cdc import (
    CDCBackend,
    CDCConfig,
    build_mssql_cdc_enable_sql,
    build_mssql_change_tracking_enable_sql,
    build_postgres_slot_sql,
)
from dpone.readiness.ddl_governance import DdlGovernancePolicy, OnlineSchemaPlanner
from dpone.readiness.doctor import DoctorService
from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.physical_design_models import PhysicalReconciliationOptions
from dpone.readiness.physical_reconciliation import PhysicalDesignReconciler
from dpone.readiness.physical_state import PhysicalTableState
from dpone.readiness.schema_contracts import SchemaContract
from dpone.readiness.schema_evolution import ColumnDef, SchemaComparator, SchemaEvolutionPolicy
from dpone.readiness.schema_identity import (
    SCHEMA_IDENTITY_PLAN_SCHEMA,
    SchemaIdentityOptions,
    SchemaIdentityResolver,
)
from dpone.services.readiness_io import load_actual_physical, load_manifest, load_rows
from dpone.strategy_intelligence.native_transfer import NativeTransferPlanBuilder, NativeTransferRequest
from dpone.type_system import TypeInferenceOptions, TypeInferenceService


class ReadinessService:
    def __init__(
        self,
        *,
        capability_snapshot: CapabilitySnapshotProtocol | None = None,
        capability_snapshot_factory: Callable[[], CapabilitySnapshotProtocol] | None = None,
    ) -> None:
        self._capability_snapshot = capability_snapshot
        self._capability_snapshot_factory = capability_snapshot_factory

    def doctor(self, *, profile: str = "local") -> dict[str, Any]:
        return DoctorService().run(profile=profile)

    def certification(self) -> tuple[int, dict[str, Any], str]:
        snapshot = self._capability_snapshot
        if snapshot is None:
            if self._capability_snapshot_factory is None:
                raise RuntimeError(
                    "ReadinessService.certification() requires an injected capability snapshot provider."
                )
            snapshot = self._capability_snapshot_factory()
        payload = legacy_certification_payload(snapshot)
        return (
            0 if payload["passed"] else 1,
            payload,
            legacy_certification_markdown(payload),
        )

    def schema_plan(
        self,
        *,
        source_path: str,
        target_path: str,
        table: str,
        dialect: str,
        mode: str,
        allow_drop: bool,
        on_type_change: str = "fail",
        new_column_prefix: str = "__dpone__nc__",
        ddl_mode: str = "online",
        lock_timeout_seconds: int | None = None,
        statement_timeout_seconds: int | None = None,
        max_table_size_for_inline_ddl: int | None = None,
    ) -> tuple[dict[str, Any], list[str], bool]:
        mode_value = cast(Literal["strict", "additive", "widening"], mode)
        type_change_value = cast(Literal["fail", "new_column"], on_type_change)
        plan = SchemaComparator(
            SchemaEvolutionPolicy(
                mode=mode_value,
                allow_drop=allow_drop,
                on_type_change=type_change_value,
                new_column_prefix=new_column_prefix,
            )
        ).compare(
            self._load_columns(source_path),
            self._load_columns(target_path),
        )
        ddl = plan.ddl_sql(dialect, table)
        payload = plan.to_dict()
        payload["ddl"] = ddl
        payload["online_schema_evolution"] = (
            OnlineSchemaPlanner()
            .plan(
                schema_plan=plan,
                dialect=dialect,
                table=table,
                policy=DdlGovernancePolicy.from_schema_evolution_options(
                    {
                        "ddl_mode": ddl_mode,
                        "lock_timeout_seconds": lock_timeout_seconds,
                        "statement_timeout_seconds": statement_timeout_seconds,
                        "max_table_size_for_inline_ddl": max_table_size_for_inline_ddl,
                    }
                ),
            )
            .to_dict()
        )
        return payload, ddl, plan.has_breaking_changes

    def schema_identity_plan(
        self,
        *,
        manifest_path: str,
        source_path: str | None = None,
        actual_path: str | None = None,
    ) -> dict[str, Any]:
        manifest = load_manifest(manifest_path)
        options = SchemaIdentityOptions.from_config(self._sink_options(manifest).get("schema_identity", {}))
        result = SchemaIdentityResolver(options).resolve(
            source=self._load_identity_source_columns(source_path, manifest),
            target=self._load_identity_actual_columns(actual_path),
        )
        payload = result.to_dict()
        payload.update(
            {
                "schema_version": SCHEMA_IDENTITY_PLAN_SCHEMA,
                "command": "identity_plan",
                "status": "blocked" if payload["blockers"] else "planned",
                "table": self._sink_table(self._sink_config(manifest)),
                "options": options.to_dict(),
            }
        )
        return payload

    def schema_infer(
        self,
        *,
        manifest_path: str | None = None,
        rows_path: str | None = None,
        source_path: str | None = None,
    ) -> dict[str, Any]:
        manifest = load_manifest(manifest_path)
        rows = load_rows(rows_path)
        source_schema = self._load_source_schema(source_path, manifest)
        contract = SchemaContract.from_config(self._schema_contract_config(manifest))
        options = TypeInferenceOptions.from_config(self._sink_options(manifest).get("type_inference", {}))
        return (
            TypeInferenceService()
            .infer(rows=rows, source_schema=source_schema, schema_contract=contract, options=options)
            .to_dict()
        )

    def physical_plan(
        self,
        *,
        manifest_path: str | None = None,
        source_path: str | None = None,
        table: str | None = None,
        sink_type: str | None = None,
    ) -> dict[str, Any]:
        manifest = load_manifest(manifest_path)
        sink = self._sink_config(manifest)
        plan = self._physical_design_plan(
            manifest=manifest,
            source_path=source_path,
            table=table or self._sink_table(sink),
            sink_type=sink_type or str(sink.get("type", "postgres")),
        )
        return plan.to_dict()

    def physical_diff(
        self,
        *,
        actual_path: str,
        manifest_path: str | None = None,
        source_path: str | None = None,
        table: str | None = None,
        sink_type: str | None = None,
        reconciliation_mode: str | None = None,
    ) -> dict[str, Any]:
        manifest = load_manifest(manifest_path)
        sink = self._sink_config(manifest)
        resolved_sink = sink_type or str(sink.get("type", "postgres"))
        plan = self._physical_design_plan(
            manifest=manifest,
            source_path=source_path,
            table=table or self._sink_table(sink),
            sink_type=resolved_sink,
        )
        actual = PhysicalTableState.from_mapping(load_actual_physical(actual_path))
        reconciliation_options = plan.options.reconciliation
        if reconciliation_mode:
            reconciliation_options = PhysicalReconciliationOptions(mode=reconciliation_mode)  # type: ignore[arg-type]
        result = PhysicalDesignReconciler().reconcile(
            desired=PhysicalTableState.from_physical_plan(plan),
            actual=actual,
            options=reconciliation_options,
            dialect=_migration_dialect(plan.sink_type),
        )
        payload = result.to_dict()
        payload["desired"] = PhysicalTableState.from_physical_plan(plan).to_dict()
        payload["actual"] = actual.to_dict()
        return payload

    def type_fidelity_plan(self, *, manifest_path: str | None = None) -> dict[str, Any]:
        manifest = load_manifest(manifest_path)
        source = self._source_config(manifest)
        sink = self._sink_config(manifest)
        source_options = dict(source.get("options", {})) if isinstance(source.get("options"), dict) else {}
        sink_options = self._sink_options(manifest)
        return (
            NativeTransferPlanBuilder()
            .build(
                NativeTransferRequest(
                    source_type=str(source.get("type", "unknown")),
                    sink_type=str(sink.get("type", "unknown")),
                    source_table=self._sink_table(source),
                    target_table=self._sink_table(sink),
                    strategy=str((sink.get("strategy") or {}).get("mode", "full_refresh"))
                    if isinstance(sink.get("strategy"), dict)
                    else "full_refresh",
                    source_options=source_options,
                    sink_options=sink_options,
                )
            )
            .type_fidelity
        )

    def cdc_plan(
        self,
        *,
        backend: str,
        schema: str,
        table: str,
        slot_name: str | None,
        publication_name: str | None,
        capture_instance: str | None,
    ) -> dict[str, Any]:
        config = CDCConfig(
            backend=CDCBackend(backend),
            source_schema=schema,
            source_table=table,
            slot_name=slot_name,
            publication_name=publication_name,
            capture_instance=capture_instance,
        )
        errors = config.validate()
        sql = "" if errors else self._cdc_sql(config)
        return {"valid": not errors, "errors": errors, "sql": sql}

    def _load_columns(self, path: str) -> list[ColumnDef]:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        return [ColumnDef(str(item["name"]), str(item["dtype"]), bool(item.get("nullable", True))) for item in raw]

    def _load_source_schema(self, source_path: str | None, manifest: dict[str, Any]) -> list[tuple[str, str]]:
        if source_path:
            raw = json.loads(Path(source_path).read_text(encoding="utf-8"))
            return [(str(item["name"]), str(item.get("dtype", item.get("type", "string")))) for item in raw]
        options = self._source_config(manifest).get("options", {})
        columns = options.get("columns", []) if isinstance(options, dict) else []
        if isinstance(columns, dict):
            return [(str(name), str(dtype)) for name, dtype in columns.items()]
        if isinstance(columns, list):
            return [
                (str(item["name"]), str(item.get("type", item.get("dtype", "string"))))
                for item in columns
                if isinstance(item, dict)
            ]
        return []

    def _load_identity_source_columns(self, source_path: str | None, manifest: dict[str, Any]) -> list[ColumnDef]:
        if source_path:
            raw = json.loads(Path(source_path).read_text(encoding="utf-8"))
            return [
                ColumnDef(
                    str(item["name"]),
                    str(item.get("dtype", item.get("type", "string"))),
                    bool(item.get("nullable", True)),
                )
                for item in raw
                if isinstance(item, dict)
            ]
        return [ColumnDef(name, dtype) for name, dtype in self._load_source_schema(None, manifest)]

    def _load_identity_actual_columns(self, actual_path: str | None) -> list[ColumnDef]:
        if not actual_path:
            return []
        actual = load_actual_physical(actual_path)
        raw_columns = actual.get("columns", {})
        if isinstance(raw_columns, dict):
            return [ColumnDef(str(name), _column_type_from_actual(raw)) for name, raw in raw_columns.items()]
        if isinstance(raw_columns, list):
            return [
                ColumnDef(
                    str(item.get("name", "")),
                    str(item.get("type", item.get("dtype", "string"))),
                    bool(item.get("nullable", True)),
                )
                for item in raw_columns
                if isinstance(item, dict) and item.get("name")
            ]
        return []

    def _physical_design_plan(
        self,
        *,
        manifest: dict[str, Any],
        source_path: str | None,
        table: str,
        sink_type: str,
    ) -> Any:
        sink_options = self._sink_options(manifest)
        return PhysicalDesignPlanner().plan(
            sink_type=sink_type,
            table=table,
            source_schema=self._load_source_schema(source_path, manifest),
            schema_contract=SchemaContract.from_config(self._schema_contract_config(manifest)),
            options=PhysicalDesignOptions.from_config(sink_options.get("physical_design", {})),
            type_fidelity=self._type_fidelity_config(manifest),
        )

    def _schema_contract_config(self, manifest: dict[str, Any]) -> dict[str, Any]:
        sink_options = self._sink_options(manifest)
        raw = sink_options.get("schema_contract", manifest.get("schema_contract", {}))
        return dict(raw) if isinstance(raw, dict) else {}

    def _source_config(self, manifest: dict[str, Any]) -> dict[str, Any]:
        if isinstance(manifest.get("source"), dict):
            return dict(manifest["source"])
        defaults = manifest.get("defaults", {})
        if isinstance(defaults, dict) and isinstance(defaults.get("source"), dict):
            return dict(defaults["source"])
        return {}

    def _sink_config(self, manifest: dict[str, Any]) -> dict[str, Any]:
        if isinstance(manifest.get("sink"), dict):
            return dict(manifest["sink"])
        defaults = manifest.get("defaults", {})
        if isinstance(defaults, dict) and isinstance(defaults.get("sink"), dict):
            return dict(defaults["sink"])
        return {}

    def _sink_options(self, manifest: dict[str, Any]) -> dict[str, Any]:
        options = self._sink_config(manifest).get("options", {})
        return dict(options) if isinstance(options, dict) else {}

    def _type_fidelity_config(self, manifest: dict[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        source_options = self._source_config(manifest).get("options", {})
        if isinstance(source_options, dict) and isinstance(source_options.get("type_fidelity"), dict):
            merged.update(source_options["type_fidelity"])
        sink_options = self._sink_options(manifest)
        if isinstance(sink_options.get("type_fidelity"), dict):
            merged.update(sink_options["type_fidelity"])
        return merged

    def _sink_table(self, sink: dict[str, Any]) -> str:
        table = sink.get("table", {})
        if isinstance(table, dict):
            return f"{table.get('schema', 'landing')}.{table.get('name', 'table')}"
        return "landing.table"

    def _cdc_sql(self, config: CDCConfig) -> str:
        if config.backend == CDCBackend.POSTGRES_LOGICAL:
            return build_postgres_slot_sql(config)
        if config.backend == CDCBackend.MSSQL_CDC:
            return build_mssql_cdc_enable_sql(config)
        if config.backend == CDCBackend.MSSQL_CHANGE_TRACKING:
            return build_mssql_change_tracking_enable_sql(config)
        return ""


def _migration_dialect(sink_type: str) -> Any:
    normalized = str(sink_type).lower()
    if normalized == "clickhouse":
        module = import_module("dpone.runtime.sinks.clickhouse_physical_reconciliation")
        return module.ClickHousePhysicalMigrationDialect()
    raise ValueError(f"physical-diff is implemented for clickhouse in v1, got {sink_type}")


def _column_type_from_actual(raw: Any) -> str:
    if isinstance(raw, dict):
        return str(raw.get("type", raw.get("dtype", "string")))
    return str(raw)
