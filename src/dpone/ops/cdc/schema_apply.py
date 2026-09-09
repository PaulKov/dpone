"""CDC schema evolution apply service."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, Literal, cast

from dpone.ops.cdc.schema_apply_clickhouse import ClickHouseCdcSchemaDdlPlanner
from dpone.ops.cdc.schema_apply_models import (
    CdcSchemaApplyPolicy,
    CdcSchemaApplyReport,
    CdcSchemaApplyResult,
    SchemaApplyMode,
    load_schema_apply_change,
)

ConnectorFactory = Callable[[str, Any, str | None, str | None], Any]
SchemaApplySink = Literal["clickhouse"]


class CdcSchemaEvolutionApplyService:
    """Plan and optionally apply target DDL for CDC schema evolution."""

    def __init__(
        self,
        *,
        connector: Any | None = None,
        connector_factory: ConnectorFactory | None = None,
        ddl_planner: ClickHouseCdcSchemaDdlPlanner | None = None,
        typed_materializer: Any | None = None,
    ) -> None:
        self._connector = connector
        self._connector_factory = connector_factory or _create_clickhouse_connector
        self._ddl_planner = ddl_planner or ClickHouseCdcSchemaDdlPlanner()
        self._typed_materializer = typed_materializer

    def apply(
        self,
        *,
        output_dir: str | Path,
        schema_change_json: str | Path,
        sink: str,
        target_dataset: str,
        mode: str = "dry_run",
        sink_connection_id: str | None = None,
        credentials_source: str = "env",
        credentials_mount_point: str | None = None,
        credentials_path: str | None = None,
        cdc_dataset: str | None = None,
        unique_key: Sequence[str] = tuple(),
        columns: Sequence[str] = tuple(),
        typed_refresh: bool = False,
        require_approval: bool = True,
        fail_on_parse_errors: bool = False,
        max_parse_error_ratio: float = 0.0,
        quarantine_dataset: str | None = None,
        schema_drift_mode: str = "warn_additive",
        quality_sample_limit: int = 100,
    ) -> CdcSchemaApplyReport:
        directory = Path(output_dir)
        policy = CdcSchemaApplyPolicy(mode=_mode(mode), require_approval=require_approval)
        change = load_schema_apply_change(schema_change_json)
        if sink != "clickhouse":
            plan = self._ddl_planner.plan(change=change, target_dataset=target_dataset, policy=policy)
            plan = _blocked_plan(plan, "cdc_schema_apply.unsupported_sink")
        else:
            plan = self._ddl_planner.plan(change=change, target_dataset=target_dataset, policy=policy)
        connector = self._connector
        should_close = False
        blockers: list[str] = list(plan.blockers)
        warnings: list[str] = list(plan.warnings)
        applied = False
        ddl_executed = False
        backfill_executed = False
        typed_payload: dict[str, object] = {"ran": False}
        try:
            if policy.mode == "apply" and plan.passed:
                connector = connector or self._make_connector(
                    sink_connection_id=sink_connection_id,
                    credentials_source=credentials_source,
                    credentials_mount_point=credentials_mount_point,
                    credentials_path=credentials_path,
                )
                should_close = self._connector is None
                connector.execute_query(plan.ddl)
                ddl_executed = True
                if policy.apply_backfill and plan.backfill_sql:
                    connector.execute_query(plan.backfill_sql)
                    backfill_executed = True
                applied = True
                if typed_refresh:
                    typed_payload = self._run_typed_refresh(
                        connector=connector,
                        output_dir=directory / "typed_refresh",
                        cdc_dataset=cdc_dataset,
                        target_dataset=target_dataset,
                        unique_key=unique_key,
                        columns=columns,
                        fail_on_parse_errors=fail_on_parse_errors,
                        max_parse_error_ratio=max_parse_error_ratio,
                        quarantine_dataset=quarantine_dataset,
                        schema_drift_mode=schema_drift_mode,
                        quality_sample_limit=quality_sample_limit,
                    )
                    if not bool(typed_payload.get("passed", False)):
                        blockers.append("cdc_schema_apply.typed_refresh_failed")
            result = CdcSchemaApplyResult(
                mode=policy.mode,
                applied=applied,
                ddl_executed=ddl_executed,
                backfill_executed=backfill_executed,
                typed_refresh=typed_payload,
                blockers=tuple(dict.fromkeys(blockers)),
                warnings=tuple(dict.fromkeys(warnings)),
            )
        finally:
            if should_close and connector is not None:
                _close(connector)
        report = CdcSchemaApplyReport(
            plan=plan,
            result=result,
            policy=policy,
            output_dir=str(directory),
            plan_json_path=str(directory / "cdc_schema_apply_plan.json"),
            result_json_path=str(directory / "cdc_schema_apply_result.json"),
            markdown_path=str(directory / "cdc_schema_apply.md"),
        )
        report.write()
        return report

    def _make_connector(
        self,
        *,
        sink_connection_id: str | None,
        credentials_source: str,
        credentials_mount_point: str | None,
        credentials_path: str | None,
    ) -> Any:
        if not sink_connection_id:
            raise ValueError("cdc-schema-apply --mode apply requires --sink-connection-id")
        return self._connector_factory(
            sink_connection_id,
            _credentials_source(credentials_source),
            credentials_mount_point,
            credentials_path,
        )

    def _run_typed_refresh(
        self,
        *,
        connector: Any,
        output_dir: Path,
        cdc_dataset: str | None,
        target_dataset: str,
        unique_key: Sequence[str],
        columns: Sequence[str],
        fail_on_parse_errors: bool,
        max_parse_error_ratio: float,
        quarantine_dataset: str | None,
        schema_drift_mode: str,
        quality_sample_limit: int,
    ) -> dict[str, object]:
        if not cdc_dataset or not unique_key or not columns:
            return {
                "ran": False,
                "passed": False,
                "blockers": ["cdc_schema_apply.typed_refresh_missing_inputs"],
            }
        materializer = self._typed_materializer
        if materializer is None:
            materializer = _RuntimeTypedMaterializer(connector)
        report = materializer.materialize(
            output_dir=output_dir,
            cdc_dataset=cdc_dataset,
            target_dataset=target_dataset,
            unique_key=tuple(unique_key),
            columns=tuple(columns),
            sink_connection_id="__injected__",
            credentials_source="env",
            delete_mode="exclude_deleted",
            fail_on_parse_errors=fail_on_parse_errors,
            max_parse_error_ratio=max_parse_error_ratio,
            quarantine_dataset=quarantine_dataset,
            schema_drift_mode=schema_drift_mode,
            quality_sample_limit=quality_sample_limit,
        )
        return {
            "ran": True,
            "passed": bool(getattr(report, "passed", False)),
            "blockers": list(getattr(report, "blockers", tuple())),
            "json_path": str(getattr(report, "json_path", output_dir / "cdc_typed_materialization.json")),
            "markdown_path": str(getattr(report, "markdown_path", output_dir / "cdc_typed_materialization.md")),
        }


class _RuntimeTypedMaterializer:
    """Adapter that reuses an already-open ClickHouse connector for typed refresh."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def materialize(self, **kwargs: object) -> Any:
        from dpone.runtime.cdc.typed_materialization import (
            ClickHouseCdcTypedColumn,
            ClickHouseCdcTypedMaterializationPlan,
            ClickHouseCdcTypedMaterializationPolicy,
            ClickHouseCdcTypedMaterializationService,
            ClickHouseCdcTypedQualityPolicy,
        )

        columns = tuple(ClickHouseCdcTypedColumn.from_cli(str(item)) for item in kwargs["columns"])  # type: ignore[index]
        plan = ClickHouseCdcTypedMaterializationPlan.from_datasets(
            cdc_dataset=str(kwargs["cdc_dataset"]),
            target_dataset=str(kwargs["target_dataset"]),
            unique_key=cast(Sequence[str], kwargs["unique_key"]),
            columns=columns,
            default_database=str(self._connector.database),
        )
        policy = ClickHouseCdcTypedMaterializationPolicy(
            delete_mode="exclude_deleted",
            quality=ClickHouseCdcTypedQualityPolicy(
                fail_on_parse_errors=bool(kwargs.get("fail_on_parse_errors", False)),
                max_parse_error_ratio=float(kwargs.get("max_parse_error_ratio", 0.0)),
                quarantine_dataset=cast(str | None, kwargs.get("quarantine_dataset")),
                schema_drift_mode=cast(str, kwargs.get("schema_drift_mode", "warn_additive")),
                sample_limit=int(kwargs.get("quality_sample_limit", 100)),
            ),
        )
        return ClickHouseCdcTypedMaterializationService(self._connector).materialize(
            plan=plan,
            policy=policy,
            output_dir=cast(str | Path, kwargs["output_dir"]),
        )


def _blocked_plan(plan: Any, blocker: str) -> Any:
    return type(plan)(
        change=plan.change,
        target_dataset=plan.target_dataset,
        operation=plan.operation,
        ddl="",
        backfill_sql="",
        source_type=plan.source_type,
        target_type=plan.target_type,
        passed=False,
        blockers=tuple(dict.fromkeys((*plan.blockers, blocker))),
        warnings=plan.warnings,
    )


def _mode(value: str) -> SchemaApplyMode:
    if value not in {"dry_run", "apply"}:
        raise ValueError("mode must be dry_run or apply")
    return cast(SchemaApplyMode, value)


def _create_clickhouse_connector(
    connection_id: str,
    credentials_source: Any,
    mount_point: str | None,
    path: str | None,
) -> Any:
    from dpone.runtime.credentials.factory import BaseFactory

    return BaseFactory._create_clickhouse_connector(
        connection_id,
        credentials_source,
        mount_point=cast(str, mount_point),
        path=cast(str, path),
    )


def _credentials_source(value: str) -> Any:
    from dpone.runtime.credentials.config import CredentialsSource

    return CredentialsSource(value)


def _close(connector: Any) -> None:
    close = getattr(connector, "close", None)
    if callable(close):
        close()


__all__ = ["CdcSchemaEvolutionApplyService"]
