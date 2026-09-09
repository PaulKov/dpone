"""Certified safe-sample copier skeleton for MSSQL -> ClickHouse incremental routes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.services.safe_sample_redaction import is_sensitive_key_name
from dpone.services.safe_sample_source_request import SafeSampleSourceRequestBuilder

if TYPE_CHECKING:
    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan
    from dpone.services.safe_sample_policy import TemporaryTargetPlan

_CERTIFICATION_ID = "mssql_clickhouse_incremental_merge_airflow_kpo"
_PROOF = f"route_certification:{_CERTIFICATION_ID}"


@dataclass(frozen=True, slots=True)
class MssqlClickHouseSafeSampleCopyConfig:
    """Non-secret source-side binding for the certified sample copier."""

    source_connection_ref: str
    source_table: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.mssql-clickhouse-safe-sample-copy-config.v1",
            "source_connection_ref": self.source_connection_ref,
            "source_table": dict(self.source_table),
        }


class MssqlClickHouseSafeSampleCopyConfigError(RuntimeError):
    """Structured configuration error for route-specific safe sample copier setup."""

    def __init__(self, code: str, message: str, *, entity: dict[str, str] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.entity = entity or {"kind": "route", "id": _CERTIFICATION_ID}

    def to_error(self) -> dict[str, Any]:
        return {
            "schema": "dpone.error.v1",
            "code": self.code,
            "stage": "mssql_clickhouse_safe_sample_copier_config",
            "severity": "error",
            "message": str(self),
            "entity": dict(self.entity),
            "fixes": [],
        }


class MssqlClickHouseSafeSampleCopyConfigBuilder:
    """Build non-secret copier config from a pipeline source object."""

    def build_from_pipeline_source(
        self,
        pipeline_source: Mapping[str, Any],
        *,
        process_name: str | None = None,
    ) -> MssqlClickHouseSafeSampleCopyConfig:
        process = _select_process(pipeline_source, process_name=process_name)
        source = _mapping(process.get("source"))
        sink = _mapping(process.get("sink"))
        source_type = canonical_endpoint_type(str(source.get("type") or ""))
        sink_type = canonical_endpoint_type(str(sink.get("type") or ""))
        if source_type != "mssql" or sink_type != "clickhouse":
            raise _route_error(process_name)
        if _strategy_mode(sink) != "incremental_merge":
            raise _route_error(process_name)
        connection_ref = str(source.get("connection_ref") or "").strip()
        table = _table(source.get("table"))
        if not connection_ref or not table["name"]:
            raise MssqlClickHouseSafeSampleCopyConfigError(
                "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_SOURCE_INVALID",
                "MSSQL -> ClickHouse safe sample copier requires source.connection_ref and source.table.name.",
                entity={"kind": "process", "id": str(process.get("name") or process_name or "")},
            )
        return MssqlClickHouseSafeSampleCopyConfig(
            source_connection_ref=connection_ref,
            source_table=table,
        )


class MssqlClickHouseSafeSampleCopyExecutor(Protocol):
    """Live-copy boundary for the certified route-specific copier."""

    def copy(self, request: dict[str, Any]) -> Mapping[str, Any]:
        """Copy a bounded source sample into the temporary target."""


class FailClosedMssqlClickHouseSafeSampleCopyExecutor:
    """Default executor that refuses live source IO until a certified adapter is injected."""

    def copy(self, request: dict[str, Any]) -> Mapping[str, Any]:
        del request
        return {
            "status": "blocked",
            "rows_read": 0,
            "rows_written": 0,
            "bytes_read": 0,
            "errors": [
                _error(
                    "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED",
                    "MSSQL -> ClickHouse safe sample copier requires an explicitly injected certified executor.",
                )
            ],
        }


class MssqlClickHouseSafeSampleCopier:
    """Build a certified copy request and delegate execution through an injected port."""

    def __init__(
        self,
        config: MssqlClickHouseSafeSampleCopyConfig,
        *,
        executor: MssqlClickHouseSafeSampleCopyExecutor | None = None,
    ) -> None:
        self._config = config
        self._executor = executor or FailClosedMssqlClickHouseSafeSampleCopyExecutor()

    def build_copy_request(
        self,
        *,
        plan: SafeSampleExecutionPlan,
        target_plan: TemporaryTargetPlan,
    ) -> dict[str, Any]:
        """Return the secret-free route-specific copy request without executing live IO."""

        return _copy_request(plan, target_plan, self._config)

    def copy(
        self,
        *,
        plan: SafeSampleExecutionPlan,
        target_plan: TemporaryTargetPlan,
        init_fetch: dict[str, Any],
    ) -> Mapping[str, Any]:
        del init_fetch
        source_request = SafeSampleSourceRequestBuilder().build(plan.policy_result, target_plan).to_dict()
        copy_request = _copy_request(plan, target_plan, self._config)
        validation_errors = _validation_errors(source_request, target_plan)
        if validation_errors:
            executor_result: Mapping[str, Any] = {
                "status": "failed",
                "rows_read": 0,
                "rows_written": 0,
                "bytes_read": 0,
                "errors": validation_errors,
            }
        else:
            executor_result = self._executor.copy(copy_request)
        return _data_copy_payload(
            source_request=source_request,
            copy_request=copy_request,
            executor_result=executor_result,
        )


def build_mssql_clickhouse_safe_sample_copier_from_pipeline_source(
    pipeline_source: Mapping[str, Any],
    *,
    process_name: str | None = None,
    executor: MssqlClickHouseSafeSampleCopyExecutor | None = None,
) -> MssqlClickHouseSafeSampleCopier:
    config = MssqlClickHouseSafeSampleCopyConfigBuilder().build_from_pipeline_source(
        pipeline_source,
        process_name=process_name,
    )
    return MssqlClickHouseSafeSampleCopier(config, executor=executor)


def build_mssql_clickhouse_safe_sample_copy_request_from_pipeline_source(
    pipeline_source: Mapping[str, Any],
    *,
    process_name: str | None = None,
    plan: SafeSampleExecutionPlan,
    target_plan: TemporaryTargetPlan,
) -> dict[str, Any]:
    return build_mssql_clickhouse_safe_sample_copier_from_pipeline_source(
        pipeline_source,
        process_name=process_name,
    ).build_copy_request(plan=plan, target_plan=target_plan)


def _copy_request(
    plan: SafeSampleExecutionPlan,
    target_plan: TemporaryTargetPlan,
    config: MssqlClickHouseSafeSampleCopyConfig,
) -> dict[str, Any]:
    return {
        "schema": "dpone.safe-sample-certified-copy-request.v1",
        "certification_id": _CERTIFICATION_ID,
        "source": {
            "type": "mssql",
            "connection_ref": config.source_connection_ref,
            "table": dict(config.source_table),
        },
        "sink": {
            "type": "clickhouse",
            "connection_ref": target_plan.connection_ref,
            "temporary_table": dict(target_plan.temporary_table),
        },
        "strategy": "incremental_merge",
        "sample_rows": plan.sample_rows,
        "max_bytes": plan.policy_result.policy.max_bytes,
        "timeout_seconds": plan.policy_result.policy.timeout_seconds,
        "source_read_only": True,
        "pii_policy": target_plan.pii_policy,
        "proof": plan.policy_result.capabilities.proof,
    }


def _select_process(pipeline_source: Mapping[str, Any], *, process_name: str | None) -> Mapping[str, Any]:
    processes = pipeline_source.get("processes")
    if not isinstance(processes, list) or not processes:
        raise MssqlClickHouseSafeSampleCopyConfigError(
            "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_PROCESS_MISSING",
            "Pipeline source must contain at least one process.",
        )
    if process_name is None:
        first = processes[0]
        if isinstance(first, Mapping):
            return first
        raise MssqlClickHouseSafeSampleCopyConfigError(
            "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_PROCESS_MISSING",
            "Pipeline source process must be a mapping.",
        )
    for process in processes:
        if isinstance(process, Mapping) and str(process.get("name") or "") == process_name:
            return process
    raise MssqlClickHouseSafeSampleCopyConfigError(
        "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_PROCESS_NOT_FOUND",
        f"Pipeline source does not contain process '{process_name}'.",
        entity={"kind": "process", "id": process_name},
    )


def _route_error(process_name: str | None) -> MssqlClickHouseSafeSampleCopyConfigError:
    return MssqlClickHouseSafeSampleCopyConfigError(
        "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_ROUTE_UNSUPPORTED",
        "MSSQL -> ClickHouse safe sample copier requires source=mssql, sink=clickhouse, strategy=incremental_merge.",
        entity={"kind": "process", "id": process_name or ""},
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _strategy_mode(sink: Mapping[str, Any]) -> str:
    strategy = sink.get("strategy")
    if isinstance(strategy, Mapping):
        return _normalized(strategy.get("mode"))
    return _normalized(strategy)


def _table(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {"schema": "", "name": ""}
    return {"schema": str(value.get("schema") or ""), "name": str(value.get("name") or "")}


def _normalized(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _validation_errors(source_request: dict[str, Any], target_plan: TemporaryTargetPlan) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    if source_request.get("status") != "planned":
        errors.append(
            _error(
                "DPONE_SAFE_SAMPLE_SOURCE_REQUEST_BLOCKED",
                "Certified copier requires a planned safe sample source request.",
            )
        )
    if source_request.get("proof") != _PROOF:
        errors.append(
            _error(
                "DPONE_SAFE_SAMPLE_ROUTE_CERTIFICATION_MISMATCH",
                "MSSQL -> ClickHouse sample copier requires the matching route certification proof.",
            )
        )
    if target_plan.sink_type != "clickhouse":
        errors.append(
            _error(
                "DPONE_SAFE_SAMPLE_TARGET_ROUTE_MISMATCH",
                "MSSQL -> ClickHouse sample copier requires a ClickHouse temporary target.",
            )
        )
    return errors


def _data_copy_payload(
    *,
    source_request: dict[str, Any],
    copy_request: dict[str, Any],
    executor_result: Mapping[str, Any],
) -> dict[str, Any]:
    safe_result = _redact(dict(executor_result))
    errors = _errors(safe_result)
    return {
        "schema": "dpone.safe-sample-data-copy.v1",
        "status": str(safe_result.get("status") or ("failed" if errors else "copied")),
        "source_request": source_request,
        "copy_request": copy_request,
        "rows_read": _non_negative_int(safe_result.get("rows_read")),
        "rows_written": _non_negative_int(safe_result.get("rows_written")),
        "bytes_read": _non_negative_int(safe_result.get("bytes_read")),
        "pii_policy": "masked",
        "diagnostics": _diagnostics(safe_result),
        "errors": errors,
    }


def _diagnostics(payload: dict[str, Any]) -> dict[str, Any]:
    diagnostics = payload.get("diagnostics")
    return dict(diagnostics) if isinstance(diagnostics, dict) else {}


def _errors(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw_errors = payload.get("errors")
    if not isinstance(raw_errors, list):
        return []
    return [dict(error) for error in raw_errors if isinstance(error, dict)]


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, raw_value in value.items():
            key_text = str(key)
            if _is_sensitive_key(key_text):
                continue
            redacted[key_text] = _redact(raw_value)
        return redacted
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


def _is_sensitive_key(key: str) -> bool:
    return is_sensitive_key_name(key)


def _non_negative_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _error(code: str, message: str) -> dict[str, Any]:
    return {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": "mssql_clickhouse_safe_sample_copier",
        "severity": "error",
        "message": message,
        "fixes": [],
    }


__all__ = [
    "FailClosedMssqlClickHouseSafeSampleCopyExecutor",
    "MssqlClickHouseSafeSampleCopier",
    "MssqlClickHouseSafeSampleCopyConfig",
    "MssqlClickHouseSafeSampleCopyConfigBuilder",
    "MssqlClickHouseSafeSampleCopyConfigError",
    "MssqlClickHouseSafeSampleCopyExecutor",
    "build_mssql_clickhouse_safe_sample_copy_request_from_pipeline_source",
    "build_mssql_clickhouse_safe_sample_copier_from_pipeline_source",
]
