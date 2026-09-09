"""Resolve one scheduler-aware invocation for CLI and Python entry points."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Protocol

from dpone.backfill.mapping import AirflowBackfillMappingViolation
from dpone.contracts import ETLConfigurationError
from dpone.contracts.run_interval import (
    RunInterval,
    parse_interval_datetime,
    run_interval_from_env,
    validate_partition_context,
)

_ISO_INTERVAL_FIELDS = (
    ("logical_date", "DPONE_LOGICAL_DATE_INVALID"),
    ("interval_start", "DPONE_INTERVAL_START_INVALID"),
    ("interval_end", "DPONE_INTERVAL_END_INVALID"),
)
REPAIR_AUTHORITY_REF_ENV = "DPONE_REPAIR_AUTHORITY_REF"
_REPAIR_AUTHORITY_REF = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9_.:-]{0,126}[A-Za-z0-9])?$")


class _MappingContextService(Protocol):
    def from_environ(self, environ: Mapping[str, str]) -> dict[str, Any]: ...


class _LoadConfigMutator(Protocol):
    def __call__(self, load_config: Any) -> Any: ...


class _IntervalContext(Protocol):
    def apply(self, load_config: Any) -> Any: ...


IntervalContextFactory = Callable[[RunInterval], _IntervalContext]


class _RunInvocationConfigurationError(ETLConfigurationError):
    """Typed pre-execution failure with a stable public error code."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class RunInvocationContext:
    """Arguments that must be equivalent at every public run entry point."""

    run_id: str | None
    dag_id: str | None
    execution_date: Any | None
    load_config_mutator: _LoadConfigMutator
    run_context_config: Mapping[str, Any]


class RunInvocationContextService:
    """Combine explicit identity and one environment snapshot into a run context."""

    def __init__(
        self,
        *,
        mapping_context_service: _MappingContextService,
        interval_context_factory: IntervalContextFactory,
    ) -> None:
        self._mapping_context_service = mapping_context_service
        self._interval_context_factory = interval_context_factory

    def resolve(
        self,
        *,
        environ: Mapping[str, str],
        dag_id: str | None = None,
        execution_date: Any | None = None,
        interval_start: str | None = None,
        interval_end: str | None = None,
        repair_authority_ref: str | None = None,
        normalize_explicit_execution_date: bool = False,
    ) -> RunInvocationContext:
        """Resolve explicit values over the supplied scheduler environment."""

        environment_snapshot = dict(environ)
        interval = _with_explicit_values(
            run_interval_from_env(environment_snapshot),
            dag_id=dag_id,
            execution_date=execution_date,
            interval_start=interval_start,
            interval_end=interval_end,
        )
        run_context_config = self._mapping_context_from_environ(environment_snapshot)
        _validate_effective_interval(interval)
        interval_context = self._interval_context_factory(interval)
        authority_ref = _repair_authority_ref(
            explicit=repair_authority_ref,
            environment=environment_snapshot,
        )

        def apply_invocation(load_config: Any) -> Any:
            configured = interval_context.apply(load_config)
            configured.repair_authority_ref = authority_ref
            return configured

        return RunInvocationContext(
            run_id=interval.dag_run_id,
            dag_id=interval.dag_id,
            execution_date=_effective_execution_date(
                execution_date,
                interval,
                normalize_explicit=normalize_explicit_execution_date,
            ),
            load_config_mutator=apply_invocation,
            run_context_config=run_context_config,
        )

    def _mapping_context_from_environ(self, environ: Mapping[str, str]) -> dict[str, Any]:
        try:
            return self._mapping_context_service.from_environ(environ)
        except AirflowBackfillMappingViolation as exc:
            raise _RunInvocationConfigurationError(exc.code, str(exc)) from exc


def _with_explicit_values(
    interval: RunInterval,
    *,
    dag_id: str | None,
    execution_date: Any | None,
    interval_start: str | None,
    interval_end: str | None,
) -> RunInterval:
    updates: dict[str, str] = {}
    if dag_id:
        updates["dag_id"] = dag_id
    if execution_date:
        updates["logical_date"] = _interval_value(execution_date)
    if interval_start:
        updates["interval_start"] = interval_start
    if interval_end:
        updates["interval_end"] = interval_end
    return replace(interval, **updates) if updates else interval


def _validate_effective_interval(interval: RunInterval) -> None:
    for field_name, error_code in _ISO_INTERVAL_FIELDS:
        value = getattr(interval, field_name)
        if value and parse_interval_datetime(value) is None:
            raise _RunInvocationConfigurationError(
                error_code,
                f"{field_name} must be a valid ISO-8601 date or datetime",
            )
    try:
        validate_partition_context(interval)
    except ValueError as exc:
        code, message = _partition_configuration_error(exc)
        raise _RunInvocationConfigurationError(code, message) from exc


def _partition_configuration_error(exc: ValueError) -> tuple[str, str]:
    message = str(exc)
    candidate, separator, detail = message.partition(":")
    if separator and candidate.startswith("DPONE_AIRFLOW_PARTITION_"):
        return candidate, detail.strip()
    return "DPONE_AIRFLOW_PARTITION_CONTEXT_INVALID", message


def _effective_execution_date(
    explicit: Any | None,
    interval: RunInterval,
    *,
    normalize_explicit: bool,
) -> Any | None:
    if not explicit:
        return interval.execution_datetime()
    if not normalize_explicit:
        return explicit
    parsed = parse_interval_datetime(_interval_value(explicit))
    return parsed if parsed is not None else explicit


def _interval_value(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _repair_authority_ref(*, explicit: str | None, environment: Mapping[str, str]) -> str | None:
    value = str(explicit or environment.get(REPAIR_AUTHORITY_REF_ENV) or "").strip()
    if not value:
        return None
    if not _REPAIR_AUTHORITY_REF.fullmatch(value):
        raise _RunInvocationConfigurationError(
            "DPONE_REPAIR_AUTHORITY_REF_INVALID",
            "repair_authority_ref must be a 1..128 character opaque identifier",
        )
    return value


__all__ = ["REPAIR_AUTHORITY_REF_ENV", "RunInvocationContext", "RunInvocationContextService"]
