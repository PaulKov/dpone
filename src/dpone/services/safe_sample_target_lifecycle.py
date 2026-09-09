"""Temporary target lifecycle contracts for safe sample runs."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from typing import Any, Protocol

from dpone.services.safe_sample_execution_common import credential_resolution_error, redact_mapping
from dpone.services.safe_sample_policy import TemporaryTargetPlan


class TemporaryTargetAdapter(Protocol):
    """Backend adapter for temporary table lifecycle operations."""

    def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
        """Create the temporary target described by ``plan`` and return safe metadata."""

    def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
        """Drop the temporary target described by ``plan`` and return safe metadata."""


class TemporaryTargetLifecycleError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


TemporaryTargetAdapterFactory = Callable[[TemporaryTargetPlan], TemporaryTargetAdapter]


@dataclass(frozen=True, slots=True)
class TemporaryTargetLifecycleResult:
    status: str
    pipeline_id: str
    process: str
    sink_type: str
    connection_ref: str
    temporary_table: dict[str, str]
    ttl_seconds: int
    cleanup_required: bool
    pii_policy: str
    adapter_metadata: dict[str, object]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.temporary-target-lifecycle.v1",
            **asdict(self),
        }


class TemporaryTargetLifecycleExecutor:
    """Execute temporary target create/drop through an injected adapter."""

    def __init__(self, *, adapter: TemporaryTargetAdapter) -> None:
        self._adapter = adapter

    def prepare(self, plan: TemporaryTargetPlan) -> TemporaryTargetLifecycleResult:
        try:
            metadata = self._adapter.create(plan)
        except Exception as exc:  # noqa: BLE001 - adapter failures need stable structured codes.
            safe_error = credential_resolution_error(exc)
            if safe_error is not None:
                raise TemporaryTargetLifecycleError(*safe_error) from None
            raise TemporaryTargetLifecycleError(
                "DPONE_RUNTIME_TEMPORARY_TARGET_CREATE_FAILED",
                f"temporary target create failed: {exc}",
            ) from exc
        return _result("prepared", plan, metadata)

    def cleanup(self, plan: TemporaryTargetPlan) -> TemporaryTargetLifecycleResult:
        try:
            metadata = self._adapter.drop(plan)
        except Exception as exc:  # noqa: BLE001 - adapter failures need stable structured codes.
            safe_error = credential_resolution_error(exc)
            if safe_error is not None:
                raise TemporaryTargetLifecycleError(*safe_error) from None
            raise TemporaryTargetLifecycleError(
                "DPONE_RUNTIME_TEMPORARY_TARGET_DROP_FAILED",
                f"temporary target drop failed: {exc}",
            ) from exc
        return _result("cleaned", plan, metadata)


class TemporaryTargetAdapterRegistry:
    """Resolve temporary target adapters without coupling runtime flow to backends."""

    def __init__(self, adapters: Mapping[str, TemporaryTargetAdapterFactory] | None = None) -> None:
        self._adapters: dict[str, TemporaryTargetAdapterFactory] = {}
        for sink_type, factory in (adapters or {}).items():
            self.register(sink_type, factory)

    def register(self, sink_type: str, factory: TemporaryTargetAdapterFactory) -> None:
        self._adapters[_normalize_sink_type(sink_type)] = factory

    def adapter_for(self, plan: TemporaryTargetPlan) -> TemporaryTargetAdapter:
        sink_type = _normalize_sink_type(plan.sink_type)
        factory = self._adapters.get(sink_type)
        if factory is None:
            raise TemporaryTargetLifecycleError(
                "DPONE_RUNTIME_TEMPORARY_TARGET_ADAPTER_NOT_FOUND",
                f"temporary target adapter is not registered for sink type {plan.sink_type!r}",
            )
        return factory(plan)

    def executor_for(self, plan: TemporaryTargetPlan) -> TemporaryTargetLifecycleExecutor:
        return TemporaryTargetLifecycleExecutor(adapter=self.adapter_for(plan))


def _result(
    status: str,
    plan: TemporaryTargetPlan,
    metadata: dict[str, object],
) -> TemporaryTargetLifecycleResult:
    return TemporaryTargetLifecycleResult(
        status=status,
        pipeline_id=plan.pipeline_id,
        process=plan.process,
        sink_type=plan.sink_type,
        connection_ref=plan.connection_ref,
        temporary_table=dict(plan.temporary_table),
        ttl_seconds=plan.ttl_seconds,
        cleanup_required=plan.cleanup_required,
        pii_policy=plan.pii_policy,
        adapter_metadata=_safe_metadata(metadata),
    )


def _safe_metadata(metadata: dict[str, object]) -> dict[str, object]:
    return redact_mapping(metadata)


def _normalize_sink_type(sink_type: str) -> str:
    return str(sink_type or "").strip().lower()


__all__ = [
    "TemporaryTargetAdapter",
    "TemporaryTargetAdapterFactory",
    "TemporaryTargetAdapterRegistry",
    "TemporaryTargetLifecycleError",
    "TemporaryTargetLifecycleExecutor",
    "TemporaryTargetLifecycleResult",
]
