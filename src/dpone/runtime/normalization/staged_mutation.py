"""Fail-closed staged mutation for nested root/child packages."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from dpone.runtime.governance.ports import (
    finalize_staged_load_after_validation,
    validate_staged_load_if_supported,
)
from dpone.runtime.process_io import add_exception_note
from dpone.runtime.sinks.load_result import LoadResult

NESTED_PACKAGE_STAGED_ABORT_REQUIRED = "nested_package_staged_abort_required"
NESTED_PACKAGE_MUTATION_PORT_REQUIRED = "nested_package_mutation_port_required"
NESTED_PACKAGE_PARTIAL_FINALIZE = "nested_package_partial_finalize"
NESTED_PACKAGE_ABORT_FAILED = "nested_package_abort_failed"


class NestedPackagePartialFinalizeError(RuntimeError):
    """Retain exact package staging when automatic replay is unsafe."""

    code = NESTED_PACKAGE_PARTIAL_FINALIZE

    def __init__(self, details: Mapping[str, Any]) -> None:
        super().__init__(self.code)
        self.details = dict(details)


def sink_supports_nested_package_abort(sink: Any) -> bool:
    """Return True when the sink can stage, finalize, and abort package members."""

    return all(
        callable(getattr(sink, name, None)) for name in ("stage_payload", "finalize_staged_load", "abort_staged_load")
    )


def require_nested_package_abort(sink: Any) -> None:
    """Fail closed when nested package mutation cannot be aborted as a unit."""

    if not sink_supports_nested_package_abort(sink):
        raise RuntimeError(NESTED_PACKAGE_STAGED_ABORT_REQUIRED)


@dataclass
class NestedPackageStagedMutation:
    """Validate every staged member before finalizing any package target."""

    sink: Any
    failure_recorder: Callable[[BaseException, Mapping[str, Any]], None] | None = field(
        default=None,
        repr=False,
    )
    entries: list[tuple[Any, Any]] = field(default_factory=list)
    finalized_tables: list[str] = field(default_factory=list)
    target_outcome_unknown: bool = False
    retention_required: bool = False
    _retained_failure_details: dict[str, Any] | None = field(default=None, init=False, repr=False)
    _failure_recorded: bool = field(default=False, init=False, repr=False)

    def stage(self, load_config: Any, payload: Any) -> LoadResult:
        handle = self.sink.stage_payload(load_config, payload)
        rows = _validated_staged_rows(handle)
        self.entries.append((load_config, handle))
        return LoadResult(inserted_rows=rows, updated_rows=0, total_rows=rows, staging_rows=rows)

    def finalize_all(self) -> list[LoadResult]:
        """Validate all members, then finalize while retaining uncertain staging."""

        results: list[LoadResult] = []
        pending = list(self.entries)
        self.entries = []
        target_invoked = False
        try:
            receipts = [
                validate_staged_load_if_supported(self.sink, load_config, handle) for load_config, handle in pending
            ]
            while pending:
                load_config, handle = pending[0]
                receipt = receipts.pop(0)
                validated_config = load_config
                validated_handle = handle
                if receipt.validated:
                    _token, validated_config, validated_handle = receipt.frozen_inputs(
                        sink=self.sink,
                        load_config=load_config,
                    )
                target_invoked = True
                result = finalize_staged_load_after_validation(self.sink, load_config, receipt)
                target_invoked = False
                self.finalized_tables.append(str(validated_config.target_table))
                results.append(result)
                cleanup = getattr(self.sink, "cleanup_staged_load", None)
                if callable(cleanup):
                    cleanup(validated_handle)
                pending.pop(0)
            return results
        except BaseException as exc:
            self.entries = pending
            if target_invoked or self.finalized_tables:
                self.target_outcome_unknown = target_invoked
                self.retention_required = True
                self._retained_failure_details = _nested_package_retention_details(
                    pending,
                    finalized_tables=self.finalized_tables,
                    target_outcome="commit_unknown" if target_invoked else "committed",
                    cleanup_attempted=not target_invoked,
                )
                error = NestedPackagePartialFinalizeError(self._retained_failure_details)
                self._record_failure(error)
                raise error from exc
            try:
                self.abort_all()
            except Exception as abort_exc:
                if self.finalized_tables:
                    raise RuntimeError(NESTED_PACKAGE_PARTIAL_FINALIZE) from abort_exc
                add_exception_note(exc, f"nested package abort failed: {type(abort_exc).__name__}")
                raise exc from abort_exc
            raise

    def abort_all(self) -> None:
        """Abort pre-target members; never destroy commit-unknown staging."""

        if self.retention_required:
            raise NestedPackagePartialFinalizeError(self._retained_failure_details or {})
        errors: list[Exception] = []
        for _load_config, handle in self.entries:
            try:
                self.sink.abort_staged_load(handle)
            except Exception as exc:
                errors.append(exc)
        self.entries.clear()
        if errors:
            raise RuntimeError(NESTED_PACKAGE_ABORT_FAILED) from errors[0]

    def _record_failure(self, error: NestedPackagePartialFinalizeError) -> None:
        if self.failure_recorder is None or self._failure_recorded:
            return
        self._failure_recorded = True
        try:
            self.failure_recorder(error, error.details)
        except Exception as recorder_error:
            add_exception_note(
                error,
                f"nested package failure evidence failed: {type(recorder_error).__name__}",
            )


def require_nested_package_mutation(package_mutation: Any) -> NestedPackageStagedMutation:
    """Fail closed unless callers provide NestedPackageStagedMutation."""

    if not isinstance(package_mutation, NestedPackageStagedMutation):
        raise RuntimeError(NESTED_PACKAGE_MUTATION_PORT_REQUIRED)
    return package_mutation


def _validated_staged_rows(handle: Any) -> int:
    staged_rows = getattr(handle, "staged_rows", None)
    if isinstance(staged_rows, bool) or not isinstance(staged_rows, int) or staged_rows < 0:
        raise RuntimeError("nested_package_staged_rows_invalid")
    return staged_rows


def _nested_package_retention_details(
    pending: list[tuple[Any, Any]],
    *,
    finalized_tables: list[str],
    target_outcome: str,
    cleanup_attempted: bool,
) -> dict[str, Any]:
    operation_tables: dict[str, str] = {}
    retained_members: list[dict[str, Any]] = []
    for index, (load_config, handle) in enumerate(pending):
        target = _qualified_target(load_config)
        member_tables = _operation_tables(handle)
        operation_tables.update({f"{target}:{role}": table for role, table in member_tables.items()})
        retained_members.append(
            {
                "target": target,
                "target_outcome": target_outcome if index == 0 else "not_invoked",
                "operation_tables": member_tables,
            }
        )
    return {
        "failure_boundary": "commit_unknown" if target_outcome == "commit_unknown" else "post_commit",
        "target_outcome": target_outcome,
        "error_code": NESTED_PACKAGE_PARTIAL_FINALIZE,
        "finalized_tables": list(finalized_tables),
        "retained_members": retained_members,
        "operation_tables": operation_tables,
        "cleanup_attempted": cleanup_attempted,
        "cleanup_status": "failed" if cleanup_attempted else "retained_for_reconciliation",
        "cleanup_verification_required": True,
        "safe_to_retry": False,
    }


def _operation_tables(handle: Any) -> dict[str, str]:
    metadata = dict(getattr(handle, "metadata", {}) or {})
    configured = metadata.get("operation_tables")
    if isinstance(configured, Mapping):
        return {str(role): str(table) for role, table in configured.items()}
    tables: dict[str, str] = {}
    for role, attribute in (
        ("staging", "staging_config"),
        ("decoded", "decoded_config"),
        ("projected", "finalization_config"),
    ):
        config = getattr(handle, attribute, None)
        table = _qualified_target(config)
        if table != ".":
            tables[role] = table
    return tables


def _qualified_target(config: Any) -> str:
    return f"{getattr(config, 'target_schema', '')}.{getattr(config, 'target_table', '')}"


__all__ = [
    "NESTED_PACKAGE_ABORT_FAILED",
    "NESTED_PACKAGE_MUTATION_PORT_REQUIRED",
    "NESTED_PACKAGE_PARTIAL_FINALIZE",
    "NESTED_PACKAGE_STAGED_ABORT_REQUIRED",
    "NestedPackagePartialFinalizeError",
    "NestedPackageStagedMutation",
    "require_nested_package_abort",
    "require_nested_package_mutation",
    "sink_supports_nested_package_abort",
]
