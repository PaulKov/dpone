"""Canonical invocation-wide route and per-operation scope identities."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime  # type: ignore[attr-defined]
from typing import Any

from dpone.backfill.execution_policy import (
    execution_policy_from_load_config,
)
from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlGenericCommitReceipt,
    MssqlOperationRequest,
    MssqlTransactionContractError,
    MssqlTransactionOperation,
    operation_owner_digest,
    operation_scope_hash,
)
from dpone.contracts.portable_relation_scope import (
    parse_portable_relation_scope,
    portable_scope_contract,
    portable_scope_sha256,
)
from dpone.contracts.postgres_source_authority import require_authority_sha256
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.runtime.etl.mssql_transaction_route_identity import (
    _backfill_execution_policy_digest,
    _canonical,
    _portable_scope_identity,
    invocation_route_fingerprint,
)

_POSTGRES_AUTHORITY_IDENTITY_VERSIONS = frozenset({2, 3})


def invocation_identity(run_context: Any, load_config: Any, *, dag_id: str | None) -> InvocationIdentity:
    """Resolve scheduler-stable run/process/task identity without using load_id."""

    run_id = _backfill_campaign_invocation_id(load_config) or str(getattr(run_context, "run_id", "") or "").strip()
    if not run_id:
        raise MssqlTransactionContractError("mssql_transaction.stable_invocation_id_required")
    context = getattr(run_context, "config", {})
    context = context if isinstance(context, Mapping) else {}
    options = getattr(load_config, "options", {}) or {}
    state_identity = options.get("state_identity") if isinstance(options, Mapping) else None
    state_identity = state_identity if isinstance(state_identity, Mapping) else {}
    process = str(state_identity.get("process") or dag_id or context.get("process") or "").strip()
    if not process:
        raise MssqlTransactionContractError("mssql_transaction.process_identity_required")
    pipeline = str(context.get("pipeline_id") or context.get("pipeline") or process).strip()
    task = str(context.get("task_id") or context.get("task") or load_config.target_table).strip()
    return InvocationIdentity(run_id=run_id, process=process, task_partition=f"{pipeline}:{task}")


def _backfill_campaign_invocation_id(load_config: Any) -> str | None:
    """Return a receipt-stable ID only for a runtime-proven campaign chunk."""

    mode = getattr(getattr(load_config, "load_strategy", None), "value", None)
    if mode != "backfill":
        return None
    options = getattr(load_config, "options", {}) or {}
    backfill = options.get("backfill") if isinstance(options, Mapping) else None
    context = backfill.get("chunk_context") if isinstance(backfill, Mapping) else None
    if context is None:
        return None
    try:
        execution_policy_from_load_config(load_config)
    except ValueError as exc:
        raise MssqlTransactionContractError("mssql_transaction.backfill_execution_policy_invalid") from exc
    run_key = str(context.get("run_key") or "").strip() if isinstance(context, Mapping) else ""
    if not run_key:
        raise MssqlTransactionContractError("mssql_transaction.backfill_run_key_required")
    return f"dpone-backfill:{run_key}"


def build_mssql_attempt_request(
    load_config: Any,
    *,
    invocation: InvocationIdentity,
    target_identity: bytes,
    source_identity: SourcePhysicalIdentity,
    load_id: str,
    request_coordinates: tuple[str, str, str],
) -> MssqlAttemptRequest:
    """Build the exact request shared by admission and migration proof."""

    return MssqlAttemptRequest(
        invocation=invocation,
        target_identity=target_identity,
        route_fingerprint=invocation_route_fingerprint(
            load_config,
            target_identity=target_identity,
            source_identity=source_identity,
        ),
        load_id=load_id,
        target_database=request_coordinates[0],
        target_schema=request_coordinates[1],
        target_table=request_coordinates[2],
        strategy=load_config.load_strategy.value,
    )


def operation_request(load_config: Any, invocation: InvocationIdentity) -> MssqlOperationRequest:
    """Resolve target-wide scope or a planner-proven disjoint backfill range."""

    options = getattr(load_config, "options", {}) or {}
    raw = options.get("__dpone_mssql_operation_scope") if isinstance(options, Mapping) else None
    if raw is None:
        scope = {"kind": "target_wide", "version": 1}
        portable_scope = _portable_scope_identity(load_config)
        if portable_scope is not None:
            scope["portable_scope"] = portable_scope
        owner = f"invocation:{invocation.invocation_digest.hex()}"
        expiry = None
    else:
        if not isinstance(raw, Mapping):
            raise MssqlTransactionContractError("mssql_transaction.operation_scope_invalid")
        if raw.get("kind") != "backfill_disjoint_range_v1" or raw.get("proven_disjoint") is not True:
            raise MssqlTransactionContractError("mssql_transaction.operation_scope_not_proven_disjoint")
        backfill = options.get("backfill") if isinstance(options, Mapping) else None
        if not isinstance(backfill, Mapping) or "chunk_context" not in backfill:
            raise MssqlTransactionContractError("mssql_transaction.backfill_runtime_scope_required")
        if _backfill_execution_policy_digest(load_config) is None:
            raise MssqlTransactionContractError("mssql_transaction.backfill_execution_policy_required")
        scope = _required_scope(raw, load_config=load_config)
        owner = str(raw.get("lease_owner") or "").strip()
        expiry = _lease_expiry(raw.get("lease_expires_at_utc"))
    return MssqlOperationRequest(
        scope_hash=operation_scope_hash(scope),
        owner_digest=operation_owner_digest(owner),
        lease_expires_at_utc=expiry,
    )


def is_mssql_transaction_operation(value: Any) -> bool:
    """Expose the canonical operation type check through the runtime facade."""

    return isinstance(value, MssqlTransactionOperation) and value.has_canonical_attempt()


def is_mssql_generic_commit_receipt(value: Any) -> bool:
    """Expose the canonical receipt type check through the runtime facade."""

    return isinstance(value, MssqlGenericCommitReceipt)


def mssql_operation_owner_digest(owner: str) -> bytes:
    """Expose the canonical owner digest without duplicating identity logic."""

    return operation_owner_digest(owner)


def resolve_source_physical_identity(source: Any, load_config: Any) -> SourcePhysicalIdentity:
    """Resolve a database-issued binding before the pure route hash is built."""

    expected, expected_digest = require_source_physical_identity_binding(load_config)
    resolver = getattr(source, "mssql_transaction_source_physical_identity", None)
    if not callable(resolver):
        raise MssqlTransactionContractError("mssql_transaction.source_physical_identity_capability_required")
    resolved = resolver(load_config)
    if not isinstance(resolved, SourcePhysicalIdentity):
        raise MssqlTransactionContractError("mssql_transaction.source_physical_identity_invalid")
    if expected in {"postgres", "postgresql"} and resolved.dialect.casefold() not in {"postgres", "postgresql"}:
        raise MssqlTransactionContractError("mssql_transaction.source_physical_identity_dialect_mismatch")
    if expected in {"postgres", "postgresql"}:
        if resolved.version not in _POSTGRES_AUTHORITY_IDENTITY_VERSIONS:
            raise MssqlTransactionContractError("mssql_transaction.postgres_source_authority_identity_required")
        assert expected_digest is not None
        if resolved.authority_sha256 != expected_digest:
            raise MssqlTransactionContractError("mssql_transaction.postgres_source_authority_digest_mismatch")
        if resolved.schema != str(getattr(load_config, "source_schema", "") or "") or resolved.relation != str(
            getattr(load_config, "source_table", "") or ""
        ):
            raise MssqlTransactionContractError("mssql_transaction.postgres_source_relation_identity_mismatch")
    configured_database = str(getattr(load_config, "source_database", "") or "").strip()
    if configured_database and configured_database != resolved.database:
        raise MssqlTransactionContractError("mssql_transaction.source_physical_identity_database_mismatch")
    return resolved


def require_source_physical_identity_binding(load_config: Any) -> tuple[str, str | None]:
    """Validate signed source-binding completeness without connector I/O."""

    options = getattr(load_config, "options", {})
    if not isinstance(options, Mapping):
        raise MssqlTransactionContractError("mssql_transaction.options_not_mapping")
    expected = str(options.get("source_type") or "").strip().casefold()
    if expected not in {"postgres", "postgresql"}:
        return expected, None
    try:
        digest = require_authority_sha256(options.get("postgres_source_authority_sha256"))
    except ValueError as exc:
        raise MssqlTransactionContractError("mssql_transaction.postgres_source_authority_digest_required") from exc
    if (
        not str(getattr(load_config, "source_schema", "") or "").strip()
        or not str(getattr(load_config, "source_table", "") or "").strip()
    ):
        raise MssqlTransactionContractError("mssql_transaction.postgres_source_relation_identity_required")
    return expected, digest


def _required_scope(raw: Mapping[str, Any], *, load_config: Any) -> dict[str, Any]:
    fields = (
        "kind",
        "run_key",
        "plan_hash",
        "chunk_index",
        "chunk_idempotency_key",
        "start",
        "end",
        "portable_scope_sha256",
        "proven_disjoint",
        "execution_policy_sha256",
    )
    scope = {field: raw.get(field) for field in fields}
    if any(scope[field] in (None, "") for field in fields if field != "proven_disjoint"):
        raise MssqlTransactionContractError("mssql_transaction.operation_scope_incomplete")
    try:
        raw_portable_scope = parse_portable_relation_scope(raw.get("portable_scope"))
    except ValueError as exc:
        raise MssqlTransactionContractError("mssql_transaction.operation_portable_scope_invalid") from exc
    ast_contract = portable_scope_contract(raw_portable_scope)
    ast_sha256 = portable_scope_sha256(raw_portable_scope).hex()
    if scope["portable_scope_sha256"] != ast_sha256:
        raise MssqlTransactionContractError("mssql_transaction.operation_portable_scope_digest_mismatch")
    bound_identity = _portable_scope_identity(load_config)
    if bound_identity is not None:
        if bound_identity["ast"] != ast_contract:
            raise MssqlTransactionContractError("mssql_transaction.operation_portable_scope_binding_mismatch")
        scope["portable_scope"] = bound_identity
    else:
        scope["portable_scope"] = {"ast": ast_contract}
    return _canonical(scope)


def _lease_expiry(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        try:
            result = datetime.fromisoformat(str(value or ""))
        except ValueError as exc:
            raise MssqlTransactionContractError("mssql_transaction.operation_lease_expiry_invalid") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise MssqlTransactionContractError("mssql_transaction.operation_lease_expiry_invalid")
    return result.astimezone(UTC)


__all__ = [
    "build_mssql_attempt_request",
    "invocation_identity",
    "invocation_route_fingerprint",
    "is_mssql_generic_commit_receipt",
    "is_mssql_transaction_operation",
    "mssql_operation_owner_digest",
    "operation_request",
    "require_source_physical_identity_binding",
    "resolve_source_physical_identity",
]
