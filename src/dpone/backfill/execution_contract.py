"""Strict authoring and runtime-proof contract for backfill execution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.backfill.execution_policy_models import (
    BackfillExecutionPolicy,
    BackfillStatePolicy,
    normalize_publication_policy,
)
from dpone.backfill.models import DEFAULT_MAX_CHUNKS, chunk_spec_from_options, inner_mode_from_options
from dpone.contracts.portable_relation_scope import (
    parse_portable_relation_scope,
    portable_scope_contract,
    portable_scope_sha256,
)

BACKFILL_RUNTIME_AUTHORITY_OPTION = "__dpone_backfill_runtime_authority"
BACKFILL_OPERATION_SCOPE_OPTION = "__dpone_mssql_operation_scope"

_EXECUTION_FIELDS = frozenset(
    {
        "inner_mode",
        "parallel_workers",
        "chunk",
        "max_chunks",
        "state",
        "publication",
        "state_dir",
        "retry_policy",
        "backfill_id",
        "predicate_dialect",
        "lease_ttl_minutes",
    }
)
_CHUNK_FIELDS = frozenset({"column", "from", "to", "step", "kind", "buckets"})
_STATE_FIELDS = frozenset({"backend", "schema", "require_distributed_lock"})
_ADVISOR_FIELDS = frozenset({"optimize_for"})
_PREDICATE_DIALECTS = frozenset({"generic", "clickhouse", "mssql", "postgres", "postgresql"})
_STATE_BACKENDS = frozenset({"local_file", "audit_schema"})
_ADVISOR_PROFILES = frozenset({"balanced", "speed", "source_safety", "worker_safety"})
_MAX_PARALLEL_WORKERS = 256
_MAX_CHUNKS = 1_000_000
_MAX_LEASE_TTL_MINUTES = 10_080
_RUNTIME_CONTEXT_SCHEMA = "dpone.backfill.chunk-context.v1"
_RUNTIME_AUTHORITY_ISSUER = object()


@dataclass(frozen=True, slots=True, init=False)
class _BackfillRuntimeAuthority:
    """In-memory marker that cannot be constructed from manifest data."""

    run_key: str
    plan_hash: str
    chunk_index: int
    chunk_idempotency_key: str
    start: str
    end: str
    portable_scope_sha256: str
    execution_policy_sha256: str

    def __init__(
        self,
        *,
        run_key: str,
        plan_hash: str,
        chunk_index: int,
        chunk_idempotency_key: str,
        start: str,
        end: str,
        portable_scope_sha256: str,
        execution_policy_sha256: str,
        _issuer: object,
    ) -> None:
        if _issuer is not _RUNTIME_AUTHORITY_ISSUER:
            raise TypeError("backfill runtime authority can only be issued by the orchestrator")
        values = {
            "run_key": run_key,
            "plan_hash": plan_hash,
            "chunk_index": chunk_index,
            "chunk_idempotency_key": chunk_idempotency_key,
            "start": start,
            "end": end,
            "portable_scope_sha256": portable_scope_sha256,
            "execution_policy_sha256": execution_policy_sha256,
        }
        for name, value in values.items():
            object.__setattr__(self, name, value)

    def __deepcopy__(self, _memo: dict[int, Any]) -> _BackfillRuntimeAuthority:
        return self


def normalize_backfill_execution_policy(
    backfill_options: Mapping[str, Any] | None,
    *,
    operation_scope: Mapping[str, Any] | None = None,
    runtime_authority: Any = None,
) -> BackfillExecutionPolicy:
    """Validate and canonicalize authoring before ledger or source I/O."""

    raw = {} if backfill_options is None else backfill_options
    if not isinstance(raw, Mapping):
        raise ValueError("backfill must be an object")
    _reject_unknown_keys("backfill", raw, _EXECUTION_FIELDS | {"advisor", "chunk_context"})
    validate_backfill_advisor_options(raw)
    _validate_chunk_shape(raw)
    max_chunks = _bounded_integer(
        raw.get("max_chunks", DEFAULT_MAX_CHUNKS),
        field="backfill.max_chunks",
        maximum=_MAX_CHUNKS,
    )
    chunk_options = dict(raw)
    chunk_options["max_chunks"] = max_chunks
    policy = BackfillExecutionPolicy(
        inner_mode=inner_mode_from_options(raw),
        parallel_workers=_bounded_integer(
            raw.get("parallel_workers", 1),
            field="backfill.parallel_workers",
            maximum=_MAX_PARALLEL_WORKERS,
        ),
        chunk=chunk_spec_from_options(chunk_options),
        max_chunks=max_chunks,
        state=_normalize_state_policy(raw["state"] if "state" in raw else {}),
        publication=normalize_publication_policy(raw["publication"] if "publication" in raw else {}),
        state_dir=(_optional_path(raw["state_dir"], field="backfill.state_dir") if "state_dir" in raw else None),
        retry_policy=_authoring_retry_policy(raw),
        backfill_id=(
            _optional_text(raw["backfill_id"], field="backfill.backfill_id", maximum=128)
            if "backfill_id" in raw
            else None
        ),
        predicate_dialect=_enum(
            raw.get("predicate_dialect", "generic"),
            field="backfill.predicate_dialect",
            values=_PREDICATE_DIALECTS,
        ),
        lease_ttl_minutes=_bounded_integer(
            raw.get("lease_ttl_minutes", 60),
            field="backfill.lease_ttl_minutes",
            maximum=_MAX_LEASE_TTL_MINUTES,
        ),
    )
    if "chunk_context" in raw:
        _verify_runtime_chunk_context(policy, raw.get("chunk_context"), operation_scope, runtime_authority)
    elif runtime_authority is not None:
        raise ValueError("backfill runtime authority requires a runtime-issued chunk_context")
    return policy


def execution_policy_from_load_config(load_config: Any) -> BackfillExecutionPolicy:
    """Resolve authored options plus the orchestrator-issued authority."""

    authored_options = getattr(load_config, "options", {})
    options = {} if authored_options is None else authored_options
    if not isinstance(options, Mapping):
        raise ValueError("load options must be an object")
    scope = options.get(BACKFILL_OPERATION_SCOPE_OPTION)
    if scope is not None and not isinstance(scope, Mapping):
        raise ValueError("backfill operation scope must be an object")
    backfill_options = options["backfill"] if "backfill" in options else None
    if "backfill" in options and not isinstance(backfill_options, Mapping):
        raise ValueError("backfill must be an object")
    return normalize_backfill_execution_policy(
        backfill_options,
        operation_scope=scope,
        runtime_authority=options.get(BACKFILL_RUNTIME_AUTHORITY_OPTION),
    )


def validate_backfill_advisor_options(backfill_options: Mapping[str, Any]) -> dict[str, str] | None:
    """Validate advisory metadata without binding it to execution identity."""

    if "advisor" not in backfill_options:
        return None
    raw = backfill_options.get("advisor")
    if not isinstance(raw, Mapping):
        raise ValueError("backfill.advisor must be an object")
    _reject_unknown_keys("backfill.advisor", raw, _ADVISOR_FIELDS)
    return {
        "optimize_for": _enum(
            raw.get("optimize_for", "balanced"),
            field="backfill.advisor.optimize_for",
            values=_ADVISOR_PROFILES,
        )
    }


def issue_backfill_runtime_authority(
    *,
    policy: BackfillExecutionPolicy,
    chunk_context: Mapping[str, Any],
    operation_scope: Mapping[str, Any],
) -> Any:
    """Issue the in-memory proof paired with one orchestrated scope."""

    authority = _BackfillRuntimeAuthority(
        run_key=str(operation_scope.get("run_key") or ""),
        plan_hash=str(operation_scope.get("plan_hash") or ""),
        chunk_index=_runtime_chunk_index(operation_scope.get("chunk_index")),
        chunk_idempotency_key=str(operation_scope.get("chunk_idempotency_key") or ""),
        start=str(operation_scope.get("start") or ""),
        end=str(operation_scope.get("end") or ""),
        portable_scope_sha256=str(operation_scope.get("portable_scope_sha256") or ""),
        execution_policy_sha256=policy.digest,
        _issuer=_RUNTIME_AUTHORITY_ISSUER,
    )
    _verify_runtime_chunk_context(policy, chunk_context, operation_scope, authority)
    return authority


def normalize_retry_policy(retry_policy: str | None) -> str:
    if retry_policy is None:
        return "non_committed"
    if not isinstance(retry_policy, str) or not retry_policy.strip():
        raise ValueError("backfill.retry_policy must be one of: non_committed, failed_only")
    policy = retry_policy.strip().lower()
    if policy not in {"non_committed", "failed_only"}:
        raise ValueError("backfill.retry_policy must be one of: non_committed, failed_only")
    return policy


def _normalize_state_policy(value: Any) -> BackfillStatePolicy:
    if not isinstance(value, Mapping):
        raise ValueError("backfill.state must be an object")
    raw = value
    _reject_unknown_keys("backfill.state", raw, _STATE_FIELDS)
    backend = _enum(raw.get("backend", "local_file"), field="backfill.state.backend", values=_STATE_BACKENDS)
    schema_value = raw["schema"] if "schema" in raw else "DWH_Tech"
    if not isinstance(schema_value, str):
        raise ValueError("backfill.state.schema must be a non-empty string of at most 128 characters")
    schema = schema_value.strip()
    if not schema or len(schema) > 128:
        raise ValueError("backfill.state.schema must be a non-empty string of at most 128 characters")
    required = raw.get("require_distributed_lock", False)
    if not isinstance(required, bool):
        raise ValueError("backfill.state.require_distributed_lock must be a boolean")
    if backend == "local_file" and required:
        raise ValueError("backfill.state.require_distributed_lock=true requires backend=audit_schema")
    return BackfillStatePolicy(backend, schema, required)


def _validate_chunk_shape(raw: Mapping[str, Any]) -> None:
    if "chunk" not in raw:
        return
    chunk = raw.get("chunk")
    if not isinstance(chunk, Mapping) or not chunk:
        raise ValueError("backfill.chunk must be a non-empty object")
    _reject_unknown_keys("backfill.chunk", chunk, _CHUNK_FIELDS)


def _verify_runtime_chunk_context(
    policy: BackfillExecutionPolicy,
    chunk_context: Any,
    operation_scope: Mapping[str, Any] | None,
    runtime_authority: Any,
) -> None:
    if not isinstance(runtime_authority, _BackfillRuntimeAuthority):
        raise ValueError("backfill.chunk_context requires a runtime-issued orchestration authority")
    if not isinstance(chunk_context, Mapping) or not isinstance(operation_scope, Mapping):
        raise ValueError("backfill.chunk_context requires a runtime-issued MSSQL operation scope")
    if chunk_context.get("schema") != _RUNTIME_CONTEXT_SCHEMA:
        raise ValueError("backfill.chunk_context schema is not runtime-issued v1")
    if (
        operation_scope.get("kind") != "backfill_disjoint_range_v1"
        or operation_scope.get("proven_disjoint") is not True
    ):
        raise ValueError("backfill.chunk_context operation scope is not proven disjoint")
    context = _proof_values(chunk_context, context=True)
    scope = _proof_values(operation_scope, context=False)
    authority = {
        field: getattr(runtime_authority, field)
        for field in (
            "run_key",
            "plan_hash",
            "chunk_index",
            "chunk_idempotency_key",
            "start",
            "end",
            "portable_scope_sha256",
            "execution_policy_sha256",
        )
    }
    if context != scope or context != authority or context["execution_policy_sha256"] != policy.digest:
        raise ValueError("backfill.chunk_context does not match its runtime-issued operation scope")
    context_scope = parse_portable_relation_scope(chunk_context.get("portable_scope"))
    operation_portable_scope = parse_portable_relation_scope(operation_scope.get("portable_scope"))
    if portable_scope_contract(context_scope) != portable_scope_contract(operation_portable_scope):
        raise ValueError("backfill.chunk_context portable scope does not match its operation scope")
    if portable_scope_sha256(context_scope).hex() != context["portable_scope_sha256"]:
        raise ValueError("backfill.chunk_context portable scope digest is invalid")
    if any(value == "" for field, value in context.items() if field != "chunk_index"):
        raise ValueError("backfill.chunk_context runtime proof fields must be non-empty")


def _proof_values(raw: Mapping[str, Any], *, context: bool) -> dict[str, Any]:
    return {
        "run_key": str(raw.get("run_key") or ""),
        "plan_hash": str(raw.get("plan_hash") or ""),
        "chunk_index": _runtime_chunk_index(raw.get("index" if context else "chunk_index")),
        "chunk_idempotency_key": str(raw.get("idempotency_key" if context else "chunk_idempotency_key") or ""),
        "start": str(raw.get("start") or ""),
        "end": str(raw.get("end") or ""),
        "portable_scope_sha256": str(raw.get("portable_scope_sha256") or ""),
        "execution_policy_sha256": str(raw.get("execution_policy_sha256") or ""),
    }


def _runtime_chunk_index(value: Any) -> int:
    if type(value) is not int:
        raise ValueError("backfill runtime chunk index must be a positive integer")
    if value < 1:
        raise ValueError("backfill runtime chunk index must be a positive integer")
    return value


def _bounded_integer(value: Any, *, field: str, maximum: int) -> int:
    if type(value) is not int:
        raise ValueError(f"{field} must be an integer")
    if not 1 <= value <= maximum:
        raise ValueError(f"{field} must be between 1 and {maximum}")
    return value


def _authoring_retry_policy(raw: Mapping[str, Any]) -> str:
    if "retry_policy" not in raw:
        return normalize_retry_policy(None)
    value = raw["retry_policy"]
    if value is None:
        raise ValueError("backfill.retry_policy must be one of: non_committed, failed_only")
    return normalize_retry_policy(value)


def _enum(value: Any, *, field: str, values: frozenset[str]) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be one of: {', '.join(sorted(values))}")
    normalized = value.strip().lower()
    if normalized not in values:
        raise ValueError(f"{field} must be one of: {', '.join(sorted(values))}")
    return normalized


def _optional_text(value: Any, *, field: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized or len(normalized) > maximum:
        raise ValueError(f"{field} must be a non-empty string of at most {maximum} characters")
    return normalized


def _optional_path(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a text filesystem path")
    normalized = value.strip()
    if not normalized or len(normalized) > 4096:
        raise ValueError(f"{field} must be a non-empty path of at most 4096 characters")
    return normalized


def _reject_unknown_keys(field: str, raw: Mapping[str, Any], allowed: frozenset[str] | set[str]) -> None:
    unknown = sorted(str(key) for key in raw if not isinstance(key, str) or key not in allowed)
    if unknown:
        raise ValueError(f"{field}: unsupported options: {', '.join(unknown)}")


__all__ = [
    "BACKFILL_OPERATION_SCOPE_OPTION",
    "BACKFILL_RUNTIME_AUTHORITY_OPTION",
    "execution_policy_from_load_config",
    "issue_backfill_runtime_authority",
    "normalize_backfill_execution_policy",
    "normalize_retry_policy",
    "validate_backfill_advisor_options",
]
