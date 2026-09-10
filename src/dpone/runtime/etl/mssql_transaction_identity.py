"""Runtime identity wiring and connector resolution; pure policy lives in contracts.

Public signatures remain stable. Runtime-owned backfill admission and route
providers are supplied explicitly to canonical request and scope construction.
"""

from __future__ import annotations

from typing import Any

from dpone.backfill.execution_policy import execution_policy_from_load_config
from dpone.contracts import mssql_transaction_identity as _policy
from dpone.contracts.mssql_transaction_identity import (
    _POSTGRES_AUTHORITY_IDENTITY_VERSIONS as _POSTGRES_AUTHORITY_IDENTITY_VERSIONS,
)
from dpone.contracts.mssql_transaction_identity import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlOperationRequest,
    MssqlTransactionContractError,
    SourcePhysicalIdentity,
)
from dpone.contracts.mssql_transaction_identity import MssqlGenericCommitReceipt as MssqlGenericCommitReceipt
from dpone.contracts.mssql_transaction_identity import MssqlTransactionOperation as MssqlTransactionOperation
from dpone.contracts.mssql_transaction_identity import _canonical as _canonical
from dpone.contracts.mssql_transaction_identity import (
    _lease_expiry as _lease_expiry,
)
from dpone.contracts.mssql_transaction_identity import _portable_scope_identity as _portable_scope_identity
from dpone.contracts.mssql_transaction_identity import (
    _required_scope as _required_scope,
)
from dpone.contracts.mssql_transaction_identity import (
    is_mssql_generic_commit_receipt as is_mssql_generic_commit_receipt,
)
from dpone.contracts.mssql_transaction_identity import (
    is_mssql_transaction_operation as is_mssql_transaction_operation,
)
from dpone.contracts.mssql_transaction_identity import (
    mssql_operation_owner_digest as mssql_operation_owner_digest,
)
from dpone.contracts.mssql_transaction_identity import operation_owner_digest as operation_owner_digest
from dpone.contracts.mssql_transaction_identity import operation_scope_hash as operation_scope_hash
from dpone.contracts.mssql_transaction_identity import parse_portable_relation_scope as parse_portable_relation_scope
from dpone.contracts.mssql_transaction_identity import portable_scope_contract as portable_scope_contract
from dpone.contracts.mssql_transaction_identity import portable_scope_sha256 as portable_scope_sha256
from dpone.contracts.mssql_transaction_identity import require_authority_sha256 as require_authority_sha256
from dpone.contracts.mssql_transaction_identity import (
    require_source_physical_identity_binding as require_source_physical_identity_binding,
)
from dpone.runtime.etl.mssql_transaction_route_identity import (
    _backfill_execution_policy_digest,
    invocation_route_fingerprint,
)


def invocation_identity(run_context: Any, load_config: Any, *, dag_id: str | None) -> InvocationIdentity:
    """Resolve scheduler-stable identity using the runtime backfill authority."""
    return _policy.invocation_identity(
        run_context, load_config, dag_id=dag_id, execution_policy_resolver=execution_policy_from_load_config
    )


def _backfill_campaign_invocation_id(load_config: Any) -> str | None:
    return _policy._backfill_campaign_invocation_id(load_config, execution_policy_from_load_config)


def build_mssql_attempt_request(
    load_config: Any,
    *,
    invocation: InvocationIdentity,
    target_identity: bytes,
    source_identity: SourcePhysicalIdentity,
    load_id: str,
    request_coordinates: tuple[str, str, str],
) -> MssqlAttemptRequest:
    """Build admission identity with the runtime route fingerprint provider."""
    return _policy.build_mssql_attempt_request(
        load_config,
        invocation=invocation,
        target_identity=target_identity,
        source_identity=source_identity,
        load_id=load_id,
        request_coordinates=request_coordinates,
        route_fingerprint=invocation_route_fingerprint,
    )


def operation_request(load_config: Any, invocation: InvocationIdentity) -> MssqlOperationRequest:
    """Resolve operation scope with the runtime-proven backfill policy digest."""
    return _policy.operation_request(load_config, invocation, backfill_policy_digest=_backfill_execution_policy_digest)


def resolve_source_physical_identity(source: Any, load_config: Any) -> SourcePhysicalIdentity:
    """Resolve a database-issued binding before the pure route hash is built."""
    expected, expected_digest = require_source_physical_identity_binding(load_config)
    resolver = getattr(source, "mssql_transaction_source_physical_identity", None)
    if not callable(resolver):
        raise MssqlTransactionContractError("mssql_transaction.source_physical_identity_capability_required")
    resolved = resolver(load_config)
    return _policy.validate_source_physical_identity(
        resolved, load_config, expected=expected, expected_digest=expected_digest
    )


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
