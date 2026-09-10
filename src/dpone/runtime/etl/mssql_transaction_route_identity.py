"""Runtime wiring for canonical MSSQL route identities, preserving v1 imports.

Only runtime codec/authority providers are selected here. All normalization,
portable-scope checks and canonical hashing belong to the contracts policy.
"""

from __future__ import annotations

from typing import Any

from dpone.backfill.execution_policy import BACKFILL_RUNTIME_AUTHORITY_OPTION, execution_policy_from_load_config
from dpone.backfill.shadow_append_authority import SHADOW_APPEND_AUTHORITY_OPTION
from dpone.config.load_config import ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION
from dpone.contracts import mssql_transaction_route_identity as _policy
from dpone.contracts.mssql_transaction_route_identity import (
    PORTABLE_SCOPE_BINDING_OPTION,
    SourcePhysicalIdentity,
    route_fingerprint,
)
from dpone.contracts.mssql_transaction_route_identity import (
    _backfill_inner_mode as _backfill_inner_mode,
)
from dpone.contracts.mssql_transaction_route_identity import (
    _canonical as _canonical,
)
from dpone.contracts.mssql_transaction_route_identity import (
    _global_source_predicate as _global_source_predicate,
)
from dpone.contracts.mssql_transaction_route_identity import (
    _global_target_predicate as _global_target_predicate,
)
from dpone.contracts.mssql_transaction_route_identity import (
    _portable_scope_identity as _portable_scope_identity,
)
from dpone.contracts.mssql_transaction_route_identity import (
    _restore_v1_authored_endpoint_types as _restore_v1_authored_endpoint_types,
)
from dpone.contracts.mssql_transaction_route_identity import (
    _route_portable_scope_identity as _route_portable_scope_identity,
)
from dpone.contracts.mssql_transaction_route_identity import (
    _v1_identity_options as _v1_identity_options,
)
from dpone.runtime.etl.mssql_transaction_wire_identity import is_postgres_mssql_wire_route, mssql_codec_contract
from dpone.runtime.mssql_spool_route import MSSQL_CHARACTER_SPOOL_PREFLIGHT_OPTION

_RUNTIME_OPTION_KEYS = {
    "__dpone_load_identity",
    BACKFILL_RUNTIME_AUTHORITY_OPTION,
    SHADOW_APPEND_AUTHORITY_OPTION,
    "__dpone_mssql_transaction_admission",
    "__dpone_mssql_transaction_lease",
    "__dpone_mssql_operation_scope",
    MSSQL_CHARACTER_SPOOL_PREFLIGHT_OPTION,
    PORTABLE_SCOPE_BINDING_OPTION,
    "__dpone_postgres_mssql_internal_wire",
    "_source_connector",
    "_source_conn_id",
    "_source_schema",
    "_source_table",
    "load_id",
    "run_id",
    "source_custom_predicate",
    ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION,
}


def invocation_route_fingerprint(
    load_config: Any, *, target_identity: bytes, source_identity: SourcePhysicalIdentity
) -> bytes:
    """Preserve the public entry point while supplying explicit runtime providers."""
    return _policy.invocation_route_fingerprint(
        load_config,
        target_identity=target_identity,
        source_identity=source_identity,
        runtime_option_keys=_RUNTIME_OPTION_KEYS,
        codec_contract=mssql_codec_contract,
        character_wire_route=is_postgres_mssql_wire_route,
        execution_policy_resolver=execution_policy_from_load_config,
        fingerprint=route_fingerprint,
    )


def _invocation_options(raw: Any) -> dict[str, Any]:
    return _policy._invocation_options(raw, _RUNTIME_OPTION_KEYS)


def _backfill_execution_policy_digest(load_config: Any) -> str | None:
    return _policy._backfill_execution_policy_digest(load_config, execution_policy_from_load_config)


__all__ = ["invocation_route_fingerprint"]
