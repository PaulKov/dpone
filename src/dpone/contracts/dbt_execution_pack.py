"""Strict immutable dbt execution pack contract."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from typing import Any

from dpone.contracts.credential_env import is_valid_connection_ref
from dpone.contracts.dbt_contract_validation import (
    canonical_fingerprint,
    contract_error,
    require_digest,
    require_positive,
    require_relative,
    require_strict_mapping,
    require_token,
)
from dpone.contracts.dbt_execution_evidence import DBT_WARNING_POLICIES
from dpone.contracts.dbt_identifiers import dbt_workflow_id
from dpone.contracts.dbt_invocation import DbtInvocationContext, DbtInvocationTarget
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2
from dpone.contracts.dbt_selection_lock import DbtSelectionLock
from dpone.contracts.dbt_sqlserver_policy import (
    DbtSqlServerAdapterPolicy,
    DbtSqlServerRuntimePolicy,
    require_dbt_process_timeout,
)
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED

DBT_EXECUTION_PACK_SCHEMA = "dpone.dbt-execution-pack.v1"
DBT_EXECUTION_PACK_SCHEMA_V2 = "dpone.dbt-execution-pack.v2"
SUPPORTED_DBT_CORE_VERSION = DBT_SQLSERVER_1_12_CERTIFIED.dbt_core_version
SUPPORTED_DBT_ADAPTER = DBT_SQLSERVER_1_12_CERTIFIED.adapter_name
SUPPORTED_DBT_ADAPTER_VERSION = DBT_SQLSERVER_1_12_CERTIFIED.adapter_version
SUPPORTED_DBT_MANIFEST_SCHEMA_VERSION = DBT_SQLSERVER_1_12_CERTIFIED.manifest_schema_version
SUPPORTED_DBT_RUN_RESULTS_SCHEMA_VERSION = DBT_SQLSERVER_1_12_CERTIFIED.run_results_schema_version
_PACK_ERROR = "DPONE_DBT_PACK_INVALID"
_PROFILE_KEYS = frozenset("profile_name target_name connection_ref adapter_type database schema threads".split())
_PACK_KEYS = frozenset(
    "schema workflow_id project_bundle_sha256 project_subdir target_path "
    "profile selection_lock invocation_context adapter_runtime adapter_policy dbt_core_version "
    "dbt_adapter_version manifest_schema_version run_results_schema_version "
    "dbt_warning_policy timeout_seconds pack_sha256".split()
)
_PACK_BUILD_KEYS = _PACK_KEYS - frozenset(
    "schema pack_sha256 dbt_core_version dbt_adapter_version manifest_schema_version run_results_schema_version".split()
)


@dataclass(frozen=True, slots=True)
class DbtProfileSpec:
    """Logical dbt profile identity with one environment binding reference."""

    profile_name: str
    target_name: str
    connection_ref: str
    adapter_type: str
    database: str
    schema: str
    threads: int

    def __post_init__(self) -> None:
        for name in (
            "profile_name",
            "target_name",
            "adapter_type",
            "database",
            "schema",
        ):
            require_token(getattr(self, name), name, _PACK_ERROR)
        if not is_valid_connection_ref(self.connection_ref):
            raise contract_error(
                _PACK_ERROR,
                "profile connection_ref is invalid",
            )
        require_positive(self.threads, "profile threads", _PACK_ERROR)

    @classmethod
    def from_mapping(cls, value: object) -> DbtProfileSpec:
        """Parse and validate one strict profile mapping."""

        raw = require_strict_mapping(
            value,
            "profile",
            _PROFILE_KEYS,
            _PACK_ERROR,
        )
        return cls(**raw)

    def to_dict(self) -> dict[str, object]:
        """Return the canonical public mapping."""

        return asdict(self)


@dataclass(frozen=True, slots=True)
class DbtExecutionPack:
    """Pinned shell-free invocation contract for one dbt workflow."""

    workflow_id: str
    project_bundle_sha256: str
    project_subdir: str
    target_path: str
    profile: DbtProfileSpec
    selection_lock: DbtSelectionLock
    invocation_context: DbtInvocationContext
    adapter_runtime: DbtSqlServerRuntimePolicy
    adapter_policy: DbtSqlServerAdapterPolicy
    dbt_warning_policy: str
    timeout_seconds: int
    pack_sha256: str
    dbt_core_version: str = SUPPORTED_DBT_CORE_VERSION
    dbt_adapter_version: str = SUPPORTED_DBT_ADAPTER_VERSION
    manifest_schema_version: str = SUPPORTED_DBT_MANIFEST_SCHEMA_VERSION
    run_results_schema_version: str = SUPPORTED_DBT_RUN_RESULTS_SCHEMA_VERSION
    schema: str = DBT_EXECUTION_PACK_SCHEMA
    invocation_target: DbtInvocationTarget | None = None

    def __post_init__(self) -> None:
        if self.schema not in (DBT_EXECUTION_PACK_SCHEMA, DBT_EXECUTION_PACK_SCHEMA_V2):
            raise contract_error(
                _PACK_ERROR,
                "dbt execution pack schema is invalid",
            )
        if (self.schema == DBT_EXECUTION_PACK_SCHEMA and self.invocation_target is not None) or (
            self.schema == DBT_EXECUTION_PACK_SCHEMA_V2 and not isinstance(self.invocation_target, DbtInvocationTarget)
        ):
            raise contract_error(_PACK_ERROR, "invocation target differs from execution pack version")
        require_token(self.workflow_id, "workflow_id", _PACK_ERROR)
        try:
            canonical_workflow_id = dbt_workflow_id(self.workflow_id)
        except ValueError as exc:
            raise contract_error(
                _PACK_ERROR,
                "dbt workflow_id must match [a-z][a-z0-9_]{0,63}",
            ) from exc
        if canonical_workflow_id != self.workflow_id:
            raise contract_error(
                _PACK_ERROR,
                "dbt workflow_id must be a canonical generated identity",
            )
        require_digest(
            self.project_bundle_sha256,
            "project bundle sha256",
            _PACK_ERROR,
        )
        require_relative(
            self.project_subdir,
            "project_subdir",
            _PACK_ERROR,
            allow_dot=True,
        )
        require_relative(self.target_path, "target_path", _PACK_ERROR)
        if (
            not isinstance(self.profile, DbtProfileSpec)
            or not isinstance(self.selection_lock, DbtSelectionLock)
            or not isinstance(self.invocation_context, DbtInvocationContext)
            or not isinstance(self.adapter_runtime, DbtSqlServerRuntimePolicy)
            or not isinstance(self.adapter_policy, DbtSqlServerAdapterPolicy)
        ):
            raise contract_error(
                _PACK_ERROR,
                "dbt execution pack nested contracts are invalid",
            )
        if (
            self.dbt_core_version,
            self.profile.adapter_type,
            self.dbt_adapter_version,
            self.manifest_schema_version,
            self.run_results_schema_version,
        ) != (
            SUPPORTED_DBT_CORE_VERSION,
            SUPPORTED_DBT_ADAPTER,
            SUPPORTED_DBT_ADAPTER_VERSION,
            SUPPORTED_DBT_MANIFEST_SCHEMA_VERSION,
            SUPPORTED_DBT_RUN_RESULTS_SCHEMA_VERSION,
        ):
            raise contract_error(
                _PACK_ERROR,
                "dbt execution pack toolchain contract is unsupported",
            )
        if self.selection_lock.toolchain_sha256 != DBT_SQLSERVER_1_12_CERTIFIED.sha256:
            raise contract_error(
                _PACK_ERROR,
                "dbt selection toolchain differs from the certified execution pack",
            )
        if self.selection_lock.invocation_context_sha256 != self.invocation_context.invocation_context_sha256:
            raise contract_error(
                _PACK_ERROR,
                "dbt selection invocation differs from the execution pack",
            )
        if self.dbt_warning_policy not in DBT_WARNING_POLICIES:
            raise contract_error(
                _PACK_ERROR,
                "dbt warning policy must be fail or allow",
            )
        require_positive(self.timeout_seconds, "timeout_seconds", _PACK_ERROR)
        require_dbt_process_timeout(self.timeout_seconds)
        if self.adapter_runtime.dbt_process_timeout_seconds != self.timeout_seconds:
            raise contract_error(
                _PACK_ERROR,
                "adapter runtime timeout hierarchy differs from timeout_seconds",
            )
        require_digest(self.pack_sha256, "pack sha256", _PACK_ERROR)
        if self.pack_sha256 != canonical_fingerprint(self._unsigned()):
            raise contract_error(
                _PACK_ERROR,
                "dbt execution pack fingerprint differs from its content",
            )

    @classmethod
    def build(cls, **values: Any) -> DbtExecutionPack:
        """Build a pack with a canonical content fingerprint."""

        if set(values) != _PACK_BUILD_KEYS:
            raise contract_error(
                _PACK_ERROR,
                f"execution pack build requires exactly {sorted(_PACK_BUILD_KEYS)}",
            )
        return cls(
            **values,
            pack_sha256=canonical_fingerprint(_pack_dict(values)),
        )

    @classmethod
    def build_v2(cls, *, invocation_target: DbtInvocationTarget, **values: Any) -> DbtExecutionPack:
        """Build an explicit v2 pack; the default builder continues emitting v1."""

        if set(values) != _PACK_BUILD_KEYS or not isinstance(invocation_target, DbtInvocationTarget):
            raise contract_error(_PACK_ERROR, "v2 execution pack build requires exact fields and invocation target")
        unsigned = _pack_dict(values, schema=DBT_EXECUTION_PACK_SCHEMA_V2, invocation_target=invocation_target)
        return cls(
            **values,
            schema=DBT_EXECUTION_PACK_SCHEMA_V2,
            invocation_target=invocation_target,
            pack_sha256=canonical_fingerprint(unsigned),
        )

    @classmethod
    def from_mapping(cls, value: object) -> DbtExecutionPack:
        """Parse and validate one strict execution pack."""

        schema = value.get("schema") if isinstance(value, Mapping) else None
        if schema not in (DBT_EXECUTION_PACK_SCHEMA, DBT_EXECUTION_PACK_SCHEMA_V2):
            raise contract_error(_PACK_ERROR, "dbt execution pack schema is invalid")
        v2 = schema == DBT_EXECUTION_PACK_SCHEMA_V2
        raw = require_strict_mapping(
            value,
            "execution_pack",
            _PACK_KEYS | {"invocation_target"} if v2 else _PACK_KEYS,
            _PACK_ERROR,
        )
        nested = {
            "profile",
            "selection_lock",
            "invocation_context",
            "adapter_runtime",
            "adapter_policy",
        }
        values = {name: raw[name] for name in _PACK_BUILD_KEYS - nested}
        return cls(
            **values,
            profile=DbtProfileSpec.from_mapping(raw.get("profile")),
            selection_lock=DbtSelectionLock.from_mapping(raw.get("selection_lock")),
            invocation_context=DbtInvocationContext.from_mapping(raw.get("invocation_context")),
            adapter_runtime=DbtSqlServerRuntimePolicy.from_mapping(raw.get("adapter_runtime")),
            adapter_policy=DbtSqlServerAdapterPolicy.from_mapping(raw.get("adapter_policy")),
            pack_sha256=raw["pack_sha256"],
            dbt_core_version=raw["dbt_core_version"],
            dbt_adapter_version=raw["dbt_adapter_version"],
            manifest_schema_version=raw["manifest_schema_version"],
            run_results_schema_version=raw["run_results_schema_version"],
            schema=raw["schema"],
            invocation_target=DbtInvocationTarget.from_mapping(raw["invocation_target"]) if v2 else None,
        )

    def _unsigned(self) -> dict[str, object]:
        return _pack_dict(
            {name: getattr(self, name) for name in _PACK_BUILD_KEYS},
            schema=self.schema,
            invocation_target=self.invocation_target,
        )

    def invocation_profile(self) -> DbtProfileSpec:
        """Render the base target without reinterpreting the effective model target."""

        if self.invocation_target is None:
            return self.profile
        return replace(self.profile, database=self.invocation_target.database, schema=self.invocation_target.schema)

    def require_wire_contract(self, wire_contract: str) -> None:
        """Reject unknown or mixed versions; never infer a wire from target fields."""

        expected = {DBT_RUNTIME_WIRE_V1: DBT_EXECUTION_PACK_SCHEMA, DBT_RUNTIME_WIRE_V2: DBT_EXECUTION_PACK_SCHEMA_V2}
        if expected.get(wire_contract) != self.schema:
            raise contract_error(_PACK_ERROR, "dbt execution pack differs from release wire version")

    def to_dict(self) -> dict[str, object]:
        """Return the canonical public mapping."""

        return {**self._unsigned(), "pack_sha256": self.pack_sha256}


def _pack_dict(
    values: Mapping[str, Any],
    *,
    schema: str = DBT_EXECUTION_PACK_SCHEMA,
    invocation_target: DbtInvocationTarget | None = None,
) -> dict[str, object]:
    nested = {
        "profile",
        "selection_lock",
        "invocation_context",
        "adapter_runtime",
        "adapter_policy",
    }
    return {
        "schema": schema,
        **({"invocation_target": invocation_target.to_dict()} if invocation_target is not None else {}),
        **{name: values[name] for name in _PACK_BUILD_KEYS - nested},
        "dbt_core_version": SUPPORTED_DBT_CORE_VERSION,
        "dbt_adapter_version": SUPPORTED_DBT_ADAPTER_VERSION,
        "manifest_schema_version": SUPPORTED_DBT_MANIFEST_SCHEMA_VERSION,
        "run_results_schema_version": SUPPORTED_DBT_RUN_RESULTS_SCHEMA_VERSION,
        "profile": values["profile"].to_dict(),
        "selection_lock": values["selection_lock"].to_dict(),
        "invocation_context": values["invocation_context"].to_dict(),
        "adapter_runtime": values["adapter_runtime"].to_dict(),
        "adapter_policy": values["adapter_policy"].to_dict(),
    }


def dbt_target_identity_sha256(profile: DbtProfileSpec) -> str:
    """Fingerprint the logical, environment-neutral dbt target."""

    return canonical_fingerprint(
        {
            "profile_name": profile.profile_name,
            "target_name": profile.target_name,
            "connection_ref": profile.connection_ref,
            "adapter_type": profile.adapter_type,
            "database": profile.database,
            "schema": profile.schema,
        }
    )


__all__ = [
    "DBT_EXECUTION_PACK_SCHEMA",
    "DBT_EXECUTION_PACK_SCHEMA_V2",
    "SUPPORTED_DBT_ADAPTER",
    "SUPPORTED_DBT_ADAPTER_VERSION",
    "SUPPORTED_DBT_CORE_VERSION",
    "SUPPORTED_DBT_MANIFEST_SCHEMA_VERSION",
    "SUPPORTED_DBT_RUN_RESULTS_SCHEMA_VERSION",
    "DbtExecutionPack",
    "DbtInvocationTarget",
    "DbtProfileSpec",
    "dbt_target_identity_sha256",
]
