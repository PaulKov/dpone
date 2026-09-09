"""Canonical SQL Server adapter execution and project safety policies."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from dpone.contracts.dbt_contract_validation import (
    canonical_fingerprint,
    contract_error,
    require_digest,
    require_strict_mapping,
)

DBT_SQLSERVER_RUNTIME_POLICY_SCHEMA = "dpone.dbt-sqlserver-runtime-policy.v1"
DBT_SQLSERVER_ADAPTER_POLICY_SCHEMA = "dpone.dbt-sqlserver-capability-policy.v1"
DBT_SQLSERVER_POLICY_ERROR = "DPONE_DBT_SQLSERVER_RUNTIME_POLICY_INVALID"
MAX_DBT_SQLSERVER_PROJECT_YAML_BYTES = 1024 * 1024
DBT_PROCESS_TIMEOUT_MIN_SECONDS = 600
DBT_PROCESS_TIMEOUT_MAX_SECONDS = 86_400
DBT_LOGIN_TIMEOUT_SECONDS = 15
DBT_QUERY_COMPLETION_RESERVE_SECONDS = 300
AIRFLOW_COMPLETION_RESERVE_SECONDS = 300

DBT_SQLSERVER_REQUIRED_PROJECT_FLAGS: Mapping[str, bool] = MappingProxyType(
    {
        "dbt_sqlserver_enable_safe_type_expansion": False,
        "dbt_sqlserver_use_dbt_transactions": True,
        "dbt_sqlserver_use_default_schema_concat": True,
        "dbt_sqlserver_use_native_string_types": True,
    }
)

_RUNTIME_KEYS = frozenset(
    {
        "schema",
        "backend",
        "retries",
        "login_timeout_seconds",
        "query_timeout_seconds",
        "adapter_runtime_sha256",
    }
)
_ADAPTER_KEYS = frozenset(
    {
        "schema",
        "required_project_flags",
        "adapter_policy_sha256",
    }
)


def require_dbt_process_timeout(value: object) -> int:
    """Return one bounded process timeout accepted by the preview policy."""

    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not DBT_PROCESS_TIMEOUT_MIN_SECONDS <= value <= DBT_PROCESS_TIMEOUT_MAX_SECONDS
    ):
        raise contract_error(
            DBT_SQLSERVER_POLICY_ERROR,
            "dbt process timeout must be an integer from 600 through 86400",
        )
    return value


@dataclass(frozen=True, slots=True)
class DbtSqlServerRuntimePolicy:
    """Effective adapter runtime values rendered into the private dbt profile."""

    backend: str
    retries: int
    login_timeout_seconds: int
    query_timeout_seconds: int
    adapter_runtime_sha256: str
    schema: str = DBT_SQLSERVER_RUNTIME_POLICY_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != DBT_SQLSERVER_RUNTIME_POLICY_SCHEMA:
            raise contract_error(DBT_SQLSERVER_POLICY_ERROR, "runtime policy schema is unsupported")
        if self.backend != "pyodbc":
            raise contract_error(DBT_SQLSERVER_POLICY_ERROR, "backend must equal pyodbc")
        if isinstance(self.retries, bool) or self.retries != 1:
            raise contract_error(DBT_SQLSERVER_POLICY_ERROR, "retries must equal 1")
        if isinstance(self.login_timeout_seconds, bool) or self.login_timeout_seconds != DBT_LOGIN_TIMEOUT_SECONDS:
            raise contract_error(DBT_SQLSERVER_POLICY_ERROR, "login timeout must equal 15 seconds")
        if isinstance(self.query_timeout_seconds, bool) or not isinstance(self.query_timeout_seconds, int):
            raise contract_error(
                DBT_SQLSERVER_POLICY_ERROR,
                "query timeout must be an integer",
            )
        if self.query_timeout_seconds <= self.login_timeout_seconds:
            raise contract_error(
                DBT_SQLSERVER_POLICY_ERROR,
                "query timeout must exceed login timeout",
            )
        process_timeout = self.query_timeout_seconds + DBT_QUERY_COMPLETION_RESERVE_SECONDS
        require_dbt_process_timeout(process_timeout)
        require_digest(
            self.adapter_runtime_sha256,
            "adapter runtime sha256",
            DBT_SQLSERVER_POLICY_ERROR,
        )
        if self.adapter_runtime_sha256 != canonical_fingerprint(self._unsigned()):
            raise contract_error(
                DBT_SQLSERVER_POLICY_ERROR,
                "adapter runtime fingerprint differs from its content",
            )

    @classmethod
    def for_process_timeout(cls, timeout_seconds: object) -> DbtSqlServerRuntimePolicy:
        """Derive query budget from one bounded outer dbt process timeout."""

        process_timeout = require_dbt_process_timeout(timeout_seconds)
        values = {
            "backend": "pyodbc",
            "retries": 1,
            "login_timeout_seconds": DBT_LOGIN_TIMEOUT_SECONDS,
            "query_timeout_seconds": process_timeout - DBT_QUERY_COMPLETION_RESERVE_SECONDS,
        }
        return cls(
            backend="pyodbc",
            retries=1,
            login_timeout_seconds=DBT_LOGIN_TIMEOUT_SECONDS,
            query_timeout_seconds=process_timeout - DBT_QUERY_COMPLETION_RESERVE_SECONDS,
            adapter_runtime_sha256=canonical_fingerprint(_runtime_dict(values)),
        )

    @classmethod
    def from_mapping(cls, value: object) -> DbtSqlServerRuntimePolicy:
        """Parse one strict serialized runtime policy."""

        raw = require_strict_mapping(
            value,
            "adapter_runtime",
            _RUNTIME_KEYS,
            DBT_SQLSERVER_POLICY_ERROR,
        )
        return cls(
            backend=raw["backend"],
            retries=raw["retries"],
            login_timeout_seconds=raw["login_timeout_seconds"],
            query_timeout_seconds=raw["query_timeout_seconds"],
            adapter_runtime_sha256=raw["adapter_runtime_sha256"],
            schema=raw["schema"],
        )

    @property
    def dbt_process_timeout_seconds(self) -> int:
        return self.query_timeout_seconds + DBT_QUERY_COMPLETION_RESERVE_SECONDS

    def airflow_execution_timeout_seconds(self, process_timeout_seconds: object) -> int:
        process_timeout = require_dbt_process_timeout(process_timeout_seconds)
        if process_timeout != self.dbt_process_timeout_seconds:
            raise contract_error(
                DBT_SQLSERVER_POLICY_ERROR,
                "runtime query timeout differs from the dbt process timeout",
            )
        return process_timeout + AIRFLOW_COMPLETION_RESERVE_SECONDS

    def _unsigned(self) -> dict[str, object]:
        return _runtime_dict(
            {
                "backend": self.backend,
                "retries": self.retries,
                "login_timeout_seconds": self.login_timeout_seconds,
                "query_timeout_seconds": self.query_timeout_seconds,
            }
        )

    def to_dict(self) -> dict[str, object]:
        return {
            **self._unsigned(),
            "adapter_runtime_sha256": self.adapter_runtime_sha256,
        }


@dataclass(frozen=True, slots=True)
class DbtSqlServerAdapterPolicy:
    """Exact project behavior flags admitted by the pinned adapter policy."""

    required_project_flags: Mapping[str, bool]
    adapter_policy_sha256: str
    schema: str = DBT_SQLSERVER_ADAPTER_POLICY_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != DBT_SQLSERVER_ADAPTER_POLICY_SCHEMA:
            raise contract_error(DBT_SQLSERVER_POLICY_ERROR, "adapter policy schema is unsupported")
        if dict(self.required_project_flags) != dict(DBT_SQLSERVER_REQUIRED_PROJECT_FLAGS):
            raise contract_error(
                DBT_SQLSERVER_POLICY_ERROR,
                "required project flags differ from the certified policy",
            )
        object.__setattr__(
            self,
            "required_project_flags",
            MappingProxyType(dict(sorted(self.required_project_flags.items()))),
        )
        require_digest(
            self.adapter_policy_sha256,
            "adapter policy sha256",
            DBT_SQLSERVER_POLICY_ERROR,
        )
        if self.adapter_policy_sha256 != canonical_fingerprint(self._unsigned()):
            raise contract_error(
                DBT_SQLSERVER_POLICY_ERROR,
                "adapter policy fingerprint differs from its content",
            )

    @classmethod
    def canonical(cls) -> DbtSqlServerAdapterPolicy:
        values = dict(DBT_SQLSERVER_REQUIRED_PROJECT_FLAGS)
        return cls(
            required_project_flags=values,
            adapter_policy_sha256=canonical_fingerprint(_adapter_dict(values)),
        )

    @classmethod
    def from_mapping(cls, value: object) -> DbtSqlServerAdapterPolicy:
        raw = require_strict_mapping(
            value,
            "adapter_policy",
            _ADAPTER_KEYS,
            DBT_SQLSERVER_POLICY_ERROR,
        )
        flags = raw["required_project_flags"]
        if not isinstance(flags, Mapping):
            raise contract_error(
                DBT_SQLSERVER_POLICY_ERROR,
                "required project flags must be a mapping",
            )
        return cls(
            required_project_flags=flags,
            adapter_policy_sha256=raw["adapter_policy_sha256"],
            schema=raw["schema"],
        )

    def _unsigned(self) -> dict[str, object]:
        return _adapter_dict(self.required_project_flags)

    def to_dict(self) -> dict[str, object]:
        return {
            **self._unsigned(),
            "adapter_policy_sha256": self.adapter_policy_sha256,
        }


def _runtime_dict(values: Mapping[str, Any]) -> dict[str, object]:
    return {
        "schema": DBT_SQLSERVER_RUNTIME_POLICY_SCHEMA,
        "backend": values["backend"],
        "retries": values["retries"],
        "login_timeout_seconds": values["login_timeout_seconds"],
        "query_timeout_seconds": values["query_timeout_seconds"],
    }


def _adapter_dict(flags: Mapping[str, bool]) -> dict[str, object]:
    return {
        "schema": DBT_SQLSERVER_ADAPTER_POLICY_SCHEMA,
        "required_project_flags": dict(sorted(flags.items())),
    }


__all__ = [
    "AIRFLOW_COMPLETION_RESERVE_SECONDS",
    "DBT_LOGIN_TIMEOUT_SECONDS",
    "DBT_PROCESS_TIMEOUT_MAX_SECONDS",
    "DBT_PROCESS_TIMEOUT_MIN_SECONDS",
    "DBT_QUERY_COMPLETION_RESERVE_SECONDS",
    "DBT_SQLSERVER_ADAPTER_POLICY_SCHEMA",
    "DBT_SQLSERVER_POLICY_ERROR",
    "DBT_SQLSERVER_REQUIRED_PROJECT_FLAGS",
    "DBT_SQLSERVER_RUNTIME_POLICY_SCHEMA",
    "DbtSqlServerAdapterPolicy",
    "DbtSqlServerRuntimePolicy",
    "MAX_DBT_SQLSERVER_PROJECT_YAML_BYTES",
    "require_dbt_process_timeout",
]
