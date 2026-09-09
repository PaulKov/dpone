"""Deterministic, secret-free dbt invocation identity."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass

from dpone.contracts.dbt_contract_validation import (
    DbtPublishingError,
    canonical_fingerprint,
    contract_error,
    require_digest,
    require_strict_mapping,
    require_strings,
    require_text,
    require_token,
)

DBT_INVOCATION_CONTEXT_SCHEMA = "dpone.dbt-invocation-context.v1"
DBT_INVOCATION_ENVIRONMENT_POLICY = "isolated_v1"
DBT_INDIRECT_SELECTION = "eager"
DBT_DYNAMIC_VARS = (
    "dpone_data_interval_end",
    "dpone_data_interval_start",
)
DBT_STATIC_ENVIRONMENT = (
    ("DBT_SEND_ANONYMOUS_USAGE_STATS", "false"),
    ("LANG", "C.UTF-8"),
    ("LC_ALL", "C.UTF-8"),
)
_ERROR = "DPONE_DBT_INVOCATION_CONTEXT_INVALID"
_TARGET_ERROR = "DPONE_DBT_PACK_INVALID"
_KEYS = frozenset(
    {
        "schema",
        "environment_policy",
        "indirect_selection",
        "static_environment",
        "dynamic_vars",
        "invocation_context_sha256",
    }
)


@dataclass(frozen=True, slots=True)
class DbtInvocationTarget:
    """Rendered, non-secret base target used to invoke the captured dbt project.

    This target is available before an execution pack exists. Its original pack
    validation error code and mapping stay stable for both import paths.
    """

    database: str
    schema: str

    def __post_init__(self) -> None:
        for name in ("database", "schema"):
            value = require_token(getattr(self, name), f"invocation {name}", _TARGET_ERROR)
            if len(value) > 256:
                raise contract_error(_TARGET_ERROR, f"invocation {name} exceeds 256 characters")

    @classmethod
    def from_mapping(cls, value: object) -> DbtInvocationTarget:
        raw = require_strict_mapping(value, "invocation_target", frozenset({"database", "schema"}), _TARGET_ERROR)
        return cls(**raw)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DbtInvocationContext:
    """Canonical dbt flags, environment policy and dynamic variable names."""

    environment_policy: str
    indirect_selection: str
    static_environment: tuple[tuple[str, str], ...]
    dynamic_vars: tuple[str, ...]
    invocation_context_sha256: str
    schema: str = DBT_INVOCATION_CONTEXT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != DBT_INVOCATION_CONTEXT_SCHEMA:
            raise _invalid("dbt invocation context schema is invalid")
        if self.environment_policy != DBT_INVOCATION_ENVIRONMENT_POLICY:
            raise _invalid("dbt invocation environment policy is unsupported")
        if self.indirect_selection != DBT_INDIRECT_SELECTION:
            raise _invalid("dbt indirect selection policy is unsupported")
        static_environment = _static_environment(self.static_environment)
        if not isinstance(self.dynamic_vars, tuple):
            raise _invalid("dbt dynamic_vars must be a tuple")
        dynamic_vars = tuple(
            sorted(
                require_strings(
                    list(self.dynamic_vars),
                    "dynamic_vars",
                    _ERROR,
                )
            )
        )
        if static_environment != DBT_STATIC_ENVIRONMENT:
            raise _invalid("dbt static environment differs from the isolated policy")
        if dynamic_vars != DBT_DYNAMIC_VARS:
            raise _invalid("dbt dynamic variables differ from the scheduler interval contract")
        object.__setattr__(self, "static_environment", static_environment)
        object.__setattr__(self, "dynamic_vars", dynamic_vars)
        require_digest(
            self.invocation_context_sha256,
            "invocation_context_sha256",
            _ERROR,
        )
        if self.invocation_context_sha256 != canonical_fingerprint(self._unsigned()):
            raise _invalid("dbt invocation context fingerprint differs from its content")

    @classmethod
    def canonical(cls) -> DbtInvocationContext:
        """Return the only supported v1 invocation context."""

        payload = _context_dict(
            environment_policy=DBT_INVOCATION_ENVIRONMENT_POLICY,
            indirect_selection=DBT_INDIRECT_SELECTION,
            static_environment=DBT_STATIC_ENVIRONMENT,
            dynamic_vars=DBT_DYNAMIC_VARS,
        )
        return cls(
            environment_policy=DBT_INVOCATION_ENVIRONMENT_POLICY,
            indirect_selection=DBT_INDIRECT_SELECTION,
            static_environment=DBT_STATIC_ENVIRONMENT,
            dynamic_vars=DBT_DYNAMIC_VARS,
            invocation_context_sha256=canonical_fingerprint(payload),
        )

    @classmethod
    def from_mapping(cls, value: object) -> DbtInvocationContext:
        raw = require_strict_mapping(value, "invocation_context", _KEYS, _ERROR)
        static_environment = raw.get("static_environment")
        if not isinstance(static_environment, Mapping):
            raise _invalid("dbt static_environment must be an object")
        return cls(
            environment_policy=require_text(
                raw.get("environment_policy"),
                "environment_policy",
                _ERROR,
            ),
            indirect_selection=require_text(
                raw.get("indirect_selection"),
                "indirect_selection",
                _ERROR,
            ),
            static_environment=tuple(
                (require_text(key, "static environment name", _ERROR), require_text(item, key, _ERROR))
                for key, item in static_environment.items()
            ),
            dynamic_vars=require_strings(raw.get("dynamic_vars"), "dynamic_vars", _ERROR),
            invocation_context_sha256=require_text(
                raw.get("invocation_context_sha256"),
                "invocation_context_sha256",
                _ERROR,
            ),
            schema=require_text(raw.get("schema"), "schema", _ERROR),
        )

    def environment(self, *, home: str) -> dict[str, str]:
        """Return one isolated process environment without ambient dbt values."""

        if not isinstance(home, str) or not home:
            raise _invalid("dbt invocation home must be non-empty")
        return {
            **dict(self.static_environment),
            "HOME": home,
            "PATH": "/usr/local/bin:/usr/bin:/bin",
        }

    def selection_vars_json(self) -> str:
        """Return deterministic build-plane placeholders for scheduler variables."""

        return json.dumps(
            {
                "dpone_data_interval_end": "2000-01-02T00:00:00Z",
                "dpone_data_interval_start": "2000-01-01T00:00:00Z",
            },
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )

    def _unsigned(self) -> dict[str, object]:
        return _context_dict(
            environment_policy=self.environment_policy,
            indirect_selection=self.indirect_selection,
            static_environment=self.static_environment,
            dynamic_vars=self.dynamic_vars,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            **self._unsigned(),
            "invocation_context_sha256": self.invocation_context_sha256,
        }


def _context_dict(
    *,
    environment_policy: str,
    indirect_selection: str,
    static_environment: tuple[tuple[str, str], ...],
    dynamic_vars: tuple[str, ...],
) -> dict[str, object]:
    return {
        "schema": DBT_INVOCATION_CONTEXT_SCHEMA,
        "environment_policy": environment_policy,
        "indirect_selection": indirect_selection,
        "static_environment": dict(static_environment),
        "dynamic_vars": list(dynamic_vars),
    }


def _static_environment(values: tuple[tuple[str, str], ...]) -> tuple[tuple[str, str], ...]:
    if not isinstance(values, tuple) or any(
        not isinstance(item, tuple)
        or len(item) != 2
        or not isinstance(item[0], str)
        or not item[0]
        or not isinstance(item[1], str)
        or not item[1]
        for item in values
    ):
        raise _invalid("dbt static environment is invalid")
    normalized = tuple(sorted(values))
    if len({name for name, _value in normalized}) != len(normalized):
        raise _invalid("dbt static environment contains duplicate names")
    return normalized


def _invalid(message: str) -> DbtPublishingError:
    return contract_error(_ERROR, message)


__all__ = [
    "DBT_DYNAMIC_VARS",
    "DBT_INDIRECT_SELECTION",
    "DBT_INVOCATION_CONTEXT_SCHEMA",
    "DBT_INVOCATION_ENVIRONMENT_POLICY",
    "DBT_STATIC_ENVIRONMENT",
    "DbtInvocationContext",
    "DbtInvocationTarget",
]
