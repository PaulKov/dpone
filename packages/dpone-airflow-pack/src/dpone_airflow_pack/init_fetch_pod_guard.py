"""Fail-closed guards for pack and DAG-spec pod override surfaces."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from dpone_airflow_pack.init_fetch_connection_bridge import (
    require_closed_init_fetch_connection_bridge,
)
from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError
from dpone_airflow_pack.init_fetch_pod_contract import (
    ALLOWED_PACK_ENV,
)
from dpone_airflow_pack.provider_retry_authority import ProviderRetryAuthority

_STRICT_ASSET_EXECUTION_FIELDS = frozenset({"inlets", "outlets"})
STRICT_OPERATOR_OVERRIDE_FIELDS = frozenset({"pool", "retries"})
_STRICT_COMPOSE_FIELDS = frozenset(
    {
        "do_xcom_push",
        "env_vars",
        "executor",
        "labels",
        "name",
        "outlets",
        "params",
        "pool",
        "retries",
        "task_group",
        "task_id",
    }
)


def validate_operator_kwargs(
    kwargs: Mapping[str, Any],
    *,
    retry_authority: ProviderRetryAuthority | None = None,
) -> None:
    validate_strict_retry_policy(kwargs.get("retries"), retry_authority=retry_authority)
    forbidden = sorted(str(key) for key in kwargs if str(key) not in _STRICT_COMPOSE_FIELDS)
    if forbidden:
        raise reserved_collision("strict init-fetch rejects reserved or unknown KPO fields: " + ", ".join(forbidden))


def validate_strict_pack_extensions(pack: Mapping[str, Any]) -> None:
    """Reject every unapproved authority outside the closed provider projection."""

    raw_airflow = pack.get("airflow")
    if raw_airflow is not None and not isinstance(raw_airflow, Mapping):
        raise reserved_collision("strict init-fetch airflow must be an object")
    airflow = raw_airflow if isinstance(raw_airflow, Mapping) else {}
    raw_execution = airflow.get("execution")
    if raw_execution is not None and not isinstance(raw_execution, Mapping):
        raise reserved_collision("strict init-fetch airflow.execution must be an object")
    execution = raw_execution if isinstance(raw_execution, Mapping) else {}
    unsupported = sorted(
        str(key)
        for key, value in execution.items()
        if str(key) not in _STRICT_ASSET_EXECUTION_FIELDS and _is_configured(value)
    )
    if unsupported:
        raise reserved_collision(
            "strict init-fetch airflow.execution contains unsupported static fields: " + ", ".join(unsupported)
        )

    if _is_configured(pack.get("connection_projection")):
        require_closed_init_fetch_connection_bridge(pack.get("connection_projection"))


def validate_strict_operator_overrides(
    overrides: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Keep caller overrides out of the strict static operator authority."""

    if not overrides:
        return {}
    forbidden = sorted(str(key) for key in overrides if str(key) not in STRICT_OPERATOR_OVERRIDE_FIELDS)
    if forbidden:
        raise reserved_collision("operator_overrides cannot replace strict init-fetch fields: " + ", ".join(forbidden))
    clean = {str(key): deepcopy(value) for key, value in overrides.items()}
    pool = clean.get("pool")
    if pool is not None and (
        not isinstance(pool, str)
        or not pool
        or len(pool) > 256
        or pool != pool.strip()
        or any(ord(character) < 32 for character in pool)
    ):
        raise reserved_collision("operator_overrides.pool must be a non-empty bounded string")
    return clean


def validate_strict_retry_policy(
    value: object,
    *,
    retry_authority: ProviderRetryAuthority | None = None,
) -> None:
    """Reject automatic task retries until a durable target fence is certified."""

    if isinstance(value, int) and not isinstance(value, bool) and value == 0:
        return
    if value is None:
        return
    if (
        isinstance(value, int)
        and not isinstance(value, bool)
        and value > 0
        and retry_authority is not None
        and value <= retry_authority.max_task_retries
    ):
        return
    raise InitFetchProviderError(
        "DPONE_AIRFLOW_RETRIES_REQUIRE_TARGET_FENCE",
        "strict init-fetch requires retries=0 until a durable target-commit fence is certified",
    )


def provider_env(value: object) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise reserved_collision("kpo_kwargs.env_vars must be an object")
    unknown = sorted(str(key) for key in value if str(key) not in ALLOWED_PACK_ENV)
    if unknown:
        raise reserved_collision("strict init-fetch rejects non-contract environment variables: " + ", ".join(unknown))
    return {str(key): deepcopy(item) for key, item in value.items()}


def _is_configured(value: object) -> bool:
    if value is None or value == "":
        return False
    if isinstance(value, Mapping | list | tuple | set | frozenset):
        return bool(value)
    return True


def reserved_collision(message: str) -> InitFetchProviderError:
    return InitFetchProviderError(
        "DPONE_INIT_FETCH_RESERVED_COLLISION",
        message,
    )


__all__ = [
    "STRICT_OPERATOR_OVERRIDE_FIELDS",
    "provider_env",
    "reserved_collision",
    "validate_operator_kwargs",
    "validate_strict_operator_overrides",
    "validate_strict_pack_extensions",
    "validate_strict_retry_policy",
]
