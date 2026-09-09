"""Dependency-light validation for compiler-issued Airflow retry authority."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError
from dpone_airflow_pack.provider_execution_contract import (
    MAX_CERTIFIED_TASK_RETRIES,
    RETRY_AUTHORITY_FIELDS,
    RETRY_AUTHORITY_MODE,
    RETRY_AUTHORITY_SCHEMA,
)


@dataclass(frozen=True, slots=True)
class ProviderRetryAuthority:
    """Compiler-issued upper bound for one exact replay-safe runtime route."""

    mode: str
    max_task_retries: int


def validated_provider_retry_authority(value: object) -> ProviderRetryAuthority | None:
    """Parse the closed authority object or fail before task construction."""

    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise _invalid("provider_execution.retry_authority must be an object")
    fields = {str(key) for key in value}
    if fields != RETRY_AUTHORITY_FIELDS:
        raise _invalid("provider_execution.retry_authority fields are unsupported")
    if value["schema"] != RETRY_AUTHORITY_SCHEMA:
        raise _invalid("provider_execution.retry_authority.schema is unsupported")
    if value["mode"] != RETRY_AUTHORITY_MODE:
        raise _invalid("provider_execution.retry_authority.mode is unsupported")
    maximum = value["max_task_retries"]
    if isinstance(maximum, bool) or maximum != MAX_CERTIFIED_TASK_RETRIES:
        raise _invalid("provider_execution.retry_authority.max_task_retries is unsupported")
    return ProviderRetryAuthority(
        mode=RETRY_AUTHORITY_MODE,
        max_task_retries=MAX_CERTIFIED_TASK_RETRIES,
    )


def _invalid(message: str) -> InitFetchProviderError:
    return InitFetchProviderError(
        "DPONE_INIT_FETCH_PROVIDER_EXECUTION_INVALID",
        message,
    )


__all__ = ["ProviderRetryAuthority", "validated_provider_retry_authority"]
