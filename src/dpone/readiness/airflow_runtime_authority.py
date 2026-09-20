"""Runtime composition for the immutable Airflow authority payload."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_authority_payload import (
    materialize_runtime_authority_payload,
    verify_materialized_runtime_authority_payload,
)
from dpone.runtime.runtime_init_fetch_plan import RuntimeInitFetchPlan

RUNTIME_AUTHORITY_PATH_ENV = "DPONE_RUNTIME_AUTHORITY_PATH"
DEFAULT_RUNTIME_AUTHORITY_PATH = Path("/run/secrets/dpone/runtime-authority/authority")


def prepare_runtime_authority(
    plan: RuntimeInitFetchPlan,
    *,
    environment: Mapping[str, str] | None,
    target: Path,
    materialize: bool,
) -> None:
    """Materialize or verify the payload only at the provider-owned path."""

    payload = plan.runtime_authority
    if payload is None:
        return
    values = os.environ if environment is None else environment
    if values.get(RUNTIME_AUTHORITY_PATH_ENV) != str(target):
        raise InitFetchError(
            "DPONE_RUNTIME_AUTHORITY_PAYLOAD_INVALID",
            "runtime authority payload path is not provider-owned",
        )
    if materialize:
        materialize_runtime_authority_payload(payload, target)
    else:
        verify_materialized_runtime_authority_payload(payload, target)


__all__ = [
    "DEFAULT_RUNTIME_AUTHORITY_PATH",
    "RUNTIME_AUTHORITY_PATH_ENV",
    "prepare_runtime_authority",
]
