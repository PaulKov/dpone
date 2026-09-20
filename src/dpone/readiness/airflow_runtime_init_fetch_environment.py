"""Strict environment adapter for runtime init-fetch plans."""

from __future__ import annotations

import os
from collections.abc import Mapping

from dpone.runtime.runtime_init_fetch_plan import RuntimeInitFetchPlan
from dpone.runtime.runtime_init_fetch_plan_codec import decode_runtime_init_fetch_plan

PLAN_B64_ENV = "DPONE_INIT_FETCH_PLAN_B64"
PLAN_SHA256_ENV = "DPONE_INIT_FETCH_PLAN_SHA256"


def plan_from_environment(environment: Mapping[str, str] | None) -> tuple[RuntimeInitFetchPlan, str]:
    """Decode one digest-bound plan from explicit or process environment."""

    values = environment if environment is not None else os.environ
    return decode_runtime_init_fetch_plan(
        str(values.get(PLAN_B64_ENV) or ""),
        str(values.get(PLAN_SHA256_ENV) or ""),
    )


__all__ = ["PLAN_B64_ENV", "PLAN_SHA256_ENV", "plan_from_environment"]
