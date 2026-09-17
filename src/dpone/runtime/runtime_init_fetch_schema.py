"""Select the backward-compatible runtime init-fetch wire schema."""

from __future__ import annotations


def runtime_init_fetch_schema(
    development_authority_required: bool,
    explicit_hook_execution: bool,
    has_runtime_payloads: bool,
) -> str:
    """Return the narrowest schema that preserves the plan's features."""

    if development_authority_required:
        return "dpone.airflow-runtime-init-fetch-plan.v4"
    if explicit_hook_execution:
        return "dpone.airflow-runtime-init-fetch-plan.v3"
    if has_runtime_payloads:
        return "dpone.airflow-runtime-init-fetch-plan.v2"
    return "dpone.airflow-runtime-init-fetch-plan.v1"


__all__ = ["runtime_init_fetch_schema"]
