"""Deterministic filesystem-safe identities for generated dbt artifacts."""

from __future__ import annotations

import re
from hashlib import sha256

_IDENTIFIER_PATTERN = re.compile(r"[^a-z0-9_]+")
_WORKFLOW_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_DAG_LIMIT = 250


def dbt_workflow_id(value: str) -> str:
    """Validate and return one stable author-owned workflow id."""

    if not isinstance(value, str) or _WORKFLOW_PATTERN.fullmatch(value) is None:
        raise ValueError("dbt workflow id must match [a-z][a-z0-9_]{0,63}")
    return value


def dbt_dag_id(domain: str, workflow: str) -> str:
    """Return an Airflow-compatible DAG id within the public length budget."""

    domain_id = _identifier(domain, limit=80)
    workflow_id = dbt_workflow_id(workflow)
    candidate = f"DAG__{domain_id}__{workflow_id}__refresh"
    if len(candidate) <= _DAG_LIMIT:
        return candidate
    suffix = sha256(candidate.encode("utf-8")).hexdigest()[:12]
    return f"{candidate[: _DAG_LIMIT - len(suffix) - 1].rstrip('_')}_{suffix}"


def dbt_workload_id(workflow: str, alias: str, unique_id: str) -> str:
    """Return the backward-compatible generated workload identity."""

    workflow_id = dbt_workflow_id(workflow)
    prefix = _identifier(f"dbt_{workflow_id}", limit=64)
    normalized_alias = _identifier(alias, limit=64)
    alias_has_workflow = normalized_alias == workflow_id or normalized_alias.startswith(f"{workflow_id}_")
    if normalized_alias.startswith(f"{prefix}_"):
        candidate = normalized_alias
    elif alias_has_workflow:
        candidate = f"dbt_{normalized_alias}"
    else:
        candidate = f"{prefix}_{normalized_alias}"
    normalized = _identifier(candidate, limit=64)
    lossy = _plain_identifier(alias) != alias.lower() or len(candidate) > 64
    if not lossy:
        return normalized
    suffix = sha256(unique_id.encode("utf-8")).hexdigest()[:10]
    return f"{normalized[: 64 - len(suffix) - 1].rstrip('_')}_{suffix}"


def _identifier(value: str, *, limit: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("dbt generated identity source must be non-empty")
    normalized = _plain_identifier(value)
    if not normalized:
        raise ValueError("dbt generated identity contains no safe characters")
    if normalized == value.lower() and len(normalized) <= limit:
        return normalized
    suffix = sha256(value.encode("utf-8")).hexdigest()[:10]
    return f"{normalized[: limit - len(suffix) - 1].rstrip('_')}_{suffix}"


def _plain_identifier(value: str) -> str:
    return _IDENTIFIER_PATTERN.sub("_", value.lower()).strip("_")


__all__ = ["dbt_dag_id", "dbt_workflow_id", "dbt_workload_id"]
