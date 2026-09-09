"""Shared IR types for colocated and legacy domain DAG discovery."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from dpone.gitops.airflow_dag_spec import DagSpecDeclaration

DOMAIN_DAG_SCHEMA = "dpone.domain-dag.v1"


@dataclass(frozen=True, slots=True)
class DomainDagLoadIssue:
    """One safe, stable validation failure for a colocated domain DAG file."""

    code: str
    message: str
    path: str | None = None
    dag_id: str | None = None
    domain: str | None = None


@dataclass(frozen=True, slots=True)
class DiscoveredDomainDag:
    """One validated colocated DAG declaration with durable identity."""

    dag_id: str
    domain: str
    path: str
    source_sha256: str
    declaration: DagSpecDeclaration
    fingerprint: str
    pipelines: tuple[str, ...]
    source: str = "colocated"


def domain_dag_fingerprint(declaration: DagSpecDeclaration) -> str:
    """Stable semantic fingerprint for dual-read conflict detection."""

    schedule: Any
    if declaration.schedule is None or isinstance(declaration.schedule, str):
        schedule = declaration.schedule
    else:
        schedule = declaration.schedule.to_jsonable()
    material = {
        "schema": "dpone.domain-dag-fingerprint.v1",
        "dag_id": declaration.dag_id,
        "description": declaration.description,
        "schedule": schedule,
        "start_date": declaration.start_date,
        "timezone": declaration.timezone,
        "catchup": declaration.catchup,
        "max_active_runs": declaration.max_active_runs,
        "tags": list(declaration.tags),
        "default_args": declaration.default_args,
        "operator_overrides": declaration.operator_overrides,
        "workloads": list(declaration.workloads or ()),
        "group": declaration.group,
        "wiring": declaration.wiring.to_jsonable(),
    }
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


__all__ = [
    "DOMAIN_DAG_SCHEMA",
    "DiscoveredDomainDag",
    "DomainDagLoadIssue",
    "domain_dag_fingerprint",
]
