"""Frozen models and authoring validation for `gitops.airflow_dag_spec`.

The dag-spec contract lets a domain catalog declare whole Airflow DAGs in a
``dags:`` block (schedule, DAG kwargs, workload membership, wiring). The
builder (:mod:`dpone.gitops.airflow_dag_spec_builder`) resolves the block into
a fully explicit artifact ``.dpone/gitops/airflow/_dags/<dag_id>.dag-spec.json``
that the scheduler-side loader materializes without reading any YAML.

Design notes:

- models are frozen dataclasses (same discipline as ``airflow_compact_pack``);
- authoring validation returns issue tuples instead of raising so the builder
  can aggregate every problem of a domain in one build report;
- ``spec_fingerprint`` is a sha256 over the canonical JSON payload excluding
  advisory fields, mirroring ``pack_fingerprint`` semantics.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from dpone.gitops.airflow_asset_partition import (
    AssetPartitionContractError,
    AssetPartitionSpec,
    parse_asset_partition,
)
from dpone.gitops.airflow_dag_spec_policy import (
    GitOpsWorkloadCatalogIssue,
    VisibleTaskBudget,
    dag_spec_issue,
    optional_authoring_str,
    parse_dag_visible_task_budget,
    parse_start_date,
    parse_workload_source,
    typed_field_issues,
    visible_task_plan_json,
)

DAG_SPEC_KIND = "gitops.airflow_dag_spec"
DAG_SPEC_SCHEMA_VERSION = "1"
DAG_SPEC_PRODUCER = "dpone gitops airflow dag-spec"
DAG_SPEC_ARTIFACT_DIR = ".dpone/gitops/airflow/_dags"

EDGE_REASON_DECLARED = "declared"
EDGE_REASON_CURATED = "curated"
EDGE_REASON_INFERRED = "inferred"
EDGE_REASONS = (EDGE_REASON_DECLARED, EDGE_REASON_CURATED, EDGE_REASON_INFERRED)

WIRING_MODES = ("waves", "explicit", "assets")
DEFAULT_MAX_PARALLEL_WORKLOADS = 2

_ALLOWED_KEYS = frozenset(
    {
        "description",
        "schedule",
        "start_date",
        "timezone",
        "catchup",
        "max_active_runs",
        "tags",
        "default_args",
        "operator_overrides",
        "workloads",
        "group",
        "wiring",
    }
)
_ADVISORY_FINGERPRINT_FIELDS = ("spec_fingerprint", "warnings")


@dataclass(frozen=True, slots=True)
class DagAssetRef:
    """One asset URI in ``schedule.assets``; external URIs skip outlet checks."""

    uri: str
    external: bool = False
    partition: AssetPartitionSpec | None = None

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"uri": self.uri, "external": self.external}
        if self.partition is not None:
            payload["partition"] = self.partition.to_jsonable()
        return payload


@dataclass(frozen=True, slots=True)
class DagScheduleAssets:
    """Asset-driven schedule: the DAG runs when all referenced assets update."""

    assets: tuple[DagAssetRef, ...]

    def to_jsonable(self) -> dict[str, Any]:
        return {"assets": [asset.to_jsonable() for asset in self.assets]}


@dataclass(frozen=True, slots=True)
class DagSpecWiring:
    """Curated wiring policy from the ``dags:`` block."""

    mode: str = "waves"
    max_parallel_workloads: int = DEFAULT_MAX_PARALLEL_WORKLOADS
    dependencies: dict[str, tuple[str, ...]] = field(default_factory=dict)
    visible_task_budget: VisibleTaskBudget = field(default_factory=VisibleTaskBudget)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "max_parallel_workloads": self.max_parallel_workloads,
            "dependencies": {key: list(value) for key, value in sorted(self.dependencies.items())},
            "visible_task_budget": self.visible_task_budget.to_jsonable(),
        }


@dataclass(frozen=True, slots=True)
class DagPartitionPlan:
    """Build-resolved partition schedule independent of Airflow capabilities."""

    mode: str
    partition: AssetPartitionSpec
    mapper: str = "identity"
    provenance: str = "inferred:asset_graph.v1"

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "dimension": self.partition.dimension,
            "type": self.partition.type,
            "granularity": self.partition.granularity,
            "timezone": self.partition.timezone,
            "key_format": self.partition.key_format,
            "source": self.partition.source,
            "mapper": self.mapper,
            "provenance": self.provenance,
            "compatibility": {
                "airflow_3_2_plus": "native",
                "airflow_3_0_3_1": "degraded_unpartitioned",
                "airflow_2": "degraded_unpartitioned",
            },
        }


@dataclass(frozen=True, slots=True)
class DagSpecNode:
    """One materializable task-group node (a workload or a batch process)."""

    node_id: str
    workload_id: str
    selector: str | None = None
    task_group: str | None = None
    visibility: str = "inline"
    estimated_visible_tasks: int = 1

    @property
    def pack_ref(self) -> str:
        return f"cached://workloads/{self.workload_id}"

    @property
    def pack_path(self) -> str:
        return f".dpone/gitops/airflow/{self.workload_id}/airflow-pack.json"

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "workload_id": self.workload_id,
            "selector": self.selector,
            "task_group": self.task_group,
            "visibility": self.visibility,
            "estimated_visible_tasks": self.estimated_visible_tasks,
            "pack_ref": self.pack_ref,
            "pack_path": self.pack_path,
        }


@dataclass(frozen=True, slots=True)
class DagSpecEdge:
    """A resolved dependency edge with its provenance layer (``reason``)."""

    upstream: str
    downstream: str
    reason: str
    origin: str

    def to_jsonable(self) -> dict[str, Any]:
        return {"upstream": self.upstream, "downstream": self.downstream, "reason": self.reason, "origin": self.origin}


@dataclass(frozen=True, slots=True)
class DagSpecDeclaration:
    """A validated ``dags:`` entry before workload/edge resolution."""

    dag_id: str
    description: str | None
    schedule: str | DagScheduleAssets | None
    start_date: str
    timezone: str | None
    catchup: bool
    max_active_runs: int | None
    tags: tuple[str, ...]
    default_args: dict[str, Any]
    operator_overrides: dict[str, Any]
    workloads: tuple[str, ...] | None
    group: str | None
    wiring: DagSpecWiring


@dataclass(frozen=True, slots=True)
class GitOpsAirflowDagSpec:
    """The fully resolved dag-spec artifact payload."""

    declaration: DagSpecDeclaration
    domain: str | None
    source_path: str
    nodes: tuple[DagSpecNode, ...]
    edges: tuple[DagSpecEdge, ...]
    topological_order: tuple[str, ...]
    partition_plan: DagPartitionPlan | None = None
    warnings: tuple[GitOpsWorkloadCatalogIssue, ...] = ()
    kind: str = DAG_SPEC_KIND
    schema_version: str = DAG_SPEC_SCHEMA_VERSION
    producer: str = DAG_SPEC_PRODUCER

    @property
    def dag_id(self) -> str:
        return self.declaration.dag_id

    @property
    def output_path(self) -> str:
        return f"{DAG_SPEC_ARTIFACT_DIR}/{self.dag_id}.dag-spec.json"

    def to_jsonable(self) -> dict[str, Any]:
        declaration = self.declaration
        schedule: Any = declaration.schedule
        if isinstance(schedule, DagScheduleAssets):
            schedule = schedule.to_jsonable()
        payload: dict[str, Any] = {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "dag_id": declaration.dag_id,
            "domain": self.domain,
            "description": declaration.description,
            "schedule": schedule,
            "start_date": declaration.start_date,
            "timezone": declaration.timezone,
            "catchup": declaration.catchup,
            "max_active_runs": declaration.max_active_runs,
            "tags": list(declaration.tags),
            "default_args": dict(declaration.default_args),
            "operator_overrides": dict(declaration.operator_overrides),
            "source": {"type": "catalog", "path": self.source_path},
            "wiring": declaration.wiring.to_jsonable(),
            "visible_task_plan": visible_task_plan_json(
                tuple(node.estimated_visible_tasks for node in self.nodes),
                budget=declaration.wiring.visible_task_budget,
            ),
            "nodes": [node.to_jsonable() for node in self.nodes],
            "edges": [edge.to_jsonable() for edge in self.edges],
            "topological_order": list(self.topological_order),
            "warnings": [warning.to_jsonable() for warning in self.warnings],
        }
        if self.partition_plan is not None:
            payload["partition_plan"] = self.partition_plan.to_jsonable()
        payload["spec_fingerprint"] = compute_spec_fingerprint(payload)
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def compute_spec_fingerprint(payload: dict[str, Any]) -> str:
    """Fingerprint the canonical spec payload, ignoring advisory fields."""

    canonical = {key: value for key, value in payload.items() if key not in _ADVISORY_FINGERPRINT_FIELDS}
    raw = json.dumps(canonical, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def parse_dag_declaration(
    dag_id: str, payload: Any
) -> tuple[DagSpecDeclaration | None, tuple[GitOpsWorkloadCatalogIssue, ...]]:
    """Validate one ``dags:`` entry; returns ``(declaration, issues)``.

    A ``None`` declaration means the entry has at least one blocking issue.
    """

    if not isinstance(payload, dict):
        return None, (_issue("dag_spec_entry_invalid", dag_id, "dags entry must be a mapping"),)
    issues = [
        _issue("dag_spec_unknown_key", dag_id, f"Unknown dags key: {key}")
        for key in sorted(set(payload) - _ALLOWED_KEYS)
    ]
    schedule, schedule_issues = _parse_schedule(dag_id, payload)
    issues.extend(schedule_issues)
    start_date = parse_start_date(payload.get("start_date"))
    if start_date is None:
        issues.append(_issue("dag_spec_start_date_missing", dag_id, "start_date is required (ISO date)"))
    workloads, group, source_issues = parse_workload_source(
        dag_id=dag_id,
        payload=payload,
        producer=DAG_SPEC_PRODUCER,
    )
    issues.extend(source_issues)
    wiring, wiring_issues = _parse_wiring(dag_id, payload.get("wiring"))
    issues.extend(wiring_issues)
    issues.extend(typed_field_issues(dag_id=dag_id, payload=payload, producer=DAG_SPEC_PRODUCER))
    if issues:
        return None, tuple(issues)
    assert start_date is not None
    return (
        DagSpecDeclaration(
            dag_id=dag_id,
            description=optional_authoring_str(payload.get("description")),
            schedule=schedule,
            start_date=start_date,
            timezone=optional_authoring_str(payload.get("timezone")),
            catchup=bool(payload.get("catchup", False)),
            max_active_runs=payload.get("max_active_runs"),
            tags=tuple(str(tag) for tag in payload.get("tags") or ()),
            default_args=dict(payload.get("default_args") or {}),
            operator_overrides=dict(payload.get("operator_overrides") or {}),
            workloads=workloads,
            group=group,
            wiring=wiring,
        ),
        (),
    )


def _parse_schedule(
    dag_id: str, payload: dict[str, Any]
) -> tuple[str | DagScheduleAssets | None, list[GitOpsWorkloadCatalogIssue]]:
    raw = payload.get("schedule")
    if raw is None:
        return None, []
    if isinstance(raw, str):
        return raw, []
    if isinstance(raw, dict) and isinstance(raw.get("assets"), list) and raw.get("assets"):
        assets: list[DagAssetRef] = []
        for item in raw["assets"]:
            if isinstance(item, str):
                assets.append(DagAssetRef(uri=item))
            elif isinstance(item, dict) and item.get("uri"):
                try:
                    partition = parse_asset_partition(item.get("partition"))
                except AssetPartitionContractError as exc:
                    return None, [_issue(exc.code, dag_id, str(exc))]
                assets.append(
                    DagAssetRef(
                        uri=str(item["uri"]),
                        external=bool(item.get("external", False)),
                        partition=partition,
                    )
                )
            else:
                return None, [_issue("dag_spec_schedule_invalid", dag_id, f"Invalid schedule asset entry: {item!r}")]
        return DagScheduleAssets(assets=tuple(assets)), []
    return None, [
        _issue(
            "dag_spec_schedule_invalid",
            dag_id,
            "schedule must be a cron string, null, or {assets: [...]} mapping",
        )
    ]


def _parse_wiring(dag_id: str, raw: Any) -> tuple[DagSpecWiring, list[GitOpsWorkloadCatalogIssue]]:
    if raw is None:
        return DagSpecWiring(), []
    if not isinstance(raw, dict):
        return DagSpecWiring(), [_issue("dag_spec_field_invalid", dag_id, "wiring must be a mapping")]
    mode = str(raw.get("mode") or "waves")
    issues: list[GitOpsWorkloadCatalogIssue] = []
    if mode not in WIRING_MODES:
        issues.append(
            _issue("dag_spec_wiring_mode_invalid", dag_id, f"wiring.mode must be one of {WIRING_MODES}, got {mode!r}")
        )
    raw_parallel = raw.get("max_parallel_workloads", DEFAULT_MAX_PARALLEL_WORKLOADS)
    if not isinstance(raw_parallel, int) or raw_parallel < 1:
        issues.append(_issue("dag_spec_field_invalid", dag_id, "wiring.max_parallel_workloads must be a positive int"))
        raw_parallel = DEFAULT_MAX_PARALLEL_WORKLOADS
    dependencies: dict[str, tuple[str, ...]] = {}
    raw_dependencies = raw.get("dependencies") or {}
    if not isinstance(raw_dependencies, dict):
        issues.append(_issue("dag_spec_field_invalid", dag_id, "wiring.dependencies must map node -> [upstreams]"))
        raw_dependencies = {}
    for downstream, upstreams in raw_dependencies.items():
        upstream_list = upstreams if isinstance(upstreams, list) else [upstreams]
        dependencies[str(downstream)] = tuple(str(item) for item in upstream_list)
    visible_task_budget, budget_issues = parse_dag_visible_task_budget(
        dag_id=dag_id,
        raw=raw.get("visible_task_budget"),
        producer=DAG_SPEC_PRODUCER,
    )
    issues.extend(budget_issues)
    return (
        DagSpecWiring(
            mode=mode,
            max_parallel_workloads=raw_parallel,
            dependencies=dependencies,
            visible_task_budget=visible_task_budget,
        ),
        issues,
    )


def _issue(code: str, dag_id: str, message: str) -> GitOpsWorkloadCatalogIssue:
    return dag_spec_issue(code=code, dag_id=dag_id, message=message, producer=DAG_SPEC_PRODUCER)


__all__ = [
    "DAG_SPEC_ARTIFACT_DIR",
    "DAG_SPEC_KIND",
    "DAG_SPEC_PRODUCER",
    "DAG_SPEC_SCHEMA_VERSION",
    "DEFAULT_MAX_PARALLEL_WORKLOADS",
    "EDGE_REASONS",
    "EDGE_REASON_CURATED",
    "EDGE_REASON_DECLARED",
    "EDGE_REASON_INFERRED",
    "WIRING_MODES",
    "DagAssetRef",
    "DagPartitionPlan",
    "DagScheduleAssets",
    "DagSpecDeclaration",
    "DagSpecEdge",
    "DagSpecNode",
    "DagSpecWiring",
    "GitOpsAirflowDagSpec",
    "compute_spec_fingerprint",
    "parse_dag_declaration",
]
