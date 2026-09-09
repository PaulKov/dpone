"""Pure structural validation for scheduler-side DAG specifications."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.dag_spec_contract import (
    DAG_SPEC_KIND,
    DAG_SPEC_SCHEMA_VERSION,
    DagSpecLoadIssue,
    compute_dag_spec_fingerprint,
)
from dpone_airflow_pack.workflow_outcome import validated_workflow_outcome_config

_EDGE_REASONS = frozenset({"declared", "curated", "inferred"})
_VISIBILITY_MODES = frozenset({"inline", "task", "group"})


def validate_dag_spec_payload(
    payload: Mapping[str, Any],
    label: str,
) -> tuple[DagSpecLoadIssue, ...]:
    """Validate one decoded DAG spec without performing I/O."""

    issues: list[DagSpecLoadIssue] = []
    if payload.get("kind") != DAG_SPEC_KIND:
        issues.append(DagSpecLoadIssue("DPONE_AIRFLOW_DAG_SPEC_KIND_INVALID", f"Expected kind {DAG_SPEC_KIND}", label))
    if str(payload.get("schema_version") or "") != DAG_SPEC_SCHEMA_VERSION:
        issues.append(
            DagSpecLoadIssue(
                "DPONE_AIRFLOW_DAG_SPEC_SCHEMA_VERSION_INVALID",
                f"Unsupported schema_version (expected {DAG_SPEC_SCHEMA_VERSION})",
                label,
            )
        )
    dag_id = payload.get("dag_id")
    if not isinstance(dag_id, str) or not dag_id.strip():
        issues.append(DagSpecLoadIssue("DPONE_AIRFLOW_DAG_SPEC_DAG_ID_MISSING", "dag_id is required", label))
    for field in ("producer", "start_date"):
        if not _non_empty_text(payload.get(field)):
            issues.append(
                DagSpecLoadIssue(f"DPONE_AIRFLOW_DAG_SPEC_{field.upper()}_MISSING", f"{field} is required", label)
            )
    if "schedule" not in payload:
        issues.append(DagSpecLoadIssue("DPONE_AIRFLOW_DAG_SPEC_SCHEDULE_MISSING", "schedule is required", label))
    _validate_default_args(payload.get("default_args"), label=label, issues=issues)
    node_ids = _validate_nodes(payload.get("nodes"), label=label, issues=issues)
    edges = _validate_edges(payload.get("edges"), node_ids=node_ids, label=label, issues=issues)
    _validate_topological_order(
        payload.get("topological_order"),
        node_ids=node_ids,
        edges=edges,
        label=label,
        issues=issues,
    )
    _validate_workflow_outcome(payload.get("workflow_outcome"), label=label, issues=issues)
    fingerprint = payload.get("spec_fingerprint")
    if not _canonical_sha256(fingerprint):
        issues.append(
            DagSpecLoadIssue(
                "DPONE_AIRFLOW_DAG_SPEC_FINGERPRINT_MISSING",
                "spec_fingerprint must be a canonical sha256 digest",
                label,
            )
        )
    elif fingerprint != compute_dag_spec_fingerprint(payload):
        issues.append(
            DagSpecLoadIssue(
                "DPONE_AIRFLOW_DAG_SPEC_FINGERPRINT_MISMATCH",
                "spec_fingerprint does not match payload",
                label,
            )
        )
    return tuple(issues)


def _validate_workflow_outcome(
    raw: object,
    *,
    label: str,
    issues: list[DagSpecLoadIssue],
) -> None:
    if raw is None:
        return
    try:
        validated_workflow_outcome_config(raw)
    except ValueError as exc:
        issues.append(DagSpecLoadIssue("DPONE_DBT_WORKFLOW_OUTCOME_INVALID", str(exc), label))


def _validate_default_args(raw: object, *, label: str, issues: list[DagSpecLoadIssue]) -> None:
    if raw is None:
        return
    if not isinstance(raw, Mapping):
        issues.append(
            DagSpecLoadIssue(
                "DPONE_AIRFLOW_DAG_SPEC_DEFAULT_ARGS_INVALID",
                "default_args must be an object",
                label,
            )
        )
        return
    retry_delay = raw.get("retry_delay_minutes")
    if retry_delay is not None and not _is_non_negative_integer(retry_delay):
        issues.append(
            DagSpecLoadIssue(
                "DPONE_AIRFLOW_DAG_SPEC_RETRY_DELAY_INVALID",
                "default_args.retry_delay_minutes must be a non-negative integer",
                label,
            )
        )


def _validate_nodes(raw: object, *, label: str, issues: list[DagSpecLoadIssue]) -> set[str]:
    if not isinstance(raw, list) or not raw:
        issues.append(DagSpecLoadIssue("DPONE_AIRFLOW_DAG_SPEC_NODES_MISSING", "nodes must be a non-empty list", label))
        return set()
    node_ids: set[str] = set()
    for index, node in enumerate(raw):
        if not isinstance(node, Mapping):
            issues.append(
                DagSpecLoadIssue("DPONE_AIRFLOW_DAG_SPEC_NODE_INVALID", f"nodes[{index}] must be an object", label)
            )
            continue
        fields = {field: str(node.get(field) or "").strip() for field in ("node_id", "workload_id")}
        missing = [field for field, value in fields.items() if not value]
        if missing:
            issues.append(
                DagSpecLoadIssue(
                    "DPONE_AIRFLOW_DAG_SPEC_NODE_FIELD_MISSING",
                    f"nodes[{index}] requires: {', '.join(missing)}",
                    label,
                )
            )
            continue
        if not _non_empty_text(node.get("pack_ref")) and not _non_empty_text(node.get("pack_path")):
            issues.append(
                DagSpecLoadIssue(
                    "DPONE_AIRFLOW_DAG_SPEC_NODE_PACK_REFERENCE_MISSING",
                    f"nodes[{index}] requires pack_ref or pack_path",
                    label,
                )
            )
            continue
        node_id = fields["node_id"]
        if node_id in node_ids:
            issues.append(
                DagSpecLoadIssue("DPONE_AIRFLOW_DAG_SPEC_NODE_ID_DUPLICATE", f"duplicate node_id: {node_id}", label)
            )
            continue
        node_ids.add(node_id)
        visibility = node.get("visibility")
        if visibility is not None and visibility not in _VISIBILITY_MODES:
            issues.append(
                DagSpecLoadIssue(
                    "DPONE_AIRFLOW_DAG_SPEC_NODE_VISIBILITY_INVALID",
                    f"nodes[{index}].visibility is unsupported",
                    label,
                )
            )
    return node_ids


def _validate_edges(
    raw: object,
    *,
    node_ids: set[str],
    label: str,
    issues: list[DagSpecLoadIssue],
) -> list[tuple[str, str]]:
    if not isinstance(raw, list):
        issues.append(DagSpecLoadIssue("DPONE_AIRFLOW_DAG_SPEC_EDGES_INVALID", "edges must be a list", label))
        return []
    edges: list[tuple[str, str]] = []
    for index, edge in enumerate(raw):
        if not isinstance(edge, Mapping):
            issues.append(
                DagSpecLoadIssue("DPONE_AIRFLOW_DAG_SPEC_EDGE_INVALID", f"edges[{index}] must be an object", label)
            )
            continue
        upstream = str(edge.get("upstream") or "").strip()
        downstream = str(edge.get("downstream") or "").strip()
        origin = str(edge.get("origin") or "").strip()
        reason = str(edge.get("reason") or "").strip()
        if not upstream or not downstream or not origin:
            issues.append(
                DagSpecLoadIssue(
                    "DPONE_AIRFLOW_DAG_SPEC_EDGE_FIELD_MISSING",
                    f"edges[{index}] requires upstream, downstream, reason and origin",
                    label,
                )
            )
            continue
        if reason not in _EDGE_REASONS:
            issues.append(
                DagSpecLoadIssue(
                    "DPONE_AIRFLOW_DAG_SPEC_EDGE_REASON_INVALID",
                    f"edges[{index}].reason is unsupported",
                    label,
                )
            )
            continue
        if upstream not in node_ids or downstream not in node_ids:
            issues.append(
                DagSpecLoadIssue(
                    "DPONE_AIRFLOW_DAG_SPEC_EDGE_ENDPOINT_UNKNOWN",
                    f"edges[{index}] references an unknown node",
                    label,
                )
            )
            continue
        edges.append((upstream, downstream))
    return edges


def _validate_topological_order(
    raw: object,
    *,
    node_ids: set[str],
    edges: list[tuple[str, str]],
    label: str,
    issues: list[DagSpecLoadIssue],
) -> None:
    if (
        not isinstance(raw, list)
        or any(not _non_empty_text(item) for item in raw)
        or len(set(raw)) != len(raw)
        or set(raw) != node_ids
    ):
        issues.append(
            DagSpecLoadIssue(
                "DPONE_AIRFLOW_DAG_SPEC_TOPOLOGICAL_ORDER_INVALID",
                "topological_order must contain each node_id exactly once",
                label,
            )
        )
        return
    positions = {str(node_id): index for index, node_id in enumerate(raw)}
    if any(positions[upstream] >= positions[downstream] for upstream, downstream in edges):
        issues.append(
            DagSpecLoadIssue(
                "DPONE_AIRFLOW_DAG_SPEC_TOPOLOGICAL_ORDER_INVALID",
                "topological_order contradicts declared edges",
                label,
            )
        )


def _non_empty_text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _canonical_sha256(value: object) -> bool:
    if not isinstance(value, str) or not value.startswith("sha256:"):
        return False
    digest = value.removeprefix("sha256:")
    return len(digest) == 64 and all(char in "0123456789abcdef" for char in digest)


def _is_non_negative_integer(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value >= 0
    return isinstance(value, str) and value.strip().isdigit()


__all__ = ["validate_dag_spec_payload"]
