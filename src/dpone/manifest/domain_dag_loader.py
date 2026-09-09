"""Load and validate colocated ``dpone.domain-dag.v1`` authoring files."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.gitops.airflow_dag_spec import parse_dag_declaration
from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import ConfinedFileError, read_confined_file
from dpone.manifest.domain_dag_models import (
    DOMAIN_DAG_SCHEMA,
    DiscoveredDomainDag,
    DomainDagLoadIssue,
    domain_dag_fingerprint,
)
from dpone.manifest.domain_identity import DomainId, DomainIdError
from dpone.manifest.pipeline_identity import PipelineId, PipelineIdError

_DOMAIN_DAG_LIMITS = BoundedYamlLimits(max_bytes=256 * 1024, max_tokens=20_000, max_depth=24, max_nodes=8_000)
_DOMAIN_DAG_FIELDS = frozenset(
    {
        "schema",
        "dag_id",
        "domain",
        "description",
        "start_date",
        "timezone",
        "schedule",
        "catchup",
        "max_active_runs",
        "tags",
        "default_args",
        "operator_overrides",
        "pipelines",
        "wiring",
    }
)


def load_domain_dag_file(
    root: Path,
    relative_path: str,
    *,
    expected_domain: str,
) -> tuple[DiscoveredDomainDag | None, tuple[DomainDagLoadIssue, ...]]:
    """Read one confined colocated DAG file and validate path identity."""

    try:
        content = read_confined_file(root, relative_path, max_bytes=_DOMAIN_DAG_LIMITS.max_bytes)
    except ConfinedFileError:
        return None, (
            DomainDagLoadIssue(
                code="DPONE_DOMAIN_DAG_READ_FAILED",
                message="Domain DAG file could not be read safely.",
                path=relative_path,
                domain=expected_domain,
            ),
        )
    try:
        payload = load_bounded_yaml(content, limits=_DOMAIN_DAG_LIMITS)
    except BoundedYamlError as exc:
        return None, (
            DomainDagLoadIssue(
                code="DPONE_DOMAIN_DAG_YAML_INVALID",
                message=f"Domain DAG YAML is invalid ({exc.code}).",
                path=relative_path,
                domain=expected_domain,
            ),
        )
    if not isinstance(payload, Mapping):
        return None, (
            DomainDagLoadIssue(
                code="DPONE_DOMAIN_DAG_PAYLOAD_INVALID",
                message="Domain DAG payload must be a mapping.",
                path=relative_path,
                domain=expected_domain,
            ),
        )
    return _validate_domain_dag_payload(
        payload,
        relative_path=relative_path,
        expected_domain=expected_domain,
        source_sha256="sha256:" + hashlib.sha256(content).hexdigest(),
    )


def _validate_domain_dag_payload(
    payload: Mapping[str, Any],
    *,
    relative_path: str,
    expected_domain: str,
    source_sha256: str,
) -> tuple[DiscoveredDomainDag | None, tuple[DomainDagLoadIssue, ...]]:
    issues: list[DomainDagLoadIssue] = []
    unknown = sorted(set(payload) - _DOMAIN_DAG_FIELDS)
    for key in unknown:
        issues.append(
            DomainDagLoadIssue(
                code="DPONE_DOMAIN_DAG_UNKNOWN_KEY",
                message=f"Unknown domain DAG key: {key}",
                path=relative_path,
                domain=expected_domain,
            )
        )
    if payload.get("schema") != DOMAIN_DAG_SCHEMA:
        issues.append(
            DomainDagLoadIssue(
                code="DPONE_DOMAIN_DAG_SCHEMA_INVALID",
                message=f"schema must be {DOMAIN_DAG_SCHEMA}.",
                path=relative_path,
                domain=expected_domain,
            )
        )
    raw_dag_id = payload.get("dag_id")
    if not isinstance(raw_dag_id, str) or not raw_dag_id.strip():
        issues.append(
            DomainDagLoadIssue(
                code="DPONE_DOMAIN_DAG_ID_INVALID",
                message="dag_id must be a non-empty string.",
                path=relative_path,
                domain=expected_domain,
            )
        )
        dag_id = None
    else:
        dag_id = raw_dag_id.strip()
        stem = PurePosixPath(relative_path).stem
        if stem != dag_id:
            issues.append(
                DomainDagLoadIssue(
                    code="DPONE_DOMAIN_DAG_ID_PATH_MISMATCH",
                    message="dag_id must equal the DAG filename stem.",
                    path=relative_path,
                    dag_id=dag_id,
                    domain=expected_domain,
                )
            )
    raw_domain = payload.get("domain")
    try:
        domain = str(DomainId.parse(raw_domain)) if isinstance(raw_domain, str) else None
    except DomainIdError:
        domain = None
    if domain is None:
        issues.append(
            DomainDagLoadIssue(
                code="DPONE_DOMAIN_DAG_DOMAIN_INVALID",
                message="domain must be a canonical domain id.",
                path=relative_path,
                dag_id=dag_id,
            )
        )
    elif domain != expected_domain:
        issues.append(
            DomainDagLoadIssue(
                code="DPONE_DOMAIN_DAG_DOMAIN_PATH_MISMATCH",
                message="domain must equal the owning path domain.",
                path=relative_path,
                dag_id=dag_id,
                domain=domain,
            )
        )
    raw_pipelines = payload.get("pipelines")
    pipelines: list[str] = []
    if not isinstance(raw_pipelines, list) or not raw_pipelines:
        issues.append(
            DomainDagLoadIssue(
                code="DPONE_DOMAIN_DAG_PIPELINES_INVALID",
                message="pipelines must be a non-empty list of pipeline ids.",
                path=relative_path,
                dag_id=dag_id,
                domain=domain or expected_domain,
            )
        )
    else:
        for item in raw_pipelines:
            try:
                pipelines.append(str(PipelineId.parse(item)))
            except PipelineIdError:
                issues.append(
                    DomainDagLoadIssue(
                        code="DPONE_DOMAIN_DAG_PIPELINE_ID_INVALID",
                        message=f"Invalid pipeline id in pipelines: {item!r}",
                        path=relative_path,
                        dag_id=dag_id,
                        domain=domain or expected_domain,
                    )
                )
    if domain is not None and pipelines:
        # v1: every referenced pipeline must live in the same domain folder.
        # Cross-domain refs are rejected by discovery when pipeline domains differ.
        pass
    if issues or dag_id is None or domain is None:
        return None, tuple(issues)

    catalog_payload = {
        key: value for key, value in payload.items() if key not in {"schema", "domain", "dag_id", "pipelines"}
    }
    catalog_payload["workloads"] = list(pipelines)
    declaration, declaration_issues = parse_dag_declaration(dag_id, catalog_payload)
    for item in declaration_issues:
        issues.append(
            DomainDagLoadIssue(
                code=item.code,
                message=item.message,
                path=relative_path,
                dag_id=dag_id,
                domain=domain,
            )
        )
    if declaration is None or issues:
        return None, tuple(issues)
    fingerprint = domain_dag_fingerprint(declaration)
    return (
        DiscoveredDomainDag(
            dag_id=dag_id,
            domain=domain,
            path=relative_path,
            source_sha256=source_sha256,
            declaration=declaration,
            fingerprint=fingerprint,
            pipelines=tuple(declaration.workloads or ()),
            source="colocated",
        ),
        (),
    )


__all__ = [
    "DOMAIN_DAG_SCHEMA",
    "DiscoveredDomainDag",
    "DomainDagLoadIssue",
    "domain_dag_fingerprint",
    "load_domain_dag_file",
]
