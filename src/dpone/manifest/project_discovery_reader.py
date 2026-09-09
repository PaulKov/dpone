"""Bounded readers and compiler adapter used by domain-first discovery."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.manifest.authoring import AuthoringCompilationError, AuthoringCompiler
from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import ConfinedFileError, project_relative_path, read_confined_file
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.pipeline_identity import PipelineId, PipelineIdError
from dpone.manifest.project_discovery_models import DiscoveredWorkload, ProjectDiscoveryIssue
from dpone.manifest.project_identity import project_identity_fingerprint
from dpone.manifest.project_selection_contracts import ProjectCheckedSource

_OWNERSHIP_SCHEMA = "dpone.domain-ownership.v1"
_MAX_FILE_BYTES = 1024 * 1024
_MAX_OWNERSHIP_TEXT = 256
_OWNERSHIP_FIELDS = frozenset({"schema", "domain", "owner", "approvers"})
_OWNER_FIELDS = frozenset({"team", "contact"})
_APPROVER_FIELDS = frozenset({"github_team"})
_YAML_LIMITS = BoundedYamlLimits(max_bytes=_MAX_FILE_BYTES, max_tokens=50_000, max_depth=64, max_nodes=25_000)
_OWNERSHIP_IDENTITY_SCHEMA = "dpone.domain-ownership-identity.v1"


@dataclass(frozen=True, slots=True)
class DomainOwnershipAuthority:
    """Normalized domain authority and its source/content identities."""

    path: str
    source_sha256: str
    owner_team: str
    fingerprint: str


class ProjectDiscoveryReader:
    """Read ownership and compile one exact-path primary source."""

    def __init__(self, root: Path, compiler: AuthoringCompiler) -> None:
        self._root = root
        self._compiler = compiler

    def ownership_issue(self, domain: str, *, layout_root: str) -> ProjectDiscoveryIssue | None:
        _, issue = self.load_ownership(domain, layout_root=layout_root)
        return issue

    def load_ownership(
        self,
        domain: str,
        *,
        layout_root: str,
    ) -> tuple[DomainOwnershipAuthority | None, ProjectDiscoveryIssue | None]:
        path = f"{layout_root}/{domain}/ownership.yaml"
        payload, digest, issue = self._load_mapping(path, code="DPONE_DOMAIN_OWNERSHIP_INVALID")
        if issue is not None:
            if issue.code == "DPONE_DISCOVERY_FILE_NOT_FOUND":
                issue = _issue(
                    "DPONE_DOMAIN_OWNERSHIP_MISSING",
                    "Domain ownership is required before pipelines can be discovered.",
                    path,
                    domain=domain,
                )
            elif issue.domain is None:
                issue = _issue(issue.code, issue.message, issue.path, domain=domain)
            return None, issue
        assert payload is not None and digest is not None
        if (
            set(payload) != _OWNERSHIP_FIELDS
            or payload.get("schema") != _OWNERSHIP_SCHEMA
            or payload.get("domain") != domain
        ):
            return (
                None,
                _issue(
                    "DPONE_DOMAIN_OWNERSHIP_INVALID",
                    "Ownership schema and path domain must agree.",
                    path,
                    domain=domain,
                ),
            )
        raw_owner = payload.get("owner")
        raw_approvers = payload.get("approvers")
        if (
            not isinstance(raw_owner, Mapping)
            or set(raw_owner) != _OWNER_FIELDS
            or not isinstance(raw_approvers, Mapping)
            or set(raw_approvers) != _APPROVER_FIELDS
        ):
            return (
                None,
                _issue(
                    "DPONE_DOMAIN_OWNERSHIP_INVALID",
                    "Ownership owner must be an object.",
                    path,
                    domain=domain,
                ),
            )
        owner_team = _optional_text(raw_owner.get("team"))
        owner_contact = _optional_text(raw_owner.get("contact"))
        approver_team = _optional_text(raw_approvers.get("github_team"))
        if (
            owner_team is None
            or owner_contact is None
            or approver_team is None
            or any(value == "TODO" for value in (owner_team, owner_contact, approver_team))
        ):
            return (
                None,
                _issue(
                    "DPONE_DOMAIN_OWNERSHIP_INVALID",
                    "Ownership requires non-empty owner.team and owner.contact.",
                    path,
                    domain=domain,
                ),
            )
        fingerprint = project_identity_fingerprint(
            {
                "schema": _OWNERSHIP_IDENTITY_SCHEMA,
                "domain": domain,
                "owner": {"team": owner_team, "contact": owner_contact},
                "approvers": {"github_team": approver_team},
            }
        )
        return DomainOwnershipAuthority(path, digest, owner_team, fingerprint), None

    def compile_pipeline(
        self,
        *,
        domain: str,
        owner: str | None,
        ownership_fingerprint: str,
        pipeline_dir: Path,
    ) -> tuple[DiscoveredWorkload | None, ProjectDiscoveryIssue | None]:
        try:
            pipeline_id = str(PipelineId.parse(pipeline_dir.name))
        except PipelineIdError:
            return None, _issue(
                "DPONE_PIPELINE_ID_INVALID",
                "Pipeline directory name is not a canonical pipeline id.",
                project_relative_path(self._root, pipeline_dir),
                domain=domain,
            )
        source_path = pipeline_dir / "pipeline.yaml"
        source_label = project_relative_path(self._root, source_path)
        if source_path.is_symlink() or not source_path.is_file():
            return None, _issue(
                "DPONE_PIPELINE_SOURCE_NOT_FOUND",
                "Every direct pipeline directory must contain pipeline.yaml.",
                source_label,
                pipeline_id=pipeline_id,
                domain=domain,
            )
        payload, digest, issue = self._load_mapping(source_label, code="DPONE_PIPELINE_SOURCE_INVALID")
        if issue is not None:
            return (
                None,
                _issue(
                    issue.code,
                    issue.message,
                    issue.path,
                    pipeline_id=pipeline_id,
                    domain=domain,
                ),
            )
        assert payload is not None and digest is not None
        metadata = payload.get("metadata")
        if not isinstance(metadata, Mapping):
            return None, _issue(
                "DPONE_PIPELINE_SOURCE_INVALID",
                "Pipeline metadata must be an object.",
                source_label,
                pipeline_id=pipeline_id,
                domain=domain,
            )
        if metadata.get("id") != pipeline_id:
            return None, _issue(
                "DPONE_PIPELINE_ID_MISMATCH",
                "Pipeline metadata.id must match its directory.",
                source_label,
                pipeline_id=pipeline_id,
                domain=domain,
            )
        if metadata.get("domain") != domain:
            return None, _issue(
                "DPONE_PIPELINE_DOMAIN_MISMATCH",
                "Pipeline metadata.domain must match its domain directory.",
                source_label,
                pipeline_id=pipeline_id,
                domain=domain,
            )
        airflow_enabled = metadata.get("airflow", True)
        if not isinstance(airflow_enabled, bool):
            return None, _issue(
                "DPONE_PIPELINE_SOURCE_INVALID",
                "Pipeline metadata.airflow must be a boolean.",
                source_label,
                pipeline_id=pipeline_id,
                domain=domain,
            )
        try:
            compilation = self._compiler.compile(payload, source_path=source_path, project_root=self._root)
        except AuthoringCompilationError as exc:
            return None, _issue(
                "DPONE_PIPELINE_COMPILATION_FAILED",
                f"Pipeline authoring source could not be compiled ({exc.code}): {exc}",
                source_label,
                pipeline_id=pipeline_id,
                domain=domain,
            )
        except (ManifestConfigurationError, OSError, ValueError):
            return None, _issue(
                "DPONE_PIPELINE_COMPILATION_FAILED",
                "Pipeline authoring source could not be compiled by the canonical compiler.",
                source_label,
                pipeline_id=pipeline_id,
                domain=domain,
            )
        checked = ProjectCheckedSource(
            pipeline_id,
            source_path,
            source_label,
            payload,
            compilation,
            digest,
        )
        return (
            DiscoveredWorkload(
                pipeline_id=pipeline_id,
                domain=domain,
                owner=owner,
                ownership_fingerprint=ownership_fingerprint,
                airflow_enabled=airflow_enabled,
                checked_source=checked,
                connection_refs=_connection_refs(compilation.processes),
            ),
            None,
        )

    def _load_mapping(
        self,
        relative_path: str,
        *,
        code: str,
    ) -> tuple[Mapping[str, Any] | None, str | None, ProjectDiscoveryIssue | None]:
        try:
            content = read_confined_file(self._root, relative_path, max_bytes=_MAX_FILE_BYTES)
            payload = load_bounded_yaml(content, limits=_YAML_LIMITS)
        except ConfinedFileError as exc:
            issue_code = "DPONE_DISCOVERY_FILE_NOT_FOUND" if exc.code == "file_not_found" else code
            return None, None, _issue(issue_code, "Project discovery input could not be read safely.", relative_path)
        except BoundedYamlError:
            return None, None, _issue(code, "Project discovery input must be bounded unique-key YAML.", relative_path)
        if not isinstance(payload, Mapping):
            return None, None, _issue(code, "Project discovery input must be a YAML object.", relative_path)
        return payload, "sha256:" + hashlib.sha256(content).hexdigest(), None


def _connection_refs(processes: tuple[Mapping[str, Any], ...]) -> tuple[str, ...]:
    refs: set[str] = set()
    for process in processes:
        for endpoint_name in ("source", "sink"):
            endpoint = process.get(endpoint_name)
            if isinstance(endpoint, Mapping):
                value = _optional_text(endpoint.get("connection_ref"))
                if value is not None:
                    refs.add(value)
    return tuple(sorted(refs))


def _issue(
    code: str,
    message: str,
    path: str | None = None,
    *,
    pipeline_id: str | None = None,
    domain: str | None = None,
) -> ProjectDiscoveryIssue:
    return ProjectDiscoveryIssue(code, message, path, pipeline_id, domain)


def _optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text if text and len(text) <= _MAX_OWNERSHIP_TEXT else None


__all__ = ["DomainOwnershipAuthority", "ProjectDiscoveryReader"]
