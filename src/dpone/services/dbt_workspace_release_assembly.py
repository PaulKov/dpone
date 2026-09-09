"""Acquire schema bytes and verify framework packs before pure release assembly."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from dpone.contracts.dbt_workspace_release import (
    DbtWorkspaceProjectArtifacts as DbtWorkspaceProjectArtifacts,
)
from dpone.contracts.dbt_workspace_release import (
    DbtWorkspaceReleaseTree as DbtWorkspaceReleaseTree,
)
from dpone.contracts.dbt_workspace_release import (
    assemble_dbt_workspace_release,
    require_workspace_projects,
)
from dpone.contracts.strict_json import strict_json_object
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.services.dbt_release_assets import canonical_schema_descriptors, canonical_schema_files

if TYPE_CHECKING:
    from dpone.contracts.dbt_workspace import DbtWorkspaceCheckReport


def assemble_workspace_release(
    check: DbtWorkspaceCheckReport,
    projects: Sequence[DbtWorkspaceProjectArtifacts],
    *,
    producer_version: str,
) -> DbtWorkspaceReleaseTree:
    """Verify actual packs, then assemble and validate one complete release.

    Checked-report receipt guards run in the pure assembly after external
    verifiers have returned. Staged source verification and atomic publication
    remain the writer's responsibility; this service never publishes files.
    """

    from dpone_airflow_pack.pack_identity import verify_pack_fingerprint

    ordered = require_workspace_projects(check, projects)
    fingerprints = {
        key: verify_pack_fingerprint(body)
        for project in ordered
        for key, body in sorted(project.artifacts.pack_files.items())
    }
    schemas = canonical_schema_files()
    tree = assemble_dbt_workspace_release(
        check,
        ordered,
        producer_version=producer_version,
        pack_fingerprints=fingerprints,
        schema_files=schemas,
        schema_descriptors=canonical_schema_descriptors(schemas),
    )
    release = strict_json_object(tree.files["release-set.json"])
    if GitOpsSchemaValidator().validate(release, expected_kind="dpone.release-set.v2"):
        raise ValueError("workspace release does not satisfy its canonical schema")
    return tree


__all__ = ["DbtWorkspaceProjectArtifacts", "DbtWorkspaceReleaseTree", "assemble_workspace_release"]
