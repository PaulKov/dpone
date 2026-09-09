"""Explicit workspace construction; execution dependencies are acquired lazily."""

from __future__ import annotations

import os
from collections.abc import Mapping
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.adapters.dbt_publish_artifact_reader import DbtArtifactReader
from dpone.adapters.dbt_sqlserver_graph_policy import DbtSqlserverPreviewGraphPolicyValidator
from dpone.app.dbt_publish_composition import build_dbt_dpone_compiler
from dpone.manifest.confined_files import read_confined_file
from dpone.manifest.dbt_workspace_discovery import DbtWorkspaceDiscovery
from dpone.manifest.dbt_workspace_inputs import ConfinedDbtWorkspaceInputs
from dpone.services.dbt_workspace import DbtWorkspaceService

if TYPE_CHECKING:
    from dpone.contracts.dbt_workspace import DbtWorkspaceProject
    from dpone.ports.dbt_publishing import DbtPublishCompiler
    from dpone.ports.dbt_workspace import DbtWorkspaceWriter


def build_dbt_workspace_service(
    *, environment: Mapping[str, str] | None = None, dbt_profiles_dir: Path | None = None
) -> DbtWorkspaceService:
    return DbtWorkspaceService(
        discovery=DbtWorkspaceDiscovery(environment=dict(os.environ if environment is None else environment)),
        compiler_factory=_project_compiler,
        writer_factory=partial(_workspace_writer, dbt_profiles_dir=dbt_profiles_dir.absolute())
        if dbt_profiles_dir is not None
        else _workspace_writer,
    )


def _workspace_writer(*, dbt_profiles_dir: Path | None = None) -> DbtWorkspaceWriter:
    from tempfile import gettempdir

    from dpone.adapters.dbt_parse_target import IsolatedDbtParseTargetResolver
    from dpone.adapters.dbt_runtime_profile import TemporaryDbtProfileStore
    from dpone.adapters.dbt_workflow_selection import DbtCliSelectionResolver
    from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
    from dpone.readiness.dbt_airflow_execution_pack import DbtAirflowExecutionPackBuilder
    from dpone.readiness.dbt_airflow_pack_adapter import DbtAirflowPackBuilder
    from dpone.readiness.dbt_publish_atomic_publisher import DbtArtifactTreePublisher
    from dpone.readiness.dbt_sqlserver_project_policy import DbtSqlserverProjectPolicyValidator
    from dpone.services.dbt_project_artifacts import DbtProjectArtifactProjector
    from dpone.services.dbt_release_integrity import DbtReleaseIntegrityService
    from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader
    from dpone.services.dbt_workspace_artifact_writer import DbtWorkspaceArtifactWriter
    from dpone.version import installed_version

    bundles = RuntimeDbtProjectBundleOperations()
    return DbtWorkspaceArtifactWriter(
        read_file=read_confined_file,
        projector=DbtProjectArtifactProjector(
            selection_resolver=DbtCliSelectionResolver(target_resolver=IsolatedDbtParseTargetResolver()),
            bundle_operations=bundles,
            project_policy=DbtSqlserverProjectPolicyValidator(),
            pack_builder=DbtAirflowPackBuilder(),
            dbt_pack_builder=DbtAirflowExecutionPackBuilder(),
        ),
        source_reader=DbtReleaseSourceReader(bundle_operations=bundles, read_file=read_confined_file),
        integrity=DbtReleaseIntegrityService(),
        publisher=DbtArtifactTreePublisher(),
        producer_version=installed_version(),
        profile_store=TemporaryDbtProfileStore(Path(gettempdir())),
        dbt_profiles_dir=dbt_profiles_dir,
    )


def _project_compiler(root: Path, project: DbtWorkspaceProject) -> DbtPublishCompiler:
    inputs = ConfinedDbtWorkspaceInputs(root=root, project=project, reader=DbtArtifactReader())
    return build_dbt_dpone_compiler(
        root=root / project.project_path,
        require_certified_routes=True,
        reader=inputs,
        profile_loader=inputs,
        graph_policy=DbtSqlserverPreviewGraphPolicyValidator(manifest_reader=inputs.graph_manifest),
    )
