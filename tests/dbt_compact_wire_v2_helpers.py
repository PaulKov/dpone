"""Synthetic offline composition; route/CLI inputs are fixtures, never live evidence."""

import json
import shutil
import subprocess
from functools import partial
from pathlib import Path

import yaml

from dpone.adapters.dbt_publish_artifact_reader import DbtArtifactReader
from dpone.adapters.dbt_runtime_profile import TemporaryDbtProfileStore
from dpone.adapters.dbt_sqlserver_graph_policy import DbtSqlserverPreviewGraphPolicyValidator
from dpone.adapters.dbt_workflow_selection import DbtCliSelectionResolver
from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.contracts.capability_discovery import (
    BeginnerRouteSupport,
    CapabilityDiscoverySnapshot,
    RouteCapability,
    RouteCertification,
    RouteCertificationVariant,
    RouteSupport,
)
from dpone.contracts.dbt_invocation import DbtInvocationTarget
from dpone.contracts.dbt_selection import manifest_preview_selected_graph
from dpone.manifest.confined_files import read_confined_file
from dpone.manifest.dbt_publish_intent_resolver import DbtPublishIntentResolver
from dpone.manifest.dbt_workspace_discovery import DbtWorkspaceDiscovery
from dpone.manifest.dbt_workspace_inputs import ConfinedDbtWorkspaceInputs
from dpone.readiness.dbt_publish_atomic_publisher import DbtArtifactTreePublisher
from dpone.readiness.dbt_publish_capability_policy import DbtRouteCapabilityPolicy
from dpone.readiness.dbt_sqlserver_project_policy import DbtSqlserverProjectPolicyValidator
from dpone.services.dbt_project_artifacts import DbtProjectArtifactProjector
from dpone.services.dbt_publish_compiler import DbtDponeCompiler
from dpone.services.dbt_publish_model_compiler import DbtModelToWorkloadCompiler
from dpone.services.dbt_publish_planning import DbtPublishPlanner
from dpone.services.dbt_release_integrity import DbtReleaseIntegrityService
from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader
from dpone.services.dbt_workspace import DbtWorkspaceService
from dpone.services.dbt_workspace_artifact_writer import DbtWorkspaceArtifactWriter
from dpone.version import installed_version

PROJECTS = ("project_alpha", "project_beta")
IMAGE = "registry.example/runtime@sha256:" + "d" * 64
SIDECAR = "registry.example/xcom@sha256:" + "b" * 64


def prepare_projects(root):
    """Derive independent synthetic projects from the public example only."""
    example = Path(__file__).parents[1] / "examples/dbt-inline-publishing"
    for name in PROJECTS:
        project = root / name
        shutil.copytree(example, project)
        config = project / "dbt_project.yml"
        config.write_text(
            config.read_text()
            .replace("dpone_dbt_demo", name)
            .replace("workflow: competitive_pricing", f"workflow: {name}")
            .replace("+schema: pricing", f"+schema: {name}")
        )
        policy = project / "dpone/dbt-publish-profiles.yml"
        policy.write_text(
            policy.read_text()
            .replace("  competitive_pricing:", f"  {name}:")
            .replace("target_schema: DWH_Stage", f"target_schema: {name}")
        )
        manifest = json.loads((project / "fixtures/manifest.v12.json").read_bytes())
        manifest["metadata"]["project_name"] = name
        for node in manifest["nodes"].values():
            if node.get("resource_type") == "model":
                node["schema"] = name
                for meta in (node.get("meta", {}), node.get("config", {}).get("meta", {})):
                    publish = meta.get("dpone", {}).get("publish", {})
                    if "workflow" in publish:
                        publish["workflow"] = name
                    if "target" in publish:
                        publish["target"]["schema"] = name
        # Keep repeated model IDs across projects to exercise project-scoped ownership.
        payload = json.dumps(manifest).replace('"workflow": "competitive_pricing"', f'"workflow": "{name}"')
        target = project / "target"
        target.mkdir(exist_ok=True)
        (target / "manifest.json").write_text(payload)
        (project / "fixtures/manifest.v12.json").write_text(payload)
        (project / "profiles.yml").write_text("dpone_runtime:\n  outputs:\n    runtime: {type: sqlserver}\n")
        (project / "models/project_marker.txt").write_text(f"-- synthetic {name}\nselect 1 as marker\n")


def route_snapshot():
    """Typed synthetic input to the real route policy; not a certification artifact."""
    routes = []
    for strategy in ("incremental_merge", "partition_replace"):
        route_id = f"mssql:clickhouse:{strategy}"
        variant = RouteCertificationVariant(
            f"{route_id}|native_bcp_to_clickhouse|widening|kpo",
            route_id,
            "native_bcp_to_clickhouse",
            "widening",
            "kpo",
            "production-certified",
            "PASS",
            ("sha256:" + "e" * 64,),
            (),
        )
        routes.append(
            RouteCapability(
                route_id,
                "mssql",
                "clickhouse",
                strategy,
                RouteSupport("supported", (), "docs/source-sink-matrix.md", ()),
                RouteCertification("production-certified", "PASS", (), (variant,)),
                BeginnerRouteSupport(False, (), None),
            )
        )
    return CapabilityDiscoverySnapshot(connectors=(), routes=tuple(routes), recipes=())


def compiler_factory(root, project):
    inputs = ConfinedDbtWorkspaceInputs(root=root, project=project, reader=DbtArtifactReader())
    return DbtDponeCompiler(
        reader=inputs,
        profile_loader=inputs,
        resolver=DbtPublishIntentResolver(),
        model_compiler=DbtModelToWorkloadCompiler(planner=DbtPublishPlanner()),
        route_capabilities=DbtRouteCapabilityPolicy(route_snapshot(), require_certified=True),
        project_policy=DbtSqlserverProjectPolicyValidator(project_root=root / project.project_path),
        graph_policy=DbtSqlserverPreviewGraphPolicyValidator(manifest_reader=inputs.graph_manifest),
        require_certified_routes=True,
    )


class FixtureTarget:
    def resolve(self, **kwargs):
        return DbtInvocationTarget("DWH_Stage", "fixture_base")


def fixture_dbt_runner(args, *, manifests, **kwargs):
    """Deterministic subprocess-port input; real CLI resolver still checks identity."""
    project = Path(args[args.index("--project-dir") + 1])
    name = yaml.safe_load((project / "dbt_project.yml").read_text())["name"]
    payload = manifests[name]
    if "parse" in args:
        target = Path(args[args.index("--target-path") + 1])
        target.mkdir(parents=True)
        (target / "manifest.json").write_bytes(payload)
        return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")
    assert "ls" in args
    manifest = json.loads(payload)
    selectors = args[args.index("--select") + 1 : args.index("--vars")]
    fqns = {selector.removeprefix("+fqn:") for selector in selectors}
    roots = tuple(node_id for node_id, node in manifest["nodes"].items() if ".".join(node["fqn"]) in fqns)
    body = b"".join(
        json.dumps({"unique_id": node_id}).encode() + b"\n"
        for node_id in manifest_preview_selected_graph(manifest, roots)
    )
    return subprocess.CompletedProcess(args, 0, stdout=body, stderr=b"")


def workspace_service(state, *, selection=None):
    bundles = RuntimeDbtProjectBundleOperations(package_environment={})
    versions = {"dbt-core": "1.12.3", "dbt-sqlserver": "1.11.1"}
    writer = DbtWorkspaceArtifactWriter(
        read_file=read_confined_file,
        projector=DbtProjectArtifactProjector(
            selection_resolver=selection
            or DbtCliSelectionResolver(
                runner=partial(
                    fixture_dbt_runner,
                    manifests={
                        name: (state.parent / "workspace" / name / "target/manifest.json").read_bytes()
                        for name in PROJECTS
                    },
                ),
                package_version=versions.__getitem__,
                target_resolver=FixtureTarget(),
            ),
            bundle_operations=bundles,
            project_policy=DbtSqlserverProjectPolicyValidator(),
        ),
        source_reader=DbtReleaseSourceReader(bundle_operations=bundles, read_file=read_confined_file),
        integrity=DbtReleaseIntegrityService(),
        publisher=DbtArtifactTreePublisher(),
        producer_version=installed_version(),
        profile_store=TemporaryDbtProfileStore(state),
    )
    return DbtWorkspaceService(
        discovery=DbtWorkspaceDiscovery(environment={}),
        compiler_factory=compiler_factory,
        writer_factory=lambda: writer,
    )
