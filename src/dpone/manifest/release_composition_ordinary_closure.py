"""Detached reconstruction of supported ordinary declarative transfer packs.

The initial capability admits a single transfer manifest and its SQL files.
Batch/authoring/recipe manifests, custom runner assets, hooks and dbt execution
need their own complete write/command policy and therefore fail closed here.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

from dpone.contracts.dbt_relation_writes import DbtRelationWrite, transfer_relation_write
from dpone.contracts.release_composition_ordinary import OrdinaryReleaseInventoryError
from dpone.gitops.workload_catalog_models import GitOpsConfigProvenance, GitOpsWorkloadDefinition
from dpone.manifest.airflow_resources import KubernetesResourceError, manifest_airflow_resources
from dpone.manifest.bounded_yaml import load_bounded_yaml
from dpone.manifest.confined_files import read_confined_file
from dpone.manifest.runtime_materialization import materialize_runtime_manifest

if TYPE_CHECKING:
    from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
    from dpone.gitops.workload_dependencies import WorkloadDependencyResolver
    from dpone.manifest.loader import SingleYamlManifestLoader
    from dpone.ports.release_composition_ordinary import OrdinaryArchiveUnpacker


class OrdinaryPackClosureVerifier:
    """Rebuild executable projections from a bounded private source archive.

    Dependencies are explicit capabilities. Reconstruction uses the real producer
    instead of trusting pins or caller-supplied producer labels. No credentials
    are resolved, and no runtime command or SQL is executed.
    """

    def __init__(
        self,
        *,
        dependencies: WorkloadDependencyResolver,
        builder: AirflowCompactPackBuilder,
        manifest_loader: SingleYamlManifestLoader,
        unpack_verified: OrdinaryArchiveUnpacker,
    ) -> None:
        self._dependencies = dependencies
        self._builder = builder
        self._manifest_loader = manifest_loader
        self._unpack_verified = unpack_verified

    def verify(self, pack: Mapping[str, Any], *, workload_id: str, dag_id: str) -> DbtRelationWrite:
        if "runtime_payload_ids" in pack or pack.get("blockers"):
            raise OrdinaryReleaseInventoryError("ordinary packs cannot contain dbt payload authority or blockers")
        if pack.get("connection_projection") not in ({}, {"query_overrides": {}}):
            raise OrdinaryReleaseInventoryError("ordinary connection projection must be empty; regenerate the pack")
        workload = self._workload(pack, workload_id)
        with TemporaryDirectory(prefix="dpone-ordinary-closure-") as temporary:
            root = Path(temporary) / "source"
            self._unpack_verified(pack, root)
            manifest = load_bounded_yaml(read_confined_file(root, workload.manifest, max_bytes=8 * 1024 * 1024))
            self._require_single_transfer(manifest, workload_id)
            assert isinstance(manifest, Mapping)
            self._manifest_loader.load(root / workload.manifest, metadata_only=True)
            dependencies = self._dependencies.resolve(repo_root=root, manifest=workload.manifest)
            if any(item.kind not in {"manifest", "sql_file"} for item in dependencies):
                raise OrdinaryReleaseInventoryError(
                    "ordinary composition supports single transfer manifests and SQL files"
                )
            if [item.to_jsonable() for item in dependencies] != pack.get("workload_dependencies"):
                raise OrdinaryReleaseInventoryError("ordinary source dependency closure differs from the producer pins")
            runtime = materialize_runtime_manifest(workload_id=workload_id, manifest=workload.manifest, repo_root=root)
            if runtime.to_jsonable() != pack.get("runtime_manifest"):
                raise OrdinaryReleaseInventoryError("ordinary runtime manifest differs from its source materialization")
            expected = {item.path for item in dependencies}
            if runtime.content is not None:
                expected.add(runtime.path)
                if read_confined_file(root, runtime.path, max_bytes=len(runtime.content)) != runtime.content:
                    raise OrdinaryReleaseInventoryError("ordinary generated runtime manifest bytes differ")
            observed = {path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()}
            if expected != observed:
                raise OrdinaryReleaseInventoryError("ordinary archive contains missing or undeclared files")
            rebuilt = self._builder.build(
                workload=workload,
                output_path=pack["output_path"],
                repo_root=root,
                mode=pack["mode"],
                runner_policy=pack["runner_policy"],
                include_live_gates=False,
            ).to_jsonable()
            # All executable views, including every bootstrap command, process
            # plan, pod projection and compatibility shell command, are checked.
            if _source_semantics(rebuilt) != _source_semantics(pack):
                raise OrdinaryReleaseInventoryError(
                    "ordinary pack differs from its declarative producer; regenerate with the supported default builder"
                )
            return transfer_relation_write(
                project_path="standalone",
                workflow_id=dag_id,
                workload_id=workload_id,
                manifest=manifest,
            )

    @staticmethod
    def _workload(pack: Mapping[str, Any], workload_id: str) -> GitOpsWorkloadDefinition:
        if pack.get("kind") != "gitops.airflow_pack" or pack.get("schema_version") != "3":
            raise OrdinaryReleaseInventoryError("ordinary composition requires current compact workload packs")
        if pack.get("include_live_gates") is not False:
            raise OrdinaryReleaseInventoryError("ordinary composition cannot include live gate commands")
        row = pack.get("workload")
        if not isinstance(row, dict) or row.get("workload_id") != workload_id or workload_id.startswith("dbt__"):
            raise OrdinaryReleaseInventoryError("ordinary workload identity is invalid")
        config = row.get("effective_config")
        provenance = row.get("provenance")
        if not isinstance(config, dict) or not isinstance(provenance, dict):
            raise OrdinaryReleaseInventoryError("ordinary workload configuration is invalid")
        if config.get("authoring") or config.get("runner"):
            raise OrdinaryReleaseInventoryError(
                "ordinary composition does not support custom authoring or runner assets"
            )
        airflow = config.get("airflow", {})
        if not isinstance(airflow, dict) or airflow.get("runner"):
            raise OrdinaryReleaseInventoryError("ordinary composition does not support custom runner assets")
        for field in ("manifest", "catalog_path"):
            if not isinstance(row.get(field), str) or not row[field]:
                raise OrdinaryReleaseInventoryError("ordinary workload source locator is invalid")
        domain = row.get("domain")
        if domain is not None and not isinstance(domain, str):
            raise OrdinaryReleaseInventoryError("ordinary workload domain is invalid")
        for field in ("output_path", "mode", "runner_policy"):
            if not isinstance(pack.get(field), str) or not pack[field]:
                raise OrdinaryReleaseInventoryError("ordinary producer configuration is invalid")
        return GitOpsWorkloadDefinition(
            workload_id=workload_id,
            manifest=row["manifest"],
            domain=domain,
            catalog_path=row["catalog_path"],
            effective_config=config,
            provenance={key: GitOpsConfigProvenance(**value) for key, value in provenance.items()},
        )

    @staticmethod
    def _require_single_transfer(manifest: object, workload_id: str) -> None:
        if not isinstance(manifest, Mapping) or manifest.get("name") != workload_id:
            raise OrdinaryReleaseInventoryError("ordinary transfer manifest identity is invalid")
        if set(manifest) - {"name", "description", "source", "sink", "gitops"}:
            raise OrdinaryReleaseInventoryError(
                "ordinary composition supports one plain transfer manifest per workload; batch, authoring and hooks are unsupported"
            )
        if "gitops" in manifest:
            _require_resource_only_gitops(manifest)
        source = manifest.get("source")
        sink = manifest.get("sink")
        allowed = {"postgres", "mssql", "mysql", "clickhouse"}
        if (
            not isinstance(source, Mapping)
            or not isinstance(sink, Mapping)
            or source.get("type") not in allowed
            or sink.get("type") not in allowed
        ):
            raise OrdinaryReleaseInventoryError(
                "ordinary composition requires explicitly declared supported SQL transfer endpoints"
            )


def _require_resource_only_gitops(manifest: Mapping[str, Any]) -> None:
    """Admit bounded Pod sizing without admitting runner or command authority."""
    gitops = manifest["gitops"]
    airflow = gitops.get("airflow") if isinstance(gitops, Mapping) else None
    if (
        not isinstance(gitops, Mapping)
        or set(gitops) != {"airflow"}
        or not isinstance(airflow, Mapping)
        or set(airflow) != {"resources"}
    ):
        raise OrdinaryReleaseInventoryError("ordinary composition permits only gitops.airflow.resources metadata")
    try:
        manifest_airflow_resources(manifest)
    except KubernetesResourceError as exc:
        raise OrdinaryReleaseInventoryError(str(exc)) from exc


def _source_semantics(pack: Mapping[str, Any]) -> dict[str, Any]:
    """Ignore only verified identity and the two equivalent empty projections."""
    result = dict(pack)
    result.pop("pack_fingerprint", None)
    if result.get("connection_projection") in ({}, {"query_overrides": {}}):
        result["connection_projection"] = {}
    return result
