"""Native two-project compile/delivery; synthetic authority is not SQL certification."""

import base64
import hashlib
import json
import shutil
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

import pytest
from dpone_airflow_pack import dag_loader
from dpone_airflow_pack.deployment_index import (
    AirflowDeploymentIndex,
    LoadReport,
    load_airflow_deployment_index,
)
from dpone_airflow_pack.init_fetch_contract import init_fetch_context_from_payload
from dpone_airflow_pack.init_fetch_pod import compose_init_fetch_operator_kwargs
from dpone_airflow_pack.pack_task_runtime import runtime_operator_kwargs

from dpone.contracts.airflow_deployment import release_id
from dpone.contracts.development_delivery_authority import (
    DEVELOPMENT_COMPOSITION_PROFILE,
    DEVELOPMENT_RELEASE_SCHEMA,
    DevelopmentAuthorityReceipt,
)
from dpone.readiness.airflow_compact_pack_release import materialize_compact_pack_release
from dpone.readiness.airflow_deployment_projection import (
    AirflowDeploymentProjectionError,
    AirflowDeploymentProjectionService,
)
from dpone.runtime.airflow_runtime_connection_inventory import runtime_connection_publication_files
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan_codec import decode_runtime_init_fetch_plan
from dpone.runtime.runtime_init_fetch_service import RuntimeInitFetchExecutor
from dpone.runtime.verified_pack_launcher import VerifiedPackLauncher
from dpone.services.dbt_release_integrity import DbtReleaseIntegrityService
from tests.dbt_compact_wire_v2_helpers import IMAGE, PROJECTS, SIDECAR, prepare_projects, workspace_service
from tests.test_airflow_runtime_init_fetch_cli import RecordingRegistry
from tests.test_dbt_airflow_release_e2e import _config_map_ref, _write_environment
from tests.test_dbt_versioned_runtime_launcher import _prepared_bundle


def test_workspace_compact_projection_fetch_and_verified_launcher(tmp_path):
    root = tmp_path / "workspace"
    prepare_projects(root)
    compiled = tmp_path / "compiled"
    report = workspace_service(tmp_path / "profiles").compile(root, output_dir=compiled)
    assert report.passed, [(row.project.project_name, row.report.blockers) for row in report.check.projects]
    assert len(report.check.projects) == 2
    verify_delivery(tmp_path, compiled)


def _development_authority() -> DevelopmentAuthorityReceipt:
    now = datetime(2026, 9, 17, 12, 0, tzinfo=UTC)
    return DevelopmentAuthorityReceipt(
        policy_sha256="sha256:" + "1" * 64,
        grant_sha256="sha256:" + "2" * 64,
        signature_subject_sha256="sha256:" + "3" * 64,
        environment="development",
        source_repository_sha256="sha256:" + "4" * 64,
        source_commit="5" * 40,
        not_before=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        expires_at=(now + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        revocation_epoch=1,
        max_workloads=64,
        max_source_bytes=1024 * 1024 * 1024,
    )


def test_development_workspace_materializes_with_distinct_authority_and_stable_wire(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "workspace"
    prepare_projects(root)
    compiled = tmp_path / "compiled"
    authority = _development_authority()
    report = workspace_service(
        tmp_path / "profiles",
        development_authority=authority,
    ).compile(root, output_dir=compiled)
    assert report.passed

    source = json.loads((compiled / "release-set.json").read_bytes())
    assert source["schema"] == DEVELOPMENT_RELEASE_SCHEMA
    assert source["producer"]["wire_contract"] == "dpone.dbt-airflow-self-service.v2"
    assert source["development_authority"] == authority.release_projection()
    assert source["provenance"]["route_certifications"] == []

    denied = materialize_compact_pack_release(
        pack_root=compiled,
        cache_root=tmp_path / "unauthorized-cache",
        xcom_sidecar_image=SIDECAR,
    )
    assert not denied.passed
    assert not (tmp_path / "unauthorized-cache").exists()

    materialized = materialize_compact_pack_release(
        pack_root=compiled,
        cache_root=tmp_path / "cache",
        xcom_sidecar_image=SIDECAR,
        development_authority=authority,
    )
    assert materialized.passed, materialized.blockers
    release = json.loads(Path(materialized.release_dir, "release-set.json").read_bytes())
    assert release["schema"] == DEVELOPMENT_RELEASE_SCHEMA
    assert release["promotion"]["profile"] == DEVELOPMENT_COMPOSITION_PROFILE
    assert release["development_authority"] == authority.release_projection()
    _write_environment(tmp_path)
    service = AirflowDeploymentProjectionService(
        root=tmp_path,
        cache_root=tmp_path / "cache",
    )
    with pytest.raises(AirflowDeploymentProjectionError) as exc_info:
        service.materialize(
            release_id=materialized.release_id,
            environment="prod",
            trust_tier="non_production",
            runtime_image_ref=IMAGE,
            runtime_image_digest=IMAGE.split("@")[-1],
            artifact_registry_ref="synthetic-artifacts",
            registry_config_ref=_config_map_ref("registry", "1"),
            trust_policy_ref=_config_map_ref("policy", "2"),
            airflow_bundle_ref="git:" + "d" * 40,
        )
    assert getattr(exc_info.value, "code", None) == "DPONE_DEPLOYMENT_RUNTIME_AUTHORITY_REQUIRED"

    projection = service.materialize(
        release_id=materialized.release_id,
        environment="prod",
        trust_tier="non_production",
        runtime_image_ref=IMAGE,
        runtime_image_digest=IMAGE.split("@")[-1],
        artifact_registry_ref="synthetic-artifacts",
        registry_config_ref=_config_map_ref("registry", "1"),
        trust_policy_ref=_config_map_ref("policy", "2"),
        runtime_authority_ref={
            "kind": "kubernetes_secret",
            "name": "dpone-runtime-authority",
            "key": "authority.json",
        },
        airflow_bundle_ref="git:" + "d" * 40,
    )
    assert projection.deployment["schema"] == "dpone.deployment-set.v4"
    assert projection.airflow_index["schema"] == "dpone.airflow-deployment-index.v4"
    expected_source = {
        "mode": "kubernetes_secret_volume",
        "secret_name": "dpone-runtime-authority",
        "secret_key": "authority.json",
    }
    assert projection.deployment["runtime_artifact_delivery"]["runtime_authority"] == expected_source
    assert projection.airflow_index["runtime_artifact_delivery"]["runtime_authority"] == expected_source
    assert "synthetic-secret-value" not in json.dumps(projection.to_dict(), sort_keys=True)
    context = init_fetch_context_from_payload(projection.airflow_index)
    assert context.development_authority_required is True

    loaded_index = load_airflow_deployment_index(
        projection.deployment_dir / "airflow-index.json",
        cache_root=tmp_path / "cache",
    )
    strict_loader_schemas: list[str] = []

    def _load_strict_dags(
        _globals: object,
        *,
        index: AirflowDeploymentIndex,
        **_kwargs: object,
    ) -> LoadReport:
        strict_loader_schemas.append(index.schema)
        return LoadReport(
            release_id=index.release_id,
            deployment_id=index.deployment_id,
        )

    monkeypatch.setattr(dag_loader, "load_preflighted_init_fetch_dags", _load_strict_dags)
    loaded_report = dag_loader.load_dpone_dags_from_index(
        {},
        index=loaded_index,
        operator_overrides=None,
        duplicate_policy="skip_and_report",
        invalid_dag_policy="skip_and_report",
    )
    assert loaded_report.release_id == projection.airflow_index["release_id"]
    assert strict_loader_schemas == ["dpone.airflow-deployment-index.v4"]

    encoded = context.encode_plan(
        workload_id=str(projection.airflow_index["workload_packs"][0]["id"]),
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )
    assert json.loads(encoded.payload)["schema"] == "dpone.airflow-runtime-init-fetch-plan.v4"

    raw_authority = b'{"mode":"synthetic"}\n'
    immutable_projection = service.materialize(
        release_id=materialized.release_id,
        environment="prod",
        trust_tier="non_production",
        runtime_image_ref=IMAGE,
        runtime_image_digest=IMAGE.split("@")[-1],
        artifact_registry_ref="synthetic-artifacts",
        registry_config_ref=_config_map_ref("registry", "1"),
        trust_policy_ref=_config_map_ref("policy", "2"),
        runtime_authority_ref={
            "kind": "immutable_payload",
            "mode": "immutable_payload",
            "encoding": "base64",
            "payload_b64": base64.b64encode(raw_authority).decode("ascii"),
            "bytes": len(raw_authority),
            "sha256": "sha256:" + hashlib.sha256(raw_authority).hexdigest(),
        },
        airflow_bundle_ref="git:" + "d" * 40,
    )
    assert immutable_projection.deployment["schema"] == "dpone.deployment-set.v5"
    assert immutable_projection.airflow_index["schema"] == "dpone.airflow-deployment-index.v5"
    immutable_context = init_fetch_context_from_payload(immutable_projection.airflow_index)
    immutable_plan = immutable_context.encode_plan(
        workload_id=str(immutable_projection.airflow_index["workload_packs"][0]["id"]),
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )
    assert json.loads(immutable_plan.payload)["schema"] == "dpone.airflow-runtime-init-fetch-plan.v5"


def verify_delivery(tmp_path, compiled):
    original = json.loads((compiled / "release-set.json").read_bytes())
    cache = tmp_path / ".dpone-cache"
    materialized = materialize_compact_pack_release(pack_root=compiled, cache_root=cache, xcom_sidecar_image=SIDECAR)
    assert materialized.passed, materialized.blockers
    release = json.loads((Path(materialized.release_dir) / "release-set.json").read_bytes())
    preserved = (
        ("producer", "constituents", "promotion")
        if original.get("schema") == "dpone.release-set.v3"
        else ("producer", "selection_authority", "selection_fingerprint", "provenance")
    )
    for key in preserved:
        assert release[key] == original[key]
    assert release["artifacts"]["runtime_payloads"] == original["artifacts"]["runtime_payloads"]
    _write_environment(tmp_path)
    projection = AirflowDeploymentProjectionService(root=tmp_path).materialize(
        release_id=materialized.release_id,
        environment="prod",
        trust_tier="non_production",
        runtime_image_ref=IMAGE,
        runtime_image_digest=IMAGE.split("@")[-1],
        artifact_registry_ref="synthetic-artifacts",
        registry_config_ref=_config_map_ref("registry", "1"),
        trust_policy_ref=_config_map_ref("policy", "2"),
        airflow_bundle_ref="git:" + "d" * 40,
    )
    context = init_fetch_context_from_payload(projection.airflow_index)
    objects = {
        PurePosixPath(path.relative_to(cache).as_posix()): path.read_bytes()
        for path in cache.rglob("*")
        if path.is_file()
    }
    for artifact in runtime_connection_publication_files(
        projection.deployment, deployment_dir=projection.deployment_dir, root=cache
    ):
        objects[artifact.key] = artifact.path.read_bytes()
    prepared_projects = set()
    for item in projection.airflow_index["workload_packs"]:
        pack = json.loads(objects[PurePosixPath(item["artifact_ref"].removeprefix("cache://"))])
        kwargs = compose_init_fetch_operator_kwargs(
            pack=pack,
            kwargs=runtime_operator_kwargs(pack, strict_provider_execution=True, expected_workload_id=item["id"]),
            context=context,
            workload_id=item["id"],
            execution_kind="runtime",
            execution_scope="workload",
            hook_execution="externalized",
        )
        assert kwargs["image"] == IMAGE
        encoded = context.encode_plan(
            workload_id=item["id"], execution_kind="runtime", execution_scope="workload", hook_execution="externalized"
        )
        plan, digest = decode_runtime_init_fetch_plan(base64.b64encode(encoded.payload).decode(), encoded.sha256)
        state = tmp_path / "runtime" / item["id"]
        registry = RecordingRegistry(objects)
        executor = RuntimeInitFetchExecutor(
            registry=registry, artifact_root=state / "artifacts", worktree_root=state / "worktree"
        )
        executor.execute(plan, plan_sha256=encoded.sha256)
        command = VerifiedPackLauncher(artifact_root=state / "artifacts", worktree_root=state / "worktree").prepare(
            plan, plan_sha256=encoded.sha256
        )
        if item["id"].startswith("dbt__"):
            assert command.argv[:3] == ("dpone", "dbt", "execute-pack")
            marker = (state / "worktree/dbt-project/models/project_marker.txt").read_text()
            own = next(name for name in PROJECTS if name in item["id"])
            assert own in marker and all(name not in marker for name in PROJECTS if name != own)
            prepared_projects.add(own)
            assert len(plan.runtime_payloads) == 3
        else:
            assert not plan.runtime_payloads
        if plan.runtime_payloads:
            expected = next(
                row["runtime_payload_ids"] for row in release["artifacts"]["workload_packs"] if row["id"] == item["id"]
            )
            assert [row.id for row in plan.runtime_payloads] == expected
            all_objects = {
                PurePosixPath("releases") / Path(materialized.release_dir).name / row["path"]
                for row in release["artifacts"]["runtime_payloads"]
            }
            downloaded = {key for operation, key in registry.calls if operation == "download"}
            selected = {PurePosixPath(row.artifact_ref.removeprefix("cache://")) for row in plan.runtime_payloads}
            assert downloaded & all_objects == selected
            before = list(registry.calls)
            executor.execute(plan, plan_sha256=digest)
            assert registry.calls == before
            # Mutation after READY must fail the actual launcher before a command is returned.
            body_path = state / "artifacts/payload" / plan.runtime_payloads[0].artifact_ref.removeprefix("cache://")
            body_path.write_bytes(b"SENSITIVE_SENTINEL")
            with pytest.raises(InitFetchError) as failure:
                VerifiedPackLauncher(artifact_root=state / "artifacts", worktree_root=state / "worktree").prepare(
                    plan, plan_sha256=digest
                )
            assert "SENSITIVE_SENTINEL" not in str(failure.value)
    assert prepared_projects == set(PROJECTS)
    return Path(materialized.release_dir)


@pytest.fixture(scope="module")
def compiled_workspace(tmp_path_factory):
    state = tmp_path_factory.mktemp("synthetic-workspace")
    prepare_projects(state / "workspace")
    destination = state / "compiled"
    report = workspace_service(state / "profiles").compile(state / "workspace", output_dir=destination)
    assert report.passed, [(row.project.project_name, row.report.blockers) for row in report.check.projects]
    return destination


def reseal_fixture(root, release):
    """Adversarial synthetic artifact: repair hashes only, never validation results."""
    release["release_id"] = release_id(release)
    (root / "release-set.json").write_text(json.dumps(release))
    (root / "release-subjects.sha256").unlink(missing_ok=True)
    DbtReleaseIntegrityService().write(root)


@pytest.mark.parametrize(
    "mutation",
    [
        "producer_missing",
        "producer_null",
        "producer_wire",
        "producer_version",
        "producer_extra",
        "schema_missing",
        "schema_v1",
        "schema_unknown",
        "selection_authority",
        "selection_fingerprint",
        "promotion_null",
        "promotion_empty",
        "promotion_wrong",
        "payload_missing",
        "payload_duplicate",
        "payload_path",
        "payload_hash",
        "payload_zero",
        "payload_bool",
        "payload_negative",
        "payload_oversize",
        "trio_missing",
        "trio_duplicate",
        "trio_reordered",
        "trio_swapped",
        "foreign_project",
        "foreign_manifest",
        "foreign_selection",
        "non_dbt_trio",
        "snapshot_hash",
        "artifact_mutation",
    ],
)
def test_native_materializer_rejects_corruption_without_publication(compiled_workspace, tmp_path, mutation):
    root = tmp_path / "compiled"
    shutil.copytree(compiled_workspace, root)
    release = json.loads((root / "release-set.json").read_bytes())
    artifacts = release["artifacts"]
    payload = artifacts["runtime_payloads"][0]
    dbt = [row for row in artifacts["workload_packs"] if row["id"].startswith("dbt__")]
    first = dbt[0]["runtime_payload_ids"]
    if mutation == "producer_missing":
        release.pop("producer")
    elif mutation == "producer_null":
        release["producer"] = None
    elif mutation.startswith("producer_"):
        key, value = {
            "producer_wire": ("wire_contract", "unsupported"),
            "producer_version": ("dpone_version", ""),
            "producer_extra": ("unsafe", "SENSITIVE_SENTINEL"),
        }[mutation]
        release["producer"][key] = value
    elif mutation == "schema_missing":
        release.pop("schema")
    elif mutation.startswith("schema_"):
        release["schema"] = "dpone.release-set.v1" if mutation == "schema_v1" else "unsupported"
    elif mutation == "selection_authority":
        release["selection_authority"] = "manifest_preview"
    elif mutation == "selection_fingerprint":
        release["selection_fingerprint"] = "sha256:" + "0" * 64
    elif mutation.startswith("promotion_"):
        release["promotion"] = {
            "promotion_null": None,
            "promotion_empty": {},
            "promotion_wrong": {"schema": "unsupported"},
        }[mutation]
    elif mutation == "payload_missing":
        artifacts["runtime_payloads"].pop()
    elif mutation == "payload_duplicate":
        artifacts["runtime_payloads"].append(deepcopy(payload))
    elif mutation == "payload_path":
        payload["path"] = "../SENSITIVE_SENTINEL"
    elif mutation == "payload_hash":
        payload["sha256"] = "sha256:" + "0" * 64
    elif mutation.startswith("payload_"):
        payload["bytes"] = {
            "payload_zero": 0,
            "payload_bool": True,
            "payload_negative": -1,
            "payload_oversize": 256 * 1024 * 1024 + 1,
        }[mutation]
    elif mutation == "trio_missing":
        first.pop()
    elif mutation == "trio_duplicate":
        first[1] = first[0]
    elif mutation == "trio_reordered":
        first.reverse()
    elif mutation == "trio_swapped":
        dbt[0]["runtime_payload_ids"], dbt[1]["runtime_payload_ids"] = dbt[1]["runtime_payload_ids"], first
    elif mutation.startswith("foreign_"):
        position = {"foreign_project": 0, "foreign_manifest": 1, "foreign_selection": 2}[mutation]
        first[position] = dbt[1]["runtime_payload_ids"][position]
    elif mutation == "non_dbt_trio":
        next(row for row in artifacts["workload_packs"] if not row["id"].startswith("dbt__"))["runtime_payload_ids"] = (
            first
        )
    elif mutation == "snapshot_hash":
        release["provenance"]["source_snapshot_sha256"] = "sha256:" + "0" * 64
    else:
        (root / payload["path"]).write_bytes(b"SENSITIVE_SENTINEL")
    reseal_fixture(root, release)
    report = materialize_compact_pack_release(pack_root=root, cache_root=tmp_path / "cache", xcom_sidecar_image=SIDECAR)
    assert not report.passed
    assert not (tmp_path / "cache").exists()
    assert "SENSITIVE_SENTINEL" not in str(report.blockers)


@pytest.mark.parametrize("target", ["release-set.json", "runtime", "_dbt/dbt-source-snapshot.json"])
def test_native_materializer_rejects_symlinked_inputs(compiled_workspace, tmp_path, target):
    root = tmp_path / "compiled"
    shutil.copytree(compiled_workspace, root)
    source = root / target
    retained = tmp_path / "retained"
    source.rename(retained)
    source.symlink_to(retained, target_is_directory=retained.is_dir())
    report = materialize_compact_pack_release(pack_root=root, cache_root=tmp_path / "cache", xcom_sidecar_image=SIDECAR)
    assert not report.passed and not (tmp_path / "cache").exists()


def test_complete_native_inventory_retry_and_subset_rejection(compiled_workspace, tmp_path):
    args = dict(pack_root=compiled_workspace, cache_root=tmp_path / "cache", xcom_sidecar_image=SIDECAR)
    first = materialize_compact_pack_release(**args)
    assert first.passed
    metadata = Path(first.release_dir) / "release-set.json"
    inode = metadata.stat().st_ino
    second = materialize_compact_pack_release(**args, dag_ids=first.dag_ids)
    assert second == first and metadata.stat().st_ino == inode
    subset = materialize_compact_pack_release(**args, dag_ids=first.dag_ids[:1])
    assert not subset.passed


@pytest.mark.parametrize("mutation", ["missing", "null", "unknown", "version", "schema_v1", "schema_missing"])
def test_actual_launcher_rejects_resealed_v2_metadata_downgrade(tmp_path, mutation):
    def corrupt(release, bodies):
        if mutation == "missing":
            release.pop("producer")
        elif mutation == "null":
            release["producer"] = None
        elif mutation == "unknown":
            release["producer"]["wire_contract"] = "unsupported"
        elif mutation == "version":
            release["producer"]["dpone_version"] = ""
        elif mutation == "schema_v1":
            release["schema"] = "dpone.release-set.v1"
        else:
            release.pop("schema")

    with pytest.raises(InitFetchError) as failure:
        bundle = _prepared_bundle(tmp_path, mutate=corrupt)
        VerifiedPackLauncher(artifact_root=tmp_path / "artifacts", worktree_root=tmp_path / "worktree").prepare(
            bundle.plan, plan_sha256=bundle.plan_sha256
        )
    assert failure.value.code == (
        "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED" if mutation == "schema_missing" else "DPONE_DBT_SELECTION_DRIFT"
    )


@pytest.mark.parametrize("development", [False, True])
def test_real_offline_toolchain_compact_delivery(tmp_path, development):
    """Real parse/ls, source verification and launcher; no build or SQL execution."""
    import importlib.metadata

    import yaml

    from dpone.adapters.dbt_executable import current_environment_dbt_executable
    from dpone.adapters.dbt_parse_target import IsolatedDbtParseTargetResolver
    from dpone.adapters.dbt_workflow_selection import DbtCliSelectionResolver
    from tests.test_dbt_workspace_real_toolchain import _parse, _prepare_project, _profile, _verify_runtime_targets

    try:
        versions = {key: importlib.metadata.version(key) for key in ("dbt-core", "dbt-sqlserver")}
    except importlib.metadata.PackageNotFoundError:
        pytest.skip("exact optional offline dbt toolchain is unavailable")
    if versions != {"dbt-core": "1.12.3", "dbt-sqlserver": "1.11.1"}:
        pytest.skip("exact optional offline dbt toolchain is unavailable")
    executable = current_environment_dbt_executable()
    root = tmp_path / "workspace"
    for name in PROJECTS:
        project = _prepare_project(root, name)
        (project / "profiles.yml").write_text(yaml.safe_dump(_profile(name)))
        (project / "models/project_marker.txt").write_text(f"-- synthetic {name}\nselect 1 as marker\n")
        _parse(executable, project, project, tmp_path / f"parse-{name}")
    compiled = tmp_path / "compiled"
    authority = _development_authority() if development else None
    service = workspace_service(
        tmp_path / "profiles",
        selection=DbtCliSelectionResolver(target_resolver=IsolatedDbtParseTargetResolver()),
        development_authority=authority,
    )
    report = service.compile(root, output_dir=compiled)
    assert report.passed, [(row.project.project_name, row.report.blockers) for row in report.check.projects]
    if development:
        from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
        from dpone.manifest.confined_files import read_confined_file
        from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader

        release = json.loads((compiled / "release-set.json").read_bytes())
        sources = DbtReleaseSourceReader(
            bundle_operations=RuntimeDbtProjectBundleOperations(),
            read_file=read_confined_file,
        ).read(compiled, expected_release_id=release["release_id"])
        assert len(sources.inventory.projects) == len(PROJECTS)
        delivered = materialize_compact_pack_release(
            pack_root=compiled,
            cache_root=tmp_path / "development-cache",
            xcom_sidecar_image=SIDECAR,
            development_authority=authority,
        )
        assert delivered.passed, delivered.blockers
    else:
        delivered_root = verify_delivery(tmp_path, compiled)
        _verify_runtime_targets(delivered_root, tmp_path / "real-preflight", executable)


@pytest.mark.parametrize("image", ["@sha256:" + "a" * 64, "", "registry.example/xcom:latest"])
def test_native_sidecar_must_pass_provider_validation(compiled_workspace, tmp_path, image):
    report = materialize_compact_pack_release(
        pack_root=compiled_workspace, cache_root=tmp_path / "cache", xcom_sidecar_image=image
    )
    assert not report.passed and not (tmp_path / "cache").exists()


def test_multiple_workflows_share_only_project_owned_objects(tmp_path):
    import yaml

    root = tmp_path / "workspace"
    prepare_projects(root)
    for name in PROJECTS:
        project = root / name
        policy_path = project / "dpone/dbt-publish-profiles.yml"
        policy = yaml.safe_load(policy_path.read_text())
        policy["workflows"][name + "_extra"] = deepcopy(policy["workflows"][name])
        policy_path.write_text(yaml.safe_dump(policy))
        manifest_path = project / "target/manifest.json"
        manifest = json.loads(manifest_path.read_bytes())
        models = [node for node in manifest["nodes"].values() if node.get("resource_type") == "model"]
        for meta in (models[-1].get("meta", {}), models[-1].get("config", {}).get("meta", {})):
            publish = meta.get("dpone", {}).get("publish", {})
            if publish:
                publish["workflow"] = name + "_extra"
        # Independent workflows may share sources, never cross-workflow ref() edges.
        models[0]["depends_on"]["nodes"] = list(models[-1]["depends_on"]["nodes"])
        manifest["parent_map"][models[0]["unique_id"]] = list(models[0]["depends_on"]["nodes"])
        manifest["child_map"][models[-1]["unique_id"]].remove(models[0]["unique_id"])
        manifest_path.write_text(json.dumps(manifest))
    compiled = tmp_path / "compiled"
    report = workspace_service(tmp_path / "profiles").compile(root, output_dir=compiled)
    assert report.passed, [(row.project.project_name, row.report.blockers) for row in report.check.projects]
    release = json.loads((compiled / "release-set.json").read_bytes())
    trios = [
        row["runtime_payload_ids"] for row in release["artifacts"]["workload_packs"] if "runtime_payload_ids" in row
    ]
    assert len(trios) == 4 and len({tuple(trio[:2]) for trio in trios}) == 2
    assert len(release["artifacts"]["runtime_payloads"]) == 8
    verify_delivery(tmp_path, compiled)


def test_concurrent_native_publication_and_existing_content_conflict(compiled_workspace, tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    def publish():
        return materialize_compact_pack_release(
            pack_root=compiled_workspace, cache_root=tmp_path / "cache", xcom_sidecar_image=SIDECAR
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        reports = list(pool.map(lambda _: publish(), range(2)))
    assert all(report.passed for report in reports) and reports[0] == reports[1]
    changed = Path(reports[0].release_dir) / "release-set.json"
    changed.write_bytes(b"conflicting retained content")
    rejected = publish()
    assert not rejected.passed and changed.read_bytes() == b"conflicting retained content"
