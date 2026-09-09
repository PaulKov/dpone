from __future__ import annotations

import json
import subprocess
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
import yaml
from dpone_airflow_pack import (
    dag_materializer,
    pack_tasks,
)
from dpone_airflow_pack import (
    workflow_outcome as workflow_outcome_module,
)
from dpone_airflow_pack.dag_loader import load_dpone_dags
from dpone_airflow_pack.init_fetch_contract import init_fetch_context_from_payload
from dpone_airflow_pack.init_fetch_pod import compose_init_fetch_operator_kwargs
from dpone_airflow_pack.pack_task_runtime import runtime_operator_kwargs

from dpone.adapters.dbt_workflow_selection import (
    DbtCliSelectionResolver,
    ManifestPreviewSelectionResolver,
)
from dpone.app import dbt_publish_composition
from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.app.dbt_publish_composition import build_dbt_dpone_compiler
from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.contracts.dbt_release import dbt_selection_fingerprint
from dpone.readiness import dbt_publish_release_materializer as release_materializer_module
from dpone.readiness.airflow_deployment_projection import AirflowDeploymentProjectionService
from dpone.readiness.airflow_deployment_projection_errors import (
    AirflowDeploymentProjectionError,
)
from dpone.readiness.airflow_release_schema_validation import (
    validate_release_set_schema,
)
from dpone.readiness.dbt_publish_release_materializer import (
    DbtReleaseMaterializationError,
    DbtReleaseMaterializer,
)
from dpone.readiness.dbt_sqlserver_project_policy import (
    DbtSqlserverProjectPolicyValidator,
)
from dpone.services.dbt_publish_artifact_writer import DbtArtifactWriter

ROOT = Path(__file__).parents[1]
DEMO = ROOT / "examples" / "dbt-inline-publishing"


def _artifact_writer(selection_resolver: object, **kwargs: object) -> DbtArtifactWriter:
    return DbtArtifactWriter(
        selection_resolver=selection_resolver,
        bundle_operations=RuntimeDbtProjectBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(project_root=DEMO),
        **kwargs,
    )


def _compiled_release(tmp_path: Path) -> Path:
    report = _certified(
        build_dbt_dpone_compiler().build(
            DEMO / "fixtures" / "manifest.v12.json",
            profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
        )
    )
    compiled_root = tmp_path / "compiled"
    written = _artifact_writer(_authoritative_selection_resolver()).write(
        report,
        compiled_root,
        project_root=DEMO,
    )
    assert written.passed
    return compiled_root


def test_dbt_compile_release_deployment_and_provider_strict_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = build_dbt_dpone_compiler().build(
        DEMO / "fixtures" / "manifest.v12.json",
        profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
    )
    report = _certified(report)
    compiled_root = tmp_path / "compiled"
    monkeypatch.setattr(
        dbt_publish_composition,
        "DbtCliSelectionResolver",
        _authoritative_selection_resolver,
    )
    written = dbt_publish_composition.build_dbt_artifact_writer(
        dbt_profiles_dir=None,
    ).write(report, compiled_root, project_root=DEMO)
    assert written.passed
    release = json.loads((compiled_root / "release-set.json").read_text(encoding="utf-8"))
    assert release["selection_authority"] == "dbt_cli"
    release_id = release["release_id"]
    dag_spec = json.loads(next((compiled_root / "_dags").glob("*.dag-spec.json")).read_text(encoding="utf-8"))
    assert dag_spec["max_active_tasks"] == 2
    assert dag_spec["workflow_outcome"]["expected_terminal_task_ids"] == [
        "dbt_competitive_pricing__outcome_gate",
        "dbt_competitive_pricing_history__outcome_gate",
    ]
    dbt_pack = json.loads(
        next(
            path for path in (compiled_root / "packs").glob("*.airflow-pack.json") if path.name.startswith("dbt__")
        ).read_text(encoding="utf-8")
    )
    assert dbt_pack["provider_execution"]["kpo_kwargs"]["pool"] == ("dpone_dbt__dpone_dbt_demo__runtime")
    assert dbt_pack["provider_execution"]["kpo_kwargs"]["execution_timeout_seconds"] == 3900
    materialized = DbtReleaseMaterializer().materialize(
        compiled_root=compiled_root,
        cache_root=tmp_path / ".dpone-cache",
    )
    assert materialized.release_id == release_id
    assert not materialized.no_op
    repeated = DbtReleaseMaterializer().materialize(
        compiled_root=compiled_root,
        cache_root=tmp_path / ".dpone-cache",
    )
    assert repeated.no_op
    _write_environment(tmp_path)

    image_digest = "sha256:" + "d" * 64
    projection = AirflowDeploymentProjectionService(root=tmp_path).materialize(
        release_id=release_id,
        environment="prod",
        trust_tier="production",
        runtime_image_ref=f"registry.example/dpone-runtime@{image_digest}",
        runtime_image_digest=image_digest,
        artifact_registry_ref="dpone-prod-artifacts",
        registry_config_ref=_config_map_ref("registry", "1"),
        trust_policy_ref=_config_map_ref("policy", "2"),
        airflow_bundle_ref="git:" + "d" * 40,
    )
    context = init_fetch_context_from_payload(projection.airflow_index)
    assert {item.kind for item in context.runtime_payloads} == {
        "dbt_project_bundle",
        "dbt_manifest",
        "dbt_selection_lock",
    }
    for item in projection.airflow_index["workload_packs"]:
        pack = json.loads(
            (tmp_path / ".dpone-cache" / item["artifact_ref"].removeprefix("cache://")).read_text(encoding="utf-8")
        )
        kwargs = compose_init_fetch_operator_kwargs(
            pack=pack,
            kwargs=runtime_operator_kwargs(
                pack,
                strict_provider_execution=True,
                expected_workload_id=item["id"],
            ),
            context=context,
            workload_id=item["id"],
            execution_kind="runtime",
            execution_scope="workload",
            hook_execution="externalized",
        )
        assert kwargs["retries"] == 0
        assert kwargs["image"].endswith("@" + image_digest)
        if item["id"].startswith("dbt__"):
            plan = context.encode_plan(
                workload_id=item["id"],
                execution_kind="runtime",
                execution_scope="workload",
                hook_execution="externalized",
            )
            assert b"dbt_project_bundle" in plan.payload


def test_dbt_release_initializes_writer_lease_before_unlocked_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    compiled_root = _compiled_release(tmp_path)
    state = {"held": False, "entries": 0}
    real_materialize = release_materializer_module.materialize_immutable_local_release

    @contextmanager
    def record_lease(_cache_root: Path):
        state["held"] = True
        state["entries"] += 1
        try:
            yield
        finally:
            state["held"] = False

    def materialize_outside_lease(release_dir: Path, files: dict[str, bytes]) -> str:
        assert state["held"] is False
        return real_materialize(release_dir, files)

    monkeypatch.setattr(release_materializer_module, "promotion_lock", record_lease)
    monkeypatch.setattr(
        release_materializer_module,
        "materialize_immutable_local_release",
        materialize_outside_lease,
    )

    materialized = DbtReleaseMaterializer().materialize(
        compiled_root=compiled_root,
        cache_root=tmp_path / ".dpone-cache",
    )

    assert state["entries"] == 1
    assert materialized.no_op is False


def test_dbt_release_translates_writer_lease_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_common import DeploymentCacheError

    compiled_root = _compiled_release(tmp_path)

    @contextmanager
    def fail_lease(_cache_root: Path):
        raise DeploymentCacheError("DPONE_CACHE_PROMOTION_LOCK_FAILED", "private lock detail")
        yield  # pragma: no cover

    monkeypatch.setattr(release_materializer_module, "promotion_lock", fail_lease)

    with pytest.raises(DbtReleaseMaterializationError) as exc:
        DbtReleaseMaterializer().materialize(
            compiled_root=compiled_root,
            cache_root=tmp_path / ".dpone-cache",
        )

    assert exc.value.code == "DPONE_DBT_RELEASE_CACHE_LOCK_FAILED"
    assert "private lock detail" not in str(exc.value)


def test_dbt_release_translates_real_cache_root_creation_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    compiled_root = _compiled_release(tmp_path)
    cache_root = (tmp_path / "blocked" / ".dpone-cache").absolute()
    original_mkdir = Path.mkdir

    def fail_cache_root(path: Path, *args: object, **kwargs: object) -> None:
        if path == cache_root:
            raise PermissionError("private filesystem detail")
        original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", fail_cache_root)

    with pytest.raises(DbtReleaseMaterializationError) as exc:
        DbtReleaseMaterializer().materialize(compiled_root=compiled_root, cache_root=cache_root)

    assert exc.value.code == "DPONE_DBT_RELEASE_CACHE_LOCK_FAILED"
    assert "private filesystem detail" not in str(exc.value)


def test_public_loader_materializes_compact_dbt_fanout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index_path, release_id, deployment_id = _materialized_index(tmp_path)
    operators: list[_RecordingOperator] = []

    class RecordingOperator(_RecordingOperator):
        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            operators.append(self)

    monkeypatch.setattr(
        dag_materializer,
        "_airflow_dag_types",
        lambda: (_RecordingDag, None),
    )
    monkeypatch.setattr(
        dag_materializer,
        "_task_group_class",
        lambda: _RecordingTaskGroup,
    )
    monkeypatch.setattr(pack_tasks, "_operator_class", lambda *_args: RecordingOperator)
    monkeypatch.setattr(
        pack_tasks,
        "build_pack_outcome_task",
        lambda **kwargs: RecordingOperator(
            dag=kwargs["dag"],
            task_id=kwargs["node"].task_id("outcome_gate"),
        ),
    )
    monkeypatch.setattr(
        pack_tasks,
        "build_pack_launch_pin_cleanup_task",
        lambda **kwargs: RecordingOperator(
            dag=kwargs["dag"],
            task_id=kwargs["node"].task_id("launch_pin_cleanup"),
        ),
    )

    def build_workflow_outcome_task(**kwargs: Any) -> RecordingOperator:
        config = kwargs["config"]
        assert isinstance(config, dict)
        expected_task_ids = tuple(config["expected_terminal_task_ids"])
        assert kwargs["expected_task_ids"] == expected_task_ids
        assert tuple(sorted(task.task_id for task in kwargs["upstream_tasks"])) == expected_task_ids
        task = RecordingOperator(dag=kwargs["dag"], task_id=config["task_id"])
        for upstream in kwargs["upstream_tasks"]:
            upstream >> task
        return task

    monkeypatch.setattr(
        workflow_outcome_module,
        "build_workflow_outcome_task",
        build_workflow_outcome_task,
    )

    namespace: dict[str, object] = {}
    load_report = load_dpone_dags(namespace, index_path=index_path)

    assert load_report.errors == ()
    assert len(load_report.loaded) == 1
    dag = namespace[load_report.loaded[0]]
    assert isinstance(dag, _RecordingDag)
    assert dag.kwargs["max_active_tasks"] == 2
    assert dag._dpone_run_identity_context["release_id"] == release_id
    assert dag._dpone_run_identity_context["deployment_id"] == deployment_id
    runtimes = [operator for operator in operators if _has_init_fetch_plan(operator)]
    assert len(runtimes) == 3
    assert {operator.kwargs["retries"] for operator in runtimes} == {0}
    dbt_runtime = next(operator for operator in runtimes if str(operator.kwargs["task_id"]).startswith("dbt__"))
    transfer_runtimes = [operator for operator in runtimes if operator is not dbt_runtime]
    assert set(transfer_runtimes).issubset(_reachable(dbt_runtime))
    assert not any(candidate in source.downstream for source in transfer_runtimes for candidate in transfer_runtimes)
    workflow_outcome = next(operator for operator in operators if operator.kwargs["task_id"] == "workflow_outcome")
    assert all(workflow_outcome in _reachable(transfer) for transfer in transfer_runtimes)


def test_release_materialization_rejects_preview_selection(tmp_path: Path) -> None:
    report = _certified(
        build_dbt_dpone_compiler().build(
            DEMO / "fixtures" / "manifest.v12.json",
            profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
        )
    )
    compiled_root = tmp_path / "compiled"
    written = _artifact_writer(ManifestPreviewSelectionResolver()).write(
        report,
        compiled_root,
        project_root=DEMO,
    )
    assert written.passed

    with pytest.raises(DbtReleaseMaterializationError, match="dbt-authoritative"):
        DbtReleaseMaterializer().materialize(
            compiled_root=compiled_root,
            cache_root=tmp_path / ".dpone-cache",
        )

    assert not (tmp_path / ".dpone-cache" / "releases").exists()


def test_release_materialization_rejects_unexpected_identity_before_install(
    tmp_path: Path,
) -> None:
    report = _certified(
        build_dbt_dpone_compiler().build(
            DEMO / "fixtures" / "manifest.v12.json",
            profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
        )
    )
    compiled_root = tmp_path / "compiled"
    written = _artifact_writer(_authoritative_selection_resolver()).write(
        report,
        compiled_root,
        project_root=DEMO,
    )
    assert written.passed

    with pytest.raises(DbtReleaseMaterializationError, match="expected release"):
        DbtReleaseMaterializer().materialize(
            compiled_root=compiled_root,
            cache_root=tmp_path / ".dpone-cache",
            expected_release_id="sha256:" + "f" * 64,
        )

    assert not (tmp_path / ".dpone-cache" / "releases").exists()


def test_release_materialization_rejects_unverified_route(tmp_path: Path) -> None:
    report = build_dbt_dpone_compiler().build(
        DEMO / "fixtures" / "manifest.v12.json",
        profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
    )
    assert report.passed
    assert {item.route_capability["evidence_status"] for item in report.models} != {"PASS"}
    compiled_root = tmp_path / "compiled"
    written = _artifact_writer(_authoritative_selection_resolver()).write(report, compiled_root, project_root=DEMO)
    assert written.passed

    with pytest.raises(
        DbtReleaseMaterializationError,
        match="production-certified route evidence",
    ):
        DbtReleaseMaterializer().materialize(
            compiled_root=compiled_root,
            cache_root=tmp_path / ".dpone-cache",
        )

    assert not (tmp_path / ".dpone-cache" / "releases").exists()


def test_release_identity_binds_selection_authority_and_certification(
    tmp_path: Path,
) -> None:
    report = _certified(
        build_dbt_dpone_compiler().build(
            DEMO / "fixtures" / "manifest.v12.json",
            profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
        )
    )
    compiled_root = tmp_path / "compiled"
    written = _artifact_writer(_authoritative_selection_resolver()).write(report, compiled_root, project_root=DEMO)
    assert written.passed
    release_path = compiled_root / "release-set.json"
    release = json.loads(release_path.read_text(encoding="utf-8"))

    changed_authority = dict(release)
    changed_authority["selection_authority"] = "manifest_preview"
    assert compute_release_id(changed_authority) != release["release_id"]

    changed_producer = json.loads(json.dumps(release))
    changed_producer["producer"]["dpone_version"] = "999.0.0"
    assert compute_release_id(changed_producer) != release["release_id"]

    changed_variant = json.loads(json.dumps(release))
    changed_variant["provenance"]["route_certifications"][0]["transport"] = "different_transport"
    changed_variant["selection_fingerprint"] = dbt_selection_fingerprint(
        source_snapshot_sha256=changed_variant["provenance"]["source_snapshot_sha256"],
        selection_fingerprints=changed_variant["provenance"]["selection_fingerprints"],
        route_certifications=changed_variant["provenance"]["route_certifications"],
    )
    assert compute_release_id(changed_variant) != release["release_id"]

    changed_evidence = json.loads(json.dumps(release))
    changed_evidence["provenance"]["route_certifications"][0]["evidence_refs"] = ["sha256:" + "f" * 64]
    changed_evidence["selection_fingerprint"] = dbt_selection_fingerprint(
        source_snapshot_sha256=changed_evidence["provenance"]["source_snapshot_sha256"],
        selection_fingerprints=changed_evidence["provenance"]["selection_fingerprints"],
        route_certifications=changed_evidence["provenance"]["route_certifications"],
    )
    assert compute_release_id(changed_evidence) != release["release_id"]

    release["provenance"]["route_certifications"][0]["certification_level"] = "experimental"
    release_path.write_text(json.dumps(release), encoding="utf-8")
    with pytest.raises(
        DbtReleaseMaterializationError,
        match="production-certified route evidence",
    ):
        DbtReleaseMaterializer().materialize(
            compiled_root=compiled_root,
            cache_root=tmp_path / ".dpone-cache",
        )


def test_generic_deployment_admission_enforces_dbt_release_authority(
    tmp_path: Path,
) -> None:
    report = _certified(
        build_dbt_dpone_compiler().build(
            DEMO / "fixtures" / "manifest.v12.json",
            profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
        )
    )
    compiled_root = tmp_path / "compiled"
    written = _artifact_writer(_authoritative_selection_resolver()).write(
        report,
        compiled_root,
        project_root=DEMO,
    )
    assert written.passed
    release = json.loads((compiled_root / "release-set.json").read_text(encoding="utf-8"))
    release["selection_fingerprint"] = "sha256:" + "0" * 64

    with pytest.raises(
        AirflowDeploymentProjectionError,
        match="selection or certification authority",
    ):
        validate_release_set_schema(
            release,
            path=compiled_root / "release-set.json",
        )


def test_release_materialization_rejects_different_dpone_version(
    tmp_path: Path,
) -> None:
    report = _certified(
        build_dbt_dpone_compiler().build(
            DEMO / "fixtures" / "manifest.v12.json",
            profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
        )
    )
    compiled_root = tmp_path / "compiled"
    written = _artifact_writer(
        _authoritative_selection_resolver(),
        producer_version="999.0.0",
    ).write(report, compiled_root, project_root=DEMO)
    assert written.passed

    with pytest.raises(DbtReleaseMaterializationError, match="different dpone version"):
        DbtReleaseMaterializer().materialize(
            compiled_root=compiled_root,
            cache_root=tmp_path / ".dpone-cache",
        )


def _write_environment(root: Path) -> None:
    environment = root / "environments" / "prod"
    environment.mkdir(parents=True)
    (environment / "binding-set.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.binding-set.v1",
                "environment": "prod",
                "bindings": {
                    "mssql_dwh_stage": {"connection_ref": "mssql_dwh_stage"},
                    "clickhouse_dwh_dev": {"connection_ref": "clickhouse_dwh_dev"},
                },
                "runtime": {
                    "kubernetes_namespace": "airflow-example",
                    "service_account": "dpone-runtime",
                    "pool": "dpone-prod",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (environment / "credential-runtime.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.credential-runtime.v1",
                "environment": "prod",
                "vault": {
                    "address": "https://vault.internal",
                    "namespace": "data-platform",
                    "auth": {"method": "kubernetes", "role": "dpone-runtime-prod"},
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    registry = root / "platform" / "connection-registries"
    registry.mkdir(parents=True)
    connections = {
        "mssql_dwh_stage": _vault_connection("mssql", 1433),
        "clickhouse_dwh_dev": _vault_connection("clickhouse", 9000),
    }
    (registry / "prod.yaml").write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": connections,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _vault_connection(connection_type: str, port: int) -> dict[str, object]:
    host = f"{connection_type}.internal"
    connection: dict[str, object] = {"host": host, "port": port, "database": "dwh"}
    if connection_type == "mssql":
        connection["asset_authority"] = {"host": host, "port": port}
    return {
        "type": connection_type,
        "connection": connection,
        "credentials": {
            "resolver": "vault_kv",
            "mount": "kv",
            "kv_version": 2,
            "path": f"dpone/prod/credentials/{connection_type}",
            "fields": {"username": "username", "password": "password"},
            "version_policy": "latest",
            "resolution_scope": "workload_start",
        },
    }


def _config_map_ref(label: str, digit: str) -> dict[str, str]:
    return {
        "kind": "kubernetes_config_map",
        "name": f"dpone-artifact-{label}",
        "key": f"{label}.json",
        "sha256": "sha256:" + digit * 64,
    }


def _authoritative_selection_resolver() -> DbtCliSelectionResolver:
    manifest_bytes = (DEMO / "fixtures" / "manifest.v12.json").read_bytes()
    manifest = json.loads(manifest_bytes)

    def runner(args, **_kwargs):
        if "parse" in args:
            target = Path(args[args.index("--target-path") + 1])
            target.mkdir(parents=True)
            (target / "manifest.json").write_bytes(manifest_bytes)
            return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")
        selectors = args[args.index("--select") + 1 :]
        selected_fqns = {selector.removeprefix("+fqn:") for selector in selectors}
        selected_unique_ids = tuple(
            sorted(unique_id for unique_id, node in manifest["nodes"].items() if ".".join(node["fqn"]) in selected_fqns)
        )
        selection = ManifestPreviewSelectionResolver().resolve(
            project_root=DEMO,
            manifest_bytes=manifest_bytes,
            selected_unique_ids=selected_unique_ids,
            profiles_dir=None,
            profile_name="dpone_runtime",
            target_name="runtime",
            dbt_core_version="1.12.3",
            dbt_adapter="sqlserver",
            dbt_adapter_version="1.11.1",
        )
        stdout = b"".join(
            json.dumps({"unique_id": unique_id}).encode() + b"\n" for unique_id in selection.selected_graph_unique_ids
        )
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr=b"")

    versions = {
        "dbt-core": "1.12.3",
        "dbt-sqlserver": "1.11.1",
    }
    return DbtCliSelectionResolver(
        runner=runner,
        package_version=versions.__getitem__,
    )


def _certified(report):
    evidence_ref = "sha256:" + "e" * 64
    models = tuple(
        replace(
            item,
            route_capability={
                **dict(item.route_capability),
                "certification_level": "production-certified",
                "evidence_status": "PASS",
                "variant_id": (str(item.route_capability["route_id"]).replace(":", "_") + "_airflow_kpo"),
                "transport": "native_bcp_to_clickhouse",
                "schema_evolution": "widening",
                "airflow_runtime_mode": "kpo",
                "evidence_refs": [evidence_ref],
                "evidence_reason_codes": [],
            },
        )
        for item in report.models
    )
    by_id = {item.model.unique_id: item for item in models}
    workflows = tuple(
        replace(
            workflow,
            models=tuple(by_id[item.model.unique_id] for item in workflow.models),
        )
        for workflow in report.workflows
    )
    return replace(report, models=models, workflows=workflows)


def _materialized_index(tmp_path: Path) -> tuple[Path, str, str]:
    report = _certified(
        build_dbt_dpone_compiler().build(
            DEMO / "fixtures" / "manifest.v12.json",
            profiles_path=DEMO / "dpone" / "dbt-publish-profiles.yml",
        )
    )
    compiled_root = tmp_path / "compiled"
    _artifact_writer(_authoritative_selection_resolver()).write(report, compiled_root, project_root=DEMO)
    release = DbtReleaseMaterializer().materialize(
        compiled_root=compiled_root,
        cache_root=tmp_path / ".dpone-cache",
    )
    _write_environment(tmp_path)
    image_digest = "sha256:" + "d" * 64
    projection = AirflowDeploymentProjectionService(root=tmp_path).materialize(
        release_id=release.release_id,
        environment="prod",
        trust_tier="production",
        runtime_image_ref=f"registry.example/dpone-runtime@{image_digest}",
        runtime_image_digest=image_digest,
        artifact_registry_ref="dpone-prod-artifacts",
        registry_config_ref=_config_map_ref("registry", "1"),
        trust_policy_ref=_config_map_ref("policy", "2"),
        airflow_bundle_ref="git:" + "d" * 40,
    )
    return (
        projection.deployment_dir / "airflow-index.json",
        release.release_id,
        projection.deployment["deployment_id"],
    )


class _RecordingDag:
    def __init__(
        self,
        *,
        dag_id: str,
        schedule: object = None,
        start_date: object = None,
        timezone: object = None,
        **kwargs: object,
    ) -> None:
        self.kwargs = {
            "dag_id": dag_id,
            "schedule": schedule,
            "start_date": start_date,
            "timezone": timezone,
            **kwargs,
        }
        self._dpone_run_identity_context: dict[str, Any] = {}


class _RecordingTaskGroup:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


class _RecordingOperator:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.task_id = str(kwargs["task_id"])
        self.downstream: list[_RecordingOperator] = []

    def __rshift__(self, other: _RecordingOperator) -> _RecordingOperator:
        self.downstream.append(other)
        return other


def _reachable(operator: _RecordingOperator) -> set[_RecordingOperator]:
    pending = list(operator.downstream)
    found: set[_RecordingOperator] = set()
    while pending:
        candidate = pending.pop()
        if candidate in found:
            continue
        found.add(candidate)
        pending.extend(candidate.downstream)
    return found


def _has_init_fetch_plan(operator: _RecordingOperator) -> bool:
    env_vars = operator.kwargs.get("env_vars")
    return isinstance(env_vars, dict) and "DPONE_INIT_FETCH_PLAN_B64" in env_vars
