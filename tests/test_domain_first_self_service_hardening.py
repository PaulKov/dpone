"""Negative and concurrency contracts for domain-first self-service."""

from __future__ import annotations

import copy
import shutil
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import get_context
from pathlib import Path
from threading import Barrier

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.manifest import project_discovery_pipelines as project_discovery_pipelines_module
from dpone.manifest import project_discovery_scan as project_discovery_module
from dpone.manifest.project_config import ProjectConfigError, resolve_project_layout
from dpone.manifest.project_discovery import (
    ProjectDiscoveryProjectionError,
    ProjectDiscoveryService,
)
from dpone.manifest.selection import SelectionEngine, SelectionRequest, state_from_graph
from dpone.readiness import airflow_pipeline_scaffold as airflow_pipeline_scaffold_module
from dpone.readiness import airflow_preview_service as airflow_preview_module
from dpone.readiness import project_selection_preview as project_selection_preview_module
from dpone.readiness.airflow_pipeline_scaffold_preflight import DomainFirstScaffoldGuard
from dpone.readiness.airflow_scaffold_apply import ScaffoldFile, ScaffoldFileSystem
from dpone.readiness.airflow_self_service import AirflowSelfServiceService
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.readiness.project_selection import ProjectSelectionService, SelectionError
from dpone.readiness.project_selection_loader import ProjectSelectionLoader
from dpone.readiness.project_selection_preview import ProjectSelectionPreviewService
from dpone.services.hermetic_test_project import HermeticProjectError, HermeticTestProject
from dpone.services.workload_discovery_projection import build_change_impact_report
from dpone.services.workload_index_contract import (
    compare_workload_indexes,
    validate_workload_index,
    workload_index_from_snapshot,
)


def _project(root: Path) -> AirflowSelfServiceService:
    service = build_airflow_self_service_service(root=root)
    assert service.init_project(airflow=True, layout="domain_first").passed
    return service


def _domain(service: AirflowSelfServiceService, domain: str) -> None:
    assert service.init_domain(
        domain=domain,
        owner_team=f"data-{domain}",
        owner_contact=f"{domain}@example.com",
        approver_team="data-platform",
    ).passed


def _pipeline(service: AirflowSelfServiceService, pipeline_id: str, domain: str):
    return service.init_pipeline(
        pipeline_id=pipeline_id,
        domain=domain,
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    )


def _process_pipeline_init(root: str, domain: str, start: object, results: object) -> None:
    start.wait()
    result = _pipeline(build_airflow_self_service_service(root=root), "orders_daily", domain)
    code = result.errors[0]["code"] if result.errors else None
    results.put((result.passed, code))


def _run_cli(
    args: list[str],
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err


@pytest.mark.parametrize(
    "target",
    [
        "crm/orders_daily",
        "workloads/crm/pipelines/orders_daily",
        "workloads/crm/pipelines/orders_daily/pipeline.yaml",
    ],
)
def test_targeted_check_and_preview_fail_on_invalid_discovery_authority(tmp_path: Path, target: str) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["metadata"]["domain"] = "finance"
    source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    checked = service.check(target)
    preview = service.preview(target)

    assert not checked.passed
    assert checked.errors[0]["code"] == "DPONE_PIPELINE_DOMAIN_MISMATCH"
    assert not preview.passed
    assert preview.errors[0]["code"] == "DPONE_PIPELINE_DOMAIN_MISMATCH"
    assert not (tmp_path / ".dpone-cache").exists()


def test_discovery_error_preserves_pipeline_identity_and_compiler_reason(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["kind"] = "dpone.unknown.v1"
    source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    result = service.check("orders_daily")

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_PIPELINE_COMPILATION_FAILED"
    assert result.errors[0]["entity"] == {"kind": "pipeline", "id": "orders_daily"}
    assert "could not be compiled (" in result.errors[0]["message"]


def test_malformed_source_error_preserves_pipeline_identity(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    source.write_text("metadata: [\n", encoding="utf-8")

    result = service.check("orders_daily")

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_PIPELINE_SOURCE_INVALID"
    assert result.errors[0]["entity"] == {"kind": "pipeline", "id": "orders_daily"}


def test_missing_pipeline_error_preserves_requested_identity(tmp_path: Path) -> None:
    service = _project(tmp_path)

    result = service.check("missing_daily")

    assert not result.passed
    assert result.errors[0]["entity"] == {"kind": "pipeline", "id": "missing_daily"}


def test_runtime_ownership_validation_matches_closed_public_schema(tmp_path: Path) -> None:
    service = _project(tmp_path)
    ownership = tmp_path / "workloads/crm/ownership.yaml"
    ownership.parent.mkdir(parents=True)
    ownership.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.domain-ownership.v1",
                "domain": "crm",
                "owner": {"team": "data-crm", "contact": "crm@example.com"},
                "unexpected": True,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = _pipeline(service, "orders_daily", "crm")

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_DOMAIN_OWNERSHIP_INVALID"
    assert not (tmp_path / "workloads/crm/pipelines/orders_daily").exists()


def test_malformed_project_config_never_falls_back_with_explicit_airflow(tmp_path: Path) -> None:
    (tmp_path / "dpone.yaml").write_text("schema: wrong\n", encoding="utf-8")
    service = build_airflow_self_service_service(root=tmp_path)

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    )

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_PROJECT_CONFIG_INVALID"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["dpone.yaml"]


@pytest.mark.parametrize("broken_path", ["root", "pipelines"])
def test_broken_discovery_symlink_fails_closed(tmp_path: Path, broken_path: str) -> None:
    service = _project(tmp_path)
    if broken_path == "root":
        (tmp_path / "workloads").symlink_to(tmp_path / "missing-root", target_is_directory=True)
    else:
        _domain(service, "crm")
        (tmp_path / "workloads/crm/pipelines").symlink_to(
            tmp_path / "missing-pipelines",
            target_is_directory=True,
        )

    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert not snapshot.ok
    assert snapshot.issues[0].code == (
        "DPONE_LAYOUT_ROOT_INVALID" if broken_path == "root" else "DPONE_DISCOVERY_PATH_INVALID"
    )


def test_change_impact_marks_sql_dependency_content_change(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["processes"][0]["source"]["query"] = {"sql_file": "query.sql"}
    source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    sql_path = source.parent / "query.sql"
    sql_path.write_text("SELECT 1\n", encoding="utf-8")
    discovery = ProjectDiscoveryService(tmp_path)
    baseline = discovery.discover()

    sql_path.write_text("SELECT 2\n", encoding="utf-8")
    current = discovery.discover()
    impact = compare_workload_indexes(baseline, current)

    assert baseline.ok and current.ok
    assert impact.modified == ("orders_daily",)


def test_failed_discovery_cannot_emit_schema_valid_partial_index(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    _domain(service, "finance")
    assert _pipeline(service, "orders_daily", "crm").passed
    crm_source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    finance_source = tmp_path / "workloads/finance/pipelines/orders_daily/pipeline.yaml"
    finance_source.parent.mkdir(parents=True)
    payload = yaml.safe_load(crm_source.read_text(encoding="utf-8"))
    payload["metadata"]["domain"] = "finance"
    payload["authoring"]["source"] = "workloads/finance/pipelines/orders_daily/pipeline.yaml"
    finance_source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert not snapshot.ok
    with pytest.raises(ProjectDiscoveryProjectionError):
        workload_index_from_snapshot(snapshot)


def test_discovery_pipeline_budget_counts_invalid_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    monkeypatch.setattr(project_discovery_pipelines_module, "MAX_PROJECT_WORKLOADS", 2)
    pipelines = tmp_path / "workloads/crm/pipelines"
    for name in ("one", "two", "three"):
        (pipelines / name).mkdir(parents=True)

    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert not snapshot.ok
    assert snapshot.issues[0].code == "DPONE_DISCOVERY_LIMIT_EXCEEDED"
    assert len(snapshot.issues) == 1


def test_discovery_domain_budget_counts_every_visible_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    monkeypatch.setattr(project_discovery_module, "MAX_PROJECT_DOMAINS", 2)
    workloads = tmp_path / "workloads"
    for name in ("a-invalid", "b-invalid", "c-invalid"):
        (workloads / name).write_text("not a domain\n", encoding="utf-8")

    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert not snapshot.ok
    assert snapshot.issues[0].code == "DPONE_DISCOVERY_LIMIT_EXCEEDED"
    assert snapshot.workloads == ()


def test_discovery_revalidates_content_digest_after_compilation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    original_size = project_discovery_pipelines_module.confined_size
    mutated = False

    def mutate_after_measurement(root: Path, path: str) -> int | None:
        nonlocal mutated
        size = original_size(root, path)
        if path.endswith("/pipeline.yaml") and not mutated:
            mutated = True
            content = source.read_text(encoding="utf-8")
            source.write_text(content.replace("orders", "ordfrs", 1), encoding="utf-8")
        return size

    monkeypatch.setattr(project_discovery_pipelines_module, "confined_size", mutate_after_measurement)

    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert not snapshot.ok
    assert {issue.code for issue in snapshot.issues} == {"DPONE_SELECTION_STATE_CHANGED"}
    with pytest.raises(ProjectDiscoveryProjectionError):
        workload_index_from_snapshot(snapshot)


def test_discovery_revalidates_namespace_membership_before_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    original = project_discovery_module.namespace_unchanged
    mutated = False

    def add_workload_before_revalidation(root: Path, expected) -> bool:
        nonlocal mutated
        if expected.label == "workloads/crm/pipelines" and not mutated:
            mutated = True
            (tmp_path / "workloads/crm/pipelines/new_pipeline").mkdir()
        return original(root, expected)

    monkeypatch.setattr(project_discovery_module, "namespace_unchanged", add_workload_before_revalidation)

    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert not snapshot.ok
    assert {issue.code for issue in snapshot.issues} == {"DPONE_SELECTION_STATE_CHANGED"}


def test_discovery_rejects_unexpected_domain_hierarchy(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    misplaced = tmp_path / "workloads/crm/pipeline/orders_daily"
    misplaced.mkdir(parents=True)

    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert not snapshot.ok
    assert snapshot.issues[0].code == "DPONE_DISCOVERY_PATH_INVALID"
    assert snapshot.issues[0].path == "workloads/crm/pipeline"


def test_change_impact_rejects_schema_incomplete_baseline(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed

    with pytest.raises(ProjectDiscoveryProjectionError):
        build_change_impact_report(
            tmp_path,
            baseline_index={"schema": "dpone.workload-index.v1", "workloads": []},
        )


def test_change_impact_rejects_noncanonical_uppercase_baseline_digest(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    baseline = workload_index_from_snapshot(ProjectDiscoveryService(tmp_path).discover())
    baseline["project_fingerprint"] = str(baseline["project_fingerprint"]).upper()

    with pytest.raises(ProjectDiscoveryProjectionError):
        build_change_impact_report(tmp_path, baseline_index=baseline)


def test_change_impact_rejects_schema_complete_inconsistent_baseline(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    baseline = workload_index_from_snapshot(ProjectDiscoveryService(tmp_path).discover())
    baseline["workloads"][0]["workload_fingerprint"] = "sha256:" + "f" * 64

    with pytest.raises(ProjectDiscoveryProjectionError, match="does not match"):
        build_change_impact_report(tmp_path, baseline_index=baseline)


def test_workload_fingerprint_binds_every_closed_index_field(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    baseline = workload_index_from_snapshot(ProjectDiscoveryService(tmp_path).discover())

    mutations = (
        (
            lambda item: item.update(authoring_source="workloads/crm/pipelines/renamed/pipeline.yaml"),
            "not canonical",
        ),
        (lambda item: item.update(source_sha256="sha256:" + "1" * 64), "workload fingerprint does not match"),
        (lambda item: item["connection_refs"].append("warehouse_readonly"), "workload fingerprint does not match"),
        (
            lambda item: item["dependencies"].append(
                {
                    "kind": "folder_file",
                    "path": "workloads/crm/pipelines/orders_daily/config.yaml",
                    "sha256": "sha256:" + "2" * 64,
                }
            ),
            "workload fingerprint does not match",
        ),
        (
            lambda item: item["airflow"].update(dag_id="renamed_orders_daily", schedule="@daily"),
            "DAG id is not canonical",
        ),
    )

    for mutate, expected_error in mutations:
        tampered = copy.deepcopy(baseline)
        mutate(tampered["workloads"][0])
        with pytest.raises(ProjectDiscoveryProjectionError, match=expected_error):
            validate_workload_index(tampered)


def test_layout_runtime_rejects_unknown_schema_fields(tmp_path: Path) -> None:
    (tmp_path / "dpone.yaml").write_text(
        "schema: dpone.project.v1\nlayout:\n  mode: domain_first\n  root_typo: private-workloads\n",
        encoding="utf-8",
    )

    with pytest.raises(ProjectConfigError, match="valid dpone.project.v1"):
        resolve_project_layout(tmp_path)


def test_project_config_runtime_rejects_schema_invalid_airflow_boolean(tmp_path: Path) -> None:
    (tmp_path / "dpone.yaml").write_text(
        "schema: dpone.project.v1\nairflow:\n  enabled: 'false'\n",
        encoding="utf-8",
    )

    with pytest.raises(ProjectConfigError, match="valid dpone.project.v1"):
        resolve_project_layout(tmp_path)


def test_project_selection_detects_project_config_change_during_discovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    original = ProjectDiscoveryService.discover

    def mutate_after_discovery(discovery: ProjectDiscoveryService, **kwargs):
        snapshot = original(discovery, **kwargs)
        (tmp_path / "dpone.yaml").write_text("schema: dpone.project.v1\nlayout:\n  mode: flat\n", encoding="utf-8")
        return snapshot

    monkeypatch.setattr(ProjectDiscoveryService, "discover", mutate_after_discovery)

    with pytest.raises(SelectionError, match="changed"):
        ProjectSelectionService(root=tmp_path).select(target=tmp_path)


def test_project_selection_accepts_filesystem_alias_of_project_root(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    alias = tmp_path / ".project-root-alias"
    alias.symlink_to(tmp_path, target_is_directory=True)

    loaded = ProjectSelectionLoader(root=tmp_path).load(alias)

    assert tuple(loaded.checked_sources) == ("orders_daily",)


def test_project_discovery_rejects_symbolic_link_project_root(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    service = _project(project)
    _domain(service, "crm")
    alias = tmp_path / "project-alias"
    alias.symlink_to(project, target_is_directory=True)

    with pytest.raises(ProjectDiscoveryProjectionError, match="symbolic link"):
        ProjectDiscoveryService(alias)


def test_concurrent_project_wide_pipeline_id_has_one_winner(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    _domain(service, "finance")
    ready = Barrier(2)

    def create(domain: str):
        ready.wait()
        return _pipeline(service, "orders_daily", domain)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(create, ("crm", "finance")))

    assert sum(result.passed for result in results) == 1
    assert {result.errors[0]["code"] for result in results if not result.passed} == {"DPONE_PIPELINE_ID_DUPLICATE"}
    snapshot = ProjectDiscoveryService(tmp_path).discover()
    assert snapshot.ok
    assert tuple(item.pipeline_id for item in snapshot.workloads) == ("orders_daily",)


def test_multiprocess_project_wide_pipeline_id_has_one_winner(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    _domain(service, "finance")
    context = get_context("spawn")
    start = context.Event()
    results = context.Queue()
    processes = [
        context.Process(target=_process_pipeline_init, args=(str(tmp_path), domain, start, results))
        for domain in ("crm", "finance")
    ]
    for process in processes:
        process.start()
    start.set()
    outcomes = tuple(results.get(timeout=30) for _ in processes)
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0

    assert sum(passed for passed, _ in outcomes) == 1
    assert {code for passed, code in outcomes if not passed} == {"DPONE_PIPELINE_ID_DUPLICATE"}
    assert ProjectDiscoveryService(tmp_path).discover().ok


def test_identical_domain_first_pipeline_retry_is_no_op(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")

    first = _pipeline(service, "orders_daily", "crm")
    retry = _pipeline(service, "orders_daily", "crm")

    assert first.passed and retry.passed
    assert retry.errors == ()
    assert retry.changes
    assert {change.action for change in retry.changes} == {"no_op"}


def test_ownership_authority_change_marks_domain_workload_modified(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    discovery = ProjectDiscoveryService(tmp_path)
    baseline = discovery.discover()
    ownership = tmp_path / "workloads/crm/ownership.yaml"
    payload = yaml.safe_load(ownership.read_text(encoding="utf-8"))
    payload["owner"]["contact"] = "new-crm@example.com"
    payload["approvers"]["github_team"] = "governance"
    ownership.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    current = discovery.discover()
    impact = compare_workload_indexes(baseline, current)

    assert baseline.ok and current.ok
    assert impact.modified == ("orders_daily",)


def test_ownership_authority_change_is_visible_to_state_modified_selector(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    loader = ProjectSelectionLoader(root=tmp_path)
    baseline_graph = loader.load(tmp_path).graph
    baseline = state_from_graph(baseline_graph)
    ownership = tmp_path / "workloads/crm/ownership.yaml"
    payload = yaml.safe_load(ownership.read_text(encoding="utf-8"))
    payload["owner"]["contact"] = "rotated-crm@example.com"
    ownership.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    current_graph = loader.load(tmp_path).graph
    report = SelectionEngine().select(
        current_graph,
        SelectionRequest(select=("state:modified",), state=baseline),
    )

    assert [item.node.node_id for item in report.selected] == ["orders_daily"]


def test_ownership_authority_change_changes_single_pipeline_preview_identity(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    before = service.preview("orders_daily")
    ownership = tmp_path / "workloads/crm/ownership.yaml"
    payload = yaml.safe_load(ownership.read_text(encoding="utf-8"))
    payload["owner"]["contact"] = "rotated-crm@example.com"
    ownership.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    after = service.preview("orders_daily")

    assert before.passed and after.passed
    assert before.details["release"]["release_id"] != after.details["release"]["release_id"]
    assert before.details["deployment"]["deployment_id"] != after.details["deployment"]["deployment_id"]
    assert (
        before.details["release"]["provenance"]["workload_fingerprint"]
        != (after.details["release"]["provenance"]["workload_fingerprint"])
    )


def test_external_ownership_change_after_scaffold_writes_rolls_back_created_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    original_after_apply = DomainFirstScaffoldGuard.after_apply

    def delete_authority_after_writes(guard: DomainFirstScaffoldGuard) -> bool:
        (tmp_path / "workloads/crm/ownership.yaml").unlink()
        return original_after_apply(guard)

    monkeypatch.setattr(DomainFirstScaffoldGuard, "after_apply", delete_authority_after_writes)

    result = _pipeline(service, "orders_daily", "crm")

    assert not result.passed
    assert any(change.action == "conflict" and change.path == "project-authority" for change in result.changes)
    pipeline_root = tmp_path / "workloads/crm/pipelines/orders_daily"
    assert not any(path.is_file() for path in pipeline_root.rglob("*"))


def test_external_pipeline_edit_during_postcondition_is_preserved_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    original_after_apply = DomainFirstScaffoldGuard.after_apply
    source_path = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"

    def edit_pipeline_after_writes(guard: DomainFirstScaffoldGuard) -> bool:
        payload = yaml.safe_load(source_path.read_text(encoding="utf-8"))
        payload["metadata"]["tags"].append("external-editor")
        source_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        return original_after_apply(guard)

    monkeypatch.setattr(DomainFirstScaffoldGuard, "after_apply", edit_pipeline_after_writes)

    result = _pipeline(service, "orders_daily", "crm")

    assert not result.passed
    assert any(change.action == "conflict" and change.path == "project-authority" for change in result.changes)
    preserved = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    assert "external-editor" in preserved["metadata"]["tags"]
    assert not (source_path.parent / "tests/pipeline.test.yaml").exists()


def test_external_workload_added_during_postcondition_is_preserved_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    original_after_apply = DomainFirstScaffoldGuard.after_apply
    concurrent_source = tmp_path / "workloads/crm/pipelines/payments_daily/pipeline.yaml"

    def add_workload_after_writes(guard: DomainFirstScaffoldGuard) -> bool:
        source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
        payload = yaml.safe_load(source.read_text(encoding="utf-8"))
        payload["metadata"]["id"] = "payments_daily"
        payload["metadata"]["source_path"] = "workloads/crm/pipelines/payments_daily/pipeline.yaml"
        payload["processes"][0]["name"] = "payments_daily"
        concurrent_source.parent.mkdir(parents=True)
        concurrent_source.write_text(
            yaml.safe_dump(payload, sort_keys=False),
            encoding="utf-8",
        )
        return original_after_apply(guard)

    monkeypatch.setattr(DomainFirstScaffoldGuard, "after_apply", add_workload_after_writes)

    result = _pipeline(service, "orders_daily", "crm")

    assert not result.passed
    assert any(change.action == "conflict" and change.path == "project-authority" for change in result.changes)
    assert concurrent_source.is_file()
    assert not (tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml").exists()


def test_ownership_change_before_single_preview_promotion_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    original_materialize = airflow_preview_module.materialize_immutable_local_tree

    def materialize_then_change_ownership(*args: object, **kwargs: object) -> object:
        result = original_materialize(*args, **kwargs)
        ownership = tmp_path / "workloads/crm/ownership.yaml"
        payload = yaml.safe_load(ownership.read_text(encoding="utf-8"))
        payload["owner"]["contact"] = "concurrent-editor@example.com"
        ownership.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        return result

    monkeypatch.setattr(
        airflow_preview_module,
        "materialize_immutable_local_tree",
        materialize_then_change_ownership,
    )

    result = service.preview("orders_daily")

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD"
    assert not (tmp_path / ".dpone-cache/current").exists()
    assert not (tmp_path / ".dpone-cache/current-pointer.json").exists()


def test_ownership_change_at_single_preview_promotion_boundary_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    original_sync = airflow_preview_module.local_cache_sync_result

    def change_ownership_then_sync(**kwargs: object):
        ownership = tmp_path / "workloads/crm/ownership.yaml"
        payload = yaml.safe_load(ownership.read_text(encoding="utf-8"))
        payload["owner"]["contact"] = "promotion-race@example.com"
        ownership.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        return original_sync(**kwargs)

    monkeypatch.setattr(
        airflow_preview_module,
        "local_cache_sync_result",
        change_ownership_then_sync,
    )

    result = service.preview("orders_daily")

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD"
    assert not (tmp_path / ".dpone-cache/current").exists()
    assert not (tmp_path / ".dpone-cache/current-pointer.json").exists()


def test_selected_preview_input_change_at_promotion_boundary_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    original_sync = project_selection_preview_module.local_cache_sync_result

    def change_source_then_sync(**kwargs: object):
        source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
        payload = yaml.safe_load(source.read_text(encoding="utf-8"))
        payload["metadata"]["tags"].append("promotion-race")
        source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        return original_sync(**kwargs)

    monkeypatch.setattr(
        project_selection_preview_module,
        "local_cache_sync_result",
        change_source_then_sync,
    )

    result = ProjectSelectionPreviewService(root=tmp_path).preview(
        target=tmp_path,
        select=(),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yml",
        max_selected=100,
    )

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_SELECTION_STATE_CHANGED"
    assert not (tmp_path / ".dpone-cache/current").exists()
    assert not (tmp_path / ".dpone-cache/current-pointer.json").exists()


def test_selected_preview_namespace_change_at_promotion_boundary_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    original_sync = project_selection_preview_module.local_cache_sync_result

    def add_workload_then_sync(**kwargs: object):
        assert _pipeline(service, "customers_daily", "crm").passed
        return original_sync(**kwargs)

    monkeypatch.setattr(
        project_selection_preview_module,
        "local_cache_sync_result",
        add_workload_then_sync,
    )

    result = ProjectSelectionPreviewService(root=tmp_path).preview(
        target=tmp_path,
        select=(),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yml",
        max_selected=100,
    )

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_SELECTION_STATE_CHANGED"
    assert not (tmp_path / ".dpone-cache/current").exists()
    assert not (tmp_path / ".dpone-cache/current-pointer.json").exists()


def test_selected_preview_namespace_removal_at_promotion_boundary_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    assert _pipeline(service, "customers_daily", "crm").passed
    original_sync = project_selection_preview_module.local_cache_sync_result

    def remove_workload_then_sync(**kwargs: object):
        shutil.rmtree(tmp_path / "workloads/crm/pipelines/customers_daily")
        return original_sync(**kwargs)

    monkeypatch.setattr(
        project_selection_preview_module,
        "local_cache_sync_result",
        remove_workload_then_sync,
    )

    result = ProjectSelectionPreviewService(root=tmp_path).preview(
        target=tmp_path,
        select=(),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yml",
        max_selected=100,
    )

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_SELECTION_STATE_CHANGED"
    assert not (tmp_path / ".dpone-cache/current").exists()
    assert not (tmp_path / ".dpone-cache/current-pointer.json").exists()


def test_selected_preview_precommit_snapshot_is_the_promotion_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    original_commit = DeploymentCacheMaterializer._commit_promotion

    def change_source_then_commit(
        materializer: DeploymentCacheMaterializer,
        *,
        deployment_path: Path,
        pointer: dict[str, object],
    ):
        source = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"
        payload = yaml.safe_load(source.read_text(encoding="utf-8"))
        payload["metadata"]["tags"].append("after-precheck")
        source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        return original_commit(materializer, deployment_path=deployment_path, pointer=pointer)

    monkeypatch.setattr(DeploymentCacheMaterializer, "_commit_promotion", change_source_then_commit)

    result = ProjectSelectionPreviewService(root=tmp_path).preview(
        target=tmp_path,
        select=(),
        exclude=(),
        state_path=None,
        selectors_path="selectors.yml",
        max_selected=100,
    )

    assert result.passed
    assert (tmp_path / ".dpone-cache/current").exists()
    assert (tmp_path / ".dpone-cache/current-pointer.json").exists()


def test_builtin_answers_change_before_scaffold_apply_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    answers = tmp_path / "answers.yaml"
    answers.write_text("source_table: orders\n", encoding="utf-8")
    original_resolve = airflow_pipeline_scaffold_module.resolve_builtin_defaults

    def resolve_then_change_answers(*args: object, **kwargs: object):
        result = original_resolve(*args, **kwargs)
        answers.write_text("source_table: payments\n", encoding="utf-8")
        return result

    monkeypatch.setattr(
        airflow_pipeline_scaffold_module,
        "resolve_builtin_defaults",
        resolve_then_change_answers,
    )

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        answers=answers,
        airflow=True,
    )

    assert not result.passed
    assert any(change.action == "conflict" and change.path == "project-authority" for change in result.changes)
    assert not (tmp_path / "workloads/crm/pipelines/orders_daily").exists()
    assert answers.read_text(encoding="utf-8") == "source_table: payments\n"


def test_flat_authoring_blocks_implicit_domain_first_migration(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=False,
    ).passed

    migration = service.init_project(airflow=True, layout="domain_first")

    assert not migration.passed
    assert migration.errors[0]["code"] == "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED"
    assert not (tmp_path / "dpone.yaml").exists()


def test_unconfigured_domain_first_authority_requires_migration_even_for_same_requested_layout(
    tmp_path: Path,
) -> None:
    ownership = tmp_path / "workloads/crm/ownership.yaml"
    ownership.parent.mkdir(parents=True)
    ownership.write_text("schema: dpone.domain-ownership.v1\n", encoding="utf-8")

    result = build_airflow_self_service_service(root=tmp_path).init_project(
        airflow=True,
        layout="domain-first",
    )

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED"
    assert result.errors[0]["detected_layout"] == "domain_first"
    assert result.errors[0]["requested_layout"] == "domain_first"
    assert not (tmp_path / "dpone.yaml").exists()


def test_domain_first_authority_blocks_implicit_flat_recovery(tmp_path: Path) -> None:
    ownership = tmp_path / "workloads/crm/ownership.yaml"
    ownership.parent.mkdir(parents=True)
    ownership.write_text("schema: dpone.domain-ownership.v1\n", encoding="utf-8")

    result = build_airflow_self_service_service(root=tmp_path).init_project(airflow=True)

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED"
    assert result.errors[0]["detected_layout"] == "domain_first"
    assert result.errors[0]["requested_layout"] == "flat"
    assert not (tmp_path / "dpone.yaml").exists()


def test_mixed_authoring_authority_blocks_project_init(tmp_path: Path) -> None:
    (tmp_path / "pipelines/orders").mkdir(parents=True)
    (tmp_path / "workloads/crm").mkdir(parents=True)
    (tmp_path / "workloads/crm/ownership.yaml").write_text(
        "schema: dpone.domain-ownership.v1\ndomain: crm\n",
        encoding="utf-8",
    )

    result = build_airflow_self_service_service(root=tmp_path).init_project(
        airflow=True,
        layout="domain_first",
    )

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED"
    assert result.errors[0]["detected_layout"] == "mixed"


def test_project_init_rolls_back_if_external_flat_authority_appears_during_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_create = ScaffoldFileSystem.create
    injected = False

    def create_after_external_authority(
        filesystem: ScaffoldFileSystem,
        file: ScaffoldFile,
    ):
        nonlocal injected
        if not injected:
            injected = True
            external = tmp_path / "pipelines/legacy_daily/pipeline.yaml"
            external.parent.mkdir(parents=True)
            external.write_text("external-authority\n", encoding="utf-8")
        return original_create(filesystem, file)

    monkeypatch.setattr(ScaffoldFileSystem, "create", create_after_external_authority)

    result = build_airflow_self_service_service(root=tmp_path).init_project(
        airflow=True,
        layout="domain-first",
    )

    assert not result.passed
    assert any(change.path == "project-authority" for change in result.changes)
    assert (tmp_path / "pipelines/legacy_daily/pipeline.yaml").read_text(encoding="utf-8") == ("external-authority\n")
    assert not (tmp_path / "dpone.yaml").exists()
    assert not (tmp_path / "dags/dpone.py").exists()


def test_domain_init_rolls_back_if_project_config_changes_during_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _project(tmp_path)
    original_create = ScaffoldFileSystem.create
    injected = False

    def create_after_config_change(
        filesystem: ScaffoldFileSystem,
        file: ScaffoldFile,
    ):
        nonlocal injected
        if not injected and file.path.name == "ownership.yaml":
            injected = True
            config = tmp_path / "dpone.yaml"
            payload = yaml.safe_load(config.read_text(encoding="utf-8"))
            payload["layout"]["mode"] = "flat"
            config.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        return original_create(filesystem, file)

    monkeypatch.setattr(ScaffoldFileSystem, "create", create_after_config_change)

    result = service.init_domain(
        domain="finance",
        owner_team="data-finance",
        owner_contact="finance@example.com",
        approver_team="data-platform",
    )

    assert not result.passed
    assert any(change.path == "project-authority" for change in result.changes)
    assert not (tmp_path / "workloads/finance/ownership.yaml").exists()
    assert yaml.safe_load((tmp_path / "dpone.yaml").read_text(encoding="utf-8"))["layout"]["mode"] == "flat"


def test_mixed_authoring_authority_blocks_domain_and_pipeline_scaffolds(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    (tmp_path / "pipelines/legacy_daily").mkdir(parents=True)

    domain = service.init_domain(
        domain="finance",
        owner_team="data-finance",
        owner_contact="finance@example.com",
        approver_team="data-platform",
    )
    pipeline = _pipeline(service, "orders_daily", "crm")

    assert not domain.passed
    assert domain.errors[0]["code"] == "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED"
    assert not pipeline.passed
    assert pipeline.errors[0]["code"] == "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED"
    assert not (tmp_path / "workloads/finance").exists()
    assert not (tmp_path / "workloads/crm/pipelines/orders_daily").exists()


def test_mixed_authoring_authority_blocks_project_selection(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    (tmp_path / "pipelines/legacy_daily").mkdir(parents=True)

    with pytest.raises(SelectionError) as exc:
        ProjectSelectionService(root=tmp_path).select(target=tmp_path)

    assert exc.value.code == "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED"
    assert service.check("orders_daily").errors[0]["code"] == "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED"
    assert service.preview("orders_daily").errors[0]["code"] == "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED"


def test_direct_discovery_rejects_mixed_authoring_authority(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True).passed
    assert service.init_pipeline(
        pipeline_id="legacy_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    ).passed
    (tmp_path / "workloads/crm").mkdir(parents=True)
    (tmp_path / "workloads/crm/ownership.yaml").write_text(
        "schema: dpone.domain-ownership.v1\ndomain: crm\n",
        encoding="utf-8",
    )

    snapshot = ProjectDiscoveryService(tmp_path).discover()

    assert not snapshot.ok
    assert snapshot.issues[0].code == "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED"
    assert snapshot.workloads == ()


@pytest.mark.parametrize("target", [".", "tests/direct.test.yaml"])
def test_mixed_layout_blocks_every_hermetic_test_target(
    tmp_path: Path,
    target: str,
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    legacy = tmp_path / "pipelines/legacy"
    legacy.mkdir(parents=True)
    (legacy / "pipeline.yaml").write_text("kind: dpone.flow.v1\n", encoding="utf-8")

    with pytest.raises(HermeticProjectError) as exc:
        HermeticTestProject(tmp_path).discover(target)

    assert exc.value.code == "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED"
    assert exc.value.exit_code == 1
    assert exc.value.path == "workloads"


def test_hermetic_pipeline_reference_preserves_canonical_error_contract(tmp_path: Path) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")

    with pytest.raises(HermeticProjectError) as exc:
        HermeticTestProject(tmp_path).discover("missing_pipeline")

    assert exc.value.code == "DPONE_PIPELINE_SOURCE_NOT_FOUND"
    assert exc.value.exit_code == 1


def test_flat_string_airflow_flag_fails_before_preview_materialization(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True).passed
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    ).passed
    source = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["metadata"]["airflow"] = "false"
    source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    checked = service.check("pipelines/orders_daily")
    preview = service.preview("orders_daily")

    assert not checked.passed
    assert checked.errors[0]["code"] == "DPONE_PIPELINE_SOURCE_INVALID"
    assert not preview.passed
    assert preview.errors[0]["code"] == "DPONE_PIPELINE_SOURCE_INVALID"
    assert not (tmp_path / ".dpone-cache").exists()


def test_unrelated_tests_do_not_claim_flat_authoring_authority(tmp_path: Path) -> None:
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_application.py").write_text("def test_application(): pass\n", encoding="utf-8")

    result = build_airflow_self_service_service(root=tmp_path).init_project(
        airflow=True,
        layout="domain_first",
    )

    assert result.passed
    assert (tmp_path / "dpone.yaml").exists()


def test_selected_cli_output_preserves_scope_and_never_invents_pipeline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    monkeypatch.chdir(tmp_path)

    check_code, check_stdout, check_stderr = _run_cli(
        ["check", ".", "--select", "domain:crm"],
        capsys,
    )
    preview_code, preview_stdout, preview_stderr = _run_cli(
        ["airflow", "preview", ".", "--select", "domain:crm"],
        capsys,
    )

    assert check_code == 0, check_stderr
    assert "next: dpone airflow preview . --select domain:crm" in check_stdout
    assert "--max-selected 1000" not in check_stdout
    assert preview_code == 0, preview_stderr
    assert "scope: project selection (1 workload(s))" in preview_stdout
    assert "workloads: orders_daily" in preview_stdout
    assert "project-selection" not in preview_stdout
    assert "dpone test" not in preview_stdout


def test_selected_cli_output_preserves_explicit_selection_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert _pipeline(service, "orders_daily", "crm").passed
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["check", ".", "--select", "domain:crm", "--max-selected", "42"],
        capsys,
    )

    assert code == 0, stderr
    assert "next: dpone airflow preview . --select domain:crm --max-selected 42" in stdout


def test_airflow_disabled_preview_text_has_specific_recovery_without_fake_deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    assert service.init_pipeline(
        pipeline_id="runtime_only",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=False,
    ).passed
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(["airflow", "preview", "runtime_only"], capsys)

    assert code == 1
    assert stderr == ""
    assert "DPONE_AIRFLOW_DISABLED" in stdout
    assert "https://paulkov.github.io/dpone/errors/DPONE_AIRFLOW_DISABLED/" in stdout
    assert "fix manual: enable_airflow_in_authoring_source" in stdout
    assert "deployment type: unknown" not in stdout
    assert "runnable: None" not in stdout
    assert "correct the target or initialize it" not in stdout


def test_init_domain_conflict_prints_exact_executable_rerun(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    service = _project(tmp_path)
    _domain(service, "crm")
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "init",
            "domain",
            "crm",
            "--owner-team",
            "new-team",
            "--owner-contact",
            "new@example.com",
            "--approver-team",
            "governance",
        ],
        capsys,
    )

    assert code == 1, stderr
    assert (
        "inspect: dpone init domain crm --owner-team new-team --owner-contact new@example.com "
        "--approver-team governance --format json"
    ) in stdout
    assert "<domain>" not in stdout


def test_invalid_domain_text_includes_docs_and_exact_suggested_fix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _project(tmp_path)
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "init",
            "domain",
            "CRM Team",
            "--owner-team",
            "data-crm",
            "--owner-contact",
            "crm@example.com",
            "--approver-team",
            "data-platform",
        ],
        capsys,
    )

    assert code == 2, stderr
    assert "https://paulkov.github.io/dpone/errors/DPONE_DOMAIN_ID_INVALID/" in stdout
    assert (
        "dpone init domain crm_team --owner-team data-crm --owner-contact crm@example.com --approver-team data-platform"
    ) in stdout
