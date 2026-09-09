"""Tests for `dpone workload init` GitOps scaffolding."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from dpone.manifest.confined_mutations import ConfinedMutationError
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.readiness.airflow_scaffold_apply import ScaffoldApplier, ScaffoldApplyPlan, ScaffoldFileSystem
from dpone.readiness.airflow_self_service_models import Change
from dpone.readiness.workload_init_catalog_merge import (
    CatalogPatchConflict,
    apply_catalog_patch,
    plan_domain_catalog_patch,
)
from dpone.readiness.workload_init_service import WorkloadInitRequest, WorkloadInitService


def test_workload_init_plan_only_creates_change_plan(tmp_path: Path) -> None:
    service = WorkloadInitService(repo_root=tmp_path)
    result = service.init(
        WorkloadInitRequest(
            workload_ref="marketing/sample_web_sync",
            source="clickhouse",
            sink="mssql",
            strategy="full_refresh",
            layout="catalog",
            owner="marketing_team",
            apply=False,
        )
    )

    assert result.passed is True
    assert result.details is not None
    assert result.details["mode"] == "plan"
    paths = {change.path for change in result.changes}
    assert "workloads/marketing/dpone/manifests/sample_web_sync.yaml" in paths
    assert "dpone_workloads/gitops/domains/marketing.yaml" in paths
    assert "ownership.yaml" in paths
    assert not (tmp_path / "workloads/marketing/dpone/manifests/sample_web_sync.yaml").exists()


def test_workload_init_apply_writes_files_and_validates_manifest(tmp_path: Path) -> None:
    service = WorkloadInitService(repo_root=tmp_path)
    result = service.init(
        WorkloadInitRequest(
            workload_ref="marketing/sample_web_sync",
            source="clickhouse",
            sink="mssql",
            strategy="full_refresh",
            layout="batch",
            dag_id="DAG__marketing__sample_web_sync__sync",
            apply=True,
        )
    )

    assert result.passed is True
    manifest_path = tmp_path / "workloads/marketing/dpone/manifests/sample_web_sync.yaml"
    domain_path = tmp_path / "dpone_workloads/gitops/domains/marketing.yaml"
    assert manifest_path.exists()
    assert domain_path.exists()
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    assert manifest["kind"] == "dpone.batch.v1"
    domain = yaml.safe_load(domain_path.read_text(encoding="utf-8"))
    assert domain["workloads"]["sample_web_sync"]["manifest"].endswith(
        "workloads/marketing/dpone/manifests/sample_web_sync.yaml"
    )
    assert "DAG__marketing__sample_web_sync__sync" in domain["dags"]


def test_workload_init_idempotent_reapply_is_no_op(tmp_path: Path) -> None:
    service = WorkloadInitService(repo_root=tmp_path)
    request = WorkloadInitRequest(
        workload_ref="marketing/sample_web_sync",
        source="clickhouse",
        sink="mssql",
        strategy="full_refresh",
        layout="catalog",
        apply=True,
    )
    first = service.init(request)
    second = service.init(request)
    assert first.passed is True
    assert second.passed is True
    assert all(change.action == "no_op" for change in second.changes)


def test_workload_init_adds_missing_dag_when_workload_already_registered(tmp_path: Path) -> None:
    manifest_rel = "../../../workloads/marketing/dpone/manifests/sample_web_sync.yaml"
    domain_path = tmp_path / "dpone_workloads/gitops/domains/marketing.yaml"
    domain_path.parent.mkdir(parents=True, exist_ok=True)
    domain_path.write_text(
        yaml.safe_dump(
            {
                "domain": "marketing",
                "workloads": {"sample_web_sync": {"manifest": manifest_rel}},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    result = WorkloadInitService(repo_root=tmp_path).init(
        WorkloadInitRequest(
            workload_ref="marketing/sample_web_sync",
            source="clickhouse",
            sink="mssql",
            strategy="full_refresh",
            layout="catalog",
            dag_id="DAG__marketing__sample_web_sync__sync",
            apply=True,
        )
    )
    assert result.passed is True
    domain = yaml.safe_load(domain_path.read_text(encoding="utf-8"))
    assert "DAG__marketing__sample_web_sync__sync" in domain["dags"]
    domain_change = next(
        change for change in result.changes if change.path.endswith("dpone_workloads/gitops/domains/marketing.yaml")
    )
    assert domain_change.action == "update"


def test_workload_init_conflict_when_domain_workload_exists(tmp_path: Path) -> None:
    domain_path = tmp_path / "dpone_workloads/gitops/domains/marketing.yaml"
    domain_path.parent.mkdir(parents=True, exist_ok=True)
    domain_path.write_text(
        yaml.safe_dump(
            {
                "domain": "marketing",
                "workloads": {"sample_web_sync": {"manifest": "existing.yaml"}},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    result = WorkloadInitService(repo_root=tmp_path).init(
        WorkloadInitRequest(
            workload_ref="marketing/sample_web_sync",
            source="clickhouse",
            sink="mssql",
            strategy="full_refresh",
            apply=False,
        )
    )
    assert result.passed is False
    assert any(change.action == "conflict" for change in result.changes)
    assert result.errors
    assert result.errors[0]["code"] == "DPONE_WORKLOAD_INIT_DOMAIN_CONFLICT"
    assert result.errors[0]["docs_url"].endswith("DPONE_WORKLOAD_INIT_DOMAIN_CONFLICT.md")
    assert result.errors[0]["fixes"]


def test_workload_init_conflict_when_ownership_differs(tmp_path: Path) -> None:
    ownership_path = tmp_path / "ownership.yaml"
    ownership_path.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.ownership.v1",
                "workloads": [
                    {
                        "domain": "marketing",
                        "workload_id": "sample_web_sync",
                        "owner": "existing_team",
                        "contacts": [],
                    }
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = WorkloadInitService(repo_root=tmp_path).init(
        WorkloadInitRequest(
            workload_ref="marketing/sample_web_sync",
            source="clickhouse",
            sink="mssql",
            strategy="full_refresh",
            owner="new_team",
            apply=False,
        )
    )

    assert result.passed is False
    assert any(change.action == "conflict" and change.path == "ownership.yaml" for change in result.changes)
    assert ownership_path.read_text(encoding="utf-8").find("existing_team") >= 0


def test_workload_init_cli_json_plan(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    from dpone.cli.main import main as cli_main

    with pytest.raises(SystemExit) as exc:
        cli_main(
            [
                "workload",
                "init",
                "marketing/sample_web_sync",
                "--source",
                "clickhouse",
                "--sink",
                "mssql",
                "--strategy",
                "full_refresh",
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 0


@pytest.mark.parametrize("catalog_kind", ("domain_leaf", "domain_parent", "ownership_leaf"))
def test_workload_init_rejects_catalog_symlink_escape(
    tmp_path: Path,
    catalog_kind: str,
) -> None:
    external_root = tmp_path.parent / f"{tmp_path.name}-{catalog_kind}-external"
    external_root.mkdir()
    external_file = external_root / "catalog.yaml"
    original = "external: unchanged\n"
    external_file.write_text(original, encoding="utf-8")

    if catalog_kind == "domain_leaf":
        catalog_path = tmp_path / "dpone_workloads/gitops/domains/marketing.yaml"
        catalog_path.parent.mkdir(parents=True)
        _symlink_or_skip(catalog_path, external_file)
    elif catalog_kind == "domain_parent":
        catalog_parent = tmp_path / "dpone_workloads/gitops/domains"
        catalog_parent.parent.mkdir(parents=True)
        _symlink_or_skip(catalog_parent, external_root, target_is_directory=True)
    else:
        _symlink_or_skip(tmp_path / "ownership.yaml", external_file)

    result = WorkloadInitService(repo_root=tmp_path).init(_apply_request())

    assert result.passed is False
    assert result.errors
    assert external_file.read_text(encoding="utf-8") == original
    assert not (external_root / "marketing.yaml").exists()


def test_workload_init_stops_before_catalog_writes_on_scaffold_apply_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    original_apply = ScaffoldApplier.apply

    def race_with_valid_manifest(self: ScaffoldApplier, files):
        manifest = next(file for file in files if "/manifests/" in file.path.as_posix())
        target = tmp_path / manifest.path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(manifest.text + "\n# created concurrently\n", encoding="utf-8")
        return original_apply(self, files)

    monkeypatch.setattr(ScaffoldApplier, "apply", race_with_valid_manifest)
    monkeypatch.chdir(tmp_path)
    from dpone.cli.main import main as cli_main

    with pytest.raises(SystemExit) as exc:
        cli_main(
            [
                "workload",
                "init",
                "marketing/sample_web_sync",
                "--source",
                "clickhouse",
                "--sink",
                "mssql",
                "--apply",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert any(change["action"] == "conflict" for change in payload["changes"])
    assert not (tmp_path / "dpone_workloads/gitops/domains/marketing.yaml").exists()
    assert not (tmp_path / "ownership.yaml").exists()


def test_workload_init_catalog_cas_preserves_concurrent_winner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.readiness import workload_init_service as service_module

    domain_path = tmp_path / "dpone_workloads/gitops/domains/marketing.yaml"
    domain_path.parent.mkdir(parents=True)
    domain_path.write_text(
        yaml.safe_dump(
            {
                "domain": "marketing",
                "workloads": {"existing": {"manifest": "../../../workloads/marketing/existing.yaml"}},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    winner = yaml.safe_dump(
        {
            "domain": "marketing",
            "workloads": {"concurrent_winner": {"manifest": "../../../workloads/marketing/winner.yaml"}},
        },
        sort_keys=False,
    )
    original_apply = service_module.apply_catalog_patch
    raced = False

    def race_catalog(plan, **kwargs):
        nonlocal raced
        if not raced and plan.path.as_posix().endswith("domains/marketing.yaml"):
            domain_path.write_text(winner, encoding="utf-8")
            raced = True
        return original_apply(plan, **kwargs)

    monkeypatch.setattr(service_module, "apply_catalog_patch", race_catalog)

    result = WorkloadInitService(repo_root=tmp_path).init(_apply_request())

    assert result.passed is False
    assert any(change.action == "conflict" for change in result.changes)
    assert domain_path.read_text(encoding="utf-8") == winner
    assert not (tmp_path / "ownership.yaml").exists()


def test_workload_init_no_op_catalog_verifies_planned_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.readiness import workload_init_service as service_module

    request = _apply_request()
    first = WorkloadInitService(repo_root=tmp_path).init(request)
    assert first.passed is True

    domain_path = tmp_path / "dpone_workloads/gitops/domains/marketing.yaml"
    winner = domain_path.read_text(encoding="utf-8") + "concurrent_marker: winner\n"
    original_apply = service_module.apply_catalog_patch
    raced = False

    def race_no_op(plan, **kwargs):
        nonlocal raced
        if not raced and plan.path.as_posix().endswith("domains/marketing.yaml"):
            assert plan.action == "no_op"
            domain_path.write_text(winner, encoding="utf-8")
            raced = True
        return original_apply(plan, **kwargs)

    monkeypatch.setattr(service_module, "apply_catalog_patch", race_no_op)

    second = WorkloadInitService(repo_root=tmp_path).init(request)
    third = WorkloadInitService(repo_root=tmp_path).init(request)

    assert second.passed is False
    assert any(change.action == "conflict" for change in second.changes)
    assert domain_path.read_text(encoding="utf-8") == winner
    assert third.passed is True


def test_workload_init_rolls_back_prior_catalog_patch_on_later_cas_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.readiness import workload_init_service as service_module

    domain_path = tmp_path / "dpone_workloads/gitops/domains/marketing.yaml"
    domain_path.parent.mkdir(parents=True)
    original_domain = "domain: marketing\nworkloads: {}\n"
    domain_path.write_text(original_domain, encoding="utf-8")
    ownership_path = tmp_path / "ownership.yaml"
    ownership_path.write_text("schema: dpone.ownership.v1\nworkloads: []\n", encoding="utf-8")
    winner = "schema: dpone.ownership.v1\nworkloads:\n  - domain: finance\n    workload_id: winner\n"
    original_apply = service_module.apply_catalog_patch
    raced = False

    def race_ownership(plan, **kwargs):
        nonlocal raced
        if not raced and plan.path.as_posix() == "ownership.yaml":
            ownership_path.write_text(winner, encoding="utf-8")
            raced = True
        return original_apply(plan, **kwargs)

    monkeypatch.setattr(service_module, "apply_catalog_patch", race_ownership)

    result = WorkloadInitService(repo_root=tmp_path).init(_apply_request())

    assert result.passed is False
    assert domain_path.read_text(encoding="utf-8") == original_domain
    assert ownership_path.read_text(encoding="utf-8") == winner


def test_workload_init_preserves_existing_workload_metadata(tmp_path: Path) -> None:
    domain_path = tmp_path / "dpone_workloads/gitops/domains/marketing.yaml"
    domain_path.parent.mkdir(parents=True)
    domain_path.write_text(
        yaml.safe_dump(
            {
                "domain": "marketing",
                "workloads": {
                    "sample_web_sync": {
                        "manifest": "../../../workloads/marketing/dpone/manifests/sample_web_sync.yaml",
                        "labels": {"tier": "gold"},
                        "retention_days": 30,
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = WorkloadInitService(repo_root=tmp_path).init(_apply_request())

    assert result.passed is True
    workload = yaml.safe_load(domain_path.read_text(encoding="utf-8"))["workloads"]["sample_web_sync"]
    assert workload["labels"] == {"tier": "gold"}
    assert workload["retention_days"] == 30


@pytest.mark.parametrize(
    ("relative_path", "content"),
    (
        pytest.param(
            "dpone_workloads/gitops/domains/marketing.yaml",
            "domain: marketing\nworkloads: []\n",
            id="domain-workloads-list",
        ),
        pytest.param(
            "dpone_workloads/gitops/domains/marketing.yaml",
            "domain: marketing\nworkloads:\n  broken: invalid\n",
            id="domain-unrelated-workload-scalar",
        ),
        pytest.param(
            "dpone_workloads/gitops/domains/marketing.yaml",
            "- invalid\n- catalog\n",
            id="domain-root-list",
        ),
        pytest.param(
            "dpone_workloads/gitops/domains/marketing.yaml",
            "domain: marketing\nmetadata:\n  owner: first\n  owner: second\nworkloads: {}\n",
            id="duplicate-yaml-key",
        ),
        pytest.param(
            "ownership.yaml",
            "schema: dpone.ownership.v1\nworkloads: {}\n",
            id="ownership-workloads-mapping",
        ),
        pytest.param(
            "ownership.yaml",
            "schema: foreign.ownership.v1\nworkloads: []\n",
            id="ownership-foreign-schema",
        ),
        pytest.param(
            "ownership.yaml",
            "schema: dpone.ownership.v1\nworkloads:\n  - invalid\n",
            id="ownership-entry-scalar",
        ),
        pytest.param(
            "ownership.yaml",
            "schema: dpone.ownership.v1\nworkloads:\n"
            "  - domain: finance\n"
            "    workload_id: daily\n"
            "    owner: finance_team\n"
            "    contacts: {}\n",
            id="ownership-contacts-mapping",
        ),
        pytest.param(
            "ownership.yaml",
            "schema: dpone.ownership.v1\nworkloads:\n"
            "  - domain: finance\n"
            "    workload_id: daily\n"
            "    owner: finance_team\n"
            "  - domain: finance\n"
            "    workload_id: daily\n"
            "    owner: finance_team\n",
            id="ownership-duplicate-identity",
        ),
    ),
)
def test_workload_init_rejects_malformed_catalog_shapes(
    tmp_path: Path,
    relative_path: str,
    content: str,
) -> None:
    catalog_path = tmp_path / relative_path
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    catalog_path.write_text(content, encoding="utf-8")

    result = WorkloadInitService(repo_root=tmp_path).init(
        WorkloadInitRequest(
            workload_ref="marketing/sample_web_sync",
            source="clickhouse",
            sink="mssql",
            strategy="full_refresh",
            apply=False,
        )
    )

    assert result.passed is False
    assert any(change.action == "conflict" and change.path == relative_path for change in result.changes)
    assert catalog_path.read_text(encoding="utf-8") == content


def test_workload_init_validates_generated_manifest_before_repository_mutation(tmp_path: Path) -> None:
    loader = _RejectingManifestLoader()

    result = WorkloadInitService(repo_root=tmp_path, manifest_loader=loader).init(_apply_request())

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_WORKLOAD_INIT_MANIFEST_INVALID"
    assert loader.observed_path is not None
    assert not loader.observed_path.is_relative_to(tmp_path)
    assert _project_files(tmp_path) == []


def test_workload_init_rolls_back_complete_unit_in_reverse_order_and_retries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.readiness import workload_init_service as service_module

    events: list[str] = []
    original_apply = service_module.apply_catalog_patch
    original_catalog_rollback = service_module.rollback_catalog_patch
    original_scaffold_rollback = ScaffoldFileSystem.rollback
    failed = False

    def fail_ownership_once(plan, **kwargs):
        nonlocal failed
        if not failed and plan.path.as_posix() == "ownership.yaml":
            failed = True
            raise CatalogPatchConflict("injected ownership apply failure")
        return original_apply(plan, **kwargs)

    def observe_catalog_rollback(receipt, **kwargs):
        events.append(f"catalog:{receipt.path.as_posix()}")
        return original_catalog_rollback(receipt, **kwargs)

    def observe_scaffold_rollback(self, receipt):
        events.append(f"scaffold:{receipt.path.as_posix()}")
        return original_scaffold_rollback(self, receipt)

    monkeypatch.setattr(service_module, "apply_catalog_patch", fail_ownership_once)
    monkeypatch.setattr(service_module, "rollback_catalog_patch", observe_catalog_rollback)
    monkeypatch.setattr(ScaffoldFileSystem, "rollback", observe_scaffold_rollback)

    first = WorkloadInitService(repo_root=tmp_path).init(_apply_request())

    assert first.passed is False
    assert events == [
        "catalog:dpone_workloads/gitops/domains/marketing.yaml",
        "scaffold:workloads/marketing/dpone/docs/sample_web_sync.md",
        "scaffold:workloads/marketing/dpone/sql/sample_web_sync.sql",
        "scaffold:workloads/marketing/dpone/manifests/sample_web_sync.yaml",
        "scaffold:dpone_workloads/gitops/gitops.yaml",
    ]
    assert _project_files(tmp_path) == []
    assert first.details is not None
    assert first.details["rollback_journal"]["entries"] == [
        {"action": "delete", "path": "dpone_workloads/gitops/gitops.yaml"},
        {"action": "delete", "path": "workloads/marketing/dpone/manifests/sample_web_sync.yaml"},
        {"action": "delete", "path": "workloads/marketing/dpone/sql/sample_web_sync.sql"},
        {"action": "delete", "path": "workloads/marketing/dpone/docs/sample_web_sync.md"},
    ]
    assert first.details["unit_of_work"] == {
        "status": "rolled_back",
        "recovery_artifacts": [],
    }

    second = WorkloadInitService(repo_root=tmp_path).init(_apply_request())
    third = WorkloadInitService(repo_root=tmp_path).init(_apply_request())

    assert second.passed is True
    assert third.passed is True
    assert all(change.action == "no_op" for change in third.changes)
    assert second.details is not None
    assert second.details["rollback_journal"]["entries"]


def test_workload_init_reports_recovery_artifact_and_continues_safe_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.readiness import workload_init_service as service_module

    domain_path = tmp_path / "dpone_workloads/gitops/domains/marketing.yaml"
    recovery_relative = "dpone_workloads/gitops/domains/.marketing.yaml.dpone-recovery-test"
    recovery_path = tmp_path / recovery_relative
    winner = "domain: marketing\nworkloads:\n  concurrent_winner:\n    manifest: winner.yaml\n"
    original_apply = service_module.apply_catalog_patch

    def fail_ownership(plan, **kwargs):
        if plan.path.as_posix() == "ownership.yaml":
            raise CatalogPatchConflict("injected ownership apply failure")
        return original_apply(plan, **kwargs)

    def preserve_winner_and_require_recovery(receipt, **kwargs):
        del receipt, kwargs
        domain_path.write_text(winner, encoding="utf-8")
        recovery_path.write_text("owned catalog bytes\n", encoding="utf-8")
        raise CatalogPatchConflict(
            "domain rollback observed a concurrent winner",
            recovery_artifacts=(recovery_relative,),
        )

    monkeypatch.setattr(service_module, "apply_catalog_patch", fail_ownership)
    monkeypatch.setattr(service_module, "rollback_catalog_patch", preserve_winner_and_require_recovery)

    result = WorkloadInitService(repo_root=tmp_path).init(_apply_request())

    assert result.passed is False
    assert domain_path.read_text(encoding="utf-8") == winner
    assert recovery_path.read_text(encoding="utf-8") == "owned catalog bytes\n"
    assert not (tmp_path / "workloads/marketing/dpone/manifests/sample_web_sync.yaml").exists()
    assert result.details is not None
    assert result.details["unit_of_work"] == {
        "status": "recovery_required",
        "recovery_artifacts": [recovery_relative],
        "issues": ["dpone_workloads/gitops/domains/marketing.yaml: domain rollback observed a concurrent winner"],
    }


def test_workload_init_scaffold_apply_error_surfaces_recovery_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recovery_relative = "workloads/marketing/dpone/docs/.sample_web_sync.md.dpone-rollback-test"
    recovery_path = tmp_path / recovery_relative
    failure_journal = {
        "schema": "dpone.scaffold-rollback-journal.v1",
        "entries": [{"action": "delete", "path": "workloads/marketing/dpone/docs/sample_web_sync.md"}],
    }

    def fail_with_recovery_receipt(self, files):
        del self, files
        recovery_path.parent.mkdir(parents=True)
        recovery_path.write_text("displaced concurrent bytes\n", encoding="utf-8")
        error = OSError("injected scaffold apply failure")
        setattr(
            error,
            "scaffold_receipt",
            ScaffoldApplyPlan(
                changes=(
                    Change("failed", "workloads/marketing/dpone/docs/sample_web_sync.md"),
                    Change(
                        "recovery",
                        recovery_relative,
                        "Concurrent bytes were preserved for manual recovery.",
                    ),
                ),
                rollback_journal=failure_journal,
            ),
        )
        raise error

    monkeypatch.setattr(ScaffoldApplier, "apply", fail_with_recovery_receipt)

    result = WorkloadInitService(repo_root=tmp_path).init(_apply_request())

    assert result.passed is False
    assert any(change.action == "recovery" and change.path == recovery_relative for change in result.changes)
    assert result.details is not None
    assert result.details["unit_of_work"] == {
        "status": "recovery_required",
        "recovery_artifacts": [recovery_relative],
    }
    assert result.details["rollback_journal"] == failure_journal
    assert recovery_path.read_text(encoding="utf-8") == "displaced concurrent bytes\n"
    assert not (tmp_path / "dpone_workloads/gitops/domains/marketing.yaml").exists()
    assert not (tmp_path / "ownership.yaml").exists()


def test_workload_init_scaffold_conflict_marks_recovery_required(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recovery_relative = "workloads/marketing/dpone/docs/.sample_web_sync.md.dpone-rollback-test"

    def conflict_with_recovery_receipt(self, files):
        del self, files
        return ScaffoldApplyPlan(
            changes=(
                Change(
                    "conflict",
                    "workloads/marketing/dpone/docs/sample_web_sync.md",
                    "injected concurrent scaffold conflict",
                ),
                Change(
                    "recovery",
                    recovery_relative,
                    "Concurrent bytes were preserved for manual recovery.",
                ),
            ),
            rollback_journal={
                "schema": "dpone.scaffold-rollback-journal.v1",
                "entries": [],
            },
        )

    monkeypatch.setattr(ScaffoldApplier, "apply", conflict_with_recovery_receipt)

    result = WorkloadInitService(repo_root=tmp_path).init(_apply_request())

    assert result.passed is False
    assert result.details is not None
    assert result.details["unit_of_work"] == {
        "status": "recovery_required",
        "recovery_artifacts": [recovery_relative],
    }
    assert not (tmp_path / "dpone_workloads/gitops/domains/marketing.yaml").exists()
    assert not (tmp_path / "ownership.yaml").exists()


def test_workload_init_catalog_storage_reports_relative_recovery_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.readiness import workload_init_catalog_storage as storage_module

    relative_path = Path("dpone_workloads/gitops/domains/marketing.yaml")
    catalog_path = tmp_path / relative_path
    catalog_path.parent.mkdir(parents=True)
    original = "domain: marketing\nworkloads: {}\n"
    catalog_path.write_text(original, encoding="utf-8")
    recovery_name = ".marketing.yaml.dpone-recovery-test"

    def fail_replace(*args, **kwargs):
        del args, kwargs
        raise ConfinedMutationError(
            "concurrent_winner",
            "injected catalog recovery boundary",
            recovery_name=recovery_name,
        )

    monkeypatch.setattr(storage_module, "replace_file_if_digest", fail_replace)
    plan = plan_domain_catalog_patch(
        repo_root=tmp_path,
        path=relative_path,
        domain="marketing",
        workload_id="sample_web_sync",
        manifest_ref="../../../workloads/marketing/dpone/manifests/sample_web_sync.yaml",
        dag_id=None,
        dag_declaration=None,
    )

    with pytest.raises(CatalogPatchConflict) as exc:
        apply_catalog_patch(plan, repo_root=tmp_path)

    assert exc.value.recovery_artifacts == (f"dpone_workloads/gitops/domains/{recovery_name}",)
    assert catalog_path.read_text(encoding="utf-8") == original


def _apply_request() -> WorkloadInitRequest:
    return WorkloadInitRequest(
        workload_ref="marketing/sample_web_sync",
        source="clickhouse",
        sink="mssql",
        strategy="full_refresh",
        layout="batch",
        apply=True,
    )


class _RejectingManifestLoader(ManifestLoaderRouter):
    def __init__(self) -> None:
        self.observed_path: Path | None = None

    def load(self, path: Path, *, metadata_only: bool = True):
        assert metadata_only is True
        self.observed_path = path
        raise ValueError("injected generated manifest validation failure")


def _project_files(root: Path) -> list[str]:
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file() or path.is_symlink())


def _symlink_or_skip(link: Path, target: Path, *, target_is_directory: bool = False) -> None:
    try:
        link.symlink_to(target, target_is_directory=target_is_directory)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")
