"""A workspace report must retain every pinned row and never recover files."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.app.dbt_promotion_composition import (
    build_dbt_workspace_mirror_service,
    build_dbt_workspace_promotion_verification_service,
)
from dpone.contracts.dbt_workspace_promotion import DbtWorkspacePromotionDescriptor
from dpone.services.dbt_prod_mirror_journal import DbtProdMirrorJournal, locked_prod_mirror
from dpone.services.dbt_prod_promotion_contract import DbtProdMirrorError
from tests.test_dbt_workspace_mirror_service import _arguments, _files


def _prepared(tmp_path: Path):
    arguments, inventory = _arguments(tmp_path)
    build_dbt_workspace_mirror_service().prepare(**arguments)
    verify = {
        key: arguments[key] for key in ("compiled_root", "repository_root", "descriptor_path", "expected_release_id")
    }
    return arguments, inventory, verify


def test_reports_every_project_and_does_not_change_git_checkout_permissions(tmp_path: Path) -> None:
    arguments, inventory, verify = _prepared(tmp_path)
    root = arguments["repository_root"]
    for path in (root / arguments["mirror_root"]).rglob("*"):
        if path.is_file():
            path.chmod(0o644)
    before = _files(root)
    report = build_dbt_workspace_promotion_verification_service().verify(**verify)
    assert report.passed
    payload = report.to_dict()
    assert payload["schema"] == "dpone.dbt-workspace-promotion-verification.v1"
    assert [row["project_path"] for row in payload["projects"]] == [p.project_path for p in inventory.projects]
    assert all(
        row["observed_project_bundle_sha256"] == row["expected_project_bundle_sha256"] for row in payload["projects"]
    )
    assert _files(root) == before


@pytest.mark.parametrize("change", ["missing", "tampered", "ignored_extra", "symlink", "deep_yaml"])
def test_project_b_failure_keeps_both_rows_and_correct_observations(tmp_path: Path, change: str) -> None:
    arguments, _, verify = _prepared(tmp_path)
    project = arguments["repository_root"] / arguments["mirror_root"] / "dbt/beta"
    if change == "missing":
        (project / "dbt_project.yml").unlink()
    elif change == "tampered":
        (project / "models/orders.sql").write_bytes(b"select 2 as id\n")
    elif change == "ignored_extra":
        (project / "target").mkdir()
    elif change == "deep_yaml":
        (project / "dbt_project.yml").write_bytes(b"name: beta\nconfig: " + b"[" * 1500 + b"0" + b"]" * 1500)
    else:
        original = project.with_name("moved-beta")
        project.rename(original)
        project.symlink_to(original, target_is_directory=True)
    report = build_dbt_workspace_promotion_verification_service().verify(**verify)
    assert not report.passed
    alpha, beta = report.projects
    assert alpha.passed and alpha.project_path == "dbt/alpha"
    assert not beta.passed and beta.project_path == "dbt/beta"
    if change in {"missing", "symlink", "deep_yaml"}:
        assert beta.observed_project_bundle_sha256 is None
    elif change == "tampered":
        assert beta.observed_project_bundle_sha256 != beta.expected_project_bundle_sha256
    else:
        assert beta.observed_project_bundle_sha256 == beta.expected_project_bundle_sha256
    if change == "deep_yaml":
        assert not build_dbt_workspace_mirror_service().prepare(**arguments).no_op
        assert build_dbt_workspace_promotion_verification_service().verify(**verify).passed


def test_deep_descriptor_json_returns_every_pinned_row_without_repair(tmp_path: Path) -> None:
    arguments, _, verify = _prepared(tmp_path)
    path = arguments["repository_root"] / arguments["descriptor_path"]
    payload = b'{"unexpected":' + b"[" * 30_000 + b"0" + b"]" * 30_000 + b"}"
    path.write_bytes(payload)
    report = build_dbt_workspace_promotion_verification_service().verify(**verify)
    assert not report.passed
    assert len(report.projects) == 2
    assert all(not row.passed and row.observed_project_bundle_sha256 is None for row in report.projects)
    assert path.read_bytes() == payload
    with pytest.raises(DbtProdMirrorError, match="ownership"):
        build_dbt_workspace_mirror_service().prepare(**arguments)
    assert path.read_bytes() == payload


def test_unlisted_aggregate_file_fails_even_when_all_project_rows_match(tmp_path: Path) -> None:
    arguments, _, verify = _prepared(tmp_path)
    (arguments["repository_root"] / arguments["mirror_root"] / "orphan.sql").write_bytes(b"unexpected")
    report = build_dbt_workspace_promotion_verification_service().verify(**verify)
    assert all(project.passed for project in report.projects)
    assert not report.passed


@pytest.mark.parametrize("missing", ["descriptor_path", "source_snapshot_path"])
def test_missing_metadata_fails_without_omitting_pinned_rows(tmp_path: Path, missing: str) -> None:
    arguments, _, verify = _prepared(tmp_path)
    (arguments["repository_root"] / arguments[missing]).unlink()
    before = _files(arguments["repository_root"])
    report = build_dbt_workspace_promotion_verification_service().verify(**verify)
    assert not report.passed
    assert [p.project_path for p in report.projects] == ["dbt/alpha", "dbt/beta"]
    assert _files(arguments["repository_root"]) == before


def test_resealed_foreign_dev_identity_fails_reviewed_comparison(tmp_path: Path) -> None:
    arguments, _, verify = _prepared(tmp_path)
    path = arguments["repository_root"] / arguments["descriptor_path"]
    expected = DbtWorkspacePromotionDescriptor.from_payload(path.read_bytes())
    forged = replace(expected, trust=replace(expected.trust, subject_sha256="sha256:" + "8" * 64))
    path.write_text(json.dumps(forged.to_dict()))
    service = build_dbt_workspace_promotion_verification_service()
    assert service.verify(**verify).passed  # Source-only shape check is NOT authorization.
    report = service.verify_reviewed(**verify, expected_descriptor=expected)
    assert not report.passed
    assert all(p.passed for p in report.projects)


def test_pending_journal_is_refused_without_recovery(tmp_path: Path) -> None:
    arguments, _, verify = _prepared(tmp_path)
    root = arguments["repository_root"]
    journal = DbtProdMirrorJournal.begin(root)
    (journal.transaction_root / "pending").write_bytes(b"uncommitted")
    before = _files(root)
    with pytest.raises(DbtProdMirrorError, match="pending.*recover"):
        build_dbt_workspace_promotion_verification_service().verify(**verify)
    assert _files(root) == before


def test_concurrent_writer_prevents_read_only_verification(tmp_path: Path) -> None:
    arguments, _, verify = _prepared(tmp_path)
    with locked_prod_mirror(arguments["repository_root"]):
        with pytest.raises(DbtProdMirrorError, match="another.*active"):
            build_dbt_workspace_promotion_verification_service().verify(**verify)
