"""Public CLI contracts for ephemeral workload-index CI projections."""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path

import pytest

import dpone.adapters.workload_index_baseline_store as promotion_store
import dpone.manifest.workload_index_io as workload_index_io
from dpone.adapters.workload_index_baseline_store import ConfinedWorkloadIndexBaselineStore
from dpone.cli import main as cli_main
from dpone.contracts.workload_index import MAX_PROJECT_WORKLOADS
from dpone.manifest.confined_mutations import replace_file_if_digest
from dpone.manifest.project_discovery_identity import project_fingerprint, workload_fingerprint
from dpone.manifest.project_root import inspect_project_root
from dpone.ports.workload_index_baseline_store import WorkloadIndexAtomicReplaceOutcome
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.services.workload_index_promotion import (
    WorkloadIndexPromotionError,
    WorkloadIndexPromotionService,
)


def _domain_first_project(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    service = build_airflow_self_service_service(root=root)
    assert service.init_project(airflow=True, layout="domain_first").passed
    assert service.init_domain(
        domain="crm",
        owner_team="data-crm",
        owner_contact="crm@example.com",
        approver_team="data-platform",
    ).passed
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=None,
    ).passed


def _run_cli(
    args: list[str],
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, dict[str, object], str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), json.loads(captured.out), captured.err


def test_workload_index_emits_public_schema_to_stdout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)

    code, payload, stderr = _run_cli(["workload", "index"], capsys)

    assert code == 0, (stderr, payload)
    assert payload["schema"] == "dpone.workload-index.v1"
    assert [item["pipeline_id"] for item in payload["workloads"]] == ["orders_daily"]


def test_workload_index_output_is_byte_identical_across_repeated_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as first_exit:
        cli_main.main(["workload", "index"])
    first = capsys.readouterr()
    with pytest.raises(SystemExit) as second_exit:
        cli_main.main(["workload", "index"])
    second = capsys.readouterr()

    assert first_exit.value.code == second_exit.value.code == 0
    assert first.out == second.out
    assert first.err == second.err == ""


def test_workload_impact_reads_confined_baseline_and_reports_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    _, baseline, _ = _run_cli(["workload", "index"], capsys)
    (tmp_path / "baseline.json").write_text(json.dumps(baseline), encoding="utf-8")
    ownership = tmp_path / "workloads/crm/ownership.yaml"
    ownership.write_text(
        ownership.read_text(encoding="utf-8").replace("crm@example.com", "new@example.com"),
        encoding="utf-8",
    )

    code, payload, stderr = _run_cli(["workload", "impact", "--baseline", "baseline.json"], capsys)

    assert code == 0, stderr
    assert payload["schema"] == "dpone.workload-change-impact.v1"
    assert payload["modified"] == ["orders_daily"]


def test_workload_impact_compares_the_exact_candidate_after_checkout_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    _, baseline, _ = _run_cli(["workload", "index"], capsys)
    (tmp_path / "baseline.json").write_text(json.dumps(baseline), encoding="utf-8")
    ownership = tmp_path / "workloads/crm/ownership.yaml"
    ownership.write_text(
        ownership.read_text(encoding="utf-8").replace("crm@example.com", "candidate@example.com"),
        encoding="utf-8",
    )
    _, candidate, _ = _run_cli(["workload", "index"], capsys)
    (tmp_path / "candidate.json").write_text(json.dumps(candidate), encoding="utf-8")
    ownership.write_text(
        ownership.read_text(encoding="utf-8").replace("candidate@example.com", "later@example.com"),
        encoding="utf-8",
    )

    code, payload, stderr = _run_cli(
        [
            "workload",
            "impact",
            "--baseline",
            "baseline.json",
            "--current",
            "candidate.json",
        ],
        capsys,
    )

    assert code == 0, stderr
    assert payload["current_fingerprint"] == candidate["project_fingerprint"]
    assert payload["current_source"] == "candidate"
    assert payload["validation_status"] == "passed"
    assert payload["discovery_status"] == "not_run"
    assert payload["current_content_sha256"] == _sha256((tmp_path / "candidate.json").read_bytes())
    assert payload["modified"] == ["orders_daily"]


def test_workload_impact_rejects_project_root_replacement_between_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    original = tmp_path / "project-original"
    _domain_first_project(project)
    _, index, _ = _run_cli(["workload", "index", "--root", str(project)], capsys)
    encoded = json.dumps(index).encode()
    (project / "baseline.json").write_bytes(encoded)
    (project / "candidate.json").write_bytes(encoded)
    read_snapshot = workload_index_io.read_confined_file_snapshot
    reads = 0

    def replace_after_baseline_read(*args, **kwargs):
        nonlocal reads
        snapshot = read_snapshot(*args, **kwargs)
        reads += 1
        if reads == 1:
            project.rename(original)
            project.mkdir()
            (project / "baseline.json").write_bytes(encoded)
            (project / "candidate.json").write_bytes(encoded)
        return snapshot

    monkeypatch.setattr(
        workload_index_io,
        "read_confined_file_snapshot",
        replace_after_baseline_read,
    )

    code, payload, stderr = _run_cli(
        [
            "workload",
            "impact",
            "--root",
            str(project),
            "--baseline",
            "baseline.json",
            "--current",
            "candidate.json",
        ],
        capsys,
    )

    assert code == 1
    assert stderr == ""
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_INVALID"


def test_workload_promote_bootstraps_the_exact_approved_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    _, candidate, _ = _run_cli(["workload", "index"], capsys)
    candidate_bytes = json.dumps(candidate).encode()
    (tmp_path / "candidate.json").write_bytes(candidate_bytes)

    code, payload, stderr = _run_cli(
        [
            "workload",
            "promote",
            "--candidate",
            "candidate.json",
            "--baseline",
            "workload-index.json",
            "--approved-candidate-sha256",
            _sha256(candidate_bytes),
            "--approved-current-fingerprint",
            str(candidate["project_fingerprint"]),
            "--expect-baseline-absent",
        ],
        capsys,
    )

    assert code == 0, (stderr, payload)
    assert payload["schema"] == "dpone.workload-index-promotion.v1"
    assert payload["mode"] == "bootstrap"
    assert (tmp_path / "workload-index.json").read_bytes() == candidate_bytes


def test_workload_promote_reports_symlink_project_root_as_safety_violation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    root_link = tmp_path / "project-link"
    _domain_first_project(project)
    _, candidate, _ = _run_cli(["workload", "index", "--root", str(project)], capsys)
    candidate_bytes = json.dumps(candidate).encode()
    (project / "candidate.json").write_bytes(candidate_bytes)
    root_link.symlink_to(project, target_is_directory=True)

    code, payload, stderr = _run_cli(
        [
            "workload",
            "promote",
            "--root",
            str(root_link),
            "--candidate",
            "candidate.json",
            "--baseline",
            "workload-index.json",
            "--approved-candidate-sha256",
            _sha256(candidate_bytes),
            "--approved-current-fingerprint",
            str(candidate["project_fingerprint"]),
            "--expect-baseline-absent",
        ],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT"
    assert payload["errors"][0]["stage"] == "workload_index_promotion"
    assert not (project / "workload-index.json").exists()


def test_workload_promote_reports_incomplete_baseline_guard_as_cli_config_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    _, candidate, _ = _run_cli(["workload", "index"], capsys)
    candidate_bytes = json.dumps(candidate).encode()
    (tmp_path / "candidate.json").write_bytes(candidate_bytes)

    code, payload, stderr = _run_cli(
        [
            "workload",
            "promote",
            "--candidate",
            "candidate.json",
            "--baseline",
            "workload-index.json",
            "--approved-candidate-sha256",
            _sha256(candidate_bytes),
            "--approved-current-fingerprint",
            str(candidate["project_fingerprint"]),
            "--expected-baseline-sha256",
            _sha256(candidate_bytes),
        ],
        capsys,
    )

    assert code == 2
    assert stderr == ""
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_PROMOTION_REQUEST_INVALID"
    assert payload["errors"][0]["fixes"] == [{"id": "use_exactly_one_baseline_guard", "safety": "manual"}]
    assert not (tmp_path / "workload-index.json").exists()


@pytest.mark.parametrize("mode", ("bootstrap", "change"))
def test_workload_promote_rejects_real_project_root_replacement(
    mode: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    original = tmp_path / "project-original"
    replacement_source = tmp_path / "replacement"
    _domain_first_project(project)
    _, candidate, _ = _run_cli(["workload", "index", "--root", str(project)], capsys)
    candidate_bytes = json.dumps(candidate).encode()
    (project / "candidate.json").write_bytes(candidate_bytes)
    replacement_source.mkdir()
    (replacement_source / "candidate.json").write_bytes(candidate_bytes)
    if mode == "change":
        (project / "workload-index.json").write_bytes(candidate_bytes)
        (replacement_source / "workload-index.json").write_bytes(candidate_bytes)

    @contextmanager
    def replacing_lock(_root: Path):
        project.rename(original)
        replacement_source.rename(project)
        yield

    root_identity = inspect_project_root(project)
    assert root_identity is not None
    service = WorkloadIndexPromotionService(
        root_identity=root_identity,
        store=ConfinedWorkloadIndexBaselineStore(
            root_identity,
            atomic_replace=_test_atomic_replace,
        ),
        lock_factory=replacing_lock,
    )

    with pytest.raises(WorkloadIndexPromotionError) as exc:
        service.promote(
            candidate_path="candidate.json",
            baseline_path="workload-index.json",
            approved_candidate_sha256=_sha256(candidate_bytes),
            approved_current_fingerprint=str(candidate["project_fingerprint"]),
            expected_baseline_sha256=_sha256(candidate_bytes) if mode == "change" else None,
            expected_baseline_fingerprint=(str(candidate["project_fingerprint"]) if mode == "change" else None),
            expect_baseline_absent=mode == "bootstrap",
        )

    assert exc.value.code == "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT"
    if mode == "bootstrap":
        assert not (project / "workload-index.json").exists()
        assert not (original / "workload-index.json").exists()
    else:
        assert (project / "workload-index.json").read_bytes() == candidate_bytes
        assert (original / "workload-index.json").read_bytes() == candidate_bytes


def test_workload_promote_preserves_recovery_leaf_after_committed_cleanup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    _, baseline, _ = _run_cli(["workload", "index", "--root", str(tmp_path)], capsys)
    baseline_bytes = json.dumps(baseline).encode()
    baseline_path = tmp_path / "workload-index.json"
    baseline_path.write_bytes(baseline_bytes)
    candidate_bytes = baseline_bytes + b"\n"
    (tmp_path / "candidate.json").write_bytes(candidate_bytes)

    def commit_without_cleanup(
        parent_fd: int,
        name: str,
        replacement_name: str,
        expected_sha256: str,
        max_bytes: int,
    ) -> WorkloadIndexAtomicReplaceOutcome:
        del expected_sha256, max_bytes
        displaced = f".{name}.displaced"
        promotion_store.os.rename(name, displaced, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        promotion_store.os.rename(
            replacement_name,
            name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        promotion_store.os.rename(
            displaced,
            replacement_name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        return WorkloadIndexAtomicReplaceOutcome(
            committed=True,
            cleanup_required=True,
            recovery_name=replacement_name,
        )

    @contextmanager
    def unlocked(_root: Path):
        yield

    root_identity = inspect_project_root(tmp_path)
    assert root_identity is not None
    service = WorkloadIndexPromotionService(
        root_identity=root_identity,
        store=ConfinedWorkloadIndexBaselineStore(
            root_identity,
            atomic_replace=commit_without_cleanup,
        ),
        lock_factory=unlocked,
    )

    with pytest.raises(WorkloadIndexPromotionError) as exc:
        service.promote(
            candidate_path="candidate.json",
            baseline_path="workload-index.json",
            approved_candidate_sha256=_sha256(candidate_bytes),
            approved_current_fingerprint=str(baseline["project_fingerprint"]),
            expected_baseline_sha256=_sha256(baseline_bytes),
            expected_baseline_fingerprint=str(baseline["project_fingerprint"]),
            expect_baseline_absent=False,
        )

    assert exc.value.code == "DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED"
    assert len(exc.value.recovery_artifacts) == 1
    recovery_path = tmp_path / exc.value.recovery_artifacts[0]
    assert recovery_path.read_bytes() == baseline_bytes
    assert baseline_path.read_bytes() == candidate_bytes


def test_workload_promote_reports_bootstrap_directory_sync_failure_as_recovery_required(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    _, candidate, _ = _run_cli(["workload", "index", "--root", str(tmp_path)], capsys)
    candidate_bytes = json.dumps(candidate).encode()
    (tmp_path / "candidate.json").write_bytes(candidate_bytes)
    real_fsync = promotion_store.os.fsync
    calls = 0

    def fail_directory_sync(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected directory fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(promotion_store.os, "fsync", fail_directory_sync)

    code, payload, stderr = _run_cli(
        [
            "workload",
            "promote",
            "--root",
            str(tmp_path),
            "--candidate",
            "candidate.json",
            "--baseline",
            "workload-index.json",
            "--approved-candidate-sha256",
            _sha256(candidate_bytes),
            "--approved-current-fingerprint",
            str(candidate["project_fingerprint"]),
            "--expect-baseline-absent",
        ],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED"
    assert payload["errors"][0]["path"] == "workload-index.json"
    assert (tmp_path / "workload-index.json").read_bytes() == candidate_bytes


def test_workload_promote_rejects_candidate_replaced_after_approval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    _, approved, _ = _run_cli(["workload", "index"], capsys)
    approved_bytes = json.dumps(approved).encode()
    candidate_path = tmp_path / "candidate.json"
    candidate_path.write_bytes(approved_bytes)
    ownership = tmp_path / "workloads/crm/ownership.yaml"
    ownership.write_text(
        ownership.read_text(encoding="utf-8").replace("crm@example.com", "changed@example.com"),
        encoding="utf-8",
    )
    _, replacement, _ = _run_cli(["workload", "index"], capsys)
    candidate_path.write_text(json.dumps(replacement), encoding="utf-8")

    code, payload, stderr = _run_cli(
        [
            "workload",
            "promote",
            "--candidate",
            "candidate.json",
            "--baseline",
            "workload-index.json",
            "--approved-candidate-sha256",
            _sha256(approved_bytes),
            "--approved-current-fingerprint",
            str(approved["project_fingerprint"]),
            "--expect-baseline-absent",
        ],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_APPROVAL_MISMATCH"
    assert not (tmp_path / "workload-index.json").exists()


@pytest.mark.parametrize("candidate_path", ("../candidate.json", "candidate-link.json"))
def test_workload_promote_reports_candidate_confinement_as_safety_violation(
    candidate_path: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    _domain_first_project(project)
    _, candidate, _ = _run_cli(["workload", "index", "--root", str(project)], capsys)
    candidate_bytes = json.dumps(candidate).encode()
    outside = tmp_path / "candidate.json"
    outside.write_bytes(candidate_bytes)
    if candidate_path == "candidate-link.json":
        (project / candidate_path).symlink_to(outside)
    monkeypatch.chdir(project)

    code, payload, stderr = _run_cli(
        [
            "workload",
            "promote",
            "--candidate",
            candidate_path,
            "--baseline",
            "workload-index.json",
            "--approved-candidate-sha256",
            _sha256(candidate_bytes),
            "--approved-current-fingerprint",
            str(candidate["project_fingerprint"]),
            "--expect-baseline-absent",
        ],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT"
    assert not (project / "workload-index.json").exists()


def test_workload_promote_rejects_baseline_changed_after_impact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    _, baseline, _ = _run_cli(["workload", "index"], capsys)
    baseline_bytes = json.dumps(baseline).encode()
    baseline_path = tmp_path / "workload-index.json"
    baseline_path.write_bytes(baseline_bytes)
    candidate_path = tmp_path / "candidate.json"
    candidate_path.write_bytes(baseline_bytes)
    baseline_path.write_bytes(baseline_bytes + b"\n")

    code, payload, stderr = _run_cli(
        [
            "workload",
            "promote",
            "--candidate",
            "candidate.json",
            "--baseline",
            "workload-index.json",
            "--approved-candidate-sha256",
            _sha256(baseline_bytes),
            "--approved-current-fingerprint",
            str(baseline["project_fingerprint"]),
            "--expected-baseline-sha256",
            _sha256(baseline_bytes),
            "--expected-baseline-fingerprint",
            str(baseline["project_fingerprint"]),
        ],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT"
    assert baseline_path.read_bytes() == baseline_bytes + b"\n"


def test_workload_impact_rejects_tampered_current_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    _, baseline, _ = _run_cli(["workload", "index"], capsys)
    (tmp_path / "baseline.json").write_text(json.dumps(baseline), encoding="utf-8")
    baseline["project_fingerprint"] = "sha256:" + "0" * 64
    (tmp_path / "candidate.json").write_text(json.dumps(baseline), encoding="utf-8")

    code, payload, stderr = _run_cli(
        [
            "workload",
            "impact",
            "--baseline",
            "baseline.json",
            "--current",
            "candidate.json",
        ],
        capsys,
    )

    assert code == 1, stderr
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_INVALID"


def test_workload_impact_requires_explicit_baseline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["workload", "impact"])
    captured = capsys.readouterr()

    assert exc.value.code == 2
    assert captured.out == ""
    assert "the following arguments are required: --baseline" in captured.err


def test_workload_impact_rejects_schema_incomplete_baseline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "baseline.json").write_text('{"schema":"dpone.workload-index.v1","workloads":[]}', encoding="utf-8")

    code, payload, stderr = _run_cli(["workload", "impact", "--baseline", "baseline.json"], capsys)

    assert code == 1, stderr
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_INVALID"


@pytest.mark.parametrize(
    "baseline",
    ("../outside.json", "/tmp/outside.json", r"..\outside.json"),
)
def test_workload_impact_rejects_baseline_path_escape(
    baseline: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)

    code, payload, stderr = _run_cli(["workload", "impact", "--baseline", baseline], capsys)

    assert code == 1
    assert stderr == ""
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_INVALID"


@pytest.mark.parametrize("command", ("index", "impact"))
def test_workload_commands_reject_symlink_project_root_consistently(
    command: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    _domain_first_project(project)
    alias = tmp_path / "alias"
    alias.symlink_to(project, target_is_directory=True)
    args = ["workload", command, "--root", str(alias)]
    if command == "impact":
        _, baseline, _ = _run_cli(["workload", "index", "--root", str(project)], capsys)
        (project / "baseline.json").write_text(json.dumps(baseline), encoding="utf-8")
        args.extend(("--baseline", "baseline.json"))

    code, payload, stderr = _run_cli(args, capsys)

    assert code == 1
    assert stderr == ""
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_INVALID"


def test_workload_impact_candidate_mode_rejects_symlink_project_root(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    _domain_first_project(project)
    _, baseline, _ = _run_cli(["workload", "index", "--root", str(project)], capsys)
    (project / "baseline.json").write_text(json.dumps(baseline), encoding="utf-8")
    (project / "candidate.json").write_text(json.dumps(baseline), encoding="utf-8")
    alias = tmp_path / "alias"
    alias.symlink_to(project, target_is_directory=True)

    code, payload, stderr = _run_cli(
        [
            "workload",
            "impact",
            "--root",
            str(alias),
            "--baseline",
            "baseline.json",
            "--current",
            "candidate.json",
        ],
        capsys,
    )

    assert code == 1
    assert stderr == ""
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_INVALID"


@pytest.mark.parametrize("target_location", ("inside", "outside"))
def test_workload_impact_rejects_symlink_baseline(
    target_location: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    _domain_first_project(project)
    monkeypatch.chdir(project)
    _, baseline, _ = _run_cli(["workload", "index"], capsys)
    target_root = project if target_location == "inside" else tmp_path
    target = target_root / f"{target_location}-baseline.json"
    target.write_text(json.dumps(baseline), encoding="utf-8")
    (project / "baseline.json").symlink_to(target)

    code, payload, stderr = _run_cli(
        ["workload", "impact", "--baseline", "baseline.json"],
        capsys,
    )

    assert code == 1
    assert stderr == ""
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_INVALID"


def test_workload_impact_rejects_symlinked_baseline_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    _, baseline, _ = _run_cli(["workload", "index"], capsys)
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    (real_parent / "baseline.json").write_text(json.dumps(baseline), encoding="utf-8")
    (tmp_path / "alias").symlink_to(real_parent, target_is_directory=True)

    code, payload, stderr = _run_cli(
        ["workload", "impact", "--baseline", "alias/baseline.json"],
        capsys,
    )

    assert code == 1
    assert stderr == ""
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_INVALID"


def test_workload_impact_rejects_current_candidate_path_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    _, baseline, _ = _run_cli(["workload", "index"], capsys)
    (tmp_path / "baseline.json").write_text(json.dumps(baseline), encoding="utf-8")

    code, payload, stderr = _run_cli(
        [
            "workload",
            "impact",
            "--baseline",
            "baseline.json",
            "--current",
            "../candidate.json",
        ],
        capsys,
    )

    assert code == 1
    assert stderr == ""
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_INVALID"


def test_workload_impact_rejects_tampered_baseline_fingerprints(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    _, baseline, _ = _run_cli(["workload", "index"], capsys)
    workloads = baseline["workloads"]
    assert isinstance(workloads, list)
    workloads[0]["workload_fingerprint"] = "sha256:" + "0" * 64
    baseline["project_fingerprint"] = "sha256:" + "1" * 64
    (tmp_path / "baseline.json").write_text(json.dumps(baseline), encoding="utf-8")

    code, payload, stderr = _run_cli(["workload", "impact", "--baseline", "baseline.json"], capsys)

    assert code == 1, stderr
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_INVALID"


def test_workload_index_fails_closed_on_discovery_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _domain_first_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml").unlink()

    code, payload, stderr = _run_cli(["workload", "index"], capsys)

    assert code == 1, stderr
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_PIPELINE_SOURCE_NOT_FOUND"


@pytest.mark.parametrize("command", [["workload", "index"], ["workload", "impact"]])
def test_workload_projections_reject_flat_project(
    command: list[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert build_airflow_self_service_service(root=tmp_path).init_project(airflow=True).passed
    monkeypatch.chdir(tmp_path)
    args = list(command)
    if args[-1] == "impact":
        baseline_source = tmp_path / "baseline-source"
        _domain_first_project(baseline_source)
        _, baseline, _ = _run_cli(["workload", "index", "--root", str(baseline_source)], capsys)
        (tmp_path / "baseline.json").write_text(json.dumps(baseline), encoding="utf-8")
        args.extend(("--baseline", "baseline.json"))

    code, payload, stderr = _run_cli(args, capsys)

    assert code == 1, stderr
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_DOMAIN_LAYOUT_REQUIRED"


def test_workload_impact_accepts_the_producer_workload_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    payload = _synthetic_index(MAX_PROJECT_WORKLOADS)
    encoded = json.dumps(payload, separators=(",", ":")).encode()
    (tmp_path / "baseline.json").write_bytes(encoded)
    (tmp_path / "candidate.json").write_bytes(encoded)

    code, report, stderr = _run_cli(
        [
            "workload",
            "impact",
            "--baseline",
            "baseline.json",
            "--current",
            "candidate.json",
        ],
        capsys,
    )

    assert code == 0, stderr
    assert report["current_source"] == "candidate"
    assert report["discovery_status"] == "not_run"
    assert report["added"] == report["modified"] == report["removed"] == []


def test_workload_impact_rejects_more_than_the_producer_workload_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    payload = _synthetic_index(MAX_PROJECT_WORKLOADS + 1)
    encoded = json.dumps(payload, separators=(",", ":")).encode()
    (tmp_path / "baseline.json").write_bytes(encoded)
    (tmp_path / "candidate.json").write_bytes(encoded)

    code, report, stderr = _run_cli(
        [
            "workload",
            "impact",
            "--baseline",
            "baseline.json",
            "--current",
            "candidate.json",
        ],
        capsys,
    )

    assert code == 1
    assert stderr == ""
    assert report["errors"][0]["code"] == "DPONE_WORKLOAD_INDEX_INVALID"


def _synthetic_index(count: int) -> dict[str, object]:
    workloads: list[dict[str, object]] = []
    for index in range(count):
        pipeline_id = f"p{index:04d}"
        item: dict[str, object] = {
            "pipeline_id": pipeline_id,
            "domain": "d0",
            "owner": "data",
            "ownership_fingerprint": "sha256:" + "1" * 64,
            "authoring_source": f"workloads/d0/pipelines/{pipeline_id}/pipeline.yaml",
            "source_sha256": "sha256:" + "2" * 64,
            "semantic_fingerprint": "sha256:" + "3" * 64,
            "connection_refs": [],
            "dependencies": [],
            "airflow": {"enabled": True, "dag_id": pipeline_id, "schedule": None},
        }
        item["workload_fingerprint"] = workload_fingerprint(item)
        workloads.append(item)
    return {
        "schema": "dpone.workload-index.v1",
        "project_fingerprint": project_fingerprint(
            layout_mode="domain_first",
            layout_root="workloads",
            pipeline_id_scope="project",
            workloads=workloads,
        ),
        "layout_mode": "domain_first",
        "layout_root": "workloads",
        "pipeline_id_scope": "project",
        "workloads": workloads,
    }


def _sha256(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def _test_atomic_replace(
    parent_fd: int,
    name: str,
    replacement_name: str,
    expected_sha256: str,
    max_bytes: int,
) -> WorkloadIndexAtomicReplaceOutcome:
    outcome = replace_file_if_digest(
        parent_fd,
        name,
        replacement_name,
        expected_sha256=expected_sha256,
        max_bytes=max_bytes,
    )
    return WorkloadIndexAtomicReplaceOutcome(
        committed=outcome.committed,
        cleanup_required=outcome.cleanup_required,
        recovery_name=outcome.recovery_name,
    )
