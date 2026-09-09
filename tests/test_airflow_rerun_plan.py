from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.cli import main as cli_main
from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.gitops.schema_contracts import get_gitops_schema_contract
from dpone.readiness.airflow_bundle_identity import airflow_bundle_identity
from dpone.readiness.airflow_rerun_plan import AirflowRerunPlanner

ORIGINAL_RELEASE = "sha256:" + "a" * 64
ORIGINAL_DEPLOYMENT = "sha256:" + "b" * 64
LATEST_RELEASE = "sha256:" + "e" * 64
LATEST_DEPLOYMENT = "sha256:" + "f" * 64
ORIGINAL_BUNDLE_REF = "git:" + "1" * 40
LATEST_BUNDLE_REF = "git:" + "2" * 40


def _original_identity(*, bundle_ref: str = ORIGINAL_BUNDLE_REF) -> AirflowRunIdentity:
    backend = "git" if bundle_ref.startswith("git:") else "s3"
    version = bundle_ref.removeprefix("git:") if backend == "git" else None
    return AirflowRunIdentity.from_mapping(
        {
            "schema": "dpone.airflow-run-identity.v1",
            "release_id": ORIGINAL_RELEASE,
            "deployment_id": ORIGINAL_DEPLOYMENT,
            "dag_spec": {"id": "orders_daily", "sha256": "sha256:" + "c" * 64},
            "workload_pack": {"id": "load_orders", "sha256": "sha256:" + "d" * 64},
            "runtime_image_digest": "sha256:" + "1" * 64,
            "binding_set_ref": "sha256:" + "2" * 64,
            "connection_registry_ref": "sha256:" + "3" * 64,
            "credential_runtime_ref": "sha256:" + "4" * 64,
            "airflow_bundle": {
                "backend": backend,
                "ref": bundle_ref,
                "versioned": backend == "git",
                "version": version,
                "snapshot_ref": None,
            },
        }
    )


def _current_index(*, bundle_ref: str = LATEST_BUNDLE_REF) -> dict[str, object]:
    return {
        "schema": "dpone.airflow-deployment-index.v1",
        "release_id": LATEST_RELEASE,
        "deployment_id": LATEST_DEPLOYMENT,
        "dag_specs": [{"id": "orders_daily", "sha256": "sha256:" + "5" * 64, "artifact_ref": "cache://dag"}],
        "workload_packs": [{"id": "load_orders", "sha256": "sha256:" + "6" * 64, "artifact_ref": "cache://pack"}],
        "runtime_image_digest": "sha256:" + "7" * 64,
        "binding_set_ref": "sha256:" + "8" * 64,
        "connection_registry_ref": "sha256:" + "9" * 64,
        "credential_runtime_ref": "sha256:" + "0" * 64,
        "airflow_bundle_ref": bundle_ref,
        "runtime_artifact_delivery": {"mode": "init_fetch"},
    }


def _write_cli_inputs(
    root: Path,
    *,
    identity: AirflowRunIdentity | None = None,
    current_index: dict[str, object] | None = None,
) -> tuple[Path, Path]:
    selected_identity = identity or _original_identity()
    evidence = root / "evidence.json"
    evidence.write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_evidence_bundle",
                "attempt": {
                    "dag_id": "orders_daily",
                    "task_id": "load_orders",
                    "run_id": "scheduled__old",
                },
                "run_identity": selected_identity.to_dict(),
            }
        ),
        encoding="utf-8",
    )
    index = root / "airflow-index.json"
    index.write_text(
        json.dumps(current_index or _index_for_identity(selected_identity)),
        encoding="utf-8",
    )
    return evidence, index


def _index_for_identity(identity: AirflowRunIdentity) -> dict[str, object]:
    assert identity.dag_spec is not None
    return {
        "schema": "dpone.airflow-deployment-index.v1",
        "release_id": identity.release_id,
        "deployment_id": identity.deployment_id,
        "dag_specs": [
            {
                "id": identity.dag_spec.id,
                "sha256": identity.dag_spec.sha256,
                "artifact_ref": "cache://dag",
            }
        ],
        "workload_packs": [
            {
                "id": identity.workload_pack.id,
                "sha256": identity.workload_pack.sha256,
                "artifact_ref": "cache://pack",
            }
        ],
        "runtime_image_digest": identity.runtime_image_digest,
        "binding_set_ref": identity.binding_set_ref,
        "connection_registry_ref": identity.connection_registry_ref,
        "credential_runtime_ref": identity.credential_runtime_ref,
        "airflow_bundle_ref": identity.airflow_bundle.ref if identity.airflow_bundle is not None else None,
        "runtime_artifact_delivery": {"mode": "init_fetch"},
    }


def _run_cli(args: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err


@pytest.mark.parametrize(
    ("bundle", "artifacts", "expected_bundle", "expected_release", "expected_mode", "run_latest"),
    [
        ("original", "original", ORIGINAL_BUNDLE_REF, ORIGINAL_RELEASE, "clear_existing_run", False),
        ("latest", "latest", LATEST_BUNDLE_REF, LATEST_RELEASE, "create_pinned_rerun", True),
        ("latest", "original", LATEST_BUNDLE_REF, ORIGINAL_RELEASE, "create_pinned_rerun", True),
        ("original", "latest", ORIGINAL_BUNDLE_REF, LATEST_RELEASE, "create_pinned_rerun", False),
    ],
)
def test_rerun_planner_selects_bundle_and_artifacts_independently(
    bundle: str,
    artifacts: str,
    expected_bundle: str,
    expected_release: str,
    expected_mode: str,
    run_latest: bool,
) -> None:
    plan = AirflowRerunPlanner().plan(
        original_identity=_original_identity(),
        source_attempt={"dag_id": "orders_daily", "task_id": "load_orders", "run_id": "scheduled__old"},
        current_index=_current_index(),
        bundle_selection=bundle,
        artifact_selection=artifacts,
        critical=True,
        original_artifacts_available=True,
    )

    payload = plan.to_dict()
    assert plan.passed
    assert payload["resolved"]["release_id"] == expected_release
    assert payload["resolved"]["airflow_bundle"]["ref"] == expected_bundle
    assert payload["airflow_request"] == {
        "run_on_latest_version": run_latest,
        "execution_mode": expected_mode,
    }
    assert payload["retention_refs"]["release_ids"] == [expected_release]
    assert (
        plan.semantic_fingerprint
        == AirflowRerunPlanner()
        .plan(
            original_identity=_original_identity(),
            source_attempt={"dag_id": "orders_daily", "task_id": "load_orders", "run_id": "scheduled__old"},
            current_index=_current_index(),
            bundle_selection=bundle,
            artifact_selection=artifacts,
            critical=True,
            original_artifacts_available=True,
        )
        .semantic_fingerprint
    )


def test_rerun_plan_matches_public_json_schema() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    contract = get_gitops_schema_contract("dpone.airflow-rerun-plan.v1")
    plan = AirflowRerunPlanner().plan(
        original_identity=_original_identity(),
        source_attempt={"dag_id": "orders_daily", "task_id": "load_orders", "run_id": "scheduled__old"},
        current_index=_current_index(),
        bundle_selection="original",
        artifact_selection="original",
        critical=True,
        original_artifacts_available=True,
    )

    assert contract is not None
    jsonschema.validate(plan.to_dict(), contract.schema)


def test_rerun_planner_blocks_non_versioned_original_bundle() -> None:
    plan = AirflowRerunPlanner().plan(
        original_identity=_original_identity(bundle_ref="s3://dpone-dags/prod"),
        source_attempt={},
        current_index=_current_index(),
        bundle_selection="original",
        artifact_selection="original",
        critical=True,
        original_artifacts_available=True,
    )

    assert not plan.passed
    assert {item["code"] for item in plan.to_dict()["blockers"]} == {"DPONE_RERUN_NOT_REPRODUCIBLE"}


def test_short_git_bundle_reference_is_not_a_reproducible_version() -> None:
    identity = airflow_bundle_identity("git:old123")

    assert identity is not None
    assert identity.backend == "git"
    assert identity.versioned is False
    assert identity.version is None


def test_rerun_planner_blocks_expired_original_artifacts() -> None:
    plan = AirflowRerunPlanner().plan(
        original_identity=_original_identity(),
        source_attempt={},
        current_index=_current_index(),
        bundle_selection="latest",
        artifact_selection="original",
        critical=True,
        original_artifacts_available=False,
    )

    assert not plan.passed
    assert {item["code"] for item in plan.to_dict()["blockers"]} == {"DPONE_DEPLOYMENT_EXPIRED"}


def test_noncritical_latest_nonversioned_bundle_is_explicit_warning() -> None:
    plan = AirflowRerunPlanner().plan(
        original_identity=_original_identity(),
        source_attempt={},
        current_index=_current_index(bundle_ref="s3://dpone-dags/prod"),
        bundle_selection="latest",
        artifact_selection="latest",
        critical=False,
        original_artifacts_available=True,
    )

    assert plan.passed
    assert {item["code"] for item in plan.to_dict()["warnings"]} == {"DPONE_RERUN_NOT_REPRODUCIBLE"}


def test_rerun_plan_cli_writes_same_ready_plan_atomically(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    evidence, index = _write_cli_inputs(tmp_path)
    output = tmp_path / "plans" / "rerun.json"

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "rerun-plan",
            "--evidence",
            str(evidence),
            "--current-index",
            str(index),
            "--critical",
            "--format",
            "json",
            "--output",
            str(output),
        ],
        capsys,
    )

    assert code == 0
    assert stderr == ""
    rendered = json.loads(stdout)
    assert rendered["schema"] == "dpone.airflow-rerun-plan.v1"
    assert rendered["status"] == "ready"
    assert rendered["airflow_request"]["execution_mode"] == "clear_existing_run"
    assert json.loads(output.read_text(encoding="utf-8")) == rendered


def test_rerun_plan_cli_blocks_critical_nonversioned_bundle_with_security_exit(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    evidence, index = _write_cli_inputs(
        tmp_path,
        identity=_original_identity(bundle_ref="s3://dpone-dags/prod"),
    )

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "rerun-plan",
            "--evidence",
            str(evidence),
            "--current-index",
            str(index),
            "--critical",
        ],
        capsys,
    )

    assert code == 4
    assert stderr == ""
    assert "dpone airflow rerun plan: BLOCKED" in stdout
    assert "DPONE_RERUN_NOT_REPRODUCIBLE" in stdout


def test_rerun_plan_cli_reports_invalid_evidence_without_echoing_payload(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    evidence, index = _write_cli_inputs(tmp_path)
    secret_marker = "password=must-not-leak"
    evidence.write_text(secret_marker, encoding="utf-8")

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "rerun-plan",
            "--evidence",
            str(evidence),
            "--current-index",
            str(index),
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 2
    assert stderr == ""
    payload = json.loads(stdout)
    assert payload["blockers"][0]["code"] == "DPONE_AIRFLOW_EVIDENCE_INVALID"
    contract = get_gitops_schema_contract("dpone.airflow-rerun-plan.v1")
    assert contract is not None
    pytest.importorskip("jsonschema").validate(payload, contract.schema)
    assert secret_marker not in stdout


def test_rerun_plan_cli_reports_invalid_current_index_as_configuration_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    evidence, index = _write_cli_inputs(tmp_path)
    index.write_text(json.dumps({"schema": "wrong"}), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "rerun-plan",
            "--evidence",
            str(evidence),
            "--current-index",
            str(index),
            "--artifacts",
            "latest",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 2
    assert stderr == ""
    assert json.loads(stdout)["blockers"][0]["code"] == "DPONE_AIRFLOW_INDEX_SCHEMA_INVALID"


def test_rerun_plan_cli_blocks_incomplete_retained_deployment(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    evidence, staged_index = _write_cli_inputs(tmp_path, current_index=_current_index())
    cache_root.mkdir()
    index = cache_root / "airflow-index.json"
    index.write_bytes(staged_index.read_bytes())
    release = cache_root / "releases" / ORIGINAL_RELEASE.replace(":", "-")
    release.mkdir(parents=True)
    (release / "release-set.json").write_text("{}", encoding="utf-8")
    incomplete = cache_root / "deployments" / "prod" / ORIGINAL_DEPLOYMENT.replace(":", "-")
    incomplete.mkdir(parents=True)

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "rerun-plan",
            "--evidence",
            str(evidence),
            "--current-index",
            str(index),
            "--cache-root",
            str(cache_root),
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 1
    assert stderr == ""
    assert json.loads(stdout)["blockers"][0]["code"] == "DPONE_DEPLOYMENT_INCOMPLETE"


def test_rerun_plan_cli_ignores_retained_deployment_symlink_escape(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    evidence, staged_index = _write_cli_inputs(tmp_path, current_index=_current_index())
    release = cache_root / "releases" / ORIGINAL_RELEASE.replace(":", "-")
    release.mkdir(parents=True)
    (release / "release-set.json").write_text("{}", encoding="utf-8")
    index = cache_root / "airflow-index.json"
    index.write_bytes(staged_index.read_bytes())
    outside = tmp_path / "outside" / ORIGINAL_DEPLOYMENT.replace(":", "-")
    outside.mkdir(parents=True)
    (outside / "_SUCCESS").write_text("ok\n", encoding="utf-8")
    (outside / "airflow-index.json").write_text(
        json.dumps(_index_for_identity(_original_identity())),
        encoding="utf-8",
    )
    environment_link = cache_root / "deployments" / "prod"
    environment_link.parent.mkdir(parents=True)
    try:
        environment_link.symlink_to(outside.parent, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    code, stdout, _ = _run_cli(
        [
            "airflow",
            "rerun-plan",
            "--evidence",
            str(evidence),
            "--current-index",
            str(index),
            "--cache-root",
            str(cache_root),
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 1
    assert json.loads(stdout)["blockers"][0]["code"] == "DPONE_DEPLOYMENT_EXPIRED"


def test_rerun_plan_cli_does_not_open_network(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence, index = _write_cli_inputs(tmp_path)

    def fail_network(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("rerun planning must remain local-only")

    monkeypatch.setattr("socket.create_connection", fail_network)
    code, _, _ = _run_cli(
        [
            "airflow",
            "rerun-plan",
            "--evidence",
            str(evidence),
            "--current-index",
            str(index),
        ],
        capsys,
    )

    assert code == 0
