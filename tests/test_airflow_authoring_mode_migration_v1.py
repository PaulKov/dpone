from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest
import yaml

import dpone.manifest.authoring_migration_io as migration_io
from dpone.cli import main as cli_main
from dpone.gitops.airflow_process_identity import resolve_airflow_process_identities
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.manifest.authoring import default_authoring_compiler
from dpone.manifest.authoring_migration_io import AuthoringMigrationFileSystem, AuthoringMigrationIoError
from dpone.manifest.authoring_migration_service import AuthoringMigrationService
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service


def _run_cli(args: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err


def _scaffold(root: Path, mode: str) -> Path:
    result = build_airflow_self_service_service(root=root).init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
        authoring_mode=mode,
    )
    assert result.passed is True
    return root / "pipelines/orders_daily/pipeline.yaml"


def _compile(root: Path, source: Path):
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    return default_authoring_compiler().compile(payload, source_path=source, project_root=root)


def _airflow_identity(source: Path) -> tuple[tuple[str, str | None], ...]:
    loaded = ManifestLoaderRouter().load(source, metadata_only=True)
    return tuple(
        (identity.node_id, identity.process.selector)
        for identity in resolve_airflow_process_identities(loaded, workload_id="orders_daily")
    )


@pytest.mark.parametrize(
    ("source_mode", "target_mode"),
    [
        ("classic", "flow"),
        ("classic", "folder"),
        ("flow", "classic"),
        ("flow", "folder"),
        ("folder", "classic"),
        ("folder", "flow"),
    ],
)
def test_all_directed_authoring_migrations_preserve_semantics_and_retry_as_noop(
    tmp_path: Path,
    source_mode: str,
    target_mode: str,
) -> None:
    source = _scaffold(tmp_path, source_mode)
    before = _compile(tmp_path, source)
    before_airflow_identity = _airflow_identity(source)
    service = AuthoringMigrationService(root=tmp_path)

    plan = service.migrate(
        target="pipelines/orders_daily",
        target_mode=target_mode,
        expected_source_mode=source_mode,
        apply=False,
    )

    assert plan.passed is True
    assert plan.status == "ready"
    assert plan.mode == "plan"
    assert plan.source.mode == source_mode
    assert plan.target.mode == target_mode
    assert plan.source.semantic_fingerprint == before.semantic_fingerprint
    assert plan.target.semantic_fingerprint == before.semantic_fingerprint
    assert plan.plan_id.startswith("sha256:")
    assert plan.changes

    applied = service.migrate(
        target="pipelines/orders_daily",
        target_mode=target_mode,
        expected_source_mode=source_mode,
        apply=True,
    )

    assert applied.passed is True
    assert applied.status == "applied"
    assert applied.mode == "apply"
    assert _compile(tmp_path, source).authoring_mode == target_mode
    assert _compile(tmp_path, source).semantic_fingerprint == before.semantic_fingerprint
    assert before_airflow_identity == (("orders_daily", "orders_daily"),)
    assert _airflow_identity(source) == before_airflow_identity

    repeated = service.migrate(
        target="pipelines/orders_daily",
        target_mode=target_mode,
        expected_source_mode=target_mode,
        apply=True,
    )
    assert repeated.passed is True
    assert repeated.status == "no_op"
    assert repeated.changes == ()


def test_folder_to_flow_retains_old_fragment_without_deleting_it(tmp_path: Path) -> None:
    source = _scaffold(tmp_path, "folder")
    fragment = source.parent / "steps/load.yaml"

    result = AuthoringMigrationService(root=tmp_path).migrate(
        target="orders_daily",
        target_mode="flow",
        expected_source_mode="folder",
        apply=True,
    )

    assert result.passed is True
    assert fragment.exists()
    assert "pipelines/orders_daily/steps/load.yaml" in result.retained_files
    assert "DPONE_AUTHORING_MIGRATION_OLD_FRAGMENTS_RETAINED" in result.warnings


def test_flow_to_folder_conflict_is_fail_closed_and_source_is_unchanged(tmp_path: Path) -> None:
    source = _scaffold(tmp_path, "flow")
    before = source.read_bytes()
    fragment = source.parent / "processes.yaml"
    fragment.write_text("user: owned\n", encoding="utf-8")

    result = AuthoringMigrationService(root=tmp_path).migrate(
        target="orders_daily",
        target_mode="folder",
        expected_source_mode="flow",
        apply=True,
    )

    assert result.passed is False
    assert result.status == "blocked"
    assert {error["code"] for error in result.errors} == {"DPONE_AUTHORING_MIGRATION_FILE_CONFLICT"}
    assert source.read_bytes() == before
    assert fragment.read_text(encoding="utf-8") == "user: owned\n"


def test_apply_rejects_source_changed_after_snapshot_with_stable_error(tmp_path: Path) -> None:
    source = _scaffold(tmp_path, "flow")
    filesystem = AuthoringMigrationFileSystem(tmp_path)
    snapshot = filesystem.read_source("orders_daily")
    source.write_bytes(snapshot.content + b"\n")

    with pytest.raises(AuthoringMigrationIoError) as exc:
        filesystem.apply(
            source=snapshot,
            desired_source=snapshot.content,
            fragment_relative=None,
            desired_fragment=None,
        )

    assert exc.value.code == "DPONE_AUTHORING_MIGRATION_SOURCE_CHANGED"
    assert source.read_bytes() == snapshot.content + b"\n"


def test_apply_rejects_source_changed_during_replace_and_keeps_concurrent_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _scaffold(tmp_path, "flow")
    filesystem = AuthoringMigrationFileSystem(tmp_path)
    snapshot = filesystem.read_source("orders_daily")
    concurrent = snapshot.content + b"# concurrent edit\n"
    real_write_temporary = migration_io._write_temporary

    def mutate_then_write(*args: object, **kwargs: object) -> str:
        source.write_bytes(concurrent)
        return real_write_temporary(*args, **kwargs)

    monkeypatch.setattr(migration_io, "_write_temporary", mutate_then_write)

    with pytest.raises(AuthoringMigrationIoError) as exc:
        filesystem.apply(
            source=snapshot,
            desired_source=b"kind: dpone.flow.v1\nmetadata: {id: overwritten}\n",
            fragment_relative=None,
            desired_fragment=None,
        )

    assert exc.value.code == "DPONE_AUTHORING_MIGRATION_SOURCE_CHANGED"
    assert source.read_bytes() == concurrent


def test_apply_preserves_primary_source_permissions(tmp_path: Path) -> None:
    source = _scaffold(tmp_path, "classic")
    source.chmod(0o640)

    result = AuthoringMigrationService(root=tmp_path).migrate(
        target="orders_daily",
        target_mode="folder",
        apply=True,
    )

    assert result.passed is True
    assert stat.S_IMODE(source.stat().st_mode) == 0o640
    assert stat.S_IMODE((source.parent / "processes.yaml").stat().st_mode) == 0o640


def test_partial_folder_apply_removes_only_its_new_fragment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _scaffold(tmp_path, "flow")
    filesystem = AuthoringMigrationFileSystem(tmp_path)
    snapshot = filesystem.read_source("orders_daily")

    def fail_primary_replace(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise OSError("simulated durable write failure")

    monkeypatch.setattr(migration_io, "_replace_file", fail_primary_replace)

    with pytest.raises(OSError, match="simulated durable write failure"):
        filesystem.apply(
            source=snapshot,
            desired_source=b"kind: dpone.flow.v1\n",
            fragment_relative="pipelines/orders_daily/processes.yaml",
            desired_fragment=b"kind: dpone.flow-fragment.v1\nprocesses: []\n",
        )

    assert source.read_bytes() == snapshot.content
    assert not (source.parent / "processes.yaml").exists()


def test_recipe_source_is_not_materialized_implicitly(tmp_path: Path) -> None:
    source = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    source.parent.mkdir(parents=True)
    source.write_text(
        yaml.safe_dump(
            {
                "kind": "dpone.flow.v1",
                "authoring": {"mode": "flow", "source": "pipelines/orders_daily/pipeline.yaml"},
                "metadata": {"id": "orders_daily", "domain": "sales"},
                "recipe": {"ref": "ingest@1.0.0"},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = AuthoringMigrationService(root=tmp_path).migrate(
        target="orders_daily",
        target_mode="classic",
        apply=False,
    )

    assert result.passed is False
    assert {error["code"] for error in result.errors} == {"DPONE_AUTHORING_MIGRATION_RECIPE_UNSUPPORTED"}


def test_expected_source_mode_mismatch_is_a_configuration_blocker(tmp_path: Path) -> None:
    _scaffold(tmp_path, "flow")

    result = AuthoringMigrationService(root=tmp_path).migrate(
        target="orders_daily",
        target_mode="classic",
        expected_source_mode="folder",
        apply=False,
    )

    assert result.passed is False
    assert result.exit_code == 2
    assert {error["code"] for error in result.errors} == {"DPONE_AUTHORING_MIGRATION_SOURCE_MODE_MISMATCH"}


def test_migration_rejects_source_symlink_escape(tmp_path: Path) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside-pipeline.yaml"
    outside.write_text("kind: dpone.flow.v1\n", encoding="utf-8")
    pipeline_dir = tmp_path / "pipelines/orders_daily"
    pipeline_dir.mkdir(parents=True)
    (pipeline_dir / "pipeline.yaml").symlink_to(outside)

    result = AuthoringMigrationService(root=tmp_path).migrate(
        target="orders_daily",
        target_mode="classic",
        apply=False,
    )

    assert result.passed is False
    assert result.exit_code == 4
    assert {error["code"] for error in result.errors} == {"DPONE_AUTHORING_MIGRATION_PATH_INVALID"}


def test_cli_json_report_is_schema_valid_and_contains_no_secret_value(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    source = _scaffold(tmp_path, "flow")
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["processes"][0]["source"]["password"] = "must-not-leak"
    source.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        [
            "migrate",
            "authoring",
            "orders_daily",
            "--from",
            "flow",
            "--to",
            "classic",
            "--plan",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    assert "must-not-leak" not in stdout
    report = json.loads(stdout)
    assert report["schema"] == "dpone.authoring-migration.v1"
    assert report["status"] == "ready"
    assert (
        GitOpsSchemaValidator().validate(
            report,
            expected_kind="dpone.authoring-migration.v1",
        )
        == ()
    )


def test_cli_text_is_concise_and_apply_changes_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    source = _scaffold(tmp_path, "classic")

    code, stdout, stderr = _run_cli(
        ["migrate", "authoring", "orders_daily", "--to", "flow", "--apply"],
        capsys,
    )

    assert code == 0, stderr
    assert "dpone migrate authoring: APPLIED" in stdout
    assert "classic -> flow" in stdout
    assert "semantic fingerprint: sha256:" in stdout
    assert _compile(tmp_path, source).authoring_mode == "flow"
