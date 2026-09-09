from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

import dpone.manifest.confined_mutations as confined_mutations
from dpone.manifest.authoring_migration_io import AuthoringMigrationFileSystem
from dpone.readiness.airflow_authoring_fix import AirflowAuthoringFixService
from dpone.readiness.airflow_local_safe_sample_deployment import (
    ensure_local_safe_sample_deployment,
)
from dpone.readiness.airflow_pipeline_source import AIRFLOW_AUTHORING_DIFF_MAX_BYTES
from dpone.readiness.airflow_scaffold_apply import (
    ScaffoldApplier,
    ScaffoldFile,
    ScaffoldFileSystem,
)
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.security_redaction import REDACTION_TOKEN


def _legacy_pipeline(*, note: str = "") -> dict[str, Any]:
    source: dict[str, Any] = {
        "type": "mssql",
        "connection_type": "vault",
        "vault_path": "dpone/dev/credentials/mssql",
        "password": "must-not-leak",
    }
    if note:
        source["note"] = note
    return {
        "kind": "dpone.flow.v1",
        "metadata": {"id": "orders_daily"},
        "processes": [
            {
                "id": "load",
                "source": source,
                "sink": {"type": "clickhouse", "connection_ref": "clickhouse_dev"},
            }
        ],
    }


def _write_legacy_pipeline(path: Path, *, note: str = "") -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = yaml.safe_dump(_legacy_pipeline(note=note), sort_keys=False).encode()
    path.write_bytes(content)
    return content


def _symlink(link: Path, target: Path, *, target_is_directory: bool = False) -> None:
    try:
        link.symlink_to(target, target_is_directory=target_is_directory)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")


def test_fix_rejects_external_absolute_source_without_modifying_it(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    external = tmp_path / "external-pipeline.yaml"
    before = _write_legacy_pipeline(external)

    result = AirflowAuthoringFixService(root=root).fix(external, apply=True)

    assert result.passed is False
    assert {error["code"] for error in result.errors} == {"DPONE_AUTHORING_MIGRATION_PATH_INVALID"}
    assert external.read_bytes() == before


def test_check_rejects_external_absolute_source_without_reading_it(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    external = tmp_path / "external-pipeline.yaml"
    _write_legacy_pipeline(external)

    result = build_airflow_self_service_service(root=root).check(external)

    assert result.passed is False
    assert result.exit_code == 4
    assert {error["code"] for error in result.errors} == {"DPONE_PIPELINE_SOURCE_PATH_INVALID"}


def test_check_rejects_symlink_source_even_when_target_is_inside_project(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    external = tmp_path / "external-pipeline.yaml"
    _write_legacy_pipeline(external)
    source = root / "pipelines/orders_daily/pipeline.yaml"
    source.parent.mkdir(parents=True)
    _symlink(source, external)

    result = build_airflow_self_service_service(root=root).check("orders_daily")

    assert result.passed is False
    assert result.exit_code == 4
    assert {error["code"] for error in result.errors} == {"DPONE_PIPELINE_SOURCE_PATH_INVALID"}


def test_local_safe_sample_rejects_external_pipeline_source(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    external = tmp_path / "external-pipeline.yaml"
    _write_legacy_pipeline(external)

    result = ensure_local_safe_sample_deployment(
        root=root,
        pipeline_source_path=external,
        policy_environment="dev",
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error["code"] == "DPONE_PIPELINE_SOURCE_PATH_INVALID"
    assert not (root / ".dpone-cache").exists()


@pytest.mark.parametrize("symlink_component", ["parent", "leaf"])
def test_fix_rejects_symlink_escape_without_modifying_target(
    tmp_path: Path,
    symlink_component: str,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    external_dir = tmp_path / "external"
    external = external_dir / "pipeline.yaml"
    before = _write_legacy_pipeline(external)
    pipeline_dir = root / "pipelines" / "orders_daily"

    if symlink_component == "parent":
        pipeline_dir.parent.mkdir(parents=True)
        _symlink(pipeline_dir, external_dir, target_is_directory=True)
    else:
        pipeline_dir.mkdir(parents=True)
        _symlink(pipeline_dir / "pipeline.yaml", external)

    result = AirflowAuthoringFixService(root=root).fix(
        "pipelines/orders_daily/pipeline.yaml",
        apply=True,
    )

    assert result.passed is False
    assert {error["code"] for error in result.errors} == {"DPONE_AUTHORING_MIGRATION_PATH_INVALID"}
    assert external.read_bytes() == before


def test_fix_apply_rejects_concurrent_update_and_preserves_winner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    source = root / "pipelines/orders_daily/pipeline.yaml"
    _write_legacy_pipeline(source)
    concurrent = b"kind: dpone.flow.v1\nmetadata: {id: concurrent-winner}\n"
    filesystem = AuthoringMigrationFileSystem(root)
    apply_snapshot = filesystem.apply

    def concurrent_apply(**kwargs: Any):
        source.write_bytes(concurrent)
        return apply_snapshot(**kwargs)

    monkeypatch.setattr(filesystem, "apply", concurrent_apply)

    result = AirflowAuthoringFixService(root=root, filesystem=filesystem).fix(
        "orders_daily",
        apply=True,
    )

    assert result.passed is False
    assert {error["code"] for error in result.errors} == {"DPONE_AUTHORING_MIGRATION_SOURCE_CHANGED"}
    assert source.read_bytes() == concurrent


def test_fix_apply_surfaces_late_recovery_path_in_structured_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    source = root / "pipelines/orders_daily/pipeline.yaml"
    _write_legacy_pipeline(source)
    winner = yaml.safe_dump(_legacy_pipeline(note="concurrent-winner"), sort_keys=False).encode()
    third_writer = yaml.safe_dump(_legacy_pipeline(note="third-writer"), sort_keys=False).encode()
    real_exchange = confined_mutations.get_native_atomic_exchange()
    exchange_calls = 0

    def write_leaf(parent_fd: int, name: str, payload: bytes) -> None:
        descriptor = confined_mutations.os.open(
            name,
            confined_mutations.os.O_WRONLY | confined_mutations.os.O_TRUNC,
            dir_fd=parent_fd,
        )
        try:
            confined_mutations.os.write(descriptor, payload)
            confined_mutations.os.fsync(descriptor)
        finally:
            confined_mutations.os.close(descriptor)

    def exchange_with_two_concurrent_writers(
        parent_fd: int,
        left_name: str,
        right_name: str,
    ) -> None:
        nonlocal exchange_calls
        exchange_calls += 1
        if exchange_calls == 1:
            write_leaf(parent_fd, left_name, winner)
        real_exchange(parent_fd, left_name, right_name)
        if exchange_calls == 2:
            write_leaf(parent_fd, right_name, third_writer)

    monkeypatch.setattr(
        confined_mutations,
        "get_native_atomic_exchange",
        lambda: exchange_with_two_concurrent_writers,
    )

    result = AirflowAuthoringFixService(root=root).fix("orders_daily", apply=True)

    assert result.passed is False
    error = result.to_dict()["errors"][0]
    assert error["code"] == "DPONE_AUTHORING_MIGRATION_SOURCE_CHANGED"
    recovery_path = Path(error["recovery_path"])
    assert recovery_path.name.startswith(".dpone-recovery-")
    assert error["recovery_path"] in error["message"]
    assert (root / recovery_path).read_bytes() == third_writer
    assert source.read_bytes() == winner


def test_fix_plan_bounds_and_redacts_every_public_diff(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    source = root / "pipelines/orders_daily/pipeline.yaml"
    _write_legacy_pipeline(source, note="visible-context-" * 2_000)

    result = AirflowAuthoringFixService(root=root).fix("orders_daily", apply=False)

    assert result.passed is True
    assert result.details is not None
    public_diffs = [
        result.changes[0].diff,
        result.details["migration_plan"]["changes"][0]["unified_diff"],
    ]
    for diff in public_diffs:
        assert "must-not-leak" not in diff
        assert len(diff.encode("utf-8")) <= AIRFLOW_AUTHORING_DIFF_MAX_BYTES
        assert "[diff truncated by dpone]" in diff


def test_scaffold_rejects_absolute_output_path(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    external = tmp_path / "external.txt"

    result = ScaffoldApplier(root).apply((ScaffoldFile(external, "dpone-owned\n"),))

    assert result.has_conflict is True
    assert result.rollback_journal["entries"] == []
    assert not external.exists()


def test_scaffold_rejects_symlink_path_component_without_external_write(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    external = tmp_path / "external"
    external.mkdir()
    _symlink(root / "dags", external, target_is_directory=True)

    result = ScaffoldApplier(root).apply((ScaffoldFile(Path("dags/dpone.py"), "dpone-owned\n"),))

    assert result.has_conflict is True
    assert result.rollback_journal["entries"] == []
    assert not (external / "dpone.py").exists()


def test_scaffold_concurrent_create_preserves_winner_and_reports_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    target = root / "dpone.yaml"
    applier = ScaffoldApplier(root)
    plan = applier.plan

    def plan_then_compete(files: tuple[ScaffoldFile, ...]):
        result = plan(files)
        target.write_text("concurrent-user-content\n", encoding="utf-8")
        return result

    monkeypatch.setattr(applier, "plan", plan_then_compete)

    result = applier.apply((ScaffoldFile(Path("dpone.yaml"), "dpone-owned\n"),))

    assert result.has_conflict is True
    assert result.rollback_journal["entries"] == []
    assert result.changes[0].action == "conflict"
    assert target.read_text(encoding="utf-8") == "concurrent-user-content\n"


def test_scaffold_rollback_keeps_file_modified_after_its_create(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    first = ScaffoldFile(Path("first.txt"), "first-operation-content\n")
    second = ScaffoldFile(Path("second.txt"), "second-operation-content\n")
    first_path = root / first.path
    filesystem = ScaffoldFileSystem(root)
    create = filesystem.create

    def fail_after_takeover(file: ScaffoldFile):
        if file.path == second.path:
            first_path.write_text("concurrent-user-content\n", encoding="utf-8")
            raise OSError("simulated second write failure")
        return create(file)

    monkeypatch.setattr(filesystem, "create", fail_after_takeover)

    with pytest.raises(OSError, match="simulated second write failure") as caught:
        ScaffoldApplier(root, filesystem=filesystem).apply((first, second))

    receipt = getattr(caught.value, "scaffold_receipt")
    assert [change.action for change in receipt.changes] == ["preserved", "failed"]
    assert receipt.rollback_journal["entries"] == []
    assert first_path.read_text(encoding="utf-8") == "concurrent-user-content\n"
    assert not (root / second.path).exists()


def test_scaffold_conflict_diff_is_bounded_and_redacted(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    target = root / "dpone.yaml"
    target.write_text(
        "password: must-not-leak\napi_token: second-must-not-leak\n" + ("existing-visible-context\n" * 8_000),
        encoding="utf-8",
    )

    result = ScaffoldApplier(root).plan((ScaffoldFile(Path("dpone.yaml"), "schema: dpone.project.v1\n"),))

    assert result.has_conflict is True
    diff = result.changes[0].diff
    assert "must-not-leak" not in diff
    assert "second-must-not-leak" not in diff
    assert REDACTION_TOKEN in diff
    assert len(diff.encode("utf-8")) <= AIRFLOW_AUTHORING_DIFF_MAX_BYTES
    assert "[diff truncated by dpone]" in diff


def test_scaffold_conflict_diff_redacts_vault_path(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    target = root / "dpone.yaml"
    target.write_text(
        "vault_path: dpone/dev/credentials/mssql\npassword: must-not-leak\n",
        encoding="utf-8",
    )

    result = ScaffoldApplier(root).plan((ScaffoldFile(Path("dpone.yaml"), "schema: dpone.project.v1\n"),))

    assert result.has_conflict is True
    diff = result.changes[0].diff
    assert "dpone/dev/credentials/mssql" not in diff
    assert "must-not-leak" not in diff
    assert REDACTION_TOKEN in diff


def test_redact_public_value_redacts_vault_path() -> None:
    from dpone.security_redaction import redact_public_value

    redacted = redact_public_value(
        {
            "vault_path": "dpone/dev/credentials/mssql",
            "password": "must-not-leak",
            "environment": "development",
        }
    )

    assert redacted["vault_path"] == REDACTION_TOKEN
    assert redacted["password"] == REDACTION_TOKEN
    assert redacted["environment"] == "development"


def test_scaffold_conflict_diff_redacts_yaml_block_and_folded_scalars(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    target = root / "dpone.yaml"
    target.write_text(
        "\n".join(
            (
                "credentials:",
                "  password: |",
                "    block-scalar-secret",
                "  api_token: >",
                "    folded-scalar-secret",
                "endpoint: https://token-only-secret@api.internal/v1",
                "",
            )
        ),
        encoding="utf-8",
    )

    result = ScaffoldApplier(root).plan((ScaffoldFile(Path("dpone.yaml"), "schema: dpone.project.v1\n"),))

    assert result.has_conflict is True
    diff = result.changes[0].diff
    assert "block-scalar-secret" not in diff
    assert "folded-scalar-secret" not in diff
    assert "token-only-secret" not in diff
    assert REDACTION_TOKEN in diff


def test_scaffold_conflict_diff_omits_unparseable_structured_content(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    target = root / "dpone.yaml"
    target.write_text(
        "password: [unterminated-secret\nvisible: context\n",
        encoding="utf-8",
    )

    result = ScaffoldApplier(root).plan((ScaffoldFile(Path("dpone.yaml"), "schema: dpone.project.v1\n"),))

    assert result.has_conflict is True
    diff = result.changes[0].diff
    assert "unterminated-secret" not in diff
    assert "unparseable structured content omitted by dpone" in diff
    assert "sha256:" in diff
