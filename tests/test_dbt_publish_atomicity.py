from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.adapters.dbt_workflow_selection import ManifestPreviewSelectionResolver
from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.app.dbt_publish_composition import build_dbt_dpone_compiler
from dpone.contracts.dbt_publishing import DbtPublishingError
from dpone.readiness.dbt_publish_atomic_publisher import (
    DbtArtifactPublicationError,
    DbtArtifactTreePublisher,
)
from dpone.readiness.dbt_sqlserver_project_policy import (
    DbtSqlserverProjectPolicyValidator,
)
from dpone.runtime import immutable_local_tree
from dpone.services.dbt_publish_artifact_writer import DbtArtifactWriter

ROOT = Path(__file__).parents[1]
DEMO = ROOT / "examples" / "dbt-inline-publishing"
MANIFEST = DEMO / "fixtures" / "manifest.v12.json"
PROFILES = DEMO / "dpone" / "dbt-publish-profiles.yml"


def _compile(manifest_path: Path = MANIFEST):
    report = build_dbt_dpone_compiler(root=DEMO).build(
        manifest_path,
        profiles_path=PROFILES,
    )
    assert report.passed
    return report


def _writer(**kwargs: object) -> DbtArtifactWriter:
    return DbtArtifactWriter(
        selection_resolver=ManifestPreviewSelectionResolver(),
        bundle_operations=RuntimeDbtProjectBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(project_root=DEMO),
        **kwargs,
    )


def _tree_bytes(root: Path) -> dict[str, bytes]:
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}


def _tree_identity(root: Path) -> dict[str, tuple[int, int]]:
    return {
        path.relative_to(root).as_posix(): (path.stat().st_ino, path.stat().st_mtime_ns)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_compile_identity_and_artifact_bytes_are_path_independent(tmp_path: Path) -> None:
    payload = MANIFEST.read_bytes()
    first_manifest = tmp_path / "source-one" / "manifest.json"
    second_manifest = tmp_path / "source-two" / "manifest.json"
    first_manifest.parent.mkdir()
    second_manifest.parent.mkdir()
    first_manifest.write_bytes(payload)
    second_manifest.write_bytes(payload)

    first_report = _compile(first_manifest)
    second_report = _compile(second_manifest)
    first_output = tmp_path / "output-one"
    second_output = tmp_path / "output-two"
    first_written = _writer().write(first_report, first_output, project_root=DEMO)
    second_written = _writer().write(second_report, second_output, project_root=DEMO)

    assert first_report.to_jsonable()["compile_fingerprint"] == second_report.to_jsonable()["compile_fingerprint"]
    assert first_written.artifacts == second_written.artifacts
    assert all(not Path(path).is_absolute() for path in first_written.artifacts.values())
    assert _tree_bytes(first_output) == _tree_bytes(second_output)


def test_identical_compile_is_an_idempotent_no_op(tmp_path: Path) -> None:
    report = _compile()
    output = tmp_path / "airflow"
    writer = _writer()
    first = writer.write(report, output, project_root=DEMO)
    before = _tree_identity(output)

    second = writer.write(report, output, project_root=DEMO)

    assert first.passed
    assert second.passed
    assert second.artifacts == first.artifacts
    assert _tree_identity(output) == before


def test_existing_different_output_is_a_cas_conflict(tmp_path: Path) -> None:
    report = _compile()
    output = tmp_path / "airflow"
    writer = _writer()
    written = writer.write(report, output, project_root=DEMO)
    conflict_path = output / written.artifacts["evidence"]
    conflict_path.write_text("owned by another writer\n", encoding="utf-8")
    before = _tree_bytes(output)

    conflicted = writer.write(report, output, project_root=DEMO)

    assert not conflicted.passed
    assert {issue.code for issue in conflicted.blockers} == {"DPONE_DBT_PUBLISH_OUTPUT_CONFLICT"}
    assert conflicted.artifacts == {}
    assert _tree_bytes(output) == before
    assert not tuple(output.parent.glob(f".{output.name}.tmp.*"))


def test_existing_empty_output_is_a_cas_conflict(tmp_path: Path) -> None:
    report = _compile()
    output = tmp_path / "airflow"
    output.mkdir()

    conflicted = _writer().write(report, output, project_root=DEMO)

    assert not conflicted.passed
    assert {issue.code for issue in conflicted.blockers} == {"DPONE_DBT_PUBLISH_OUTPUT_CONFLICT"}
    assert tuple(output.iterdir()) == ()
    assert not tuple(output.parent.glob(f".{output.name}.tmp.*"))


def test_assembly_failure_leaves_no_destination(tmp_path: Path) -> None:
    class FailingPackBuilder:
        def build(self, **_kwargs):
            raise RuntimeError("private assembly detail")

    output = tmp_path / "airflow"

    result = _writer(pack_builder=FailingPackBuilder()).write(_compile(), output, project_root=DEMO)

    assert not result.passed
    assert {issue.code for issue in result.blockers} == {"DPONE_DBT_COMPILE_FAILED"}
    assert result.artifacts == {}
    assert not output.exists()
    assert "private assembly detail" not in json.dumps(result.to_jsonable())


def test_authoritative_graph_failure_preserves_structured_remediation(
    tmp_path: Path,
) -> None:
    class UnsupportedGraphResolver:
        def resolve(self, **_kwargs: object) -> None:
            raise DbtPublishingError(
                "DPONE_DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED",
                "SQL Server v1 does not admit 'model.analytics.foreign'",
                path="models/foreign.sql",
                remediation=(
                    "Configure config.as_columnstore to equal false explicitly, then compile a new immutable selection."
                ),
            )

    output = tmp_path / "airflow"
    result = DbtArtifactWriter(
        selection_resolver=UnsupportedGraphResolver(),  # type: ignore[arg-type]
        bundle_operations=RuntimeDbtProjectBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(project_root=DEMO),
    ).write(_compile(), output, project_root=DEMO)

    assert not result.passed
    assert result.artifacts == {}
    assert not output.exists()
    assert result.blockers[0].code == ("DPONE_DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED")
    assert result.blockers[0].path == "models/foreign.sql"
    assert result.blockers[0].remediation is not None
    assert "as_columnstore" in result.blockers[0].remediation


def test_post_rename_fsync_failure_is_not_misreported_as_cas_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "airflow"
    renamed = False
    real_rename = immutable_local_tree._rename_no_replace_at
    real_fsync = immutable_local_tree.os.fsync

    def track_rename(*args, **kwargs):
        nonlocal renamed
        real_rename(*args, **kwargs)
        renamed = True

    def fail_parent_fsync(descriptor: int) -> None:
        if renamed:
            raise OSError("simulated parent durability failure")
        real_fsync(descriptor)

    monkeypatch.setattr(immutable_local_tree, "_rename_no_replace_at", track_rename)
    monkeypatch.setattr(immutable_local_tree.os, "fsync", fail_parent_fsync)

    with pytest.raises(DbtArtifactPublicationError, match="durable publication"):
        DbtArtifactTreePublisher().publish(output, {"release-set.json": b"{}"})

    assert output.exists()
    assert not tuple(output.parent.glob(f".{output.name}.tmp.*"))
