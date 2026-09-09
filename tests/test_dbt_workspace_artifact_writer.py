"""Single-publication boundary: failure in any project leaves no active tree."""

import json
from pathlib import Path

import pytest

from dpone.adapters.dbt_runtime_profile import TemporaryDbtProfileStore
from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.contracts.dbt_workspace import DbtWorkspaceCheckReport, DbtWorkspaceDiscoveryReport
from dpone.manifest.confined_files import read_confined_file
from dpone.readiness.dbt_publish_atomic_publisher import DbtArtifactOutputConflict, DbtArtifactTreePublisher
from dpone.services.dbt_release_integrity import DbtReleaseIntegrityService
from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader
from dpone.services.dbt_workspace_artifact_writer import DbtWorkspaceArtifactWriter
from tests.test_dbt_workspace_release_assembly import _project


def _case(
    tmp_path: Path,
    *,
    fail_project=None,
    source_reader=None,
    profile_observer=None,
    dbt_profiles_dir=None,
    profile_store=None,
    collision_kind=None,
):
    namespaces = {} if collision_kind is None else {f"{collision_kind}_namespace": "shared"}
    projects = [_project(tmp_path, name, **namespaces) for name in ("alpha", "beta")]
    for name in ("alpha", "beta"):
        (tmp_path / name / "profiles.yml").write_text("dpone_runtime:\n  outputs:\n    runtime: {type: sqlserver}\n")
    check = DbtWorkspaceCheckReport(
        DbtWorkspaceDiscoveryReport(tuple(row.check.project for row in projects)),
        tuple(row.check for row in projects),
    )
    projections = {row.check.project.project_path: row.artifacts for row in projects}
    calls = []

    class Projector:
        def project(self, report, *, project_root, **kwargs):
            calls.append(project_root.name)
            assert kwargs["manifest_payload"] == Path(report.manifest_path).read_bytes()
            if profile_observer is not None:
                profile_observer(project_root, kwargs["profiles_dir"])
            if project_root.name == fail_project:
                raise ValueError("second project failed")
            return projections[project_root.name]

    publications = []

    class Publisher(DbtArtifactTreePublisher):
        def publish(self, output, files):
            publications.append(output)
            return super().publish(output, files)

    writer = DbtWorkspaceArtifactWriter(
        read_file=read_confined_file,
        projector=Projector(),
        source_reader=source_reader
        or DbtReleaseSourceReader(bundle_operations=RuntimeDbtProjectBundleOperations(), read_file=read_confined_file),
        integrity=DbtReleaseIntegrityService(),
        publisher=Publisher(),
        producer_version="0.74.28",
        profile_store=profile_store or TemporaryDbtProfileStore(tmp_path / "private-profiles"),
        dbt_profiles_dir=dbt_profiles_dir,
    )
    return writer, check, calls, publications


def test_complete_verified_tree_is_published_once_and_retry_is_idempotent(tmp_path: Path):
    writer, check, calls, publications = _case(tmp_path)
    output = tmp_path / "release"
    first = writer.write(check, root=tmp_path, output_dir=output)
    assert calls == ["alpha", "beta"] and publications == [output]
    assert "release-subjects.sha256" in first.files
    assert DbtReleaseIntegrityService().verify(output).file_count == len(first.files) - 1
    identity = {p: (output / p).stat().st_ino for p in first.files}
    second = writer.write(check, root=tmp_path, output_dir=output)
    assert second == first
    assert {p: (output / p).stat().st_ino for p in first.files} == identity
    release = json.loads(first.files["release-set.json"])
    assert release["release_id"] == first.release_id
    from dpone.app.dbt_promotion_composition import build_dbt_expected_release_loader

    expected = build_dbt_expected_release_loader()(output, first.release_id)
    assert set(expected.dbt_workflows) == {"alpha", "beta"}
    assert len(expected.required_workloads) == 6


@pytest.mark.parametrize("kind", ["model", "transfer"])
def test_relation_collision_never_calls_publisher_or_replaces_output(tmp_path, kind):
    from dpone.contracts.dbt_contract_validation import DbtPublishingError

    writer, check, calls, publications = _case(tmp_path, collision_kind=kind)
    output = tmp_path / "release"
    output.mkdir()
    marker = output / "owned"
    marker.write_bytes(b"previous release")
    before = marker.stat()
    with pytest.raises(DbtPublishingError) as failure:
        writer.write(check, root=tmp_path, output_dir=output)
    assert failure.value.code == "DPONE_DBT_WORKSPACE_TARGET_COLLISION"
    assert calls == ["alpha", "beta"] and not publications
    assert tuple(output.iterdir()) == (marker,)
    assert marker.read_bytes() == b"previous release"
    assert (marker.stat().st_ino, marker.stat().st_mtime_ns) == (before.st_ino, before.st_mtime_ns)


def test_parse_target_failure_names_owner_without_exposing_profile(tmp_path: Path):
    from dpone.contracts.dbt_contract_validation import DbtPublishingError

    def reject_profile(project_root, profile_dir):
        if project_root.name == "beta":
            raise DbtPublishingError("DPONE_DBT_PROFILE_INVALID", "DUMMY_SECRET")

    writer, check, _, publications = _case(tmp_path, profile_observer=reject_profile)
    with pytest.raises(DbtPublishingError) as caught:
        writer.write(check, root=tmp_path, output_dir=tmp_path / "release")
    assert caught.value.code == "DPONE_DBT_WORKSPACE_PARSE_PROFILE_INVALID"
    assert caught.value.path == "beta" and "DUMMY_SECRET" not in str(caught.value)
    assert publications == [] and not (tmp_path / "release").exists()


def test_second_project_failure_never_calls_publisher(tmp_path: Path):
    writer, check, calls, publications = _case(tmp_path, fail_project="beta")
    output = tmp_path / "release"
    with pytest.raises(ValueError, match="second project"):
        writer.write(check, root=tmp_path, output_dir=output)
    assert calls == ["alpha", "beta"] and publications == [] and not output.exists()


def test_complete_source_verification_failure_never_calls_publisher(tmp_path: Path):
    class FailingReader:
        def read(self, *args, **kwargs):
            raise ValueError("source verification failed")

    writer, check, calls, publications = _case(tmp_path, source_reader=FailingReader())
    output = tmp_path / "release"
    with pytest.raises(ValueError, match="source verification"):
        writer.write(check, root=tmp_path, output_dir=output)
    assert publications == [] and not output.exists()


def test_existing_output_conflict_is_not_replaced(tmp_path: Path):
    writer, check, _, _ = _case(tmp_path)
    output = tmp_path / "release"
    output.mkdir()
    (output / "owned").write_bytes(b"existing")
    with pytest.raises(DbtArtifactOutputConflict):
        writer.write(check, root=tmp_path, output_dir=output)
    assert tuple(p.name for p in output.iterdir()) == ("owned",)
    assert (output / "owned").read_bytes() == b"existing"


def test_manifest_change_after_check_cannot_publish_a_mixed_snapshot(tmp_path: Path):
    writer, check, calls, publications = _case(tmp_path)
    manifest = Path(check.projects[1].report.manifest_path)
    manifest.write_bytes(manifest.read_bytes() + b" ")
    with pytest.raises(ValueError, match="changed after"):
        writer.write(check, root=tmp_path, output_dir=tmp_path / "release")
    assert calls == ["alpha"] and publications == []


def test_missing_check_row_fails_before_any_project_capture(tmp_path: Path):
    from dataclasses import replace

    writer, check, calls, publications = _case(tmp_path)
    check = replace(check, projects=check.projects[:1])
    with pytest.raises(ValueError, match="complete publishing"):
        writer.write(check, root=tmp_path, output_dir=tmp_path / "release")
    assert calls == [] and publications == []


@pytest.mark.parametrize("shared", [False, True])
@pytest.mark.parametrize("fail_project", [None, "beta"])
def test_profile_snapshots_are_private_stable_and_cleaned(tmp_path, shared, fail_project):
    import stat

    profile = b"dpone_runtime:\n  outputs:\n    runtime: {type: sqlserver, password: PRIVATE_MARKER}\n"
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    (shared_dir / "profiles.yml").write_bytes(profile)
    observed = []

    def observe(project, directory):
        snapshot = directory / "profiles.yml"
        assert stat.S_IMODE(directory.stat().st_mode) == 0o700
        assert stat.S_IMODE(snapshot.stat().st_mode) == 0o600
        assert snapshot.read_bytes() == profile
        original = shared_dir / "profiles.yml" if shared else project / "profiles.yml"
        original.write_bytes(b"replaced")
        assert snapshot.read_bytes() == profile
        observed.append(directory)

    writer, check, _, publications = _case(
        tmp_path,
        fail_project=fail_project,
        profile_observer=observe,
        dbt_profiles_dir=shared_dir if shared else None,
    )
    for name in ("alpha", "beta"):
        (tmp_path / name / "profiles.yml").write_bytes(profile)
    if fail_project:
        with pytest.raises(ValueError, match="second project"):
            writer.write(check, root=tmp_path, output_dir=tmp_path / "release")
        assert not publications
    else:
        tree = writer.write(check, root=tmp_path, output_dir=tmp_path / "release")
        assert all(b"PRIVATE_MARKER" not in value for value in tree.files.values())
    assert len(observed) == 2 and (observed[0] == observed[1]) is shared
    assert all(not directory.exists() for directory in observed)


def test_missing_second_profile_fails_before_any_project_capture(tmp_path):
    from dpone.contracts.dbt_contract_validation import DbtPublishingError

    writer, check, calls, publications = _case(tmp_path)
    (tmp_path / "beta" / "profiles.yml").unlink()
    with pytest.raises(DbtPublishingError) as failure:
        writer.write(check, root=tmp_path, output_dir=tmp_path / "release")
    assert failure.value.path == "beta"
    assert not calls and not publications
    assert not tuple((tmp_path / "private-profiles").iterdir())


def test_profile_cleanup_failure_cannot_happen_after_publication(tmp_path):
    from contextlib import contextmanager

    class FailingCleanupStore:
        @contextmanager
        def materialize(self, content):
            with TemporaryDbtProfileStore(tmp_path / "private-profiles").materialize(content) as profile:
                yield profile
            raise OSError("cleanup failed")

    writer, check, calls, publications = _case(tmp_path, profile_store=FailingCleanupStore())
    with pytest.raises(OSError, match="cleanup failed"):
        writer.write(check, root=tmp_path, output_dir=tmp_path / "release")
    assert calls == ["alpha", "beta"] and not publications
    assert not (tmp_path / "release").exists()
