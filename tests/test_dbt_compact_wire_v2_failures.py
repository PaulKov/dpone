"""Native-path I/O fault injection; real validators remain enabled throughout."""

import base64
import io
import json
import shutil
import tarfile
from contextlib import contextmanager
from pathlib import Path

import pytest
from dpone_airflow_pack.pack_identity import compute_pack_fingerprint

from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.readiness.airflow_compact_pack_release import materialize_compact_pack_release
from dpone.services.dbt_release_integrity import DbtReleaseIntegrityService
from tests.dbt_compact_wire_v2_helpers import SIDECAR
from tests.test_dbt_compact_wire_v2 import compiled_workspace as compiled_workspace
from tests.test_dbt_compact_wire_v2 import reseal_fixture


def materialize(root, cache):
    return materialize_compact_pack_release(pack_root=root, cache_root=cache, xcom_sidecar_image=SIDECAR)


@pytest.mark.parametrize("phase", ["write", "integrity", "cleanup"])
def test_native_private_stage_failure_preserves_cache(compiled_workspace, tmp_path, monkeypatch, phase):
    import dpone.manifest.dbt_compact_release as builder

    cache = tmp_path / "cache"
    cache.mkdir()
    retained = cache / "retained"
    retained.write_bytes(b"unchanged")
    reached = []
    if phase == "write":
        write = Path.write_bytes

        def fail(path, data):
            if any(part.startswith("dpone-compact-workspace-") for part in path.parts):
                reached.append(True)
                raise OSError("SENSITIVE_SENTINEL")
            return write(path, data)

        monkeypatch.setattr(Path, "write_bytes", fail)
    elif phase == "integrity":
        write = DbtReleaseIntegrityService.write

        def fail(self, root):
            if root.name.startswith("dpone-compact-workspace-"):
                reached.append(True)
                raise OSError("SENSITIVE_SENTINEL")
            return write(self, root)

        monkeypatch.setattr(DbtReleaseIntegrityService, "write", fail)
    else:
        temporary = builder.TemporaryDirectory

        @contextmanager
        def fail(*args, **kwargs):
            with temporary(*args, **kwargs) as stage:
                yield stage
            reached.append(True)
            raise OSError("SENSITIVE_SENTINEL")

        monkeypatch.setattr(builder, "TemporaryDirectory", fail)
    report = materialize(compiled_workspace, cache)
    assert reached and not report.passed and "SENSITIVE_SENTINEL" not in str(report)
    assert list(cache.iterdir()) == [retained] and retained.read_bytes() == b"unchanged"


@pytest.mark.parametrize("phase", ["before_rename", "after_rename"])
def test_native_publication_failure_and_identical_retry(compiled_workspace, tmp_path, monkeypatch, phase):
    import dpone.runtime.immutable_local_tree as tree

    cache = tmp_path / "cache"
    rename, fsync = tree._rename_no_replace_at, tree.os.fsync
    renamed = False

    def fail_rename(*args, **kwargs):
        nonlocal renamed
        if phase == "before_rename":
            raise OSError("SENSITIVE_SENTINEL")
        rename(*args, **kwargs)
        renamed = True

    def fail_fsync(descriptor):
        if renamed and tree.os.fstat(descriptor).st_ino == (cache / "releases").stat().st_ino:
            raise OSError("SENSITIVE_SENTINEL")
        return fsync(descriptor)

    with monkeypatch.context() as faults:
        faults.setattr(tree, "_rename_no_replace_at", fail_rename)
        faults.setattr(tree.os, "fsync", fail_fsync)
        report = materialize(compiled_workspace, cache)
        assert not report.passed and "SENSITIVE_SENTINEL" not in str(report)
        visible = list((cache / "releases").glob("sha256-*"))
        assert bool(visible) == (phase == "after_rename")
        assert not list((cache / "releases").glob(".*.tmp.*"))
        if phase == "after_rename":
            assert "DURABILITY_UNCERTAIN" in report.blockers[0]
            # A retry must not claim success while the durability fault persists.
            retry_with_fault = materialize(compiled_workspace, cache)
            assert not retry_with_fault.passed and "DURABILITY_UNCERTAIN" in retry_with_fault.blockers[0]
    retry = materialize(compiled_workspace, cache)
    assert retry.passed and materialize(compiled_workspace, cache) == retry


@pytest.mark.parametrize("when", ["during", "after"])
def test_native_capture_freezes_or_rejects_mutation(compiled_workspace, tmp_path, monkeypatch, when):
    import dpone.app.dbt_promotion_composition as composition
    from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader

    root = tmp_path / "compiled"
    shutil.copytree(compiled_workspace, root)
    release = json.loads((root / "release-set.json").read_bytes())
    victim = root / release["artifacts"]["runtime_payloads"][0]["path"]
    original = victim.read_bytes()
    reached = []
    if when == "during":
        read = composition.read_confined_file

        def changing_read(base, path, **kwargs):
            body = read(base, path, **kwargs)
            if Path(base) == root and path == victim.relative_to(root).as_posix() and not reached:
                victim.write_bytes(b"changed")
                reached.append(True)
            return body

        monkeypatch.setattr(composition, "read_confined_file", changing_read)
    else:
        capture = DbtReleaseSourceReader.capture_verified_files

        def changing_capture(self, base, **kwargs):
            files = capture(self, base, **kwargs)
            if Path(base) == root:
                victim.write_bytes(b"changed")
                reached.append(True)
            return files

        monkeypatch.setattr(DbtReleaseSourceReader, "capture_verified_files", changing_capture)
    cache = tmp_path / "cache"
    report = materialize(root, cache)
    assert reached
    if when == "during":
        assert not report.passed and not cache.exists()
    else:
        assert report.passed
        assert (Path(report.release_dir) / victim.relative_to(root)).read_bytes() == original


@pytest.mark.parametrize("payload", [b"{", b"[]", b'{"schema":"x","schema":"y"}', None])
def test_descriptor_presence_never_falls_back_to_legacy(tmp_path, payload):
    root = tmp_path / "compiled"
    (root / "_dags").mkdir(parents=True)
    descriptor = root / "release-set.json"
    if payload is None:
        descriptor.symlink_to(root / "absent")
    else:
        descriptor.write_bytes(payload)
    report = materialize(root, tmp_path / "cache")
    assert not report.passed and "WORKSPACE_INVALID" in report.blockers[0]
    assert not (tmp_path / "cache").exists()


@pytest.mark.parametrize("kind", ["traversal", "symlink"])
def test_resealed_hazardous_transfer_archive_fails_native_capture(compiled_workspace, tmp_path, kind):
    root = tmp_path / "compiled"
    shutil.copytree(compiled_workspace, root)
    release = json.loads((root / "release-set.json").read_bytes())
    descriptor = next(row for row in release["artifacts"]["workload_packs"] if not row["id"].startswith("dbt__"))
    pack_path = root / descriptor["path"]
    pack = json.loads(pack_path.read_bytes())
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as archive:
        member = tarfile.TarInfo("../escaped" if kind == "traversal" else pack["workload"]["manifest"])
        if kind == "symlink":
            member.type = tarfile.SYMTYPE
            member.linkname = "../escaped"
        archive.addfile(member)
    body = stream.getvalue()
    pack["runtime_payload"]["archive"].update(
        data=base64.b64encode(body).decode(), bytes=len(body), sha256=sha256_bytes(body)
    )
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    payload = json.dumps(pack).encode()
    pack_path.write_bytes(payload)
    descriptor.update(bytes=len(payload), sha256=sha256_bytes(payload), pack_fingerprint=pack["pack_fingerprint"])
    reseal_fixture(root, release)
    DbtReleaseIntegrityService().verify(root)
    report = materialize(root, tmp_path / "cache")
    assert not report.passed and not (tmp_path / "cache").exists() and not (tmp_path / "escaped").exists()
