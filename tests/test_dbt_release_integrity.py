from __future__ import annotations

import os
from pathlib import Path

import pytest

from dpone.services import bounded_tree_integrity
from dpone.services.dbt_release_integrity import (
    DBT_RELEASE_SUBJECTS_FILENAME,
    DbtReleaseIntegrityError,
    DbtReleaseIntegrityService,
)


def _release(root: Path) -> Path:
    root.mkdir()
    (root / "release-set.json").write_text('{"schema":"dpone.release-set.v2"}\n')
    (root / "packs").mkdir()
    (root / "packs" / "load.pack.json").write_text('{"kind":"workload"}\n')
    return root


def test_release_integrity_subject_is_deterministic_and_idempotent(
    tmp_path: Path,
) -> None:
    release = _release(tmp_path / "release")
    service = DbtReleaseIntegrityService()

    created = service.write(release)
    first_bytes = (release / DBT_RELEASE_SUBJECTS_FILENAME).read_bytes()
    repeated = service.write(release)
    verified = service.verify(release)

    assert created.no_op is False
    assert repeated.no_op is True
    assert verified.subject_sha256 == created.subject_sha256
    assert verified.file_count == 2
    assert verified.total_bytes > 0
    assert (release / DBT_RELEASE_SUBJECTS_FILENAME).read_bytes() == first_bytes


def test_release_integrity_rejects_tampered_or_added_bytes(tmp_path: Path) -> None:
    release = _release(tmp_path / "release")
    service = DbtReleaseIntegrityService()
    service.write(release)
    (release / "packs" / "load.pack.json").write_text('{"kind":"tampered"}\n')

    with pytest.raises(DbtReleaseIntegrityError):
        service.verify(release)

    service.write(_release(tmp_path / "second"))
    second = tmp_path / "second"
    (second / "unexpected.json").write_text("{}\n")
    with pytest.raises(DbtReleaseIntegrityError):
        service.verify(second)


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="symlinks unavailable")
def test_release_integrity_rejects_symlink_escape(tmp_path: Path) -> None:
    release = _release(tmp_path / "release")
    outside = tmp_path / "outside.json"
    outside.write_text('{"secret":"outside"}\n')
    os.symlink(outside, release / "escaped.json")

    with pytest.raises(DbtReleaseIntegrityError):
        DbtReleaseIntegrityService().write(release)


def test_release_integrity_rejects_noncanonical_subject(tmp_path: Path) -> None:
    release = _release(tmp_path / "release")
    subject = release / DBT_RELEASE_SUBJECTS_FILENAME
    subject.write_text(
        "# dpone.dbt-release-subjects.v1\n" + ("a" * 64) + "  ../release-set.json\n",
        encoding="utf-8",
    )

    with pytest.raises(DbtReleaseIntegrityError):
        DbtReleaseIntegrityService().verify(release)


def test_release_integrity_does_not_report_pass_when_directory_fsync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = _release(tmp_path / "release")
    real_fsync = bounded_tree_integrity.os.fsync
    calls = 0

    def fail_directory_fsync(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("directory fsync unavailable")
        real_fsync(descriptor)

    monkeypatch.setattr(bounded_tree_integrity.os, "fsync", fail_directory_fsync)

    with pytest.raises(
        DbtReleaseIntegrityError,
        match="durability could not be proven",
    ):
        DbtReleaseIntegrityService().write(release)

    assert (release / DBT_RELEASE_SUBJECTS_FILENAME).exists()


@pytest.mark.parametrize("sizes", [[0], [256 * 1024 * 1024] * 8, [1] * 50_000])
def test_capture_budget_accepts_exact_existing_limits_without_allocating_payloads(sizes):
    DbtReleaseIntegrityService.require_capture_budget(iter(sizes))


@pytest.mark.parametrize(
    "sizes", [[True], [-1], [1.5], [256 * 1024 * 1024 + 1], [256 * 1024 * 1024] * 8 + [1], [1] * 50_001]
)
def test_capture_budget_rejects_bad_sizes_and_excess_before_capture(sizes):
    with pytest.raises(DbtReleaseIntegrityError):
        DbtReleaseIntegrityService.require_capture_budget(iter(sizes))
