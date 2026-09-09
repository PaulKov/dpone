from __future__ import annotations

import importlib.util
import io
import json
import os
import stat
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tools/agent_policy/tenant_hygiene.py"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("dpone_tenant_hygiene_tests", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


hygiene = _load_module()


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ("git", *args),
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return completed.stdout.strip()


def _commit(tmp_path: Path, files: dict[str, bytes]) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "hygiene@example.invalid")
    _git(repo, "config", "user.name", "Hygiene Test")
    for relative, content in files.items():
        target = repo / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "fixture")
    return repo, _git(repo, "rev-parse", "HEAD")


def _policy(tmp_path: Path, pattern: str = "protected-tenant-value") -> Path:
    path = tmp_path / "deny-policy.txt"
    path.write_text(f"{pattern}\n", encoding="utf-8")
    return path


def _wheel(path: Path, members: dict[str, bytes], *, compressed: bool = False) -> Path:
    compression = zipfile.ZIP_DEFLATED if compressed else zipfile.ZIP_STORED
    with zipfile.ZipFile(path, "w", compression=compression) as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return path


def _sdist(path: Path, members: dict[str, bytes]) -> Path:
    with tarfile.open(path, "w:gz") as archive:
        for name, content in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))
    return path


def _codes(report: Any) -> list[str]:
    return [finding.code for finding in report.findings]


def test_source_reads_only_the_frozen_full_sha(tmp_path: Path) -> None:
    secret = "protected-tenant-value"
    repo, clean_sha = _commit(tmp_path, {"pkg/module.py": b"clean\n"})
    policy = _policy(tmp_path, secret)
    (repo / "pkg/module.py").write_text(secret, encoding="utf-8")

    clean = hygiene.evaluate_source(root=repo, commit_sha=clean_sha, policy_path=policy)
    assert clean.status == "PASS"

    _git(repo, "add", "pkg/module.py")
    _git(repo, "commit", "-qm", "protected fixture")
    protected_sha = _git(repo, "rev-parse", "HEAD")
    (repo / "pkg/module.py").write_text("clean worktree", encoding="utf-8")
    protected = hygiene.evaluate_source(root=repo, commit_sha=protected_sha, policy_path=policy)

    assert protected.status == "FAIL"
    assert protected.findings == (hygiene.Finding(code="DPONE_HYGIENE_TENANT_DEFAULT", path="pkg/module.py"),)
    assert secret not in hygiene.render_report(protected)


@pytest.mark.parametrize("commit_sha", ["HEAD", "deadbeef", "g" * 40, "a" * 64])
def test_source_rejects_every_non_full_sha_without_echoing_policy(
    tmp_path: Path,
    commit_sha: str,
) -> None:
    repo, _ = _commit(tmp_path, {"module.py": b"clean"})
    secret = "protected-tenant-value"

    report = hygiene.evaluate_source(root=repo, commit_sha=commit_sha, policy_path=_policy(tmp_path, secret))

    assert report.status == "UNABLE_TO_CERTIFY"
    assert _codes(report) == ["DPONE_HYGIENE_SOURCE_SHA_INVALID"]
    assert secret not in hygiene.render_report(report)


@pytest.mark.parametrize(
    ("limit_name", "limit", "code"),
    [
        ("MAX_SOURCE_BLOBS", 1, "DPONE_HYGIENE_SOURCE_BLOB_COUNT_LIMIT"),
        ("MAX_SOURCE_BLOB_BYTES", 3, "DPONE_HYGIENE_SOURCE_BLOB_SIZE_LIMIT"),
        ("MAX_SOURCE_AGGREGATE_BYTES", 7, "DPONE_HYGIENE_SOURCE_AGGREGATE_LIMIT"),
    ],
)
def test_source_limits_fail_closed(
    tmp_path: Path,
    monkeypatch: Any,
    limit_name: str,
    limit: int,
    code: str,
) -> None:
    repo, commit_sha = _commit(tmp_path, {"a.py": b"aaaa", "b.py": b"bbbb"})
    monkeypatch.setattr(hygiene, limit_name, limit)

    report = hygiene.evaluate_source(root=repo, commit_sha=commit_sha, policy_path=_policy(tmp_path))

    assert report.status == "UNABLE_TO_CERTIFY"
    assert code in _codes(report)


def test_source_rejects_unsafe_paths_and_redacts_protected_paths(tmp_path: Path) -> None:
    repo, commit_sha = _commit(
        tmp_path,
        {
            "bad\\path.py": b"clean",
            "protected-tenant-value.py": b"clean",
        },
    )

    report = hygiene.evaluate_source(root=repo, commit_sha=commit_sha, policy_path=_policy(tmp_path))
    rendered = hygiene.render_report(report)

    assert report.status == "UNABLE_TO_CERTIFY"
    assert "DPONE_HYGIENE_SOURCE_PATH_UNSAFE" in _codes(report)
    assert "protected-tenant-value" not in rendered


@pytest.mark.parametrize("kind", ["wheel", "sdist"])
def test_archive_scans_bounded_member_bodies_without_extracting(
    tmp_path: Path,
    monkeypatch: Any,
    kind: str,
) -> None:
    secret = "protected-tenant-value"
    policy = _policy(tmp_path, secret)
    monkeypatch.setattr(hygiene, "READ_CHUNK_BYTES", 4)
    if kind == "wheel":
        archive = _wheel(tmp_path / "example.whl", {"pkg/module.py": secret.encode()})
        monkeypatch.setattr(zipfile.ZipFile, "extract", _unexpected_extraction)
        monkeypatch.setattr(zipfile.ZipFile, "extractall", _unexpected_extraction)
    else:
        archive = _sdist(tmp_path / "example.tar.gz", {"pkg/module.py": secret.encode()})
        monkeypatch.setattr(tarfile.TarFile, "extract", _unexpected_extraction)
        monkeypatch.setattr(tarfile.TarFile, "extractall", _unexpected_extraction)

    report = hygiene.evaluate_archives(paths=[archive], policy_path=policy)
    rendered = hygiene.render_report(report)

    assert report.status == "FAIL"
    assert report.findings == (hygiene.Finding(code="DPONE_HYGIENE_TENANT_DEFAULT", path="pkg/module.py"),)
    assert secret not in rendered
    assert set(json.loads(rendered)) == {"findings", "mode", "schema", "status"}


def _unexpected_extraction(*_args: object, **_kwargs: object) -> None:
    raise AssertionError("filesystem extraction is forbidden")


@pytest.mark.parametrize(
    ("filename", "member", "code"),
    [
        ("unsafe.whl", "../escape.py", "DPONE_HYGIENE_ARCHIVE_MEMBER_PATH_UNSAFE"),
        ("unsafe.tar.gz", "/absolute.py", "DPONE_HYGIENE_ARCHIVE_MEMBER_PATH_UNSAFE"),
        ("nested.whl", "pkg/payload.tar.gz", "DPONE_HYGIENE_ARCHIVE_NESTED_ARCHIVE"),
        ("nested.tar.gz", "pkg/payload.zip", "DPONE_HYGIENE_ARCHIVE_NESTED_ARCHIVE"),
    ],
)
def test_archive_rejects_unsafe_and_nested_members(
    tmp_path: Path,
    filename: str,
    member: str,
    code: str,
) -> None:
    archive = (
        _wheel(tmp_path / filename, {member: b"content"})
        if filename.endswith(".whl")
        else _sdist(tmp_path / filename, {member: b"content"})
    )

    report = hygiene.evaluate_archives(paths=[archive], policy_path=_policy(tmp_path))

    assert report.status == "UNABLE_TO_CERTIFY"
    assert code in _codes(report)


@pytest.mark.parametrize(
    ("limit_name", "limit", "code"),
    [
        ("MAX_ARCHIVE_MEMBERS", 1, "DPONE_HYGIENE_ARCHIVE_MEMBER_COUNT_LIMIT"),
        ("MAX_ARCHIVE_MEMBER_BYTES", 3, "DPONE_HYGIENE_ARCHIVE_MEMBER_SIZE_LIMIT"),
        ("MAX_ARCHIVE_UNCOMPRESSED_BYTES", 7, "DPONE_HYGIENE_ARCHIVE_AGGREGATE_LIMIT"),
    ],
)
def test_archive_member_limits_fail_closed(
    tmp_path: Path,
    monkeypatch: Any,
    limit_name: str,
    limit: int,
    code: str,
) -> None:
    archive = _wheel(tmp_path / "bounded.whl", {"a.py": b"aaaa", "b.py": b"bbbb"})
    monkeypatch.setattr(hygiene, limit_name, limit)

    report = hygiene.evaluate_archives(paths=[archive], policy_path=_policy(tmp_path))

    assert report.status == "UNABLE_TO_CERTIFY"
    assert code in _codes(report)


@pytest.mark.parametrize("kind", ["wheel", "sdist"])
def test_archive_rejects_excessive_compression(tmp_path: Path, monkeypatch: Any, kind: str) -> None:
    archive = (
        _wheel(tmp_path / "bomb.whl", {"payload.bin": b"x" * 4096}, compressed=True)
        if kind == "wheel"
        else _sdist(tmp_path / "bomb.tar.gz", {"payload.bin": b"x" * 4096})
    )
    monkeypatch.setattr(hygiene, "MAX_COMPRESSION_RATIO", 2)

    report = hygiene.evaluate_archives(paths=[archive], policy_path=_policy(tmp_path))

    assert report.status == "UNABLE_TO_CERTIFY"
    assert "DPONE_HYGIENE_ARCHIVE_COMPRESSION_LIMIT" in _codes(report)


def test_archive_rejects_nested_magic_and_redacts_protected_member_names(tmp_path: Path) -> None:
    nested = _wheel(tmp_path / "nested.whl", {"pkg/payload.bin": b"PK\x03\x04hidden"})
    protected = _wheel(tmp_path / "protected.whl", {"pkg/protected-tenant-value.py": b"clean"})

    report = hygiene.evaluate_archives(paths=[nested, protected], policy_path=_policy(tmp_path))
    rendered = hygiene.render_report(report)

    assert report.status == "UNABLE_TO_CERTIFY"
    assert "DPONE_HYGIENE_ARCHIVE_NESTED_ARCHIVE" in _codes(report)
    assert "protected-tenant-value" not in rendered
    assert "$PROTECTED_PATH" in rendered


def test_archive_rejects_wheel_links_and_tar_devices(tmp_path: Path) -> None:
    wheel = tmp_path / "link.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        info = zipfile.ZipInfo("pkg/link")
        info.create_system = 3
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(info, "target")
    sdist = tmp_path / "device.tar.gz"
    with tarfile.open(sdist, "w:gz") as archive:
        info = tarfile.TarInfo("pkg/device")
        info.type = tarfile.CHRTYPE
        archive.addfile(info)

    report = hygiene.evaluate_archives(paths=[wheel, sdist], policy_path=_policy(tmp_path))

    assert report.status == "UNABLE_TO_CERTIFY"
    assert _codes(report).count("DPONE_HYGIENE_ARCHIVE_MEMBER_TYPE_UNSAFE") == 2


@pytest.mark.skipif(not hasattr(os, "O_NOFOLLOW"), reason="no-follow archive contract requires POSIX O_NOFOLLOW")
def test_archive_input_must_be_a_no_follow_regular_file(tmp_path: Path) -> None:
    target = _wheel(tmp_path / "target.whl", {"module.py": b"clean"})
    link = tmp_path / "link.whl"
    link.symlink_to(target)
    fifo = tmp_path / "pipe.whl"
    os.mkfifo(fifo)

    report = hygiene.evaluate_archives(paths=[link, fifo], policy_path=_policy(tmp_path))

    assert report.status == "UNABLE_TO_CERTIFY"
    assert _codes(report).count("DPONE_HYGIENE_ARCHIVE_INPUT_UNSAFE") == 2


def test_archive_replacement_during_scan_fails_closed(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    archive = _wheel(tmp_path / "candidate.whl", {"module.py": b"clean"})
    replacement = _wheel(tmp_path / "replacement.whl", {"module.py": b"changed"})
    original_scan = hygiene._scan_members

    def replacing_scan(archive_handle: Any, policy: Any, archive_size: int, *, wheel: bool) -> list[Any]:
        findings = original_scan(archive_handle, policy, archive_size, wheel=wheel)
        os.replace(replacement, archive)
        return findings

    monkeypatch.setattr(hygiene, "_scan_members", replacing_scan)

    report = hygiene.evaluate_archives(paths=[archive], policy_path=_policy(tmp_path))

    assert report.status == "UNABLE_TO_CERTIFY"
    assert _codes(report) == ["DPONE_HYGIENE_ARCHIVE_REPLACED"]


def test_archive_count_and_unsupported_format_are_unable_to_certify(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    unsupported = tmp_path / "package.zip"
    unsupported.write_bytes(b"not supported")
    malformed = tmp_path / "broken.whl"
    malformed.write_bytes(b"not a wheel")
    monkeypatch.setattr(hygiene, "MAX_ARCHIVES", 1)

    too_many = hygiene.evaluate_archives(
        paths=[unsupported, unsupported],
        policy_path=_policy(tmp_path),
    )
    unsupported_report = hygiene.evaluate_archives(
        paths=[unsupported],
        policy_path=_policy(tmp_path),
    )
    malformed_report = hygiene.evaluate_archives(paths=[malformed], policy_path=_policy(tmp_path))

    assert _codes(too_many) == ["DPONE_HYGIENE_ARCHIVE_COUNT_LIMIT"]
    assert _codes(unsupported_report) == ["DPONE_HYGIENE_ARCHIVE_FORMAT_UNSUPPORTED"]
    assert _codes(malformed_report) == ["DPONE_HYGIENE_ARCHIVE_MALFORMED"]


def test_cli_exit_codes_and_outputs_never_disclose_patterns(tmp_path: Path) -> None:
    secret = "protected-tenant-value"
    policy = _policy(tmp_path, secret)
    clean = _wheel(tmp_path / "clean.whl", {"module.py": b"clean"})
    protected = _wheel(tmp_path / "protected.whl", {"module.py": secret.encode()})
    missing = tmp_path / "missing.whl"

    results = [
        subprocess.run(
            (sys.executable, str(SCRIPT), "archive", "--policy", str(policy), str(path)),
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        for path in (clean, protected, missing)
    ]

    assert [result.returncode for result in results] == [0, 2, 3]
    assert all(result.stderr == "" for result in results)
    assert all(secret not in result.stdout + result.stderr for result in results)
    assert [json.loads(result.stdout)["status"] for result in results] == [
        "PASS",
        "FAIL",
        "UNABLE_TO_CERTIFY",
    ]
