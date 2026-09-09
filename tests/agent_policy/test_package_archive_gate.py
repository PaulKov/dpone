from __future__ import annotations

import importlib.util
import io
import json
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tools/agent_policy/package_archive_gate.py"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("dpone_package_archive_gate_tests", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


archive_gate = _load_module()


def _wheel(path: Path, members: dict[str, bytes]) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
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


@pytest.mark.parametrize("kind", ["wheel", "sdist"])
def test_normal_package_members_pass_without_reading_or_extracting_payload(
    tmp_path: Path,
    monkeypatch: Any,
    kind: str,
) -> None:
    secret = b"payload-must-never-appear-in-report"
    if kind == "wheel":
        path = _wheel(
            tmp_path / "example-1.0-py3-none-any.whl",
            {
                "example/__init__.py": secret,
                "example-1.0.dist-info/METADATA": b"Name: example\n",
            },
        )
        monkeypatch.setattr(zipfile.ZipFile, "read", _unexpected_payload_access)
        monkeypatch.setattr(zipfile.ZipFile, "extract", _unexpected_payload_access)
        monkeypatch.setattr(zipfile.ZipFile, "extractall", _unexpected_payload_access)
    else:
        path = _sdist(
            tmp_path / "example-1.0.tar.gz",
            {
                "example-1.0/example/__init__.py": secret,
                "example-1.0/PKG-INFO": b"Name: example\n",
            },
        )
        monkeypatch.setattr(tarfile.TarFile, "extract", _unexpected_payload_access)
        monkeypatch.setattr(tarfile.TarFile, "extractall", _unexpected_payload_access)
        monkeypatch.setattr(tarfile.TarFile, "extractfile", _unexpected_payload_access)

    report = archive_gate.inspect_archives([path])
    encoded = archive_gate.render_report(report)

    assert report.status == "PASS"
    assert report.archives[0].member_count == 2
    assert report.archives[0].forbidden_members == ()
    assert secret.decode() not in encoded


def _unexpected_payload_access(*_args: object, **_kwargs: object) -> None:
    raise AssertionError("archive payload access is forbidden")


@pytest.mark.parametrize(
    ("member", "reason"),
    [
        ("example-1.0/AGENTS.md", "INSTRUCTION_FILE"),
        ("example-1.0/.gitignore", "VCS_FILE"),
        ("example-1.0/.env", "ENV_FILE"),
        ("example-1.0/config/.env.production", "ENV_FILE"),
        ("example-1.0/.git/config", "INTERNAL_DIRECTORY"),
        ("example-1.0/.codex/config.toml", "INTERNAL_DIRECTORY"),
        ("example-1.0/.agents/policy.yml", "INTERNAL_DIRECTORY"),
        (r"example-1.0\.git\config", "INTERNAL_DIRECTORY"),
    ],
)
@pytest.mark.parametrize("kind", ["wheel", "sdist"])
def test_forbidden_members_fail_closed(
    tmp_path: Path,
    kind: str,
    member: str,
    reason: str,
) -> None:
    if kind == "wheel":
        path = _wheel(tmp_path / "example.whl", {member: b"do not report me"})
    else:
        path = _sdist(tmp_path / "example.tar.gz", {member: b"do not report me"})

    report = archive_gate.inspect_archives([path])

    assert report.status == "FAIL"
    assert report.decision == "NO-GO"
    assert report.archives[0].forbidden_members[0].reason == reason
    assert report.archives[0].forbidden_members[0].member == member.replace("\\", "/")


@pytest.mark.parametrize(
    ("filename", "content", "code"),
    [
        ("broken.whl", b"not a zip archive", "ARCHIVE_MALFORMED"),
        ("broken.tar.gz", b"not a gzip tar archive", "ARCHIVE_MALFORMED"),
        ("package.zip", b"not a supported release archive", "ARCHIVE_FORMAT_UNSUPPORTED"),
    ],
)
def test_malformed_and_unsupported_archives_fail_closed(
    tmp_path: Path,
    filename: str,
    content: bytes,
    code: str,
) -> None:
    path = tmp_path / filename
    path.write_bytes(content)

    report = archive_gate.inspect_archives([path])

    assert report.status == "FAIL"
    assert report.archives[0].blockers[0].code == code


def test_report_is_deterministic_and_bounded(tmp_path: Path) -> None:
    members = {
        f"example/.agents/{index:04d}-{'x' * 400}.yml": b"sensitive payload"
        for index in reversed(range(archive_gate.MAX_REPORTED_FINDINGS + 20))
    }
    path = _wheel(tmp_path / f"{'archive-' * 20}.whl", members)

    first = archive_gate.render_report(archive_gate.inspect_archives([path]))
    second = archive_gate.render_report(archive_gate.inspect_archives([path]))
    payload = json.loads(first)

    assert first == second
    assert len(first.encode("utf-8")) <= archive_gate.MAX_OUTPUT_BYTES
    archive = payload["archives"][0]
    assert len(archive["forbidden_members"]) == archive_gate.MAX_REPORTED_FINDINGS
    assert archive["omitted_forbidden_member_count"] == 20
    assert all(len(item["member"]) <= archive_gate.MAX_MEMBER_NAME_LENGTH for item in archive["forbidden_members"])
    assert "sensitive payload" not in first


def test_too_many_members_fails_closed_without_unbounded_report(tmp_path: Path) -> None:
    path = _wheel(
        tmp_path / "large.whl",
        {f"example/module_{index}.py": b"" for index in range(archive_gate.MAX_ARCHIVE_MEMBERS + 1)},
    )

    report = archive_gate.inspect_archives([path])

    assert report.status == "FAIL"
    assert report.archives[0].blockers[0].code == "ARCHIVE_MEMBER_LIMIT_EXCEEDED"
    assert len(archive_gate.render_report(report).encode("utf-8")) <= archive_gate.MAX_OUTPUT_BYTES


def test_maximum_unicode_metadata_stays_within_output_bound() -> None:
    findings = tuple(
        archive_gate.ForbiddenMember(
            member="😀" * archive_gate.MAX_MEMBER_NAME_LENGTH,
            reason="INTERNAL_DIRECTORY",
        )
        for _ in range(archive_gate.MAX_REPORTED_FINDINGS)
    )
    archive = archive_gate.ArchiveReport(
        archive="😀" * archive_gate.MAX_ARCHIVE_LABEL_LENGTH,
        format="wheel",
        status="FAIL",
        member_count=archive_gate.MAX_ARCHIVE_MEMBERS,
        forbidden_members=findings,
        omitted_forbidden_member_count=1,
        blockers=(),
    )
    report = archive_gate.GateReport(
        status="FAIL",
        archives=(archive,) * archive_gate.MAX_ARCHIVES,
    )

    encoded = archive_gate.render_report(report).encode("utf-8")

    assert len(encoded) <= archive_gate.MAX_OUTPUT_BYTES


def test_cli_emits_json_and_returns_nonzero_when_any_archive_fails(tmp_path: Path) -> None:
    good = _wheel(tmp_path / "good.whl", {"example/__init__.py": b""})
    bad = _sdist(tmp_path / "bad.tar.gz", {"example/.env.local": b"secret"})

    completed = subprocess.run(
        [sys.executable, str(SCRIPT), str(bad), str(good)],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    payload = json.loads(completed.stdout)

    assert completed.returncode == 1
    assert completed.stderr == ""
    assert payload["schema_version"] == 1
    assert payload["status"] == "FAIL"
    assert payload["decision"] == "NO-GO"
    assert [item["archive"] for item in payload["archives"]] == ["bad.tar.gz", "good.whl"]


def test_cli_returns_nonzero_json_for_malformed_archive(tmp_path: Path) -> None:
    malformed = tmp_path / "broken.whl"
    malformed.write_bytes(b"not a wheel")

    completed = subprocess.run(
        [sys.executable, str(SCRIPT), str(malformed)],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    payload = json.loads(completed.stdout)

    assert completed.returncode == 1
    assert completed.stderr == ""
    assert payload["archives"][0]["blockers"][0]["code"] == "ARCHIVE_MALFORMED"


def test_cli_returns_zero_json_for_safe_archive(tmp_path: Path) -> None:
    safe = _sdist(tmp_path / "example.tar.gz", {"example/module.py": b""})

    completed = subprocess.run(
        [sys.executable, str(SCRIPT), str(safe)],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    payload = json.loads(completed.stdout)

    assert completed.returncode == 0
    assert completed.stderr == ""
    assert payload["status"] == "PASS"
    assert payload["decision"] == "GO"
