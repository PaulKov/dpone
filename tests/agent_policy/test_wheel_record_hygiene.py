"""Exercise archive scanning, not a synthetic digest allowlist."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import zipfile
from pathlib import Path

import pytest
from tests.agent_policy.test_tenant_hygiene import _policy, hygiene

RECORD = "example-1.0.dist-info/RECORD"


def _rows(files: dict[str, bytes]) -> list[list[str]]:
    return [
        [name, "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(body).digest()).decode().rstrip("="), str(len(body))]
        for name, body in files.items()
    ] + [[RECORD, "", ""]]


def _archive(tmp_path: Path, files: dict[str, bytes], rows: list[list[str]]) -> Path:
    output = io.StringIO(newline="")
    csv.writer(output).writerows(rows)
    path = tmp_path / "example-1.0-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as archive:
        for name, body in {**files, RECORD: output.getvalue().encode()}.items():
            archive.writestr(name, body)
    return path


def test_verified_record_digest_is_not_tenant_text(tmp_path: Path) -> None:
    files = {"example/data.txt": b"hello\n"}
    rows = _rows(files)
    # Synthetic policy deliberately matches a real checksum, never the member.
    policy = _policy(tmp_path, rows[0][1][7:19])
    report = hygiene.evaluate_archives(paths=[_archive(tmp_path, files, rows)], policy_path=policy)
    assert report.status == "PASS"


@pytest.mark.parametrize("location", ["body", "path"])
def test_record_does_not_hide_protected_paths_or_bodies(tmp_path: Path, location: str) -> None:
    files = {"example/data.txt": b"protected-tenant-value"}
    if location == "path":
        files = {"example/protected-tenant-value.txt": b"safe"}
    report = hygiene.evaluate_archives(paths=[_archive(tmp_path, files, _rows(files))], policy_path=_policy(tmp_path))
    assert report.status == "FAIL"
    assert "protected-tenant-value" not in hygiene.render_report(report)


@pytest.mark.parametrize("damage", ["hash", "size", "missing", "extra", "duplicate", "columns", "algorithm", "self"])
def test_unproven_record_fails_closed(tmp_path: Path, damage: str) -> None:
    files = {"example/data.txt": b"hello\n"}
    rows = _rows(files)
    if damage == "hash":
        rows[0][1] = "sha256=" + "a" * 43
    elif damage == "size":
        rows[0][2] = "0"
    elif damage == "missing":
        rows.pop(0)
    elif damage == "extra":
        rows[0][0] = "example/absent.txt"
    elif damage == "duplicate":
        rows.append(rows[0])
    elif damage == "columns":
        rows[0].append("protected-tenant-value")
    elif damage == "algorithm":
        rows[0][1] = rows[0][1].replace("sha256", "md5")
    else:
        rows[-1][1] = rows[0][1]
    report = hygiene.evaluate_archives(paths=[_archive(tmp_path, files, rows)], policy_path=_policy(tmp_path))
    assert report.status == "UNABLE_TO_CERTIFY"
    assert "protected-tenant-value" not in hygiene.render_report(report)


def test_quoted_csv_paths_are_scanned_after_decoding(tmp_path: Path) -> None:
    files = {'example/comma,"value.txt': b"hello\n"}
    report = hygiene.evaluate_archives(
        paths=[_archive(tmp_path, files, _rows(files))], policy_path=_policy(tmp_path, 'comma,"value')
    )
    assert report.status == "FAIL"


def test_non_metadata_record_is_never_exempted(tmp_path: Path) -> None:
    files = {"example/RECORD": b"protected-tenant-value"}
    report = hygiene.evaluate_archives(paths=[_archive(tmp_path, files, _rows(files))], policy_path=_policy(tmp_path))
    assert report.status == "FAIL"
