"""Bounded, non-disclosing tenant hygiene checks for frozen release inputs."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import stat
import struct
import subprocess
import sys
import tarfile
import zipfile
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, BinaryIO, NoReturn, Protocol

SCHEMA, TENANT_CODE = "dpone.tenant-hygiene.v1", "DPONE_HYGIENE_TENANT_DEFAULT"
FULL_SHA = re.compile(r"^[0-9a-fA-F]{40}$")
MAX_SOURCE_BLOBS, MAX_SOURCE_BLOB_BYTES = 50_000, 16 * 1024 * 1024
MAX_SOURCE_AGGREGATE_BYTES, MAX_TREE_LISTING_BYTES = 1024**3, 64 * 1024 * 1024
MAX_ARCHIVES, MAX_ARCHIVE_MEMBERS = 32, 20_000
MAX_ARCHIVE_MEMBER_BYTES, MAX_ARCHIVE_UNCOMPRESSED_BYTES, MAX_ARCHIVE_FILE_BYTES = 32 * 1024**2, 1024**3, 1024**3
MAX_COMPRESSION_RATIO, READ_CHUNK_BYTES = 100, 64 * 1024
MAX_POLICY_BYTES, MAX_PATH_BYTES, MAX_REPORTED_PATH_BYTES = 1024 * 1024, 4096, 512
_NESTED = tuple(".whl .zip .tar .tar.gz .tgz .tar.bz2 .tbz2 .tar.xz .txz .gz .bz2 .xz .7z .rar".split())


@dataclass(frozen=True, order=True, slots=True)
class Finding:
    code: str
    path: str


@dataclass(frozen=True, slots=True)
class HygieneReport:
    mode: str
    status: str
    findings: tuple[Finding, ...]


class _Policy(Protocol):
    overlap: int
    full_member: bool

    def matches(self, value: bytes) -> bool: ...

    def scan(self, stream: BinaryIO, size: int, chunk_size: int) -> tuple[bool, bytes]: ...


class _Unable(Exception):
    def __init__(self, code: str, path: str) -> None:
        super().__init__("Tenant hygiene input could not be certified.")
        self.finding = Finding(code, path)


class _SafeParser(argparse.ArgumentParser):
    def error(self, _message: str) -> NoReturn:
        raise _Unable("DPONE_HYGIENE_ARGUMENT_INVALID", "$INPUT")


def _report(mode: str, findings: Sequence[Finding]) -> HygieneReport:
    ordered = sorted(set(findings))[:200]
    status = "UNABLE_TO_CERTIFY" if any(item.code != TENANT_CODE for item in ordered) else "FAIL" if ordered else "PASS"
    return HygieneReport(mode, status, tuple(ordered))


def render_report(report: HygieneReport) -> str:
    payload = {**asdict(report), "schema": SCHEMA}
    return f"{json.dumps(payload, ensure_ascii=True, separators=(',', ':'), sort_keys=True)}\n"


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_mode, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _open_regular(path: Path, limit: int, code: str, label: str) -> tuple[BinaryIO, tuple[int, ...]]:
    no_follow = getattr(os, "O_NOFOLLOW", None)
    descriptor: int | None = None
    try:
        if no_follow is None:
            raise OSError
        flags = os.O_RDONLY | no_follow | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
        descriptor = os.open(path, flags)
        observed = os.fstat(descriptor)
        if not stat.S_ISREG(observed.st_mode) or observed.st_size > limit:
            raise OSError
        stream = os.fdopen(descriptor, "rb")
        descriptor = None
        return stream, _identity(observed)
    except OSError as exc:
        if descriptor is not None:
            os.close(descriptor)
        raise _Unable(code, label) from exc


def _unchanged(stream: BinaryIO, path: Path, expected: tuple[int, ...]) -> bool:
    try:
        return _identity(os.fstat(stream.fileno())) == expected == _identity(os.stat(path, follow_symlinks=False))
    except OSError:
        return False


def _load_policy(path: Path) -> _Policy:
    stream, identity = _open_regular(path, MAX_POLICY_BYTES, "DPONE_HYGIENE_POLICY_INVALID", "$POLICY")
    with stream:
        payload = stream.read(MAX_POLICY_BYTES + 1)
        if len(payload) > MAX_POLICY_BYTES or not _unchanged(stream, path, identity):
            raise _Unable("DPONE_HYGIENE_POLICY_INVALID", "$POLICY")
    try:
        name = "dpone_tenant_hygiene_policy"
        spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name("tenant_hygiene_policy.py"))
        if spec is None or spec.loader is None:
            raise ValueError
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module.parse_policy(payload)
    except (UnicodeError, ValueError, TypeError, RecursionError) as exc:
        raise _Unable("DPONE_HYGIENE_POLICY_INVALID", "$POLICY") from exc


def _path(raw: bytes, policy: _Policy, *, source: bool = False, directory: bool = False) -> tuple[str | None, str]:
    protected = policy.matches(raw if source else raw.replace(b"\\", b"/"))
    try:
        value = raw.decode()
    except UnicodeError:
        return None, "$PROTECTED_PATH" if protected else "$UNSAFE_PATH"
    if not source:
        value = value.replace("\\", "/")
        value = value[:-1] if directory and value.endswith("/") else value
    controls = any(ord(char) < 32 or ord(char) == 127 for char in value)
    unsafe = (
        not raw
        or len(raw) > MAX_PATH_BYTES
        or b"\0" in raw
        or not value
        or value.startswith("/")
        or (len(value) > 1 and value[0].isalpha() and value[1] == ":")
        or (source and "\\" in value)
        or any(part in {"", ".", ".."} for part in value.split("/"))
        or controls
    )
    display = value
    if controls:
        display = "$UNSAFE_PATH"
    if len(raw) > MAX_REPORTED_PATH_BYTES:
        display = "$OVERSIZED_PATH"
    if protected:
        display = "$PROTECTED_PATH"
    return (None if unsafe else value), display


def _run_git(root: Path, arguments: tuple[str, ...], limit: int) -> tuple[int, bytes, bool]:
    try:
        process = subprocess.Popen(
            ("git", *arguments),
            cwd=root,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except OSError:
        return 127, b"", False
    assert process.stdout is not None
    output = process.stdout.read(limit + 1)
    overflow = len(output) > limit
    if overflow:
        process.kill()
    try:
        returncode = process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        return 127, b"", False
    return returncode, output, overflow


def evaluate_source(*, root: Path, commit_sha: str, policy_path: Path) -> HygieneReport:
    try:
        policy = _load_policy(policy_path)
        if FULL_SHA.fullmatch(commit_sha) is None:
            raise _Unable("DPONE_HYGIENE_SOURCE_SHA_INVALID", "$SOURCE")
        commit_sha = commit_sha.lower()
        if _run_git(root, ("cat-file", "-t", commit_sha), 32) != (0, b"commit\n", False):
            raise _Unable("DPONE_HYGIENE_SOURCE_COMMIT_UNAVAILABLE", "$SOURCE")
        result, listing, overflow = _run_git(
            root, ("ls-tree", "-r", "-z", "-l", "--full-tree", commit_sha), MAX_TREE_LISTING_BYTES
        )
        if result or overflow or (listing and not listing.endswith(b"\0")):
            raise _Unable("DPONE_HYGIENE_SOURCE_TREE_UNAVAILABLE", "$SOURCE")
        findings: list[Finding] = []
        blobs: list[tuple[bytes, str, int]] = []
        aggregate = blob_count = 0
        for record in listing[:-1].split(b"\0") if listing else ():
            try:
                metadata, raw_path = record.split(b"\t", 1)
                _, kind, raw_object, raw_size = metadata.split()
                object_id, size = raw_object.decode("ascii"), int(raw_size)
            except (UnicodeError, ValueError) as exc:
                raise _Unable("DPONE_HYGIENE_SOURCE_TREE_UNAVAILABLE", "$SOURCE") from exc
            path, display = _path(raw_path, policy, source=True)
            if path is None:
                label = "$PROTECTED_PATH" if display == "$PROTECTED_PATH" else "$UNSAFE_PATH"
                raise _Unable("DPONE_HYGIENE_SOURCE_PATH_UNSAFE", label)
            if kind != b"blob" or FULL_SHA.fullmatch(object_id) is None:
                raise _Unable("DPONE_HYGIENE_SOURCE_ENTRY_UNSUPPORTED", display)
            blob_count += 1
            if blob_count > MAX_SOURCE_BLOBS:
                raise _Unable("DPONE_HYGIENE_SOURCE_BLOB_COUNT_LIMIT", "$SOURCE")
            if size < 0 or size > MAX_SOURCE_BLOB_BYTES:
                raise _Unable("DPONE_HYGIENE_SOURCE_BLOB_SIZE_LIMIT", display)
            aggregate += size
            if aggregate > MAX_SOURCE_AGGREGATE_BYTES:
                raise _Unable("DPONE_HYGIENE_SOURCE_AGGREGATE_LIMIT", "$SOURCE")
            blobs.append((raw_path, object_id, size))
            if policy.matches(raw_path):
                findings.append(Finding(TENANT_CODE, display))
        for raw_path, object_id, size in sorted(blobs):
            result, content, overflow = _run_git(root, ("cat-file", "blob", object_id), size)
            if result or overflow or len(content) != size:
                raise _Unable("DPONE_HYGIENE_SOURCE_BLOB_UNREADABLE", _path(raw_path, policy, source=True)[1])
            if policy.matches(content):
                findings.append(Finding(TENANT_CODE, _path(raw_path, policy, source=True)[1]))
        return _report("source", findings)
    except _Unable as exc:
        return _report("source", [exc.finding])


def _stream_matches(stream: BinaryIO, size: int, policy: _Policy) -> tuple[bool, bool]:
    matched, prefix = policy.scan(stream, size, READ_CHUNK_BYTES)
    magic = prefix.startswith((b"PK\x03\x04", b"\x1f\x8b", b"BZh", b"\xfd7zXZ\x00", b"7z\xbc\xaf'\x1c"))
    return matched, magic or prefix[257:262] == b"ustar"


def _scan_members(archive: Any, policy: _Policy, archive_size: int, *, wheel: bool) -> list[Finding]:
    findings: list[Finding] = []
    aggregate = 0
    seen: set[str] = set()
    members = archive.infolist() if wheel else archive
    if wheel and len(members) > MAX_ARCHIVE_MEMBERS:
        return [Finding("DPONE_HYGIENE_ARCHIVE_MEMBER_COUNT_LIMIT", "$ARCHIVE")]
    for count, member in enumerate(members, start=1):
        if count > MAX_ARCHIVE_MEMBERS:
            return [*findings, Finding("DPONE_HYGIENE_ARCHIVE_MEMBER_COUNT_LIMIT", "$ARCHIVE")]
        name = member.filename if wheel else member.name
        raw = name.encode("utf-8", "surrogateescape")
        directory = member.is_dir() if wheel else member.isdir()
        path, display = _path(raw, policy, directory=directory)
        if policy.matches(raw):
            findings.append(Finding(TENANT_CODE, "$PROTECTED_PATH"))
        if path is None or path in seen:
            return [*findings, Finding("DPONE_HYGIENE_ARCHIVE_MEMBER_PATH_UNSAFE", "$UNSAFE_PATH")]
        seen.add(path)
        size = member.file_size if wheel else member.size
        if wheel:
            file_type = stat.S_IFMT(member.external_attr >> 16)
            unsafe_type = file_type not in {0, stat.S_IFREG, stat.S_IFDIR} or bool(member.flag_bits & 1)
            unsupported_compression = member.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}
            compressed, opener = member.compress_size, lambda: archive.open(member, mode="r")
        else:
            unsafe_type, unsupported_compression = not member.isfile() and not directory, False
            compressed, opener = archive_size, lambda: archive.extractfile(member)
        if unsafe_type or (wheel and file_type and directory != (file_type == stat.S_IFDIR)):
            return [*findings, Finding("DPONE_HYGIENE_ARCHIVE_MEMBER_TYPE_UNSAFE", display)]
        if not directory and path.lower().endswith(_NESTED):
            return [*findings, Finding("DPONE_HYGIENE_ARCHIVE_NESTED_ARCHIVE", display)]
        if unsupported_compression:
            return [*findings, Finding("DPONE_HYGIENE_ARCHIVE_COMPRESSION_UNSUPPORTED", display)]
        if size < 0 or size > MAX_ARCHIVE_MEMBER_BYTES:
            return [*findings, Finding("DPONE_HYGIENE_ARCHIVE_MEMBER_SIZE_LIMIT", display)]
        if aggregate + size > MAX_ARCHIVE_UNCOMPRESSED_BYTES:
            return [*findings, Finding("DPONE_HYGIENE_ARCHIVE_AGGREGATE_LIMIT", "$ARCHIVE")]
        projected = size if wheel else aggregate + size
        if projected and (compressed <= 0 or projected > compressed * MAX_COMPRESSION_RATIO):
            return [*findings, Finding("DPONE_HYGIENE_ARCHIVE_COMPRESSION_LIMIT", display)]
        aggregate += size
        if directory:
            continue
        opened = opener()
        with opened:
            matched, nested = _stream_matches(opened, size, policy)
            if nested:
                return [*findings, Finding("DPONE_HYGIENE_ARCHIVE_NESTED_ARCHIVE", display)]
            if matched:
                findings.append(Finding(TENANT_CODE, display))
    return findings


def _check_wheel_directory(stream: BinaryIO, archive_size: int) -> None:
    tail_size = min(archive_size, 65_557)
    stream.seek(archive_size - tail_size)
    tail = stream.read(tail_size)
    offset = tail.rfind(b"PK\x05\x06")
    if offset < 0 or len(tail) - offset < 22:
        raise ValueError
    _, disk, start_disk, disk_entries, entries, size, _, comment = struct.unpack_from("<4s4H2IH", tail, offset)
    if disk or start_disk or disk_entries != entries or offset + 22 + comment != len(tail):
        raise ValueError
    if entries > MAX_ARCHIVE_MEMBERS:
        raise _Unable("DPONE_HYGIENE_ARCHIVE_MEMBER_COUNT_LIMIT", "$ARCHIVE")
    if size > 64 * 1024**2:
        raise ValueError
    stream.seek(0)


def _inspect_archive(path: Path, archive_format: str, policy: _Policy) -> list[Finding]:
    raw_label = path.name.encode("utf-8", "surrogateescape")
    label = _path(raw_label, policy)[1]
    stream, identity = _open_regular(path, MAX_ARCHIVE_FILE_BYTES, "DPONE_HYGIENE_ARCHIVE_INPUT_UNSAFE", label)
    with stream:
        try:
            if archive_format == "wheel":
                _check_wheel_directory(stream, identity[3])
                with zipfile.ZipFile(stream, mode="r") as archive:
                    findings = _scan_members(archive, policy, identity[3], wheel=True)
            else:
                with tarfile.open(fileobj=stream, mode="r:gz") as archive:
                    findings = _scan_members(archive, policy, identity[3], wheel=False)
        except _Unable:
            raise
        except Exception:
            findings = [Finding("DPONE_HYGIENE_ARCHIVE_MALFORMED", label)]
        if not _unchanged(stream, path, identity):
            return [Finding("DPONE_HYGIENE_ARCHIVE_REPLACED", label)]
    return findings


def evaluate_archives(*, paths: Sequence[Path], policy_path: Path) -> HygieneReport:
    try:
        policy = _load_policy(policy_path)
    except _Unable as exc:
        return _report("archive", [exc.finding])
    ordered = sorted(map(Path, paths), key=os.fspath)
    if not ordered or len(ordered) > MAX_ARCHIVES:
        return _report("archive", [Finding("DPONE_HYGIENE_ARCHIVE_COUNT_LIMIT", "$ARCHIVE")])
    findings: list[Finding] = []
    for path in ordered:
        name = path.name.lower()
        archive_format = "wheel" if name.endswith(".whl") else "sdist" if name.endswith(".tar.gz") else None
        if archive_format is None:
            raw_name = path.name.encode("utf-8", "surrogateescape")
            findings.append(Finding("DPONE_HYGIENE_ARCHIVE_FORMAT_UNSUPPORTED", _path(raw_name, policy)[1]))
            continue
        try:
            findings.extend(_inspect_archive(path, archive_format, policy))
        except _Unable as exc:
            findings.append(exc.finding)
    return _report("archive", findings)


def _parser() -> argparse.ArgumentParser:
    parser = _SafeParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    source = modes.add_parser("source")
    source.add_argument("--root", type=Path, default=Path("."))
    source.add_argument("--commit-sha", required=True)
    source.add_argument("--policy", type=Path, required=True)
    archive = modes.add_parser("archive")
    archive.add_argument("--policy", type=Path, required=True)
    archive.add_argument("archives", nargs="+", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    mode = raw[0] if raw and raw[0] in {"source", "archive"} else "source"
    try:
        args = _parser().parse_args(raw)
        report = (
            evaluate_source(root=args.root, commit_sha=args.commit_sha, policy_path=args.policy)
            if args.mode == "source"
            else evaluate_archives(paths=args.archives, policy_path=args.policy)
        )
    except _Unable as exc:
        report = _report(mode, [exc.finding])
    except Exception:
        report = _report(mode, [Finding("DPONE_HYGIENE_INTERNAL_UNAVAILABLE", "$INPUT")])
    sys.stdout.write(render_report(report))
    return {"PASS": 0, "FAIL": 2}.get(report.status, 3)


if __name__ == "__main__":
    raise SystemExit(main())
