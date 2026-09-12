"""Private, exact-input receipts and explicit review for public-clean scans.

These local records bind maintainer decisions to bytes. They do not authenticate
a hostile local operator and must never be published or treated as route evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import stat
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from tools.agent_policy.public_clean_limits import MAX_PRIVATE_JSON_BYTES, MAX_REVIEW_EXCEPTIONS


class GateError(Exception):
    """A stable diagnostic without input values or underlying exception text."""

    def __init__(self, code: str) -> None:
        super().__init__("Public-clean input cannot be certified.")
        self.code = code


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def strict_keys(payload: Any, required: set[str], optional: set[str] | None = None) -> None:
    if (
        not isinstance(payload, dict)
        or not required <= payload.keys()
        or payload.keys() - required - (optional or set())
    ):
        raise GateError("STRUCTURE_INVALID")


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return value.st_dev, value.st_ino, value.st_mode, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def read_file(path: Path, limit: int) -> bytes:
    """Bound a regular-file read and reject a path or inode replacement."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
            raise GateError("FILE_UNSUPPORTED")
        data = stream.read(limit + 1)
        if len(data) > limit or _identity(before) != _identity(os.fstat(stream.fileno())):
            raise GateError("FILE_CHANGED")
        if _identity(before) != _identity(path.stat(follow_symlinks=False)):
            raise GateError("FILE_CHANGED")
        return data


@contextmanager
def private_directory(path: Path, root: Path):
    """Pin every traversed directory without following substituted ancestors."""
    path = path.absolute()
    if ".." in path.parts or path.is_relative_to(root.resolve()):
        raise GateError("PRIVATE_PATH_UNSAFE")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    descriptor = os.open(path.anchor, flags)
    try:
        for component in path.parent.parts[1:]:
            try:
                os.stat(".git", dir_fd=descriptor, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise GateError("PRIVATE_PATH_UNSAFE")
            child = os.open(component, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        try:
            os.stat(".git", dir_fd=descriptor, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise GateError("PRIVATE_PATH_UNSAFE")
        observed = os.fstat(descriptor)
        if observed.st_mode & 0o077 or observed.st_uid != os.getuid():
            raise GateError("PRIVATE_PATH_UNSAFE")
        yield descriptor, path.name
    finally:
        try:
            os.close(descriptor)
        except OSError:
            pass  # Closing a private directory is cleanup, never publication.


def private_parent(path: Path, root: Path) -> Path:
    with private_directory(path, root):
        return path.absolute().parent


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise GateError("JSON_DUPLICATE_KEY")
        result[key] = value
    return result


def _nonfinite(_value: str) -> None:
    raise GateError("JSON_NONFINITE")


def read_json(path: Path, root: Path, limit: int = MAX_PRIVATE_JSON_BYTES) -> dict[str, Any]:
    with private_directory(path, root) as (directory, name):
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(descriptor, "rb") as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_mode & 0o077 or before.st_size > limit:
                raise GateError("PRIVATE_INPUT_UNSAFE")
            raw = stream.read(limit + 1)
            if len(raw) > limit or _identity(before) != _identity(os.fstat(stream.fileno())):
                raise GateError("PRIVATE_INPUT_CHANGED")
            if _identity(before) != _identity(os.stat(name, dir_fd=directory, follow_symlinks=False)):
                raise GateError("PRIVATE_INPUT_CHANGED")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_nonfinite)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise GateError("JSON_INVALID") from exc
    if not isinstance(value, dict):
        raise GateError("STRUCTURE_INVALID")
    return value


def write_receipt(path: Path, root: Path, payload: dict[str, Any]) -> None:
    """Publish exclusively at atomic link; later cleanup cannot undo success.

    File data and temporary-directory entry are synced before publication. A
    crash after the atomic link may leave a valid receipt without a CLI response;
    postpublication directory sync/temporary cleanup are best-effort recovery.
    """
    raw = canonical(payload) + b"\n"
    if len(raw) > MAX_PRIVATE_JSON_BYTES:
        raise GateError("RECEIPT_LIMIT")
    with private_directory(path, root) as (directory, name):
        temporary = ".public-clean-" + secrets.token_hex(16)
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        published = False
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.fsync(directory)
            with private_directory(path, root) as (current, _):
                if _identity(os.fstat(current))[:3] != _identity(os.fstat(directory))[:3]:
                    raise GateError("PRIVATE_PATH_CHANGED")
            try:
                os.link(temporary, name, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
            except FileExistsError as exc:
                raise GateError("RECEIPT_EXISTS") from exc
            published = True  # Atomic publication is the visible commit point.
        finally:
            try:
                os.unlink(temporary, dir_fd=directory)
                os.fsync(directory)
            except OSError:
                if not published:
                    raise


def scanner_identity() -> str:
    paths = sorted(Path(__file__).parent.glob("public_clean_*.py"))
    paths.append(Path(__file__).with_name("tenant_hygiene.py"))
    return digest({path.name: hashlib.sha256(read_file(path, 1024**2)).hexdigest() for path in paths})


def review_binding(binding: dict[str, Any]) -> dict[str, Any]:
    return {key: binding[key] for key in ("tree", "parent", "metadata_digest")}


def apply_review(
    review: dict[str, Any] | None,
    binding: dict[str, Any],
    policy: Any,
    scanner: str,
    findings: list[dict[str, Any]],
    syntax_validator: Any = None,
) -> list[dict[str, Any]]:
    """Clear only explicit exact-occurrence decisions by a permitted reviewer."""
    if review is None:
        return findings
    strict_keys(
        review, {"schema", "binding", "policy_digest", "scanner_identity", "reviewer", "approved", "exceptions"}
    )
    if (
        review["schema"] != "dpone.public-clean-review.v1"
        or review["binding"] != review_binding(binding)
        or review["policy_digest"] != policy.digest
        or review["scanner_identity"] != scanner
        or review["reviewer"] not in policy.reviewers
        or review["approved"] is not True
    ):
        raise GateError("REVIEW_MISMATCH")
    exceptions = review["exceptions"]
    if not isinstance(exceptions, list) or len(exceptions) > MAX_REVIEW_EXCEPTIONS:
        raise GateError("REVIEW_INVALID")
    coordinates = ("label", "content_digest", "code", "offset")
    occurrences: dict[tuple[Any, ...], int | None] = {}
    for index, finding in enumerate(findings):
        key = tuple(finding[field] for field in coordinates)
        occurrences[key] = None if key in occurrences else index
    cleared: set[int] = set()
    for entry in exceptions:
        strict_keys(
            entry,
            {"label", "content_digest", "code", "offset", "justification", "synthetic"},
            {"classification", "context_evidence"},
        )
        classified = "classification" in entry or "context_evidence" in entry
        if classified and (
            entry.get("classification") != "NON_CREDENTIAL_SYNTAX"
            or not isinstance(entry.get("context_evidence"), dict)
            or entry.get("code") != "CREDENTIAL_SHAPE"
            or entry.get("synthetic") is not False
            or syntax_validator is None
        ):
            raise GateError("REVIEW_EXCEPTION_INVALID")
        if (
            any(not isinstance(entry[field], str) for field in coordinates[:3])
            or not isinstance(entry["justification"], str)
            or not entry["justification"].strip()
            or type(entry["offset"]) is not int
            or not isinstance(entry["synthetic"], bool)
            or entry["code"] in {"PROTECTED_TERM", "PROTECTED_TICKET", "COMPILED_ARTIFACT"}
            or (entry["code"] == "CREDENTIAL_SHAPE" and entry["synthetic"] is not True and not classified)
        ):
            raise GateError("REVIEW_EXCEPTION_INVALID")
        key = tuple(entry[field] for field in coordinates)
        match = occurrences.get(key)
        if match is None or match in cleared:
            raise GateError("REVIEW_EXCEPTION_MISMATCH")
        if classified:
            syntax_validator(entry)
        cleared.add(match)
    return [finding for index, finding in enumerate(findings) if index not in cleared]
