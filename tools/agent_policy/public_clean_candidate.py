"""Bounded Git acquisition and strict metadata for an exact local candidate."""

from __future__ import annotations

import hashlib
import os
import re
import selectors
import signal
import subprocess
import time
import unicodedata
from pathlib import Path
from typing import Any

from tools.agent_policy.public_clean_git_batch import GitBatch, git_invocation
from tools.agent_policy.public_clean_policy import Budget, Scanner
from tools.agent_policy.public_clean_receipts import GateError, strict_keys
from tools.agent_policy.tenant_hygiene import MAX_SOURCE_BLOB_BYTES, MAX_SOURCE_BLOBS, MAX_TREE_LISTING_BYTES

SHA = re.compile(r"[0-9a-f]{40}\Z")
DATE = re.compile(r"(?:0|[1-9][0-9]{0,11}) [+-](?:0[0-9]|1[0-4])[0-5][0-9]\Z")


class Git:
    """Inject root and budget; never use ambient Git routing or output errors."""

    def __init__(self, root: Path, budget: Budget, timeout: float = 30) -> None:
        if not 0 < timeout <= 30:
            raise GateError("GIT_TIMEOUT_INVALID")
        self.root = root.resolve(strict=True)
        self.budget = budget
        self.timeout = timeout

    def run(self, *args: str, limit: int = MAX_TREE_LISTING_BYTES) -> bytes:
        self.budget.tick()
        command, environment = git_invocation(*args)
        deadline = time.monotonic() + self.timeout
        process = subprocess.Popen(
            command,
            cwd=self.root,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        result = bytearray()
        try:
            assert process.stdout is not None
            os.set_blocking(process.stdout.fileno(), False)
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while selector.get_map():
                    self.budget.tick()
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise GateError("GIT_TIMEOUT")
                    for key, _ in selector.select(min(remaining, 0.1)):
                        chunk = os.read(key.fd, min(65536, limit + 1 - len(result)))
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        result.extend(chunk)
                        if len(result) > limit:
                            raise GateError("GIT_OUTPUT_LIMIT")
            if process.wait(timeout=max(0.001, deadline - time.monotonic())):
                raise GateError("GIT_UNAVAILABLE")
            return bytes(result)
        except subprocess.TimeoutExpired as exc:
            raise GateError("GIT_TIMEOUT") from exc
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=1)
            if process.stdout is not None:
                process.stdout.close()

    def blob(self, oid: str) -> bytes:
        if not SHA.fullmatch(oid):
            raise GateError("GIT_ID_INVALID")
        size = int(self.run("cat-file", "-s", oid, limit=32))
        if size > MAX_SOURCE_BLOB_BYTES:
            raise GateError("BLOB_LIMIT")
        raw = self.run("cat-file", "blob", oid, limit=size)
        if len(raw) != size:
            raise GateError("BLOB_CHANGED")
        return raw


def parse_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    strict_keys(
        payload,
        {
            "message",
            "author_name",
            "author_email",
            "author_date",
            "committer_name",
            "committer_email",
            "committer_date",
            "parent",
        },
    )
    if not all(isinstance(value, str) and value and "\0" not in value for value in payload.values()):
        raise GateError("METADATA_INVALID")
    if not SHA.fullmatch(payload["parent"]):
        raise GateError("METADATA_INVALID")
    for role in ("author", "committer"):
        name, email, date = (payload[f"{role}_{field}"] for field in ("name", "email", "date"))
        if name != name.strip() or any(unicodedata.category(c).startswith("C") or c in "<>" for c in name):
            raise GateError("METADATA_INVALID")
        if not re.fullmatch(r"[^\s<>@]+@[^\s<>@]+\.[^\s<>@]+", email) or not DATE.fullmatch(date):
            raise GateError("METADATA_INVALID")
        if abs(int(date.split()[1])) > 1400:
            raise GateError("METADATA_INVALID")
    return payload


def parse_commit(raw: bytes) -> dict[str, Any]:
    try:
        headers, message = raw.decode("utf-8").split("\n\n", 1)
        lines = headers.split("\n")
        if len(lines) != 4 or [line.split(" ", 1)[0] for line in lines] != ["tree", "parent", "author", "committer"]:
            raise ValueError
        tree, parent = (line.split(" ", 1)[1] for line in lines[:2])
        if not SHA.fullmatch(tree) or not SHA.fullmatch(parent):
            raise ValueError
        metadata: dict[str, Any] = {"message": message, "parent": parent}
        for role, line in zip(("author", "committer"), lines[2:], strict=True):
            match = re.fullmatch(rf"{role} (.+) <([^<>]+)> (.+)", line)
            if match is None:
                raise ValueError
            metadata.update(
                {f"{role}_{key}": value for key, value in zip(("name", "email", "date"), match.groups(), strict=True)}
            )
        return {"tree": tree, "parent": parent, "metadata": parse_metadata(metadata)}
    except (ValueError, UnicodeError) as exc:
        raise GateError("COMMIT_METADATA_UNSUPPORTED") from exc


def scan_commit_metadata(scanner: Scanner, metadata: dict[str, Any]) -> None:
    for key, value in sorted(metadata.items()):
        scanner.scan(f"metadata:{key}", value.encode())
        if key.endswith("_email") and value not in scanner.policy.allowed_identities:
            scanner.add("IDENTITY_UNAPPROVED", f"metadata:{key}", 0, hashlib.sha256(value.encode()).hexdigest())


def scan_tree(git: Git, scanner: Scanner, tree: str) -> None:
    from tools.agent_policy.public_clean_archives import inspect_content

    listing = git.run("ls-tree", "-r", "-z", "--full-tree", tree)
    if not listing or not listing.endswith(b"\0"):
        raise GateError("TREE_EMPTY_OR_INVALID")
    contents: dict[str, bytes] = {}
    if listing.count(b"\0") > MAX_SOURCE_BLOBS:
        raise GateError("TREE_ENTRY_LIMIT")
    with GitBatch(git.root, git.budget, git.timeout) as batch:
        for record in listing[:-1].split(b"\0"):
            header, path = record.split(b"\t", 1)
            mode, kind, oid = header.decode("ascii").split()
            if mode not in {"100644", "100755"} or kind != "blob":
                raise GateError("TREE_ENTRY_UNSUPPORTED")
            label = path.decode("utf-8")
            scanner.scan(f"path:{label}", path)
            if oid not in contents:
                contents[oid] = batch.blob(oid)
            inspect_content(label, contents[oid], scanner)


def index_identity(git: Git) -> str:
    return hashlib.sha256(git.run("ls-files", "--stage", "-z")).hexdigest()


def scan_candidate(git: Git, scanner: Scanner) -> dict[str, Any]:
    initial = git.run("ls-files", "--stage", "-z")
    if not initial:
        raise GateError("INDEX_EMPTY")
    for record in initial.rstrip(b"\0").split(b"\0"):
        header, _ = record.split(b"\t", 1)
        mode, _, stage = header.split()
        if stage != b"0" or mode not in {b"100644", b"100755"}:
            raise GateError("INDEX_UNSUPPORTED")
    parent = git.run("rev-parse", "--verify", "HEAD", limit=64).decode().strip()
    tree = git.run("write-tree", limit=64).decode().strip()
    scan_tree(git, scanner, tree)
    if (
        git.run("ls-files", "--stage", "-z") != initial
        or git.run("rev-parse", "HEAD", limit=64).decode().strip() != parent
    ):
        raise GateError("CANDIDATE_CHANGED")
    return {"tree": tree, "parent": parent, "index_digest": hashlib.sha256(initial).hexdigest()}


def review_blob(git: Git, tree: str, label: str, expected_digest: str) -> bytes:
    """Resolve a reviewed regular blob only from the exact scanned Git tree.

    Metadata, archive members and arbitrary filesystem paths have no authority
    here. Literal pathspec matching prevents a review label becoming Git syntax.
    """
    if not isinstance(label, str) or not label or "\0" in label:
        raise GateError("REVIEW_SOURCE_INVALID")
    listing = git.run("ls-tree", "-r", "-z", "--full-tree", tree, "--", f":(literal){label}")
    records = listing.split(b"\0")
    if len(records) != 2 or records[-1]:
        raise GateError("REVIEW_SOURCE_INVALID")
    header, actual_label = records[0].split(b"\t", 1)
    mode, kind, oid = header.decode("ascii").split()
    if actual_label.decode("utf-8") != label or mode not in {"100644", "100755"} or kind != "blob":
        raise GateError("REVIEW_SOURCE_INVALID")
    raw = git.blob(oid)
    if hashlib.sha256(raw).hexdigest() != expected_digest:
        raise GateError("REVIEW_SOURCE_CHANGED")
    return raw
