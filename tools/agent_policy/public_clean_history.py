"""Read-only, frozen Git history and complete parent-relative acquisition.

Discovery bindings are private evidence, never commit authorization. Raw object
metadata is scanned without imposing the candidate gate's single-parent format.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

from tools.agent_policy.public_clean_archives import inspect_content
from tools.agent_policy.public_clean_candidate import Git
from tools.agent_policy.public_clean_policy import Scanner
from tools.agent_policy.public_clean_receipts import GateError, digest

_OID = re.compile(rb"[0-9a-f]{40}\Z")
_LIMIT = 100_000
_REGULAR = {b"100644", b"100755"}


def _oid(raw: bytes) -> str:
    if not _OID.fullmatch(raw):
        raise GateError("OBJECT_ID_INVALID")
    return raw.decode("ascii")


def _path(raw: bytes) -> str:
    try:
        value = raw.decode("utf-8", "strict")
    except UnicodeError as exc:
        raise GateError("PATH_INVALID") from exc
    if not value or value.startswith("/") or any(part in {"", ".", ".."} for part in value.split("/")):
        raise GateError("PATH_INVALID")
    return value


def _refs(git: Git) -> dict[str, Any]:
    raw = git.run("for-each-ref", "--format=%(refname)%09%(objectname)")
    refs = []
    for line in raw.splitlines():
        fields = line.split(b"\t")
        if len(fields) != 2:
            raise GateError("REF_INVALID")
        refs.append({"name": _path(fields[0]), "target": _oid(fields[1])})
        if len(refs) > _LIMIT:
            raise GateError("REF_LIMIT")
    head = _oid(git.run("rev-parse", "--verify", "HEAD").strip())
    if len({item["name"] for item in refs}) != len(refs):
        raise GateError("REF_INVALID")
    return {"refs": sorted(refs, key=lambda item: item["name"]), "head": head}


def _commit(raw: bytes) -> tuple[str, list[str]]:
    header, separator, _ = raw.partition(b"\n\n")
    if not separator:
        raise GateError("COMMIT_INVALID")
    trees, parents = [], []
    for line in header.splitlines():
        if line.startswith(b"tree "):
            trees.append(_oid(line[5:]))
        elif line.startswith(b"parent "):
            parents.append(_oid(line[7:]))
    if len(trees) != 1 or len(set(parents)) != len(parents):
        raise GateError("COMMIT_INVALID")
    return trees[0], parents


class _Objects:
    """Scan each path occurrence and each immutable blob payload once."""

    def __init__(self, git: Git, scanner: Scanner) -> None:
        self.git, self.scanner = git, scanner
        self.blobs: dict[str, bytes] = {}
        self.trees: dict[str, str] = {}

    def blob(self, oid: str, path: str) -> None:
        if oid not in self.blobs:
            self.blobs[oid] = self.git.blob(oid)
        inspect_content(path, self.blobs[oid], self.scanner)

    def tree(self, oid: str) -> None:
        if oid in self.trees:
            return
        raw = self.git.run("ls-tree", "-r", "-t", "-z", "--full-tree", oid)
        if raw and not raw.endswith(b"\0"):
            raise GateError("TREE_INVALID")
        seen: set[str] = set()
        for record in raw.split(b"\0")[:-1]:
            self.scanner.budget.tick()
            metadata, separator, name = record.partition(b"\t")
            fields = metadata.split(b" ")
            if (
                not separator
                or len(fields) != 3
                or (fields[0], fields[1]) not in {(b"100644", b"blob"), (b"100755", b"blob"), (b"040000", b"tree")}
            ):
                raise GateError("TREE_ENTRY_UNSUPPORTED")
            path = _path(name)
            if path in seen:
                raise GateError("TREE_INVALID")
            seen.add(path)
            self.scanner.scan(f"path:{path}", name)
            child_oid = _oid(fields[2])
            if fields[1] == b"blob":
                self.blob(child_oid, path)
        self.trees[oid] = hashlib.sha256(raw).hexdigest()


def scan_history(git: Git, scanner: Scanner) -> dict[str, Any]:
    """Scan all frozen refs, detached HEAD, raw tags, commit graph and trees."""
    frozen = _refs(git)
    for item in frozen["refs"]:
        scanner.scan("ref", item["name"].encode("utf-8"))
    pending = [frozen["head"], *(item["target"] for item in frozen["refs"])]
    visited: set[str] = set()
    graph: dict[str, Any] = {}
    tags: dict[str, Any] = {}
    objects = _Objects(git, scanner)
    while pending:
        scanner.budget.tick()
        oid = pending.pop()
        if oid in visited:
            continue
        visited.add(oid)
        kind = git.run("cat-file", "-t", oid).strip()
        if kind == b"commit":
            if len(graph) >= _LIMIT:
                raise GateError("COMMIT_LIMIT")
            raw = git.run("cat-file", "commit", oid)
            scanner.scan(f"commit:{oid}", raw)
            tree, parents = _commit(raw)
            graph[oid] = {"tree": tree, "parents": parents, "raw_digest": hashlib.sha256(raw).hexdigest()}
            objects.tree(tree)
            pending.extend(parents)
        elif kind == b"tag":
            if len(tags) >= _LIMIT:
                raise GateError("TAG_LIMIT")
            raw = git.run("cat-file", "tag", oid)
            scanner.scan(f"tag:{oid}", raw)
            header, separator, _ = raw.partition(b"\n\n")
            targets = [line[7:] for line in header.splitlines() if line.startswith(b"object ")]
            types = [line[5:] for line in header.splitlines() if line.startswith(b"type ")]
            if not separator or len(targets) != 1 or len(types) != 1:
                raise GateError("TAG_INVALID")
            target = _oid(targets[0])
            if git.run("cat-file", "-t", target).strip() != types[0]:
                raise GateError("TAG_INVALID")
            tags[oid] = {"target": target, "raw_digest": hashlib.sha256(raw).hexdigest()}
            pending.append(target)
        elif kind == b"tree":
            objects.tree(oid)
        elif kind == b"blob":
            objects.blob(oid, f"blob:{oid}")
        else:
            raise GateError("OBJECT_UNSUPPORTED")
    if frozen != _refs(git):
        raise GateError("HISTORY_CHANGED")
    return {
        **frozen,
        "graph_digest": digest(graph),
        "tags_digest": digest(tags),
        "trees_digest": digest(objects.trees),
        "commits": len(graph),
        "blobs": len(objects.blobs),
    }


def _delta(git: Git, scanner: Scanner, objects: _Objects, commit: str, parent: str | None) -> dict[str, Any]:
    comparison = (parent, commit) if parent else ("--root", commit)
    options = ("diff-tree", "--no-commit-id", "-r", "--no-renames", "--no-ext-diff", "--no-textconv")
    raw = git.run(*options, "--raw", "-z", *comparison)
    records = raw.split(b"\0")
    if records[-1] or (len(records) - 1) % 2:
        raise GateError("DELTA_INVALID")
    entries = []
    for offset in range(0, len(records) - 1, 2):
        scanner.budget.tick()
        fields = records[offset].split(b" ")
        if len(fields) != 5 or not fields[0].startswith(b":") or fields[4] not in {b"A", b"M", b"D", b"T"}:
            raise GateError("DELTA_INVALID")
        old_mode, new_mode = fields[0][1:], fields[1]
        if old_mode not in _REGULAR | {b"000000"} or new_mode not in _REGULAR | {b"000000"}:
            raise GateError("DELTA_ENTRY_UNSUPPORTED")
        old, new = _oid(fields[2]), _oid(fields[3])
        path = _path(records[offset + 1])
        scanner.scan(f"path:{path}", records[offset + 1])
        for mode, oid in ((old_mode, old), (new_mode, new)):
            if (mode == b"000000") != (oid == "0" * 40):
                raise GateError("DELTA_INVALID")
            if mode != b"000000":
                objects.blob(oid, path)
        entries.append(
            {
                "path": path,
                "old_mode": old_mode.decode(),
                "new_mode": new_mode.decode(),
                "old_blob": old,
                "new_blob": new,
            }
        )
    patch = git.run(*options, "-p", "--binary", "--full-index", *comparison)
    scanner.scan(f"patch:{parent or 'root'}", patch)
    if raw != git.run(*options, "--raw", "-z", *comparison):
        raise GateError("SOURCE_CHANGED")
    return {"parent": parent, "patch_digest": hashlib.sha256(patch).hexdigest(), "entries": entries}


def scan_delta(git: Git, scanner: Scanner, commit_sha: str) -> dict[str, Any]:
    """Inspect every complete source-parent delta without writing Git objects."""
    commit = _oid(commit_sha.encode("ascii", "strict"))
    frozen = _refs(git)
    raw = git.run("cat-file", "commit", commit)
    scanner.scan(f"commit:{commit}", raw)
    _, parents = _commit(raw)
    objects = _Objects(git, scanner)
    parent_raw = {parent: git.run("cat-file", "commit", parent) for parent in parents}
    for parent, content in parent_raw.items():
        scanner.scan(f"parent:{parent}", content)
    targets: list[str | None] = [*parents] if parents else [None]
    deltas = [_delta(git, scanner, objects, commit, parent) for parent in targets]
    if frozen != _refs(git) or raw != git.run("cat-file", "commit", commit):
        raise GateError("SOURCE_CHANGED")
    if any(content != git.run("cat-file", "commit", parent) for parent, content in parent_raw.items()):
        raise GateError("SOURCE_CHANGED")
    return {"commit": commit, "parents": parents, "metadata_digest": hashlib.sha256(raw).hexdigest(), "deltas": deltas}
