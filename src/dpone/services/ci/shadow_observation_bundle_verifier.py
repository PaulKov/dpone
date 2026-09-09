"""Verify a source-bound static acquisition closure against immutable Git objects."""

from __future__ import annotations

import ast
import base64
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path

import yaml

from dpone.ports.ci_shadow_reconciliation import CiShadowGitObjectProvider
from dpone.services.ci.shadow_observation_bundle import (
    ObservationBundleEntry,
    ObservationBundleError,
    entries_from_manifest,
    observation_bundle_digest,
)
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget


class ObservationBundleVerificationError(ObservationBundleError):
    """The manifest, Git object graph, or checked-out trusted source is not exact."""


def verify_observation_bundle(
    manifest_path: Path,
    *,
    source_root: Path,
    source_commit_sha: str,
    provider: CiShadowGitObjectProvider,
    budget: RequestBudget,
) -> tuple[tuple[ObservationBundleEntry, ...], str, str]:
    """Return a verified ordered closure and digest, or reject before acquisition.

    The local default-branch checkout is useful only after its regular-file
    bytes and mode match the same objects selected from the immutable source
    commit.  No mutable ref, symlink, or unlisted local import is accepted.
    """

    entries = _load_manifest(manifest_path)
    tree = _tree_for_commit(source_commit_sha, provider, budget)
    manifest_sha256 = _verify_manifest_source(manifest_path, source_root, tree, provider, budget)
    _verify_static_import_closure(entries, source_root)
    for entry in entries:
        tree_entry = tree.get(entry.path)
        if tree_entry is None or tree_entry[0] != entry.mode:
            raise ObservationBundleVerificationError("source tree does not contain the declared regular file")
        blob = _git_blob(tree_entry[1], provider, budget)
        actual_digest = "sha256:" + hashlib.sha256(blob).hexdigest()
        if actual_digest != entry.blob_sha256:
            raise ObservationBundleVerificationError("source Git blob digest does not match the declared bundle")
        local_path = _local_file(source_root, entry.path)
        if local_path.read_bytes() != blob or _mode(local_path) != entry.mode:
            raise ObservationBundleVerificationError("local source does not match the immutable Git blob")
    return entries, observation_bundle_digest(entries, manifest_sha256=manifest_sha256), manifest_sha256


def _verify_manifest_source(
    manifest_path: Path,
    root: Path,
    tree: Mapping[str, tuple[str, str]],
    provider: CiShadowGitObjectProvider,
    budget: RequestBudget,
) -> str:
    try:
        relative = manifest_path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise ObservationBundleVerificationError("bundle manifest escapes source root") from exc
    entry = tree.get(relative)
    if entry is None or entry[0] != "100644":
        raise ObservationBundleVerificationError("source tree does not contain the bundle manifest")
    manifest_bytes = _git_blob(entry[1], provider, budget)
    if _local_file(root, relative).read_bytes() != manifest_bytes:
        raise ObservationBundleVerificationError("local bundle manifest does not match its immutable Git blob")
    return "sha256:" + hashlib.sha256(manifest_bytes).hexdigest()


def _load_manifest(path: Path) -> tuple[ObservationBundleEntry, ...]:
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ObservationBundleVerificationError("observation bundle manifest is unavailable") from exc
    if not isinstance(value, Mapping):
        raise ObservationBundleVerificationError("observation bundle manifest is malformed")
    try:
        return entries_from_manifest(value)
    except ObservationBundleError as exc:
        raise ObservationBundleVerificationError(str(exc)) from exc


def _tree_for_commit(
    commit_sha: str, provider: CiShadowGitObjectProvider, budget: RequestBudget
) -> dict[str, tuple[str, str]]:
    commit = _object(
        budget.dispatch(
            "git_object_requests",
            lambda timeout: provider.get_git_commit_tree(
                commit_sha=_full_sha(commit_sha),
                timeout_seconds=timeout,
                max_response_bytes=budget.remaining_response_bytes,
            ),
        ),
        "Git commit",
    )
    tree_value = commit.get("tree")
    if not isinstance(tree_value, Mapping):
        raise ObservationBundleVerificationError("Git commit root tree is unavailable")
    tree_sha = tree_value.get("sha")
    payload = _object(
        budget.dispatch(
            "git_object_requests",
            lambda timeout: provider.get_git_tree(
                tree_sha=_full_sha(tree_sha),
                timeout_seconds=timeout,
                max_response_bytes=budget.remaining_response_bytes,
            ),
        ),
        "Git tree",
    )
    raw_tree = payload.get("tree")
    if payload.get("truncated") is not False or not isinstance(raw_tree, list):
        raise ObservationBundleVerificationError("Git tree is incomplete")
    result: dict[str, tuple[str, str]] = {}
    for value in raw_tree:
        if not isinstance(value, Mapping):
            raise ObservationBundleVerificationError("Git tree entry is malformed")
        path, mode, kind, sha = value.get("path"), value.get("mode"), value.get("type"), value.get("sha")
        if not isinstance(path, str) or path in result:
            raise ObservationBundleVerificationError("Git tree has an ambiguous path")
        if kind == "blob" and mode in {"100644", "100755"} and isinstance(sha, str):
            result[path] = (mode, _full_sha(sha))
    return result


def _git_blob(blob_sha: str, provider: CiShadowGitObjectProvider, budget: RequestBudget) -> bytes:
    payload = _object(
        budget.dispatch(
            "git_object_requests",
            lambda timeout: provider.get_git_blob(
                blob_sha=blob_sha, timeout_seconds=timeout, max_response_bytes=budget.remaining_response_bytes
            ),
        ),
        "Git blob",
    )
    content, encoding, returned_sha = payload.get("content"), payload.get("encoding"), payload.get("sha")
    if not isinstance(content, str) or encoding != "base64" or _full_sha(returned_sha) != blob_sha:
        raise ObservationBundleVerificationError("Git blob serialization is malformed")
    try:
        raw = base64.b64decode(content.replace("\n", ""), validate=True)
    except ValueError as exc:
        raise ObservationBundleVerificationError("Git blob content is not base64") from exc
    git_oid = hashlib.sha1(f"blob {len(raw)}\0".encode("ascii") + raw).hexdigest()  # noqa: S324
    if git_oid != blob_sha:
        raise ObservationBundleVerificationError("Git blob object identity does not match its bytes")
    return raw


def _verify_static_import_closure(entries: tuple[ObservationBundleEntry, ...], root: Path) -> None:
    declared = {entry.path for entry in entries}
    for entry in entries:
        if not entry.path.endswith(".py"):
            continue
        path = _local_file(root, entry.path)
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, UnicodeDecodeError, SyntaxError) as exc:
            raise ObservationBundleVerificationError("bundle Python source is unreadable") from exc
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in {"__import__", "eval", "exec"}
            ):
                raise ObservationBundleVerificationError("bundle source has a dynamic execution escape hatch")
            if (
                isinstance(node, ast.ImportFrom)
                and node.level == 0
                and node.module
                and node.module.startswith("dpone.")
            ):
                dependency = Path("src", *node.module.split(".")).with_suffix(".py").as_posix()
                if dependency not in declared:
                    raise ObservationBundleVerificationError("bundle omits a local dpone import")


def _object(payload: bytes, label: str) -> Mapping[str, object]:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ObservationBundleVerificationError(f"{label} serialization is invalid") from exc
    if not isinstance(value, Mapping):
        raise ObservationBundleVerificationError(f"{label} serialization is malformed")
    return value


def _local_file(root: Path, relative: str) -> Path:
    candidate = (root / relative).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as exc:
        raise ObservationBundleVerificationError("bundle local path escapes source root") from exc
    if not candidate.is_file() or candidate.is_symlink():
        raise ObservationBundleVerificationError("bundle local entry is not a regular file")
    return candidate


def _mode(path: Path) -> str:
    return "100755" if path.stat().st_mode & 0o111 else "100644"


def _full_sha(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 40
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ObservationBundleVerificationError("Git object SHA is invalid")
    return value


__all__ = ["ObservationBundleVerificationError", "verify_observation_bundle"]
