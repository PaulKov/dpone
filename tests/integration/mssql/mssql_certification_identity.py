"""Exact-source identity helpers for immutable MSSQL certification runners."""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

_SHA = re.compile(r"[0-9a-f]{40}\Z")
_TREE = re.compile(r"[0-9a-f]{40,64}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_BAKED_COMMIT = Path("/etc/dpone/source-commit")
_BAKED_TREE = Path("/etc/dpone/source-tree")


def baked_source_identity(
    *, identity_file: Path = _BAKED_COMMIT, tree_file: Path = _BAKED_TREE
) -> tuple[str, str, str]:
    """Return commit, tree, and image identities only when all baked bindings match."""
    try:
        discovered = identity_file.read_text(encoding="ascii").strip()
        discovered_tree = tree_file.read_text(encoding="ascii").strip()
    except OSError as error:
        pytest.fail(f"certification source identity unavailable: {type(error).__name__}")
    requested = os.environ.get("DPONE_CERTIFICATION_COMMIT_SHA", "")
    requested_tree = os.environ.get("DPONE_CERTIFICATION_TREE_OID", "")
    image_digest = os.environ.get("DPONE_CERTIFICATION_IMAGE_SHA256", "")
    if _SHA.fullmatch(discovered) is None or requested != discovered:
        pytest.fail("DPONE_CERTIFICATION_COMMIT_SHA does not match the baked source")
    if _TREE.fullmatch(discovered_tree) is None or requested_tree != discovered_tree:
        pytest.fail("DPONE_CERTIFICATION_TREE_OID does not match the baked source")
    if _DIGEST.fullmatch(image_digest) is None:
        pytest.fail("DPONE_CERTIFICATION_IMAGE_SHA256 is required in certification mode")
    return discovered, discovered_tree, image_digest


def baked_source_commit(*, identity_file: Path = _BAKED_COMMIT, tree_file: Path = _BAKED_TREE) -> str:
    """Return the requested commit only when it matches the runner's baked identity."""
    return baked_source_identity(identity_file=identity_file, tree_file=tree_file)[0]


__all__ = ["baked_source_commit", "baked_source_identity"]
