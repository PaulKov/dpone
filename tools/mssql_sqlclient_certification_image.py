#!/usr/bin/env python3
"""Build the SqlClient certification image from an exact committed Git archive."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Any

_SHA = re.compile(r"[0-9a-f]{40}\Z")
_TREE = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_IMAGE = re.compile(r"sha256:([0-9a-f]{64})\Z")


def build_image(
    root: Path,
    *,
    docker: str,
    tag: str,
    output: Path,
    source_commit: str | None = None,
) -> dict[str, Any]:
    """Build only from `git archive`, then attest the immutable local image identity."""
    repository = root.resolve(strict=True)
    commit = _git(repository, "rev-parse", f"{source_commit or 'HEAD'}^{{commit}}")
    tree = _git(repository, "rev-parse", f"{commit}^{{tree}}")
    if _SHA.fullmatch(commit) is None or _TREE.fullmatch(tree) is None:
        raise ValueError("sqlclient_image.invalid_source_identity")
    if commit != _git(repository, "rev-parse", "HEAD"):
        raise ValueError("sqlclient_image.source_is_not_head")
    if _git(repository, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("sqlclient_image.worktree_dirty")
    if output.exists() or output.is_symlink():
        raise FileExistsError(output)
    with tempfile.TemporaryDirectory(prefix="dpone-sqlclient-image-") as temporary:
        context = Path(temporary) / "context"
        context.mkdir()
        archive = Path(temporary) / "source.tar"
        _run(("git", "-C", str(repository), "archive", "--format=tar", f"--output={archive}", commit))
        with tarfile.open(archive, "r:") as bundle:
            bundle.extractall(context, filter="data")
        dockerfile = context / "docker/mssql-sqlclient-certification/Dockerfile"
        _run(
            (
                docker,
                "build",
                "--platform",
                "linux/amd64",
                "--file",
                str(dockerfile),
                "--build-arg",
                f"SOURCE_COMMIT={commit}",
                "--build-arg",
                f"SOURCE_TREE_OID={tree}",
                "--tag",
                tag,
                str(context),
            )
        )
    inspection = json.loads(_capture((docker, "image", "inspect", tag)))
    if not isinstance(inspection, list) or len(inspection) != 1:
        raise ValueError("sqlclient_image.inspect_failed")
    image = inspection[0]
    image_match = _IMAGE.fullmatch(str(image.get("Id", "")))
    labels = ((image.get("Config") or {}).get("Labels") or {}) if isinstance(image, dict) else {}
    if (
        image_match is None
        or image.get("Os") != "linux"
        or image.get("Architecture") != "amd64"
        or labels.get("org.opencontainers.image.revision") != commit
        or labels.get("dev.dpone.certification.source-tree") != tree
        or labels.get("dev.dpone.certification.route") != "clickhouse-mssql-sqlclient-v1"
    ):
        raise ValueError("sqlclient_image.identity_mismatch")
    receipt = {
        "schema_version": "dpone.mssql-sqlclient.certification-image.v2",
        "status": "PASS",
        "source_commit_sha": commit,
        "source_tree_oid": tree,
        "runner_image_sha256": image_match.group(1),
        "runner_platform": "linux/amd64",
        "source_mode": "exact_git_archive",
        "worktree_dirty": False,
    }
    _atomic_create(output, json.dumps(receipt, indent=2, sort_keys=True).encode() + b"\n")
    return receipt


def _git(root: Path, *args: str) -> str:
    return _capture(("git", "-C", str(root), *args)).strip()


def _capture(command: tuple[str, ...]) -> str:
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"sqlclient_image.command_failed:{Path(command[0]).name}")
    return result.stdout


def _run(command: tuple[str, ...]) -> None:
    result = subprocess.run(command, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"sqlclient_image.command_failed:{Path(command[0]).name}")


def _atomic_create(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--tag", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-commit")
    args = parser.parse_args()
    build_image(
        args.root,
        docker=args.docker,
        tag=args.tag,
        output=args.output,
        source_commit=args.source_commit,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
