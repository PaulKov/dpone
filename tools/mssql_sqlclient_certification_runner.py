#!/usr/bin/env python3
"""Run the complete SqlClient campaign in containers bound to one image digest."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

_SHA = re.compile(r"[0-9a-f]{40}\Z")
_TREE = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_ENV_NAME = re.compile(r"[A-Z][A-Z0-9_]*\Z")


@dataclass(frozen=True, slots=True)
class ExecutionCell:
    """One fixture executed in one isolated container and Python process."""

    scenario: str
    fixture_id: str
    row_count: int
    layout_version: int
    import_parallelism: int


EXECUTION_CELLS = (
    ExecutionCell("success", "narrow-sqlclient-v1", 10_000, 1, 1),
    ExecutionCell("success", "wide100-sqlclient-v1", 10_000, 1, 1),
    ExecutionCell("success", "narrow-sqlclient-v1", 10_000, 2, 2),
    ExecutionCell("success", "wide100-sqlclient-v1", 10_000, 2, 2),
    ExecutionCell("success", "narrow-sqlclient-v1", 1_000_000, 2, 2),
    ExecutionCell("success", "wide100-sqlclient-v1", 1_000_000, 2, 2),
    ExecutionCell("force_kill_recovery", "wide100-sqlclient-v1", 10_000, 2, 2),
)


def run_campaign(
    *,
    docker: str,
    image: str,
    image_receipt: Path,
    network: str,
    evidence_dir: Path,
    output: Path,
    pass_env: tuple[str, ...],
) -> dict[str, Any]:
    """Run every cell by immutable image ID and emit a content-bound receipt."""
    receipt = _image_receipt(image_receipt)
    digest = receipt["runner_image_sha256"]
    immutable_image = f"sha256:{digest}"
    inspection = _inspect_image(docker, image)
    if inspection.get("Id") != immutable_image:
        raise ValueError("sqlclient_runner.image_digest_mismatch")
    if inspection.get("Os") != "linux" or inspection.get("Architecture") != "amd64":
        raise ValueError("sqlclient_runner.image_platform_mismatch")
    labels = (inspection.get("Config") or {}).get("Labels") or {}
    if (
        labels.get("org.opencontainers.image.revision") != receipt["source_commit_sha"]
        or labels.get("dev.dpone.certification.source-tree") != receipt["source_tree_oid"]
    ):
        raise ValueError("sqlclient_runner.image_labels_mismatch")
    if any(_ENV_NAME.fullmatch(name) is None or name not in os.environ for name in pass_env):
        raise ValueError("sqlclient_runner.pass_environment_unavailable")
    root = evidence_dir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        raise ValueError("sqlclient_runner.evidence_directory_not_empty")
    if output.exists() or output.is_symlink():
        raise FileExistsError(output)

    executions = []
    for cell in EXECUTION_CELLS:
        container = _create_container(
            docker=docker,
            immutable_image=immutable_image,
            receipt=receipt,
            network=network,
            evidence_dir=root,
            pass_env=pass_env,
            cell=cell,
        )
        try:
            container_image = _capture((docker, "inspect", "--format", "{{.Image}}", container)).strip()
            if container_image != immutable_image:
                raise ValueError("sqlclient_runner.container_image_mismatch")
            _run_attached((docker, "start", "--attach", container))
            executions.append({**asdict(cell), "container_image_sha256": digest})
        finally:
            _run((docker, "rm", "--force", container), allow_failure=True)

    artifacts = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(root.glob("*.json"))}
    if len(artifacts) != 7:
        raise ValueError("sqlclient_runner.evidence_closure_mismatch")
    result = {
        "schema_version": "dpone.mssql-sqlclient.certification-runner.v2",
        "status": "PASS",
        "source_commit_sha": receipt["source_commit_sha"],
        "source_tree_oid": receipt["source_tree_oid"],
        "runner_image_sha256": digest,
        "runner_platform": "linux/amd64",
        "source_mode": "exact_git_archive",
        "worktree_dirty": False,
        "execution_count": len(executions),
        "executions": executions,
        "evidence_artifacts": artifacts,
    }
    _atomic_create(output, json.dumps(result, indent=2, sort_keys=True).encode() + b"\n")
    return result


def _create_container(
    *,
    docker: str,
    immutable_image: str,
    receipt: dict[str, Any],
    network: str,
    evidence_dir: Path,
    pass_env: tuple[str, ...],
    cell: ExecutionCell,
) -> str:
    command = [
        docker,
        "create",
        "--network",
        network,
        "--platform",
        "linux/amd64",
        "--mount",
        f"type=bind,src={evidence_dir},dst=/evidence",
        "--env",
        "DPONE_RUN_SQLCLIENT_LIVE=1",
        "--env",
        "DPONE_SQLCLIENT_EVIDENCE_DIR=/evidence",
        "--env",
        f"DPONE_CERTIFICATION_COMMIT_SHA={receipt['source_commit_sha']}",
        "--env",
        f"DPONE_CERTIFICATION_TREE_OID={receipt['source_tree_oid']}",
        "--env",
        f"DPONE_CERTIFICATION_IMAGE_SHA256={receipt['runner_image_sha256']}",
        "--env",
        f"DPONE_SQLCLIENT_CERT_ROWS={cell.row_count}",
        "--env",
        f"DPONE_SQLCLIENT_CERT_LAYOUT_VERSION={cell.layout_version}",
        "--env",
        f"DPONE_SQLCLIENT_CERT_IMPORT_PARALLELISM={cell.import_parallelism}",
    ]
    if cell.scenario == "force_kill_recovery":
        command.extend(("--env", "DPONE_SQLCLIENT_FORCE_KILL=1"))
    for name in pass_env:
        command.extend(("--env", name))
    command.extend(
        (
            immutable_image,
            (
                "tests/integration/mssql/test_mssql_sqlclient_route_live.py::"
                f"test_sqlclient_transport_certification_matrix[{cell.fixture_id}]"
            ),
            "-q",
            "-o",
            "addopts=",
            "-p",
            "no:cacheprovider",
        )
    )
    container = _capture(tuple(command)).strip()
    if not re.fullmatch(r"[0-9a-f]{12,64}", container):
        raise RuntimeError("sqlclient_runner.container_create_failed")
    return container


def _image_receipt(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("sqlclient_runner.invalid_image_receipt") from error
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != "dpone.mssql-sqlclient.certification-image.v2"
        or value.get("status") != "PASS"
        or _SHA.fullmatch(str(value.get("source_commit_sha", ""))) is None
        or _TREE.fullmatch(str(value.get("source_tree_oid", ""))) is None
        or _DIGEST.fullmatch(str(value.get("runner_image_sha256", ""))) is None
        or value.get("runner_platform") != "linux/amd64"
        or value.get("source_mode") != "exact_git_archive"
        or value.get("worktree_dirty") is not False
    ):
        raise ValueError("sqlclient_runner.invalid_image_receipt")
    return value


def _inspect_image(docker: str, image: str) -> dict[str, Any]:
    try:
        value = json.loads(_capture((docker, "image", "inspect", image)))
    except json.JSONDecodeError as error:
        raise ValueError("sqlclient_runner.image_inspect_failed") from error
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
        raise ValueError("sqlclient_runner.image_inspect_failed")
    return value[0]


def _capture(command: tuple[str, ...]) -> str:
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"sqlclient_runner.command_failed:{Path(command[0]).name}")
    return result.stdout


def _run_attached(command: tuple[str, ...]) -> None:
    result = subprocess.run(command, check=False)
    if result.returncode != 0:
        raise RuntimeError("sqlclient_runner.cell_failed")


def _run(command: tuple[str, ...], *, allow_failure: bool = False) -> None:
    result = subprocess.run(command, check=False, capture_output=True)
    if result.returncode != 0 and not allow_failure:
        raise RuntimeError(f"sqlclient_runner.command_failed:{Path(command[0]).name}")


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
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--image", required=True)
    parser.add_argument("--image-receipt", type=Path, required=True)
    parser.add_argument("--network", required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pass-env", action="append", default=[])
    args = parser.parse_args()
    run_campaign(
        docker=args.docker,
        image=args.image,
        image_receipt=args.image_receipt,
        network=args.network,
        evidence_dir=args.evidence_dir,
        output=args.output,
        pass_env=tuple(args.pass_env),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
