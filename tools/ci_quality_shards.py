"""Deterministic pytest sharding with auditable membership evidence.

The module is both a small CLI producer and a pytest plugin.  It deliberately
uses only node IDs and a fixed SHA-256 mapping: a test can neither disappear
from the selected population nor move between shards without an observable
change to the manifest digest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SHARD_ENVIRONMENT_VARIABLE = "DPONE_CI_TEST_SHARD"
SHARD_COUNT_ENVIRONMENT_VARIABLE = "DPONE_CI_TEST_SHARD_COUNT"
SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ShardManifest:
    """Serializable exact test population partition."""

    shard_count: int
    nodeids: tuple[str, ...]

    def payload(self) -> dict[str, Any]:
        assignments = {nodeid: shard_for(nodeid, self.shard_count) for nodeid in self.nodeids}
        return {
            "schema_version": SCHEMA_VERSION,
            "shard_count": self.shard_count,
            "node_count": len(self.nodeids),
            "nodeids_sha256": nodeids_digest(self.nodeids),
            "shards": [
                {"index": index, "nodeids": [nodeid for nodeid in self.nodeids if assignments[nodeid] == index]}
                for index in range(self.shard_count)
            ],
        }


def shard_receipt(
    *,
    nodeids: Iterable[str],
    shard_index: int,
    shard_count: int,
    head_sha: str,
    python_version: str,
    duration_seconds: float | None = None,
) -> dict[str, Any]:
    """Return auditable membership evidence for one interpreter/shard execution."""

    population = normalize_nodeids(nodeids)
    _validate_shard_count(shard_count)
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard index is outside the configured shard count")
    if len(head_sha) != 40 or any(character not in "0123456789abcdef" for character in head_sha):
        raise ValueError("head_sha must be a lowercase 40-character Git SHA")
    if duration_seconds is not None and duration_seconds < 0:
        raise ValueError("duration_seconds must not be negative")
    if python_version not in {"3.11", "3.12"}:
        raise ValueError("python_version is outside the required compatibility matrix")
    selected = tuple(nodeid for nodeid in population if shard_for(nodeid, shard_count) == shard_index)
    if not selected:
        raise ValueError(f"pytest shard {shard_index}/{shard_count} selected no tests")
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "head_sha": head_sha,
        "python_version": python_version,
        "shard_count": shard_count,
        "shard_index": shard_index,
        "population_count": len(population),
        "population_sha256": nodeids_digest(population),
        "selected_count": len(selected),
        "selected_nodeids": list(selected),
        "selected_sha256": nodeids_digest(selected),
    }
    if duration_seconds is not None:
        receipt["duration_seconds"] = duration_seconds
    return receipt


def validate_receipts(
    *, receipts: Iterable[dict[str, Any]], head_sha: str, python_version: str, shard_count: int
) -> dict[str, Any]:
    """Fail closed unless one exact-head receipt exists for every shard."""

    _validate_shard_count(shard_count)
    materialized = tuple(receipts)
    if len(materialized) != shard_count:
        raise ValueError("receipt count does not match the configured shard count")
    expected_indexes = set(range(shard_count))
    indexes = {receipt.get("shard_index") for receipt in materialized}
    if indexes != expected_indexes:
        raise ValueError("receipts do not cover every shard exactly once")
    if any(receipt.get("head_sha") != head_sha for receipt in materialized):
        raise ValueError("receipt head SHA does not match the exact candidate")
    if any(receipt.get("python_version") != python_version for receipt in materialized):
        raise ValueError("receipt interpreter does not match the required compatibility matrix")
    if any(receipt.get("shard_count") != shard_count for receipt in materialized):
        raise ValueError("receipt shard count does not match the configured shard count")
    population_counts = {receipt.get("population_count") for receipt in materialized}
    population_digests = {receipt.get("population_sha256") for receipt in materialized}
    if len(population_counts) != 1 or len(population_digests) != 1:
        raise ValueError("receipts disagree about the collected test population")
    selected = tuple(nodeid for receipt in materialized for nodeid in receipt.get("selected_nodeids", []))
    normalized = normalize_nodeids(selected)
    if len(normalized) != next(iter(population_counts)):
        raise ValueError("receipt membership does not cover the complete test population")
    if any(
        shard_for(nodeid, shard_count) != receipt["shard_index"]
        for receipt in materialized
        for nodeid in receipt["selected_nodeids"]
    ):
        raise ValueError("receipt contains a node ID assigned to another shard")
    return {
        "schema_version": SCHEMA_VERSION,
        "head_sha": head_sha,
        "python_version": python_version,
        "shard_count": shard_count,
        "population_count": next(iter(population_counts)),
        "population_sha256": next(iter(population_digests)),
        "shard_durations_seconds": [
            receipt.get("duration_seconds") for receipt in sorted(materialized, key=lambda item: item["shard_index"])
        ],
        "status": "PASS",
    }


def shard_for(nodeid: str, shard_count: int) -> int:
    """Return the stable zero-based shard for one canonical pytest node ID."""

    _validate_shard_count(shard_count)
    if not nodeid or "\n" in nodeid:
        raise ValueError("nodeid must be a non-empty single line")
    return int.from_bytes(hashlib.sha256(nodeid.encode("utf-8")).digest()[:8], "big") % shard_count


def nodeids_digest(nodeids: Iterable[str]) -> str:
    """Hash the sorted exact population with an unambiguous newline separator."""

    normalized = normalize_nodeids(nodeids)
    return hashlib.sha256("".join(f"{nodeid}\n" for nodeid in normalized).encode("utf-8")).hexdigest()


def normalize_nodeids(nodeids: Iterable[str]) -> tuple[str, ...]:
    """Return a canonical population while rejecting duplicate or malformed IDs."""

    normalized = tuple(sorted(nodeid.strip() for nodeid in nodeids if nodeid.strip()))
    if not normalized:
        raise ValueError("at least one nodeid is required")
    if len(set(normalized)) != len(normalized):
        raise ValueError("nodeids must be unique")
    if any("\n" in nodeid for nodeid in normalized):
        raise ValueError("nodeids must be single line")
    return normalized


def pytest_collection_modifyitems(config: Any, items: list[Any]) -> None:
    """Deselect every collected item not assigned to the requested shard."""

    shard = os.environ.get(SHARD_ENVIRONMENT_VARIABLE)
    shard_count = os.environ.get(SHARD_COUNT_ENVIRONMENT_VARIABLE)
    if shard is None and shard_count is None:
        return
    if shard is None or shard_count is None:
        raise ValueError(f"set both {SHARD_ENVIRONMENT_VARIABLE} and {SHARD_COUNT_ENVIRONMENT_VARIABLE}")
    try:
        index = int(shard)
        count = int(shard_count)
    except ValueError as exc:
        raise ValueError("pytest shard settings must be integers") from exc
    _validate_shard_count(count)
    if index < 0 or index >= count:
        raise ValueError("pytest shard index is outside the configured shard count")
    selected = [item for item in items if shard_for(item.nodeid, count) == index]
    deselected = [item for item in items if shard_for(item.nodeid, count) != index]
    if not selected:
        raise ValueError(f"pytest shard {index}/{count} selected no tests")
    items[:] = selected
    config.hook.pytest_deselected(items=deselected)


def _validate_shard_count(shard_count: int) -> None:
    if shard_count < 2 or shard_count > 64:
        raise ValueError("shard_count must be between 2 and 64")


def _read_nodeids(path: Path) -> tuple[str, ...]:
    return normalize_nodeids(path.read_text(encoding="utf-8").splitlines())


def main(argv: list[str] | None = None) -> int:
    """Write a canonical manifest from a preflight-collected node ID file."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodeids-file", type=Path)
    parser.add_argument("--shard-count", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--shard-index", type=int)
    parser.add_argument("--head-sha")
    parser.add_argument("--python-version")
    parser.add_argument("--duration-seconds", type=float)
    parser.add_argument("--receipts-directory", type=Path)
    args = parser.parse_args(argv)
    if args.receipts_directory is not None:
        if args.head_sha is None or args.python_version is None or args.shard_index is not None:
            parser.error("receipt validation requires --head-sha, --python-version and no --shard-index")
        receipts = [
            json.loads(path.read_text(encoding="utf-8")) for path in args.receipts_directory.rglob("receipt.json")
        ]
        payload = validate_receipts(
            receipts=receipts, head_sha=args.head_sha, python_version=args.python_version, shard_count=args.shard_count
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return 0
    if args.nodeids_file is None:
        parser.error("--nodeids-file is required when producing a manifest or shard receipt")
    nodeids = _read_nodeids(args.nodeids_file)
    if (args.shard_index is None) != (args.head_sha is None) or (args.shard_index is None) != (
        args.python_version is None
    ):
        parser.error("--shard-index, --head-sha and --python-version must be supplied together")
    if args.shard_index is None:
        payload = ShardManifest(args.shard_count, nodeids).payload()
    else:
        payload = shard_receipt(
            nodeids=nodeids,
            shard_index=args.shard_index,
            shard_count=args.shard_count,
            head_sha=args.head_sha,
            python_version=args.python_version,
            duration_seconds=args.duration_seconds,
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
