"""Helm post-renderer for parser-owned Airflow loader acknowledgements."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

import yaml

from dpone_airflow_pack.helm_ack_mounts import (
    LoaderAckMountPolicyError,
    harden_loader_ack_mounts,
    verify_loader_ack_mounts,
)

_MAX_RENDER_BYTES = 32 * 1024 * 1024


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Restrict dpone loader-ACK write access to the Airflow parser container",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Validate already rendered manifests without changing them",
    )
    args = parser.parse_args(argv)
    try:
        raw = sys.stdin.buffer.read(_MAX_RENDER_BYTES + 1)
        if not raw:
            raise LoaderAckMountPolicyError("Helm render input is empty")
        if len(raw) > _MAX_RENDER_BYTES:
            raise LoaderAckMountPolicyError("Helm render exceeds the 32 MiB safety limit")
        documents = list(yaml.safe_load_all(raw))
        if args.verify_only:
            verify_loader_ack_mounts(documents)
            sys.stdout.buffer.write(raw)
            return 0
        hardened = harden_loader_ack_mounts(documents)
        sys.stdout.write(yaml.safe_dump_all(hardened, explicit_start=True, sort_keys=False))
        return 0
    except (LoaderAckMountPolicyError, yaml.YAMLError) as exc:
        print(f"DPONE_AIRFLOW_LOADER_ACK_MOUNT_INVALID: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
