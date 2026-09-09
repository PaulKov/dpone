"""CLI for bounded, secret-free Airflow Helm structural evidence."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

import yaml

from dpone_airflow_pack.helm_ack_evidence import build_helm_ack_evidence
from dpone_airflow_pack.helm_ack_mounts import LoaderAckMountPolicyError

_MAX_RENDER_BYTES = 32 * 1024 * 1024


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Emit secret-free evidence for one verified Airflow Helm render")
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--helm-version", required=True)
    parser.add_argument("--chart-name", required=True)
    parser.add_argument("--chart-version", required=True)
    parser.add_argument("--chart-sha256", required=True)
    parser.add_argument("--pack-wheel-sha256", required=True)
    parser.add_argument("--provider-wheel-sha256", required=True)
    parser.add_argument("--values-sha256", required=True)
    parser.add_argument("--renderer-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        raw = sys.stdin.buffer.read(_MAX_RENDER_BYTES + 1)
        if not raw:
            raise ValueError("Helm render input is empty")
        if len(raw) > _MAX_RENDER_BYTES:
            raise ValueError("Helm render exceeds the 32 MiB safety limit")
        evidence = build_helm_ack_evidence(
            list(yaml.safe_load_all(raw)),
            source_commit=args.source_commit,
            helm_version=args.helm_version,
            chart_name=args.chart_name,
            chart_version=args.chart_version,
            chart_sha256=args.chart_sha256,
            pack_wheel_sha256=args.pack_wheel_sha256,
            provider_wheel_sha256=args.provider_wheel_sha256,
            values_sha256=args.values_sha256,
            renderer_sha256=args.renderer_sha256,
        )
        json.dump(evidence, sys.stdout, ensure_ascii=True, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return 0
    except (LoaderAckMountPolicyError, ValueError, yaml.YAMLError) as exc:
        print(f"DPONE_AIRFLOW_HELM_EVIDENCE_INVALID: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
