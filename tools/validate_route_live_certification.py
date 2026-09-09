#!/usr/bin/env python3
"""Validate and consolidate release-authoritative route-live evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from tools.route_live_certification import (
        CertificationValidationError,
        WorkflowBinding,
        require_output_path_disjoint,
        validate_certification,
    )
    from tools.route_live_certification.io_authority import write_create_only_exact
    from tools.route_live_certification.registry import reviewed_inventory_entries
except ModuleNotFoundError as exc:
    if exc.name != "tools":
        raise
    from route_live_certification import (  # type: ignore[no-redef]
        CertificationValidationError,
        WorkflowBinding,
        require_output_path_disjoint,
        validate_certification,
    )
    from route_live_certification.io_authority import write_create_only_exact  # type: ignore[no-redef]
    from route_live_certification.registry import reviewed_inventory_entries  # type: ignore[no-redef]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--junit-root", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--workflow-run-id", required=True)
    parser.add_argument("--workflow-run-attempt", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        require_output_path_disjoint(
            args.output,
            inventory_path=args.inventory,
            evidence_root=args.evidence_root,
            junit_root=args.junit_root,
        )
        manifest = validate_certification(
            inventory_path=args.inventory,
            evidence_root=args.evidence_root,
            junit_root=args.junit_root,
            binding=WorkflowBinding(
                commit_sha=args.commit_sha,
                run_id=args.workflow_run_id,
                run_attempt=args.workflow_run_attempt,
            ),
            reviewed_suite_entries=reviewed_inventory_entries(),
        )
        write_create_only_exact(
            args.output,
            (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8"),
            code="output.manifest",
        )
    except (CertificationValidationError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(manifest["manifest_sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
