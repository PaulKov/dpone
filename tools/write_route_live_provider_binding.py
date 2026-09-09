#!/usr/bin/env python3
"""Write an exact provider binding for route-live release authority."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from tools.route_live_certification import CertificationValidationError
    from tools.route_live_certification.io_authority import write_create_only_exact
    from tools.route_live_certification.provider_binding import build_provider_binding
except ModuleNotFoundError as exc:
    if exc.name != "tools":
        raise
    from route_live_certification import CertificationValidationError  # type: ignore[no-redef]
    from route_live_certification.io_authority import write_create_only_exact  # type: ignore[no-redef]
    from route_live_certification.provider_binding import build_provider_binding  # type: ignore[no-redef]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--artifact-name", required=True)
    parser.add_argument("--artifact-id", type=int, required=True)
    parser.add_argument("--artifact-digest", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--workflow-run-id", type=int, required=True)
    parser.add_argument("--workflow-run-attempt", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        payload = build_provider_binding(
            manifest_path=args.manifest,
            artifact_name=args.artifact_name,
            artifact_id=args.artifact_id,
            artifact_digest=args.artifact_digest,
            commit_sha=args.commit_sha,
            workflow_run_id=args.workflow_run_id,
            workflow_run_attempt=args.workflow_run_attempt,
        )
        write_create_only_exact(
            args.output,
            (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8"),
            code="provider_binding.output",
        )
    except (CertificationValidationError, OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
