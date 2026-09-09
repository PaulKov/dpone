#!/usr/bin/env python3
"""Create a new, immutable compatibility-candidate manifest from three wheels."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dpone.manifest.compatibility_candidate import (
    CandidateManifestError,
    build_manifest_from_directory,
    installed_expected_versions,
    write_new_manifest,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.dist == args.output.parent:
            raise CandidateManifestError("candidate output parent must differ from candidate directory")
        manifest = build_manifest_from_directory(args.dist, expected_versions=installed_expected_versions())
        write_new_manifest(args.output, manifest)
    except CandidateManifestError as exc:
        print(f"CANDIDATE_MANIFEST_UNVERIFIED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
