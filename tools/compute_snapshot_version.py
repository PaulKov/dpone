#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dpone.services.ci.snapshot_version import build_snapshot_version_from_pyproject


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute dpone dev snapshot version from pyproject + pipeline iid.")
    parser.add_argument("--pyproject", default="pyproject.toml", help="Path to pyproject.toml")
    parser.add_argument("--pipeline-iid", required=True, help="CI pipeline IID used to create a unique dev version")
    args = parser.parse_args()
    print(build_snapshot_version_from_pyproject(args.pyproject, pipeline_iid=args.pipeline_iid))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
