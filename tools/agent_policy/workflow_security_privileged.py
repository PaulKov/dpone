"""Standalone, read-only CLI for the PR3B semantic privilege boundary."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

if __name__ == "__main__":
    sys.dont_write_bytecode = True

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))

from tools.agent_policy.workflow_privilege_report import (  # noqa: E402
    canonical_json_bytes,
    render_text,
)
from tools.agent_policy.workflow_privilege_service import scan_repository  # noqa: E402

_INTERNAL_ERROR = "PRIVILEGE_INTERNAL_REPORT_INVALID: report construction or schema validation failed\n"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="workflow_security_privileged.py",
        usage=(
            "workflow_security_privileged.py [-h] --root ROOT\n"
            "                                       [--format {text,json}]"
        ),
        description="Prove the closed PR-reachable workflow privilege boundary.",
    )
    parser.add_argument(
        "--root",
        required=True,
        type=Path,
        help="repository checkout containing the fixed policy and .github/workflows",
    )
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Render exactly one trusted report, or one fixed internal error."""

    arguments = _parser().parse_args(argv)
    try:
        report = scan_repository(arguments.root)
        output = canonical_json_bytes(report).decode("ascii") if arguments.format == "json" else render_text(report)
    except Exception:
        sys.stderr.write(_INTERNAL_ERROR)
        return 3
    sys.stdout.write(output)
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
