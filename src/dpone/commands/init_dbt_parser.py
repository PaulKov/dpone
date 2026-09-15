"""Target-local native dbt starter arguments; no service or resource lookup."""

from __future__ import annotations

import argparse


def register_dbt_parser(target_parsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = target_parsers.add_parser(
        "dbt",
        help="Create an offline dbt starter from an explicit platform policy",
        description="Create source files only; no dbt, database, network or credential access. Put dbt options after this target.",
    )
    parser.add_argument("dbt_path", metavar="PATH", help="Destination project; its parent directory must exist")
    parser.add_argument(
        "--profiles", dest="dbt_profiles", required=True, help="Explicit nonsecret platform policy file"
    )
    parser.add_argument(
        "--profile", dest="dbt_profile", required=True, help="Native policy profile, not a recipe profile"
    )
    parser.add_argument("--workflow", dest="dbt_workflow", required=True, help="Named workflow admitted by the policy")
    parser.add_argument(
        "--dry-run",
        dest="dbt_dry_run",
        action="store_true",
        help="Plan without creating project files; default applies",
    )
    parser.add_argument("--format", choices=("text", "json", "md"), default=argparse.SUPPRESS)
    return parser
