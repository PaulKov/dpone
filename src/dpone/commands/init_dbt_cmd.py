"""Compose the reviewed installed starter service without dbt execution."""

from __future__ import annotations

import argparse
import shlex
from collections.abc import Callable


def cmd_init_dbt(args: argparse.Namespace, *, emit: Callable[[str, dict[str, object], str], None]) -> int:
    """Delegate policy and writes; preserve the service's redacted result/exit."""
    from dpone.adapters.dbt_starter_resources import InstalledDbtStarterResources
    from dpone.adapters.project_authoring_lock import project_authoring_lock
    from dpone.manifest.dbt_publish_profiles import DbtPublishProfileRegistry
    from dpone.readiness.dbt_starter import DbtStarterService

    service = DbtStarterService(
        resources=InstalledDbtStarterResources(),
        parse_profiles=DbtPublishProfileRegistry.from_mapping,
        authoring_lock=project_authoring_lock,
    )
    result = service.init(
        args.dbt_path,
        profiles=args.dbt_profiles,
        profile=args.dbt_profile,
        workflow=args.dbt_workflow,
        dry_run=args.dbt_dry_run,
    )
    payload = result.to_dict()
    target = "./" + args.dbt_path if args.dbt_path.startswith("-") else args.dbt_path
    rerun = [
        "dpone",
        "init",
        "dbt",
        target,
        "--profiles=" + args.dbt_profiles,
        "--profile=" + args.dbt_profile,
        "--workflow=" + args.dbt_workflow,
    ]
    if any(change.action == "conflict" for change in result.changes):
        payload["rerun_command"] = shlex.join(
            [*rerun, "--format", args.format, *(["--dry-run"] if args.dbt_dry_run else [])]
        )
    if result.passed:
        payload["next_command"] = (
            shlex.join([*rerun, "--format", args.format])
            if args.dbt_dry_run
            else f"cd -- {shlex.quote(args.dbt_path)} && dbt parse --profiles-dir profiles --no-partial-parse && dpone dbt check ."
        )
    emit("dpone init dbt", payload, args.format)
    return result.exit_code if result.exit_code is not None else (0 if result.passed else 1)
