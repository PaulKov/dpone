"""CLI adapter for deterministic, local-only Airflow rerun planning."""

from __future__ import annotations

import argparse
import logging

from dpone.adapters.airflow_rerun_files import (
    LocalAirflowRerunPlanInputAdapter,
    LocalAirflowRerunPlanOutputAdapter,
)
from dpone.commands.airflow_rerun_plan_rendering import airflow_rerun_plan_text
from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.services.airflow_rerun_plan_service import AirflowRerunPlanInputError, AirflowRerunPlanService


def register_rerun_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("rerun-plan", help="Plan a pinned reproducible Airflow rerun without mutation")
    parser.add_argument("--evidence", required=True, help="Original gitops.airflow_evidence_bundle JSON")
    parser.add_argument("--current-index", required=True, help="Current local airflow-index.json")
    parser.add_argument("--cache-root", help="Local cache root; inferred from --current-index by default")
    parser.add_argument("--bundle", choices=("original", "latest"), default="original")
    parser.add_argument("--artifacts", choices=("original", "latest"), default="original")
    parser.add_argument("--critical", action="store_true", help="Require fully reproducible retained inputs")
    parser.add_argument("--output", help="Optional plan JSON path written atomically")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser


def cmd_airflow_rerun_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    input_error = False
    try:
        plan = AirflowRerunPlanService(input_port=LocalAirflowRerunPlanInputAdapter()).plan(
            evidence_path=args.evidence,
            current_index_path=args.current_index,
            cache_root=args.cache_root,
            bundle_selection=args.bundle,
            artifact_selection=args.artifacts,
            critical=bool(args.critical),
        )
        payload = plan.to_dict()
    except AirflowRerunPlanInputError as exc:
        input_error = True
        payload = {
            "schema": "dpone.airflow-rerun-plan.v1",
            "status": "blocked",
            "critical": bool(args.critical),
            "source_attempt": {},
            "selection": {"bundle": args.bundle, "artifacts": args.artifacts},
            "resolved": None,
            "airflow_request": None,
            "retention_refs": {"release_ids": [], "deployment_ids": []},
            "warnings": [],
            "blockers": [exc.to_dict()],
        }
    if args.output:
        LocalAirflowRerunPlanOutputAdapter().write(args.output, payload)
    if args.format == "json":
        write_json(payload)
    else:
        write_text(airflow_rerun_plan_text(payload))
    blocker_codes = {str(item.get("code")) for item in payload.get("blockers", ()) if isinstance(item, dict)}
    if not blocker_codes:
        return 0
    if input_error:
        return 2
    return 4 if "DPONE_RERUN_NOT_REPRODUCIBLE" in blocker_codes else 1


__all__ = ["cmd_airflow_rerun_plan", "register_rerun_plan_parser"]
