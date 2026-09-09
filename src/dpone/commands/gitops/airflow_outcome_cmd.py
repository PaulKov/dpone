from __future__ import annotations

import argparse
import logging
from typing import cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.gitops.airflow_rendering import render_gitops_airflow_outcome_gate_markdown
from dpone.services.gitops.airflow_outcome_gate_service import (
    GitOpsAirflowOutcomeGateContext,
    GitOpsAirflowOutcomeGateService,
)


def register_outcome_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("outcome-gate", help="Evaluate an Airflow final XCom outcome summary")
    p.add_argument("xcom_summary_path", help="Repo-relative gitops.airflow_xcom_summary JSON path")
    p.add_argument("--required-status", default="passed", help="Required final XCom status")
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def cmd_gitops_airflow_outcome_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    view = GitOpsAirflowOutcomeGateService(ctx=cast(GitOpsAirflowOutcomeGateContext, ctx)).build_view(args)
    if getattr(args, "format", "json") == "markdown":
        rendered = render_gitops_airflow_outcome_gate_markdown(view.report)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


__all__ = [
    "cmd_gitops_airflow_outcome_gate",
    "register_outcome_gate_parser",
]
