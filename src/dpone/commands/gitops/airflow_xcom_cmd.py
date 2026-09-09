from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.gitops.airflow_xcom_from_evidence import write_airflow_xcom_from_evidence


def register_xcom_from_evidence_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("xcom-from-evidence", help="Build final Airflow XCom summary from runtime evidence")
    p.add_argument("--evidence-path", required=True, help="Runtime evidence JSON file path")
    p.add_argument("--runtime-evidence-path", help="Path label stored in XCom summary")
    p.add_argument("--stderr-path", help="Optional stderr file captured from the runtime command")
    p.add_argument("--xcom-output", required=True, help="Output path for /airflow/xcom/return.json")
    p.add_argument(
        "--status", help="Optional fallback status hint; evidence status remains authoritative when readable"
    )
    return p


def cmd_gitops_airflow_xcom_from_evidence(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = ctx, logger
    write_airflow_xcom_from_evidence(
        evidence_path=Path(args.evidence_path),
        xcom_output=Path(args.xcom_output),
        runtime_evidence_path=getattr(args, "runtime_evidence_path", None),
        stderr_path=getattr(args, "stderr_path", None),
        status=getattr(args, "status", None),
    )
    return 0


__all__ = [
    "cmd_gitops_airflow_xcom_from_evidence",
    "register_xcom_from_evidence_parser",
]
