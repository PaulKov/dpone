"""Pure next-step and inspection commands for init result rendering."""

from __future__ import annotations

from collections.abc import Callable, Mapping


def _next_command(title: str, payload: Mapping[str, object], *, pipeline_id_from_target: Callable[[str], str]) -> str:
    if payload.get("passed") is not True:
        return ""
    if title == "dpone init dbt":
        command = payload.get("next_command")
        return command if isinstance(command, str) else ""
    if title == "dpone init project":
        if payload.get("layout_mode") == "domain_first":
            return "dpone init domain sales --owner-team <team> --owner-contact <contact> --approver-team <github-team>"
        return "dpone init pipeline orders_daily --recipe mssql-to-clickhouse-incremental"
    if title == "dpone init domain":
        domain = payload.get("domain") or "<domain>"
        return (
            f"dpone init pipeline orders_daily --domain {domain} "
            "--route mssql:clickhouse:incremental_merge "
            "--from <source-ref>:<schema>.<table> "
            "--to <sink-ref>:<schema>.<table> --key <column>"
        )
    if title == "dpone init pipeline":
        pipeline_path = str(payload.get("pipeline_path") or "").strip()
        if pipeline_path.endswith("/pipeline.yaml"):
            return f"dpone check {pipeline_id_from_target(pipeline_path)}"
        if pipeline_path:
            return f"dpone check {pipeline_path}"
    if title == "dpone workload init":
        next_commands = payload.get("next_commands")
        if isinstance(next_commands, list) and next_commands:
            first = next_commands[0]
            if isinstance(first, str) and first:
                return first if payload.get("mode") == "apply" else f"{first}  # after --apply"
    return ""


def _init_json_command(title: str, payload: Mapping[str, object]) -> str:
    rerun = payload.get("rerun_command")
    if isinstance(rerun, str) and rerun:
        return f"{rerun} --format json"
    return {
        "dpone init project": "dpone init project --airflow --format json",
        "dpone init pipeline": "dpone init pipeline <pipeline_id> --format json",
        "dpone init domain": (
            "dpone init domain <domain> --owner-team <team> "
            "--owner-contact <contact> --approver-team <github-team> --format json"
        ),
    }.get(title, "rerun the init command with --format json")
