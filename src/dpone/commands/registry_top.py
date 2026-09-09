from __future__ import annotations

from . import (
    certify_cmd,
    doctor_cmd,
    init_cmd,
    plan_cmd,
    resume_cmd,
    resync_cmd,
    run_cmd,
    run_report_cmd,
    schema_plan_cmd,
    studio_cmd,
    test_cmd,
)
from .base import Command
from .func_command import FuncCommand


def top_level_commands() -> list[Command]:
    return [
        FuncCommand("doctor", doctor_cmd.register_parser, doctor_cmd.cmd_doctor),
        FuncCommand("init", init_cmd.register_parser, init_cmd.cmd_init),
        FuncCommand("plan", plan_cmd.register_parser, plan_cmd.cmd_plan),
        FuncCommand("run", run_cmd.register_parser, run_cmd.cmd_run),
        FuncCommand("test", test_cmd.register_parser, test_cmd.cmd_test, _requires_app_context=False),
        FuncCommand("resync", resync_cmd.register_parser, resync_cmd.cmd_resync),
        FuncCommand("resume", resume_cmd.register_parser, resume_cmd.cmd_resume),
        FuncCommand("run-report", run_report_cmd.register_parser, run_report_cmd.cmd_run_report),
        FuncCommand("studio", studio_cmd.register_parser, studio_cmd.cmd_studio),
        FuncCommand("certify", certify_cmd.register_parser, certify_cmd.cmd_certify),
        FuncCommand("schema-plan", schema_plan_cmd.register_parser, schema_plan_cmd.cmd_schema_plan),
    ]
