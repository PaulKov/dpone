from __future__ import annotations

import argparse

from . import (
    backfill_cmd,
    connectors_cmd,
    normalize_cmd,
    perf_cmd,
    profile_cmd,
    runtime_native_accel_cmd,
    runtime_storage_cmd,
    state_cmd,
)
from .base import Command
from .func_command import CommandGroup, FuncCommand


def normalize_group() -> Command:
    sub = [
        FuncCommand("preview", normalize_cmd.register_preview_parser, normalize_cmd.cmd_normalize_preview),
        FuncCommand("infer", normalize_cmd.register_infer_parser, normalize_cmd.cmd_normalize_infer),
        FuncCommand("lint", normalize_cmd.register_lint_parser, normalize_cmd.cmd_normalize_lint),
        FuncCommand("certify", normalize_cmd.register_certify_parser, normalize_cmd.cmd_normalize_certify),
        FuncCommand("benchmark", normalize_cmd.register_benchmark_parser, normalize_cmd.cmd_normalize_benchmark),
        FuncCommand("stress", normalize_cmd.register_stress_parser, normalize_cmd.cmd_normalize_stress),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("normalize", help="Nested normalization preview and inference utilities")

    return CommandGroup(
        name="normalize",
        help="Nested normalization preview and inference utilities",
        build_parser=build,
        subcommands=sub,
        subdest="normalize_cmd",
    )


def backfill_group() -> Command:
    sub = [
        FuncCommand("plan", backfill_cmd.register_plan_parser, backfill_cmd.cmd_backfill_plan),
        FuncCommand("run", backfill_cmd.register_run_parser, backfill_cmd.cmd_backfill_run),
        FuncCommand("resume", backfill_cmd.register_resume_parser, backfill_cmd.cmd_backfill_resume),
        FuncCommand("status", backfill_cmd.register_status_parser, backfill_cmd.cmd_backfill_status),
        FuncCommand("retry-failed", backfill_cmd.register_retry_failed_parser, backfill_cmd.cmd_backfill_retry_failed),
        FuncCommand("cancel", backfill_cmd.register_cancel_parser, backfill_cmd.cmd_backfill_cancel),
        FuncCommand("doctor", backfill_cmd.register_doctor_parser, backfill_cmd.cmd_backfill_doctor),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("backfill", help="Resumable chunked historical load planning and execution")

    return CommandGroup(
        name="backfill",
        help="Resumable chunked historical load planning and execution",
        build_parser=build,
        subcommands=sub,
        subdest="backfill_cmd",
    )


def state_group() -> Command:
    sub = [
        FuncCommand("inspect", state_cmd.register_inspect_parser, state_cmd.cmd_state_inspect),
        FuncCommand("reset", state_cmd.register_reset_parser, state_cmd.cmd_state_reset),
        FuncCommand("export", state_cmd.register_export_parser, state_cmd.cmd_state_export),
        FuncCommand("replay-from", state_cmd.register_replay_from_parser, state_cmd.cmd_state_replay_from),
        FuncCommand("rewind", state_cmd.register_rewind_parser, state_cmd.cmd_state_rewind),
        FuncCommand("compare", state_cmd.register_compare_parser, state_cmd.cmd_state_compare),
        FuncCommand(
            "render-mssql-transaction-ddl",
            state_cmd.register_render_mssql_transaction_ddl_parser,
            state_cmd.cmd_state_render_mssql_transaction_ddl,
            _requires_app_context=False,
        ),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("state", help="State inspection and replay utilities")

    return CommandGroup(
        name="state",
        help="State inspection and replay utilities",
        build_parser=build,
        subcommands=sub,
        subdest="state_cmd",
    )


def connectors_group() -> Command:
    sub = [
        FuncCommand("list", connectors_cmd.register_list_parser, connectors_cmd.cmd_connectors_list),
        FuncCommand("certify", connectors_cmd.register_certify_parser, connectors_cmd.cmd_connectors_certify),
        FuncCommand("scaffold", connectors_cmd.register_scaffold_parser, connectors_cmd.cmd_connectors_scaffold),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("connectors", help="Connector catalog, certification, and SDK utilities")

    return CommandGroup(
        name="connectors",
        help="Connector catalog, certification, and SDK utilities",
        build_parser=build,
        subcommands=sub,
        subdest="connectors_cmd",
    )


def perf_group() -> Command:
    sub = [FuncCommand("advise", perf_cmd.register_advise_parser, perf_cmd.cmd_perf_advise)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("perf", help="Performance advisor utilities")

    return CommandGroup(
        name="perf",
        help="Performance advisor utilities",
        build_parser=build,
        subcommands=sub,
        subdest="perf_cmd",
    )


def profile_group() -> Command:
    sub = [
        FuncCommand("advise", profile_cmd.register_advise_parser, profile_cmd.cmd_profile_advise),
        FuncCommand("wizard", profile_cmd.register_wizard_parser, profile_cmd.cmd_profile_wizard),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("profile", help="Load profile advisor and patch wizard")

    return CommandGroup(
        name="profile",
        help="Load profile advisor and patch wizard",
        build_parser=build,
        subcommands=sub,
        subdest="profile_cmd",
    )


def runtime_group() -> Command:
    storage = CommandGroup(
        name="storage",
        help="Runtime storage maintenance utilities",
        build_parser=lambda subparsers: subparsers.add_parser("storage", help="Runtime storage maintenance utilities"),
        subcommands=[
            FuncCommand("gc", runtime_storage_cmd.register_gc_parser, runtime_storage_cmd.cmd_runtime_storage_gc)
        ],
        subdest="runtime_storage_cmd",
    )
    native_accel = CommandGroup(
        name="native-accel",
        help="Optional native acceleration diagnostics and benchmark planning",
        build_parser=lambda subparsers: subparsers.add_parser(
            "native-accel", help="Optional native acceleration diagnostics and benchmark planning"
        ),
        subcommands=[
            FuncCommand(
                "doctor",
                runtime_native_accel_cmd.register_doctor_parser,
                runtime_native_accel_cmd.cmd_runtime_native_accel_doctor,
            ),
            FuncCommand(
                "benchmark",
                runtime_native_accel_cmd.register_benchmark_parser,
                runtime_native_accel_cmd.cmd_runtime_native_accel_benchmark,
            ),
        ],
        subdest="runtime_native_accel_cmd",
    )

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("runtime", help="Runtime execution utilities")

    return CommandGroup(
        name="runtime",
        help="Runtime execution utilities",
        build_parser=build,
        subcommands=[storage, native_accel],
        subdest="runtime_cmd",
    )
