from __future__ import annotations

import argparse

from . import observability_cmd, orchestrate_cmd, strategy_cmd, supply_chain_catalog_cmd, supply_chain_cmd
from .base import Command
from .func_command import CommandGroup, FuncCommand


def orchestration_group() -> Command:
    sub = [FuncCommand("run", orchestrate_cmd.register_run_parser, orchestrate_cmd.cmd_orchestrate_run)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("orchestrate", help="Run orchestration, locks, and scheduler handoff utilities")

    return CommandGroup(
        name="orchestrate",
        help="Run orchestration, locks, and scheduler handoff utilities",
        build_parser=build,
        subcommands=sub,
        subdest="orchestrate_cmd",
    )


def observability_group() -> Command:
    sub = [
        FuncCommand(
            "metrics-export",
            observability_cmd.register_metrics_export_parser,
            observability_cmd.cmd_metrics_export,
        )
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("observability", help="Metrics and runtime observability utilities")

    return CommandGroup(
        name="observability",
        help="Metrics and runtime observability utilities",
        build_parser=build,
        subcommands=sub,
        subdest="observability_cmd",
    )


def supply_chain_group() -> Command:
    sub = [
        FuncCommand(
            "attest",
            supply_chain_cmd.register_attest_parser,
            supply_chain_cmd.cmd_supply_chain_attest,
        ),
        FuncCommand(
            "catalog-bundle-build",
            supply_chain_catalog_cmd.register_catalog_bundle_build_parser,
            supply_chain_catalog_cmd.cmd_catalog_bundle_build,
        ),
        FuncCommand(
            "catalog-bundle-verify",
            supply_chain_catalog_cmd.register_catalog_bundle_verify_parser,
            supply_chain_catalog_cmd.cmd_catalog_bundle_verify,
        ),
        FuncCommand(
            "extension-conformance",
            supply_chain_catalog_cmd.register_extension_conformance_parser,
            supply_chain_catalog_cmd.cmd_extension_conformance,
        ),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("supply-chain", help="SBOM, provenance, signing, and attestation utilities")

    return CommandGroup(
        name="supply-chain",
        help="SBOM, provenance, signing, and attestation utilities",
        build_parser=build,
        subcommands=sub,
        subdest="supply_chain_cmd",
    )


def strategy_group() -> Command:
    sub = [
        FuncCommand("advise", strategy_cmd.register_advise_parser, strategy_cmd.cmd_strategy_advise),
        FuncCommand("preflight", strategy_cmd.register_preflight_parser, strategy_cmd.cmd_strategy_preflight),
        FuncCommand("repair-plan", strategy_cmd.register_repair_plan_parser, strategy_cmd.cmd_strategy_repair_plan),
        FuncCommand(
            "certification-artifact",
            strategy_cmd.register_certification_artifact_parser,
            strategy_cmd.cmd_strategy_certification_artifact,
        ),
        FuncCommand(
            "certification-bundle",
            strategy_cmd.register_certification_bundle_parser,
            strategy_cmd.cmd_strategy_certification_bundle,
        ),
        FuncCommand(
            "native-transfer-evidence",
            strategy_cmd.register_native_transfer_evidence_parser,
            strategy_cmd.cmd_strategy_native_transfer_evidence,
        ),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("strategy", help="Strategy advisor and auto-selection utilities")

    return CommandGroup(
        name="strategy",
        help="Strategy advisor and auto-selection utilities",
        build_parser=build,
        subcommands=sub,
        subdest="strategy_cmd",
    )
