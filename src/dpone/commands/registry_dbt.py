from __future__ import annotations

import argparse

from .base import Command
from .dbt_dev_evidence_campaign_cmd import (
    cmd_run_dev_evidence_campaign,
    register_run_dev_evidence_campaign_parser,
)
from .dbt_dev_evidence_cmd import (
    cmd_finalize_dev_evidence,
    cmd_prepare_dev_evidence_request,
    cmd_verify_dev_evidence_integrity,
    register_finalize_dev_evidence_parser,
    register_prepare_dev_evidence_request_parser,
    register_verify_dev_evidence_integrity_parser,
)
from .dbt_prod_mirror_cmd import (
    cmd_prepare_prod_mirror,
    register_prepare_prod_mirror_parser,
)
from .dbt_promotion_cmd import (
    cmd_materialize_release,
    cmd_render_ci_report,
    cmd_verify_dev_evidence,
    cmd_verify_promotion,
    register_materialize_release_parser,
    register_render_ci_report_parser,
    register_verify_dev_evidence_parser,
    register_verify_promotion_parser,
)
from .dbt_publish_cmd import (
    cmd_check,
    cmd_compile,
    cmd_execute_pack,
    cmd_explain,
    register_check_parser,
    register_compile_parser,
    register_execute_pack_parser,
    register_explain_parser,
)
from .dbt_release_integrity_cmd import (
    cmd_verify_release_checksums,
    cmd_write_release_checksums,
    register_verify_release_checksums_parser,
    register_write_release_checksums_parser,
)
from .dbt_workspace_cmd import workspace_group
from .func_command import CommandGroup, FuncCommand


def dbt_group() -> Command:
    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("dbt", help="Compile dbt model metadata into governed dpone publishing artifacts")

    return CommandGroup(
        name="dbt",
        help="dbt-authored dpone publishing",
        build_parser=build,
        subcommands=[
            workspace_group(),
            FuncCommand("check", register_check_parser, cmd_check),
            FuncCommand("explain", register_explain_parser, cmd_explain),
            FuncCommand("compile", register_compile_parser, cmd_compile),
            FuncCommand(
                "execute-pack",
                register_execute_pack_parser,
                cmd_execute_pack,
                _requires_app_context=False,
            ),
            FuncCommand(
                "verify-promotion",
                register_verify_promotion_parser,
                cmd_verify_promotion,
            ),
            FuncCommand(
                "verify-dev-evidence",
                register_verify_dev_evidence_parser,
                cmd_verify_dev_evidence,
            ),
            FuncCommand(
                "finalize-dev-evidence",
                register_finalize_dev_evidence_parser,
                cmd_finalize_dev_evidence,
            ),
            FuncCommand(
                "prepare-dev-evidence-request",
                register_prepare_dev_evidence_request_parser,
                cmd_prepare_dev_evidence_request,
            ),
            FuncCommand(
                "run-dev-evidence-campaign",
                register_run_dev_evidence_campaign_parser,
                cmd_run_dev_evidence_campaign,
            ),
            FuncCommand(
                "verify-dev-evidence-integrity",
                register_verify_dev_evidence_integrity_parser,
                cmd_verify_dev_evidence_integrity,
            ),
            FuncCommand(
                "materialize-release",
                register_materialize_release_parser,
                cmd_materialize_release,
            ),
            FuncCommand(
                "render-ci-report",
                register_render_ci_report_parser,
                cmd_render_ci_report,
            ),
            FuncCommand(
                "write-release-checksums",
                register_write_release_checksums_parser,
                cmd_write_release_checksums,
            ),
            FuncCommand(
                "verify-release-checksums",
                register_verify_release_checksums_parser,
                cmd_verify_release_checksums,
            ),
            FuncCommand(
                "prepare-prod-mirror",
                register_prepare_prod_mirror_parser,
                cmd_prepare_prod_mirror,
            ),
        ],
        subdest="dbt_cmd",
    )


__all__ = ["dbt_group"]
