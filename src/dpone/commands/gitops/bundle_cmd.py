from __future__ import annotations

import argparse
import logging
from typing import cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.gitops.rendering import render_gitops_bundle_markdown, render_gitops_bundle_verify_markdown
from dpone.services.gitops.bundle_service import GitOpsBundleContext, GitOpsBundleService
from dpone.services.gitops.bundle_verify_service import GitOpsBundleVerifyContext, GitOpsBundleVerifyService

_POLICY_PROFILE_CHOICES = ("custom", "advisory", "pr", "release")


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("bundle", help="Build a deterministic GitOps scheduler handoff bundle")
    p.add_argument("bundle_action", nargs="?", choices=["verify"], help="Use 'verify' to check an emitted bundle")
    p.add_argument("bundle_path", nargs="?", help="Repo-relative bundle.json path for 'verify'")
    p.add_argument("--changed-files", nargs="+", default=[], help="Repo-relative files changed by Git or CI")
    p.add_argument("--changed-files-file", help="Repo-relative newline-delimited changed-files list")
    p.add_argument("--from-ref", help="Git ref used as the left side of git diff --name-only")
    p.add_argument("--to-ref", help="Git ref used as the right side of git diff --name-only")
    p.add_argument("--workload-root", help="Workload root that bounds manifest discovery")
    p.add_argument(
        "--manifest-glob",
        default="manifests/**/*.yaml",
        help="Manifest glob evaluated under the workload root",
    )
    p.add_argument("--include-global-overrides", action="store_true", help="Include overrides/global.yaml")
    p.add_argument(
        "--include-env-overrides", action="append", default=[], metavar="ENV", help="Include overrides/<env>.yaml"
    )
    p.add_argument("--include-registry", action="store_true", help="Include the workload registry/ directory")
    p.add_argument("--registry", action="append", default=[], help="Include an explicit registry path")
    p.add_argument("--support-path", action="append", default=[], help="Include an extra support file or directory")
    p.add_argument(
        "--runner",
        choices=["generic", "airflow", "kubernetes_pod_operator", "github_actions", "dagster"],
        default="generic",
        help="Runner family used for suggested plan commands",
    )
    p.add_argument("--worktree", default=".", help="Repo-relative sparse worktree to verify")
    p.add_argument("--verify-lock", action="store_true", help="Verify SHA-256 file digests from emitted plan locks")
    p.add_argument(
        "--fail-on-empty-impact", action="store_true", help="Return a blocker when no manifests are impacted"
    )
    p.add_argument("--fail-on-warnings", action="store_true", help="Return a blocker when warnings are present")
    p.add_argument("--require-lock", action="store_true", help="Require lock verification for the bundle policy gate")
    p.add_argument(
        "--policy-profile",
        choices=_POLICY_PROFILE_CHOICES,
        default="custom",
        help="Policy profile defaults for bundle gates",
    )
    p.add_argument("--attest", action="store_true", help="Embed SHA-256 artifact attestation in bundle.json")
    p.add_argument(
        "--require-attestation",
        action="store_true",
        help="Require bundle attestation when running 'dpone gitops bundle verify'",
    )
    p.add_argument(
        "--output-dir",
        default=".dpone/gitops/bundle",
        help="Repo-relative directory for bundle artifacts",
    )
    p.add_argument("--output", help="Optional repo-relative output artifact path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def cmd_gitops_bundle(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    if getattr(args, "bundle_action", None) == "verify":
        view = GitOpsBundleVerifyService(ctx=cast(GitOpsBundleVerifyContext, ctx)).build_view(args)
        if getattr(args, "format", "json") == "markdown":
            rendered = render_gitops_bundle_verify_markdown(view.report)
            write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
            write_text(rendered)
            return view.exit_code
        payload = view.to_jsonable()
        rendered = dumps_json(payload)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_json(payload)
        return view.exit_code

    view = GitOpsBundleService(ctx=cast(GitOpsBundleContext, ctx)).build_view(args)
    if getattr(args, "format", "json") == "markdown":
        rendered = render_gitops_bundle_markdown(view.report)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


__all__ = ["cmd_gitops_bundle", "register_parser"]
