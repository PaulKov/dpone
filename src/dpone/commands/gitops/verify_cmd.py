from __future__ import annotations

import argparse
import logging
from typing import cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.gitops.rendering import render_gitops_verify_markdown
from dpone.services.gitops.verify_service import GitOpsVerifyContext, GitOpsVerifyService


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("verify", help="Verify a sparse worktree against a GitOps plan")
    p.add_argument("plan", help="Path to a GitOps plan JSON artifact")
    p.add_argument("--worktree", default=".", help="Repo-relative sparse worktree to verify")
    p.add_argument("--verify-lock", action="store_true", help="Verify SHA-256 file digests from the plan lock")
    p.add_argument("--output", help="Optional repo-relative output artifact path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def cmd_gitops_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    view = GitOpsVerifyService(ctx=cast(GitOpsVerifyContext, ctx)).build_view(args)
    if getattr(args, "format", "json") == "markdown":
        rendered = render_gitops_verify_markdown(view.report)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


__all__ = ["cmd_gitops_verify", "register_parser"]
