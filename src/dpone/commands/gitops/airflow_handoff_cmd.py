"""Airflow runner handoff CLI parsers and thin service adapters."""

from __future__ import annotations

import argparse
import logging
import os
from importlib import import_module
from typing import Any, cast

_RUNNER_POLICY_CHOICES = ("advisory", "pr", "release")


def _load(module_name: str) -> Any:
    return import_module(module_name)


def _outcome_mode_choices() -> tuple[str, ...]:
    return cast(tuple[str, ...], _load("dpone.gitops.airflow_outcome_gate").airflow_outcome_mode_names())


def register_render_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("render", help="Render Airflow Kubernetes runner artifacts from a GitOps bundle")
    p.add_argument("bundle_path", help="Repo-relative gitops.bundle JSON path")
    p.add_argument("--output-dir", default=".dpone/gitops/airflow", help="Repo-relative output directory")
    p.add_argument("--image", required=True, help="Custom dpone runner image")
    p.add_argument("--image-digest", help="Optional immutable image digest for generated image contract")
    p.add_argument("--dpone-version", help="dpone version baked into the image")
    p.add_argument("--python-version", help="Python version baked into the image")
    p.add_argument("--airflow-provider-version", help="apache-airflow-providers-cncf-kubernetes version")
    p.add_argument("--tool", action="append", default=[], help="Executable expected in the image; repeatable")
    p.add_argument("--user", help="Container user")
    p.add_argument("--workdir", help="Container working directory")
    p.add_argument("--entrypoint", help="Container entrypoint path")
    p.add_argument("--image-contract", help="Repo-relative image contract path")
    p.add_argument("--namespace", default="default", help="Kubernetes namespace for generated examples")
    p.add_argument("--service-account", default="default", help="Kubernetes service account for generated examples")
    p.add_argument("--dag-id", default="dpone_gitops", help="Generated Airflow DAG id")
    p.add_argument("--task-id", default="dpone_gitops_task", help="Generated Airflow task id")
    p.add_argument(
        "--outcome-mode",
        choices=_outcome_mode_choices(),
        default="strict_fail",
        help="Pod exit strategy for final XCom outcomes",
    )
    p.add_argument("--require-attestation", action="store_true", help="Require bundle attestation before rendering")
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def register_doctor_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("doctor", help="Validate Airflow Kubernetes runner artifacts offline")
    p.add_argument("bundle_path", help="Repo-relative gitops.bundle JSON path")
    p.add_argument("--pod-template", required=True, help="Repo-relative Airflow pod_template_file path")
    p.add_argument("--image-contract", required=True, help="Repo-relative custom dpone image contract JSON")
    p.add_argument("--image", help="Expected custom dpone image")
    p.add_argument("--require-attestation", action="store_true", help="Require bundle attestation")
    p.add_argument(
        "--runner-policy",
        choices=_RUNNER_POLICY_CHOICES,
        default="advisory",
        help="Airflow runner policy profile",
    )
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def register_run_spec_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("run-spec", help="Build an Airflow runtime contract from a GitOps bundle")
    p.add_argument("bundle_path", help="Repo-relative gitops.bundle JSON path")
    p.add_argument(
        "--output-path",
        default=".dpone/gitops/airflow/run-spec.json",
        help="Repo-relative run-spec JSON artifact path",
    )
    p.add_argument(
        "--evidence-output",
        default=".dpone/gitops/airflow/runtime-evidence.json",
        help="Repo-relative runtime evidence artifact path",
    )
    p.add_argument("--image", required=True, help="Custom dpone runner image")
    p.add_argument("--image-digest", help="Optional immutable image digest")
    p.add_argument("--worktree", default=".", help="Repo-relative worktree path inside the runner pod")
    p.add_argument("--require-attestation", action="store_true", help="Require bundle attestation")
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def register_run_spec_exec_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("run-spec-exec", help="Execute an Airflow run-spec and write runtime evidence")
    p.add_argument("run_spec_path", help="Repo-relative Airflow run-spec JSON path")
    p.add_argument(
        "--evidence-output",
        default=".dpone/gitops/airflow/runtime-evidence.json",
        help="Repo-relative runtime evidence artifact path",
    )
    p.add_argument(
        "--xcom-output",
        default=".dpone/gitops/airflow/xcom-summary.json",
        help="Repo-relative final XCom summary artifact path",
    )
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def register_evidence_verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("evidence-verify", help="Verify Airflow runtime evidence against a run-spec")
    p.add_argument("evidence_path", help="Repo-relative runtime evidence JSON path")
    p.add_argument("--run-spec-path", required=True, help="Repo-relative Airflow run-spec JSON path")
    p.add_argument("--require-all-steps", action="store_true", help="Require evidence for every required run-spec step")
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def register_image_contract_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("image-contract", help="Write a custom dpone runner image contract")
    p.add_argument("--image", required=True, help="Custom dpone runner image")
    p.add_argument("--image-digest", help="Optional immutable image digest")
    p.add_argument("--dpone-version", help="dpone version baked into the image")
    p.add_argument("--python-version", help="Python version baked into the image")
    p.add_argument("--airflow-provider-version", help="apache-airflow-providers-cncf-kubernetes version")
    p.add_argument("--tool", action="append", default=[], help="Executable expected in the image; repeatable")
    p.add_argument("--user", help="Container user")
    p.add_argument("--workdir", help="Container working directory")
    p.add_argument("--entrypoint", help="Container entrypoint path")
    p.add_argument(
        "--output-path",
        default=".dpone/gitops/airflow/image-contract.json",
        help="Repo-relative image contract JSON path",
    )
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def cmd_gitops_airflow_render(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    service_module = _load("dpone.services.gitops.airflow_render_service")
    view_module = _load("dpone.commands.gitops.airflow_view_output")
    rendering = _load("dpone.gitops.airflow_rendering")
    view = service_module.GitOpsAirflowRenderService(ctx=cast(Any, ctx)).build_view(args)
    return view_module.emit_airflow_view(
        ctx=ctx, args=args, view=view, markdown_renderer=rendering.render_gitops_airflow_render_markdown
    )


def cmd_gitops_airflow_doctor(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    service_module = _load("dpone.services.gitops.airflow_doctor_service")
    view_module = _load("dpone.commands.gitops.airflow_view_output")
    rendering = _load("dpone.gitops.airflow_rendering")
    view = service_module.GitOpsAirflowDoctorService(ctx=cast(Any, ctx)).build_view(args)
    return view_module.emit_airflow_view(
        ctx=ctx, args=args, view=view, markdown_renderer=rendering.render_gitops_airflow_doctor_markdown
    )


def cmd_gitops_airflow_run_spec(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    service_module = _load("dpone.services.gitops.airflow_runtime_service")
    view_module = _load("dpone.commands.gitops.airflow_view_output")
    rendering = _load("dpone.gitops.airflow_rendering")
    view = service_module.GitOpsAirflowRunSpecService(ctx=cast(Any, ctx)).build_view(args)
    return view_module.emit_airflow_view(
        ctx=ctx, args=args, view=view, markdown_renderer=rendering.render_gitops_airflow_run_spec_markdown
    )


def cmd_gitops_airflow_run_spec_exec(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
    runner: Any | None = None,
) -> int:
    _ = logger
    contracts = _load("dpone.contracts.airflow_run_identity")
    service_module = _load("dpone.services.gitops.airflow_runtime_exec_service")
    view_module = _load("dpone.commands.gitops.airflow_view_output")
    rendering = _load("dpone.gitops.airflow_rendering")
    view = service_module.GitOpsAirflowRunSpecExecService(
        ctx=cast(Any, ctx),
        runner=runner,
        run_identity_json=os.environ.get(contracts.AIRFLOW_RUN_IDENTITY_ENV),
    ).build_view(args)
    return view_module.emit_airflow_view(
        ctx=ctx,
        args=args,
        view=view,
        markdown_renderer=rendering.render_gitops_airflow_runtime_evidence_markdown,
    )


def cmd_gitops_airflow_evidence_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    service_module = _load("dpone.services.gitops.airflow_runtime_service")
    view_module = _load("dpone.commands.gitops.airflow_view_output")
    rendering = _load("dpone.gitops.airflow_rendering")
    view = service_module.GitOpsAirflowEvidenceVerifyService(ctx=cast(Any, ctx)).build_view(args)
    return view_module.emit_airflow_view(
        ctx=ctx,
        args=args,
        view=view,
        markdown_renderer=rendering.render_gitops_airflow_runtime_evidence_markdown,
    )


def cmd_gitops_airflow_image_contract(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    service_module = _load("dpone.services.gitops.airflow_image_contract_service")
    view_module = _load("dpone.commands.gitops.airflow_view_output")
    rendering = _load("dpone.gitops.airflow_rendering")
    view = service_module.GitOpsAirflowImageContractService(ctx=cast(Any, ctx)).build_view(args)
    return view_module.emit_airflow_view(
        ctx=ctx,
        args=args,
        view=view,
        markdown_renderer=rendering.render_gitops_airflow_image_contract_markdown,
    )


__all__ = [
    "cmd_gitops_airflow_doctor",
    "cmd_gitops_airflow_evidence_verify",
    "cmd_gitops_airflow_image_contract",
    "cmd_gitops_airflow_render",
    "cmd_gitops_airflow_run_spec",
    "cmd_gitops_airflow_run_spec_exec",
    "register_doctor_parser",
    "register_evidence_verify_parser",
    "register_image_contract_parser",
    "register_render_parser",
    "register_run_spec_exec_parser",
    "register_run_spec_parser",
]
