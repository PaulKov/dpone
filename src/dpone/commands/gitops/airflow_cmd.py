"""Airflow GitOps command group composition root."""

from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any

from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.gitops.release_composition_cmd import (
    cmd_release_compose,
    cmd_release_inventory,
    register_release_compose_parser,
    register_release_inventory_parser,
)


def _load(module_name: str) -> Any:
    return import_module(module_name)


def airflow_group() -> CommandGroup:
    subcommands = [
        FuncCommand("deps", register_deps_parser, cmd_gitops_airflow_deps),
        FuncCommand("render", register_render_parser, cmd_gitops_airflow_render),
        FuncCommand("run-spec", register_run_spec_parser, cmd_gitops_airflow_run_spec),
        FuncCommand("runtime-profile", register_runtime_profile_parser, cmd_gitops_airflow_runtime_profile),
        FuncCommand("pod-contract", register_pod_contract_parser, cmd_gitops_airflow_pod_contract),
        FuncCommand("pod-doctor", register_pod_doctor_parser, cmd_gitops_airflow_pod_doctor),
        FuncCommand(
            "connection-bridge-plan",
            register_connection_bridge_plan_parser,
            cmd_gitops_airflow_connection_bridge_plan,
        ),
        FuncCommand("cluster-doctor", register_cluster_doctor_parser, cmd_gitops_airflow_cluster_doctor),
        FuncCommand("k8s-manifests", register_k8s_manifests_parser, cmd_gitops_airflow_k8s_manifests),
        FuncCommand("admission-check", register_admission_check_parser, cmd_gitops_airflow_admission_check),
        FuncCommand("pack", register_pack_parser, cmd_gitops_airflow_pack),
        FuncCommand("reconcile", register_reconcile_parser, cmd_gitops_airflow_reconcile),
        FuncCommand(
            "release-materialize",
            register_release_materialize_parser,
            cmd_gitops_airflow_release_materialize,
        ),
        FuncCommand("release-compose", register_release_compose_parser, cmd_release_compose),
        FuncCommand("release-inventory", register_release_inventory_parser, cmd_release_inventory),
        FuncCommand("publish", register_publish_parser, cmd_gitops_airflow_publish),
        FuncCommand("artifact-index", register_artifact_index_parser, cmd_gitops_airflow_artifact_index),
        FuncCommand("preflight", register_preflight_parser, cmd_gitops_airflow_preflight),
        FuncCommand("outcome-gate", register_outcome_gate_parser, cmd_gitops_airflow_outcome_gate),
        FuncCommand("k8s-smoke", register_k8s_smoke_parser, cmd_gitops_airflow_k8s_smoke),
        FuncCommand("pod-watch", register_pod_watch_parser, cmd_gitops_airflow_pod_watch),
        FuncCommand("evidence-bundle", register_evidence_bundle_parser, cmd_gitops_airflow_evidence_bundle),
        FuncCommand("run-spec-exec", register_run_spec_exec_parser, cmd_gitops_airflow_run_spec_exec),
        FuncCommand("xcom-from-evidence", register_xcom_from_evidence_parser, cmd_gitops_airflow_xcom_from_evidence),
        FuncCommand("evidence-verify", register_evidence_verify_parser, cmd_gitops_airflow_evidence_verify),
        FuncCommand("doctor", register_doctor_parser, cmd_gitops_airflow_doctor),
        FuncCommand("image-contract", register_image_contract_parser, cmd_gitops_airflow_image_contract),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser(
            "airflow",
            help="Render and validate Airflow Kubernetes runner handoff artifacts",
        )

    return CommandGroup(
        name="airflow",
        help="Airflow Kubernetes runner helpers",
        build_parser=build,
        subcommands=subcommands,
        subdest="gitops_airflow_cmd",
    )


def register_deps_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_deps_cmd").register_deps_parser(subparsers)


def cmd_gitops_airflow_deps(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_deps_cmd").cmd_gitops_airflow_deps(args, ctx=ctx, logger=logger)


def register_runtime_profile_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_runtime_profile_cmd").register_runtime_profile_parser(subparsers)


def cmd_gitops_airflow_runtime_profile(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_runtime_profile_cmd").cmd_gitops_airflow_runtime_profile(
        args, ctx=ctx, logger=logger
    )


def register_pod_contract_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_pod_cmd").register_pod_contract_parser(subparsers)


def register_pod_doctor_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_pod_cmd").register_pod_doctor_parser(subparsers)


def cmd_gitops_airflow_pod_contract(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_pod_cmd").cmd_gitops_airflow_pod_contract(args, ctx=ctx, logger=logger)


def cmd_gitops_airflow_pod_doctor(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_pod_cmd").cmd_gitops_airflow_pod_doctor(args, ctx=ctx, logger=logger)


def register_connection_bridge_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_connection_bridge_plan_cmd").register_connection_bridge_plan_parser(
        subparsers
    )


def cmd_gitops_airflow_connection_bridge_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_connection_bridge_plan_cmd").cmd_gitops_airflow_connection_bridge_plan(
        args, ctx=ctx, logger=logger
    )


def register_cluster_doctor_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_cluster_doctor_cmd").register_cluster_doctor_parser(subparsers)


def cmd_gitops_airflow_cluster_doctor(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
    runner: Any | None = None,
) -> int:
    return _load("dpone.commands.gitops.airflow_cluster_doctor_cmd").cmd_gitops_airflow_cluster_doctor(
        args, ctx=ctx, logger=logger, runner=runner
    )


def register_k8s_manifests_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_k8s_manifests_cmd").register_k8s_manifests_parser(subparsers)


def cmd_gitops_airflow_k8s_manifests(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_k8s_manifests_cmd").cmd_gitops_airflow_k8s_manifests(
        args, ctx=ctx, logger=logger
    )


def register_admission_check_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_admission_check_cmd").register_admission_check_parser(subparsers)


def cmd_gitops_airflow_admission_check(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
    runner: Any | None = None,
) -> int:
    return _load("dpone.commands.gitops.airflow_admission_check_cmd").cmd_gitops_airflow_admission_check(
        args, ctx=ctx, logger=logger, runner=runner
    )


def register_pack_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_pack_cmd").register_pack_parser(subparsers)


def register_reconcile_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_pack_cmd").register_reconcile_parser(subparsers)


def register_publish_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_pack_cmd").register_publish_parser(subparsers)


def register_release_materialize_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_compact_pack_release_cmd").register_release_materialize_parser(
        subparsers
    )


def cmd_gitops_airflow_pack(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_pack_cmd").cmd_gitops_airflow_pack(args, ctx=ctx, logger=logger)


def cmd_gitops_airflow_reconcile(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_pack_cmd").cmd_gitops_airflow_reconcile(args, ctx=ctx, logger=logger)


def cmd_gitops_airflow_release_materialize(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_compact_pack_release_cmd").cmd_gitops_airflow_release_materialize(
        args, ctx=ctx, logger=logger
    )


def cmd_gitops_airflow_publish(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_pack_cmd").cmd_gitops_airflow_publish(args, ctx=ctx, logger=logger)


def register_artifact_index_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_preflight_cmd").register_artifact_index_parser(subparsers)


def register_preflight_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_preflight_cmd").register_preflight_parser(subparsers)


def cmd_gitops_airflow_artifact_index(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_preflight_cmd").cmd_gitops_airflow_artifact_index(
        args, ctx=ctx, logger=logger
    )


def cmd_gitops_airflow_preflight(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_preflight_cmd").cmd_gitops_airflow_preflight(
        args, ctx=ctx, logger=logger
    )


def register_outcome_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_outcome_cmd").register_outcome_gate_parser(subparsers)


def cmd_gitops_airflow_outcome_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_outcome_cmd").cmd_gitops_airflow_outcome_gate(
        args, ctx=ctx, logger=logger
    )


def register_k8s_smoke_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_k8s_smoke_cmd").register_k8s_smoke_parser(subparsers)


def cmd_gitops_airflow_k8s_smoke(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
    runner: Any | None = None,
) -> int:
    return _load("dpone.commands.gitops.airflow_k8s_smoke_cmd").cmd_gitops_airflow_k8s_smoke(
        args, ctx=ctx, logger=logger, runner=runner
    )


def register_pod_watch_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_pod_launch_evidence_cmd").register_pod_watch_parser(subparsers)


def cmd_gitops_airflow_pod_watch(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
    runner: Any | None = None,
) -> int:
    return _load("dpone.commands.gitops.airflow_pod_launch_evidence_cmd").cmd_gitops_airflow_pod_watch(
        args, ctx=ctx, logger=logger, runner=runner
    )


def register_evidence_bundle_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_evidence_bundle_cmd").register_evidence_bundle_parser(subparsers)


def cmd_gitops_airflow_evidence_bundle(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_evidence_bundle_cmd").cmd_gitops_airflow_evidence_bundle(
        args, ctx=ctx, logger=logger
    )


def register_xcom_from_evidence_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_xcom_cmd").register_xcom_from_evidence_parser(subparsers)


def cmd_gitops_airflow_xcom_from_evidence(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_xcom_cmd").cmd_gitops_airflow_xcom_from_evidence(
        args, ctx=ctx, logger=logger
    )


def register_render_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_handoff_cmd").register_render_parser(subparsers)


def register_doctor_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_handoff_cmd").register_doctor_parser(subparsers)


def register_run_spec_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_handoff_cmd").register_run_spec_parser(subparsers)


def register_run_spec_exec_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_handoff_cmd").register_run_spec_exec_parser(subparsers)


def register_evidence_verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_handoff_cmd").register_evidence_verify_parser(subparsers)


def register_image_contract_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _load("dpone.commands.gitops.airflow_handoff_cmd").register_image_contract_parser(subparsers)


def cmd_gitops_airflow_render(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_handoff_cmd").cmd_gitops_airflow_render(args, ctx=ctx, logger=logger)


def cmd_gitops_airflow_doctor(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_handoff_cmd").cmd_gitops_airflow_doctor(args, ctx=ctx, logger=logger)


def cmd_gitops_airflow_run_spec(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_handoff_cmd").cmd_gitops_airflow_run_spec(args, ctx=ctx, logger=logger)


def cmd_gitops_airflow_run_spec_exec(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
    runner: Any | None = None,
) -> int:
    return _load("dpone.commands.gitops.airflow_handoff_cmd").cmd_gitops_airflow_run_spec_exec(
        args, ctx=ctx, logger=logger, runner=runner
    )


def cmd_gitops_airflow_evidence_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_handoff_cmd").cmd_gitops_airflow_evidence_verify(
        args, ctx=ctx, logger=logger
    )


def cmd_gitops_airflow_image_contract(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    return _load("dpone.commands.gitops.airflow_handoff_cmd").cmd_gitops_airflow_image_contract(
        args, ctx=ctx, logger=logger
    )


__all__ = [
    "airflow_group",
    "cmd_gitops_airflow_artifact_index",
    "cmd_gitops_airflow_doctor",
    "cmd_gitops_airflow_evidence_verify",
    "cmd_gitops_airflow_image_contract",
    "cmd_gitops_airflow_k8s_smoke",
    "cmd_gitops_airflow_outcome_gate",
    "cmd_gitops_airflow_pack",
    "cmd_gitops_airflow_pod_contract",
    "cmd_gitops_airflow_pod_doctor",
    "cmd_gitops_airflow_pod_watch",
    "cmd_gitops_airflow_preflight",
    "cmd_gitops_airflow_connection_bridge_plan",
    "cmd_gitops_airflow_cluster_doctor",
    "cmd_gitops_airflow_publish",
    "cmd_gitops_airflow_reconcile",
    "cmd_gitops_airflow_release_materialize",
    "cmd_gitops_airflow_render",
    "cmd_gitops_airflow_runtime_profile",
    "cmd_gitops_airflow_run_spec",
    "cmd_gitops_airflow_run_spec_exec",
    "register_artifact_index_parser",
    "register_doctor_parser",
    "register_evidence_verify_parser",
    "register_image_contract_parser",
    "register_k8s_smoke_parser",
    "register_outcome_gate_parser",
    "register_pack_parser",
    "register_pod_contract_parser",
    "register_pod_doctor_parser",
    "register_pod_watch_parser",
    "register_preflight_parser",
    "register_connection_bridge_plan_parser",
    "register_cluster_doctor_parser",
    "register_publish_parser",
    "register_reconcile_parser",
    "register_release_materialize_parser",
    "register_render_parser",
    "register_runtime_profile_parser",
    "register_run_spec_exec_parser",
    "register_run_spec_parser",
]
