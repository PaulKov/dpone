from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from .command_handlers_release_context import release_ops as _ops
from .command_helpers import _emit, _parse_artifacts, _parse_post_checks


def cmd_go_live_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    payload = json.loads(Path(args.bundle_json).read_text(encoding="utf-8"))
    decision = _ops(ctx).go_live_gate().evaluate(_ops(ctx).evidence_bundle_from_payload(payload))
    _emit(decision.to_dict(), decision.to_markdown(), args.format)
    return 0 if decision.passed else 1


def cmd_policy_evaluate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    bundle_payload = json.loads(Path(args.bundle_json).read_text(encoding="utf-8"))
    policy_payload = json.loads(args.policy_json)
    decision = _ops(ctx).policy().evaluate(_ops(ctx).evidence_bundle_from_payload(bundle_payload), policy_payload)
    _emit(decision.to_dict(), decision.to_markdown(), args.format)
    return 0 if decision.passed else 1


def cmd_diff(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .diff()
        .compare(
            source_rows=json.loads(args.source_rows_json),
            target_rows=json.loads(args.target_rows_json),
            key_columns=[item.strip() for item in args.key.split(",") if item.strip()],
            compare_columns=[item.strip() for item in args.compare_columns.split(",") if item.strip()]
            if args.compare_columns
            else None,
            sample_limit=args.sample_limit,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_security_audit(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    manifest = json.loads(args.manifest_json) if args.manifest_json else None
    report = _ops(ctx).security_audit().audit(manifest=manifest, log_text=args.log_text)
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_slo_evaluate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .slo()
        .evaluate(
            metrics=json.loads(args.metrics_json),
            objectives=json.loads(args.objectives_json),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_incident_pack(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .incident_pack()
        .build(
            artifact_dir=args.artifact_dir,
            incident_id=args.incident_id,
            title=args.title,
            severity=args.severity,
            artifacts=_parse_artifacts(args.artifact),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_release_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .release_gate()
        .evaluate(
            artifact_dir=args.artifact_dir,
            release=args.release,
            artifacts=_parse_artifacts(args.artifact),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_release_rc_finalize(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    required = (
        tuple(args.require_artifact) if args.require_artifact else ("route_release_finalizer", "release_evidence_pack")
    )
    report = (
        _ops(ctx)
        .release_rc_finalizer()
        .finalize(
            output_dir=args.output_dir,
            release=args.release,
            previous_release=args.previous_release,
            package_version=args.package_version,
            merge_train_json=args.merge_train_json,
            mode=args.mode,
            artifacts=_parse_artifacts(args.artifact or []),
            required_artifacts=required,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_release_rc_collect(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    required = (
        tuple(args.require_artifact) if args.require_artifact else ("route_release_finalizer", "release_evidence_pack")
    )
    pull_request_json = tuple((args.pull_request_json or []) + (args.pr_json or []))
    report = (
        _ops(ctx)
        .release_rc_collector()
        .collect(
            output_dir=args.output_dir,
            release=args.release,
            previous_release=args.previous_release,
            package_version=args.package_version,
            base_branch=args.base_branch,
            head_branch=args.head_branch,
            pull_request_json=pull_request_json,
            artifacts=_parse_artifacts(args.artifact or []),
            required_artifacts=required,
            finalizer_output_dir=args.finalizer_output_dir,
            mode=args.mode,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_production_maturity(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .production_maturity()
        .evaluate(
            output_dir=args.output_dir,
            release=args.release,
            artifacts=_parse_artifacts(args.artifact),
            required_domains=tuple(args.require) if args.require else _ops(ctx).default_required_domains,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_industrial_readiness(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .industrial_readiness()
        .evaluate(
            output_dir=args.output_dir,
            release=args.release,
            artifacts=_parse_artifacts(args.artifact),
            required_domains=tuple(args.require) if args.require else _ops(ctx).default_industrial_domains,
            required_matrix_cases=tuple(args.case or ()),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_release_orchestrator(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .release_orchestrator()
        .run(
            output_dir=args.output_dir,
            release=args.release,
            roots=args.root or [".dpone", "test_artifacts"],
            artifacts=_parse_artifacts(args.artifact or []),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_release_promote(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .release_promotion()
        .promote(
            output_dir=args.output_dir,
            release=args.release,
            from_environment=args.from_env,
            to_environment=args.to_env,
            artifacts=_parse_artifacts(args.artifact or []),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_env_drift(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .environment_drift()
        .compare(
            output_dir=args.output_dir,
            source_environment=args.source_env,
            target_environment=args.target_env,
            source_path=args.source,
            target_path=args.target,
            allowlist_paths=tuple(args.allowlist_path or []),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_change_request(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .change_request()
        .create(
            output_dir=args.output_dir,
            change_id=args.change_id,
            release=args.release,
            target_environment=args.target_env,
            risk_level=args.risk_level,
            requested_by=args.requested_by,
            approvers=tuple(args.approver or []),
            artifacts=_parse_artifacts(args.artifact or []),
            expires_at=args.expires_at,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_approval_record(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .approval_record()
        .record(
            output_dir=args.output_dir,
            change_request_path=args.change_request,
            actor=args.actor,
            decision=args.decision,
            comment=args.comment,
            quorum_required=args.quorum_required,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_deployment_record(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .deployment_record()
        .record(
            output_dir=args.output_dir,
            deployment_id=args.deployment_id,
            environment=args.environment,
            actor=args.actor,
            approval_record_path=args.approval_record,
            status=args.status,
            post_checks=_parse_post_checks(args.post_check or []),
            rollback_artifact_path=args.rollback_artifact,
            started_at=args.started_at,
            finished_at=args.finished_at,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_post_deploy_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .post_deploy_verify()
        .verify(
            output_dir=args.output_dir,
            deployment_record_path=args.deployment_record,
            artifacts=_parse_artifacts(args.artifact or []),
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1


def cmd_release_close(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = (
        _ops(ctx)
        .release_close()
        .close(
            output_dir=args.output_dir,
            release=args.release,
            closed_by=args.closed_by,
            post_deploy_verify_path=args.post_deploy_verify,
            notes=args.notes,
        )
    )
    _emit(report.to_dict(), report.to_markdown(), args.format)
    return 0 if report.passed else 1
