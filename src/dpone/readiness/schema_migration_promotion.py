"""Provider-neutral migration promotion contracts.

The module models environment promotion as immutable artifacts. It does not
connect to source-control systems or databases; CI, GitHub, GitLab, Bitbucket
and target adapters can all consume the same receipts.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.readiness.migration_control import MigrationPack, stable_fingerprint
from dpone.readiness.schema_migration_environment import (
    ENVIRONMENT_CONTRACT_SCHEMA,
    ArtifactEnvironmentLedgerStore,
    MigrationEnvironmentContract,
    MigrationEnvironmentPolicy,
    MigrationEnvironmentRef,
    load_mapping_file,
)

ENVIRONMENT_REPORT_SCHEMA = "dpone.schema_migration_environment_report.v1"
ENVIRONMENT_CERTIFICATION_SCHEMA = "dpone.schema_migration_environment_certification.v1"
PROMOTION_SCHEMA = "dpone.schema_migration_promotion.v1"
PROMOTION_APPROVAL_SCHEMA = "dpone.schema_migration_promotion_approval.v1"


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


class MigrationEnvironmentVerifier:
    """Verify and certify that a pack reached an environment."""

    def verify(
        self,
        *,
        pack: MigrationPack,
        contract: MigrationEnvironmentContract,
        environment: str,
        actual_path: str | Path | None = None,
    ) -> dict[str, Any]:
        env = contract.environment(environment)
        actual, actual_blockers = _load_actual(actual_path or env.actual)
        actual_fingerprint = stable_fingerprint(actual) if actual is not None else None
        records = _records(env.ledger)
        ledger_view = _ledger_view(pack, records)
        blockers = [*actual_blockers, *_actual_blockers(pack, actual_fingerprint), *ledger_view["blockers"]]
        return {
            "schema_version": ENVIRONMENT_REPORT_SCHEMA,
            "command": "verify-env",
            "status": "blocked" if blockers else "ok",
            "environment": environment,
            "pack_id": pack.pack_id,
            "target": pack.target.to_dict(),
            "desired_fingerprint": pack.desired_fingerprint,
            "actual_fingerprint": actual_fingerprint,
            "ledger": ledger_view,
            "blockers": blockers,
            "warnings": [],
        }

    def certify(
        self,
        *,
        pack: MigrationPack,
        contract: MigrationEnvironmentContract,
        environment: str,
        actual_path: str | Path | None = None,
    ) -> dict[str, Any]:
        report = self.verify(pack=pack, contract=contract, environment=environment, actual_path=actual_path)
        payload: dict[str, Any] = {
            "schema_version": ENVIRONMENT_CERTIFICATION_SCHEMA,
            "command": "certify",
            "status": "blocked" if report["blockers"] else "certified",
            "environment": environment,
            "pack_id": pack.pack_id,
            "target": pack.target.to_dict(),
            "desired_fingerprint": pack.desired_fingerprint,
            "actual_fingerprint": report.get("actual_fingerprint"),
            "ledger_digest": report["ledger"]["digest"],
            "blockers": list(report["blockers"]),
            "warnings": list(report["warnings"]),
            "created_at": _utc_now(),
        }
        if not payload["blockers"]:
            payload["certification_id"] = stable_fingerprint(
                {
                    "pack_id": pack.pack_id,
                    "environment": environment,
                    "actual_fingerprint": payload["actual_fingerprint"],
                    "ledger_digest": payload["ledger_digest"],
                    "policy": contract.policy.to_dict(),
                }
            )
        return payload


class MigrationPromotionPlanner:
    """Build immutable promotion receipts from a certified source environment."""

    def promote(
        self,
        *,
        pack: MigrationPack,
        contract: MigrationEnvironmentContract,
        from_environment: str,
        to_environment: str,
        certificate: Mapping[str, Any],
        approval: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        blockers = [
            *_certificate_blockers(pack, from_environment, certificate),
            *_chain_blockers(contract, from_environment, to_environment),
            *_approval_blockers(pack, contract, from_environment, to_environment, approval),
        ]
        payload: dict[str, Any] = {
            "schema_version": PROMOTION_SCHEMA,
            "command": "promote",
            "status": "blocked" if blockers else "promoted",
            "pack_id": pack.pack_id,
            "from_environment": from_environment,
            "to_environment": to_environment,
            "certification_id": certificate.get("certification_id"),
            "blockers": blockers,
            "warnings": [],
            "created_at": _utc_now(),
        }
        if not blockers:
            payload["promotion_id"] = stable_fingerprint(
                {
                    "pack_id": pack.pack_id,
                    "from_environment": from_environment,
                    "to_environment": to_environment,
                    "certification_id": certificate.get("certification_id"),
                    "approval": dict(approval) if approval else None,
                    "policy": contract.policy.to_dict(),
                }
            )
            payload["approved_by"] = approval.get("approved_by") if approval else None
        return payload


class MigrationPromotionGate:
    """Pre-apply gate for environment-bound migration apply."""

    def evaluate(
        self,
        *,
        pack: MigrationPack,
        contract: MigrationEnvironmentContract,
        environment: str,
        promotion: Mapping[str, Any] | None,
    ) -> tuple[str, ...]:
        contract.environment(environment)
        if not contract.policy.require_previous_certification or contract.previous_environment(environment) is None:
            return ()
        if not promotion:
            return ("migration_promotion.promotion_required",)
        if promotion.get("schema_version") != PROMOTION_SCHEMA or promotion.get("status") != "promoted":
            return ("migration_promotion.invalid_receipt",)
        if promotion.get("pack_id") != pack.pack_id:
            return ("migration_promotion.pack_id_mismatch",)
        if promotion.get("to_environment") != environment:
            return ("migration_promotion.environment_mismatch",)
        return ()


def _records(path: Path) -> tuple[dict[str, Any], ...]:
    return ArtifactEnvironmentLedgerStore(path).records()


def _ledger_view(pack: MigrationPack, records: tuple[dict[str, Any], ...]) -> dict[str, Any]:
    matched = [record for record in records if record.get("pack_id") == pack.pack_id]
    blockers: list[str] = []
    if matched and matched[-1].get("status") == "failed":
        blockers.append("migration_environment.latest_record_failed")
    if pack.phases:
        applied = [str(record.get("phase")) for record in matched if record.get("status") == "phase_applied"]
        required = [str(phase.get("name")) for phase in pack.phases if phase.get("name")]
        if applied[: len(required)] != required:
            blockers.append("migration_environment.required_phases_not_applied")
    elif not any(record.get("status") == "applied" for record in matched):
        blockers.append("migration_environment.pack_not_applied")
    return {
        "digest": stable_fingerprint(records),
        "matched_records": len(matched),
        "latest_status": str(matched[-1].get("status")) if matched else None,
        "blockers": blockers,
    }


def _actual_blockers(pack: MigrationPack, actual_fingerprint: str | None) -> tuple[str, ...]:
    if pack.actual_fingerprint and actual_fingerprint and pack.actual_fingerprint != actual_fingerprint:
        return ("migration_environment.actual_fingerprint_mismatch",)
    return ()


def _load_actual(path: str | Path | None) -> tuple[dict[str, Any] | None, tuple[str, ...]]:
    if not path:
        return None, ("migration_environment.actual_state_required",)
    full_path = Path(path)
    if not full_path.exists():
        return None, ("migration_environment.actual_state_missing",)
    raw = load_mapping_file(full_path)
    actual = raw.get("actual", raw)
    return dict(actual) if isinstance(actual, Mapping) else None, ()


def _certificate_blockers(pack: MigrationPack, environment: str, cert: Mapping[str, Any]) -> tuple[str, ...]:
    if cert.get("schema_version") != ENVIRONMENT_CERTIFICATION_SCHEMA or cert.get("status") != "certified":
        return ("migration_promotion.invalid_certificate",)
    if cert.get("pack_id") != pack.pack_id:
        return ("migration_promotion.certificate_pack_id_mismatch",)
    if cert.get("environment") != environment:
        return ("migration_promotion.certificate_environment_mismatch",)
    return ()


def _chain_blockers(
    contract: MigrationEnvironmentContract, from_environment: str, to_environment: str
) -> tuple[str, ...]:
    contract.environment(from_environment)
    contract.environment(to_environment)
    if contract.previous_environment(to_environment) != from_environment:
        return ("migration_promotion.invalid_environment_chain",)
    return ()


def _approval_blockers(
    pack: MigrationPack,
    contract: MigrationEnvironmentContract,
    from_environment: str,
    to_environment: str,
    approval: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    if not (contract.policy.prod_requires_approval and contract.is_final_environment(to_environment)):
        return ()
    if not approval:
        return ("migration_promotion.approval_required",)
    if approval.get("pack_id") != pack.pack_id:
        return ("migration_promotion.approval_pack_id_mismatch",)
    if approval.get("from_environment") != from_environment or approval.get("to_environment") != to_environment:
        return ("migration_promotion.approval_environment_mismatch",)
    if "prod_promotion" not in set(approval.get("approved_risks", [])):
        return ("migration_promotion.prod_promotion_not_approved",)
    return ()


__all__ = [
    "ENVIRONMENT_CERTIFICATION_SCHEMA",
    "ENVIRONMENT_CONTRACT_SCHEMA",
    "ENVIRONMENT_REPORT_SCHEMA",
    "PROMOTION_APPROVAL_SCHEMA",
    "PROMOTION_SCHEMA",
    "MigrationEnvironmentContract",
    "MigrationEnvironmentPolicy",
    "MigrationEnvironmentRef",
    "MigrationEnvironmentVerifier",
    "MigrationPromotionGate",
    "MigrationPromotionPlanner",
]
