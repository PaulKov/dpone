"""Application facade for migration environment promotion artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from dpone.readiness.migration_control import MigrationPack, read_json_object
from dpone.readiness.schema_migration_promotion import (
    MigrationEnvironmentContract,
    MigrationEnvironmentVerifier,
    MigrationPromotionGate,
    MigrationPromotionPlanner,
)


@dataclass(frozen=True, slots=True)
class MigrationApplyEnvironmentContext:
    ledger_path: str
    actual_path: str | None
    target_connection_path: str | None
    environment: str | None = None
    promotion_id: str | None = None
    blockers: tuple[str, ...] = ()


class MigrationPromotionFacade:
    """Thin file-IO facade for promotion CLI commands."""

    def verify_env(
        self,
        *,
        pack_path: str,
        environment: str,
        environment_contract_path: str,
        actual_path: str | None = None,
    ) -> dict[str, Any]:
        pack = _read_pack(pack_path)
        contract = MigrationEnvironmentContract.from_file(environment_contract_path)
        return MigrationEnvironmentVerifier().verify(
            pack=pack,
            contract=contract,
            environment=environment,
            actual_path=actual_path,
        )

    def certify(
        self,
        *,
        pack_path: str,
        environment: str,
        environment_contract_path: str,
        actual_path: str | None = None,
    ) -> tuple[int, dict[str, Any]]:
        pack = _read_pack(pack_path)
        contract = MigrationEnvironmentContract.from_file(environment_contract_path)
        payload = MigrationEnvironmentVerifier().certify(
            pack=pack,
            contract=contract,
            environment=environment,
            actual_path=actual_path,
        )
        return (2 if payload.get("blockers") else 0), payload

    def promote(
        self,
        *,
        pack_path: str,
        from_environment: str,
        to_environment: str,
        certificate_path: str,
        environment_contract_path: str,
        approval_path: str | None = None,
    ) -> tuple[int, dict[str, Any]]:
        pack = _read_pack(pack_path)
        contract = MigrationEnvironmentContract.from_file(environment_contract_path)
        payload = MigrationPromotionPlanner().promote(
            pack=pack,
            contract=contract,
            from_environment=from_environment,
            to_environment=to_environment,
            certificate=_read_object(certificate_path),
            approval=_read_optional_object(approval_path),
        )
        return (2 if payload.get("blockers") else 0), payload


def resolve_apply_environment_context(
    *,
    pack: MigrationPack,
    ledger_path: str,
    actual_path: str | None,
    target_connection_path: str | None,
    environment: str | None,
    environment_contract_path: str | None,
    promotion_path: str | None,
) -> MigrationApplyEnvironmentContext:
    if not environment:
        return MigrationApplyEnvironmentContext(
            ledger_path=ledger_path,
            actual_path=actual_path,
            target_connection_path=target_connection_path,
        )
    if not environment_contract_path:
        return MigrationApplyEnvironmentContext(
            ledger_path=ledger_path,
            actual_path=actual_path,
            target_connection_path=target_connection_path,
            environment=environment,
            blockers=("migration_environment.contract_required",),
        )
    contract = MigrationEnvironmentContract.from_file(environment_contract_path)
    env = contract.environment(environment)
    promotion = _read_optional_object(promotion_path)
    blockers = MigrationPromotionGate().evaluate(
        pack=pack,
        contract=contract,
        environment=environment,
        promotion=promotion,
    )
    return MigrationApplyEnvironmentContext(
        ledger_path=str(env.ledger),
        actual_path=str(env.actual) if env.actual and env.actual.exists() else actual_path,
        target_connection_path=str(env.target_connection) if env.target_connection else target_connection_path,
        environment=environment,
        promotion_id=str(promotion.get("promotion_id")) if promotion and promotion.get("promotion_id") else None,
        blockers=blockers,
    )


def _read_pack(path: str) -> MigrationPack:
    return MigrationPack.from_mapping(read_json_object(path))


def _read_optional_object(path: str | None) -> dict[str, Any] | None:
    return _read_object(path) if path else None


def _read_object(path: str | Path) -> dict[str, Any]:
    full_path = Path(path)
    raw_text = full_path.read_text(encoding="utf-8")
    raw = json.loads(raw_text) if full_path.suffix.lower() == ".json" else yaml.safe_load(raw_text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{full_path} must contain an object")
    return dict(raw)


__all__ = [
    "MigrationApplyEnvironmentContext",
    "MigrationPromotionFacade",
    "resolve_apply_environment_context",
]
