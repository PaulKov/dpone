"""Locked deployment identity query for MSSQL activation receipts."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, Protocol, cast

from dpone.contracts.dbt_semantic_refresh_activation import (
    SemanticRefreshActivationAuthorityReceipt,
)

_UTC = timezone.utc  # noqa: UP017 - package type-checks against Python 3.10
_RECEIPT_FIELDS = {
    "activation_authority_receipt_sha256",
    "authority_store_ref",
    "baseline_receipts",
    "deployment_id",
    "persisted_at",
    "plan_bundle_sha256",
    "release_id",
    "route_certification_receipt_sha256",
    "runtime_assurance_receipts",
    "schema",
}


class _IdentityCursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _IdentityCursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...


class _ReceiptCursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _ReceiptCursor: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...


def activation_deployment_identity_conflicts(
    cursor: _IdentityCursor,
    *,
    activation_authority_table: str,
    deployment_id: str,
    release_id: str,
    authority_store_ref: str,
) -> bool:
    """Lock a deployment receipt range and detect cross-release/store reuse."""

    cursor.execute(
        f"""
SELECT TOP (1) release_id, authority_store_ref, status
FROM {activation_authority_table} WITH (UPDLOCK, HOLDLOCK)
WHERE deployment_id COLLATE Latin1_General_100_BIN2
      = CONVERT(varchar(71), ?) COLLATE Latin1_General_100_BIN2
  AND (
      release_id COLLATE Latin1_General_100_BIN2
          <> CONVERT(varchar(71), ?) COLLATE Latin1_General_100_BIN2
      OR authority_store_ref COLLATE Latin1_General_100_BIN2
          <> CONVERT(nvarchar(512), ?) COLLATE Latin1_General_100_BIN2
      OR status COLLATE Latin1_General_100_BIN2
          <> N'ACTIVE' COLLATE Latin1_General_100_BIN2
  )
ORDER BY plan_bundle_sha256;
""".strip(),
        deployment_id,
        release_id,
        authority_store_ref,
    )
    return cursor.fetchone() is not None


def activation_receipt_is_exact(
    cursor: _ReceiptCursor,
    *,
    activation_authority_table: str,
    deployment_id: str,
    release_id: str,
    plan_bundle_sha256: str,
    authority_store_ref: str,
    activation_authority_receipt_sha256: str,
) -> bool:
    """Lock and authenticate the one receipt identified by deployment and plan."""

    cursor.execute(
        activation_receipt_query(activation_authority_table),
        deployment_id,
        plan_bundle_sha256,
    )
    rows = tuple(tuple(row) for row in cursor.fetchall())
    if len(rows) != 1:
        return False
    try:
        receipt = activation_receipt_from_row(
            rows[0],
            deployment_id=deployment_id,
            release_id=release_id,
            plan_bundle_sha256=plan_bundle_sha256,
            authority_store_ref=authority_store_ref,
        )
    except (TypeError, ValueError):
        return False
    return receipt.activation_authority_receipt_sha256 == activation_authority_receipt_sha256


def activation_receipt_query(activation_authority_table: str) -> str:
    """Return the binary-exact locked composite receipt query."""

    return f"""
SELECT deployment_id, release_id, plan_bundle_sha256, authority_store_ref,
       authority_receipt_json, activation_authority_receipt_sha256,
       persisted_at, status
FROM {activation_authority_table} WITH (UPDLOCK, HOLDLOCK)
WHERE deployment_id COLLATE Latin1_General_100_BIN2
          = CONVERT(varchar(71), ?) COLLATE Latin1_General_100_BIN2
  AND plan_bundle_sha256 COLLATE Latin1_General_100_BIN2
          = CONVERT(varchar(71), ?) COLLATE Latin1_General_100_BIN2;
""".strip()


def activation_receipt_from_row(
    row: Sequence[object],
    *,
    deployment_id: str,
    release_id: str,
    plan_bundle_sha256: str,
    authority_store_ref: str,
) -> SemanticRefreshActivationAuthorityReceipt:
    """Reconstruct and authenticate one full locked receipt row."""

    if len(row) != 8 or row[7] != "ACTIVE" or not isinstance(row[4], str):
        raise ValueError("ACTIVE deployment authority receipt row is invalid")
    receipt = activation_receipt_from_json(row[4])
    if (
        row[0] != deployment_id
        or row[1] != release_id
        or row[2] != plan_bundle_sha256
        or row[3] != authority_store_ref
        or row[5] != receipt.activation_authority_receipt_sha256
        or receipt.release_id != release_id
        or receipt.deployment_id != deployment_id
        or receipt.plan_bundle_sha256 != plan_bundle_sha256
        or receipt.authority_store_ref != authority_store_ref
        or normalized_activation_value(row[6]) != normalized_activation_value(receipt.persisted_at)
        or canonical_activation_json(receipt.to_dict()) != row[4]
    ):
        raise ValueError("deployment authority receipt differs from protected storage")
    return receipt


def activation_receipt_from_json(raw: str) -> SemanticRefreshActivationAuthorityReceipt:
    """Parse a closed receipt document and revalidate its canonical digest."""

    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError("activation receipt JSON is invalid") from exc
    if not isinstance(value, dict) or set(value) != _RECEIPT_FIELDS:
        raise ValueError("activation receipt fields are not closed")
    baselines = value.get("baseline_receipts")
    assurances = value.get("runtime_assurance_receipts")
    if not isinstance(baselines, list) or not isinstance(assurances, list):
        raise ValueError("activation receipt closures are invalid")
    return SemanticRefreshActivationAuthorityReceipt(
        release_id=cast(str, value["release_id"]),
        deployment_id=cast(str, value["deployment_id"]),
        plan_bundle_sha256=cast(str, value["plan_bundle_sha256"]),
        authority_store_ref=cast(str, value["authority_store_ref"]),
        baseline_receipts=_baseline_pairs(baselines),
        route_certification_receipt_sha256=cast(
            str,
            value["route_certification_receipt_sha256"],
        ),
        runtime_assurance_receipts=_assurance_triples(assurances),
        persisted_at=cast(str, value["persisted_at"]),
        activation_authority_receipt_sha256=cast(
            str,
            value["activation_authority_receipt_sha256"],
        ),
        schema=cast(str, value["schema"]),
    )


def canonical_activation_json(value: dict[str, object]) -> str:
    """Serialize one activation document to the sole canonical storage form."""

    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def normalized_activation_value(value: object) -> object:
    """Normalize SQL datetime values for exact replay comparison."""

    if isinstance(value, str) and "T" in value and value.endswith("Z"):
        try:
            value = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
        except ValueError:
            return value
    if not isinstance(value, datetime):
        return value
    aware = value.replace(tzinfo=_UTC) if value.tzinfo is None else value.astimezone(_UTC)
    return aware.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _baseline_pairs(values: list[object]) -> tuple[tuple[str, str], ...]:
    result: list[tuple[str, str]] = []
    for item in values:
        if not isinstance(item, list) or len(item) != 2 or any(not isinstance(value, str) for value in item):
            raise ValueError("activation baseline closure is invalid")
        result.append((cast(str, item[0]), cast(str, item[1])))
    return tuple(result)


def _assurance_triples(values: list[object]) -> tuple[tuple[str, str, str], ...]:
    result: list[tuple[str, str, str]] = []
    for item in values:
        if not isinstance(item, list) or len(item) != 3 or any(not isinstance(value, str) for value in item):
            raise ValueError("activation assurance closure is invalid")
        result.append((cast(str, item[0]), cast(str, item[1]), cast(str, item[2])))
    return tuple(result)


__all__ = [
    "activation_receipt_from_json",
    "activation_receipt_from_row",
    "activation_deployment_identity_conflicts",
    "activation_receipt_query",
    "activation_receipt_is_exact",
    "canonical_activation_json",
    "normalized_activation_value",
]
