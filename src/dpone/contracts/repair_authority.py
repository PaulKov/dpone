"""Immutable one-shot authority for exceptional snapshot reconciliation.

The authority is provisioned by the environment owner, referenced by one run,
and consumed transactionally by the MSSQL state service.  It is deliberately
separate from workload manifests so promotion cannot grant a standing repair
permission.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

_UTC = timezone.utc  # noqa: UP017 - mypy baseline supports Python 3.10 stubs.


class RepairAuthorityError(RuntimeError):
    """Fail-closed repair admission error with a stable diagnostic code."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class ExpectedCheckpoint:
    """Checkpoint predicate frozen when the environment owner approves repair."""

    absent: bool
    xmin: int | None
    revision: int | None

    def __post_init__(self) -> None:
        if self.absent:
            if self.xmin is not None or self.revision is not None:
                raise RepairAuthorityError("repair_authority.expected_checkpoint_invalid")
            return
        if self.xmin is None or self.revision is None or self.xmin < 0 or self.revision < 0:
            raise RepairAuthorityError("repair_authority.expected_checkpoint_invalid")

    def matches(self, checkpoint: Any | None) -> bool:
        if self.absent:
            return checkpoint is None
        return bool(
            checkpoint is not None
            and int(getattr(checkpoint, "xmin_value", -1)) == self.xmin
            and int(getattr(checkpoint, "revision", -1)) == self.revision
        )

    def canonical_value(self) -> str | dict[str, int]:
        if self.absent:
            return "absent"
        assert self.xmin is not None and self.revision is not None
        return {"revision": self.revision, "xmin": self.xmin}


@dataclass(frozen=True, slots=True)
class RepairAllowance:
    """Strict upper bounds for the exceptional mutations one run may perform."""

    full_baseline: bool
    max_delete_rows: int | None
    max_delete_ratio: float | None

    def __post_init__(self) -> None:
        if self.max_delete_rows is not None and self.max_delete_rows < 0:
            raise RepairAuthorityError("repair_authority.allowance_invalid")
        if self.max_delete_ratio is not None and not 0 <= self.max_delete_ratio <= 1:
            raise RepairAuthorityError("repair_authority.allowance_invalid")

    def permits_delete_override(
        self,
        *,
        missing_rows: int,
        missing_ratio: float,
        configured_max_rows: int,
        configured_max_ratio: float,
    ) -> bool:
        rows_exceeded = missing_rows > configured_max_rows
        ratio_exceeded = missing_ratio > configured_max_ratio
        return bool(
            (not rows_exceeded or (self.max_delete_rows is not None and missing_rows <= self.max_delete_rows))
            and (not ratio_exceeded or (self.max_delete_ratio is not None and missing_ratio <= self.max_delete_ratio))
        )


@dataclass(frozen=True, slots=True)
class TargetAuthorityTransfer:
    """Exact active checkpoint owner that one baseline may supersede."""

    state_key: bytes
    xmin: int
    revision: int

    def __post_init__(self) -> None:
        if len(self.state_key) != 32 or self.xmin < 0 or self.revision < 0:
            raise RepairAuthorityError("repair_authority.transfer_binding_invalid")

    def canonical_value(self) -> dict[str, int | str]:
        """Return the stable digest representation of the old authority."""

        return {
            "revision": self.revision,
            "state_key": self.state_key.hex(),
            "xmin": self.xmin,
        }


@dataclass(frozen=True, slots=True)
class RepairAuthority:
    """Canonical immutable approval loaded under the target transaction lock."""

    authority_id: str
    state_key: bytes
    expected_checkpoint: ExpectedCheckpoint
    scope_hash: str
    reason: str
    expires_at_utc: datetime
    allow: RepairAllowance
    authority_digest: str
    transfer_from: TargetAuthorityTransfer | None = None

    def __post_init__(self) -> None:
        if not self.authority_id or len(self.authority_id) > 128:
            raise RepairAuthorityError("repair_authority.identity_invalid")
        if len(self.state_key) != 32 or not self.scope_hash.startswith("sha256:"):
            raise RepairAuthorityError("repair_authority.binding_invalid")
        if not self.reason.strip() or len(self.reason) > 2048:
            raise RepairAuthorityError("repair_authority.reason_invalid")
        if self.expires_at_utc.tzinfo is None:
            raise RepairAuthorityError("repair_authority.expiry_invalid")
        if self.transfer_from is not None:
            if (
                self.transfer_from.state_key == self.state_key
                or not self.expected_checkpoint.absent
                or not self.allow.full_baseline
            ):
                raise RepairAuthorityError("repair_authority.transfer_binding_invalid")
        if self.authority_digest != self.digest():
            raise RepairAuthorityError("repair_authority.digest_mismatch")

    @classmethod
    def from_record(cls, row: Mapping[str, Any]) -> RepairAuthority:
        """Parse one exact MSSQL row without applying any admission decision."""

        expires = row.get("expires_at_utc")
        if not isinstance(expires, datetime):
            raise RepairAuthorityError("repair_authority.expiry_invalid")
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=_UTC)
        raw_state_key = row.get("state_key")
        if isinstance(raw_state_key, bytes):
            state_key = raw_state_key
        elif isinstance(raw_state_key, (bytearray, memoryview)):
            state_key = bytes(raw_state_key)
        else:
            raise RepairAuthorityError("repair_authority.binding_invalid")
        transfer_from = _transfer_from_record(row)
        return cls(
            authority_id=str(row.get("authority_id") or ""),
            state_key=state_key,
            expected_checkpoint=ExpectedCheckpoint(
                absent=bool(row.get("expected_checkpoint_absent")),
                xmin=_optional_int(row.get("expected_xmin")),
                revision=_optional_int(row.get("expected_revision")),
            ),
            scope_hash=str(row.get("scope_hash") or ""),
            reason=str(row.get("reason") or ""),
            expires_at_utc=expires.astimezone(_UTC),
            allow=RepairAllowance(
                full_baseline=bool(row.get("allow_full_baseline")),
                max_delete_rows=_optional_int(row.get("allow_max_delete_rows")),
                max_delete_ratio=_optional_float(row.get("allow_max_delete_ratio")),
            ),
            authority_digest=str(row.get("authority_digest") or ""),
            transfer_from=transfer_from,
        )

    def digest(self) -> str:
        return repair_authority_digest(
            authority_id=self.authority_id,
            state_key=self.state_key,
            expected_checkpoint=self.expected_checkpoint,
            scope_hash=self.scope_hash,
            reason=self.reason,
            expires_at_utc=self.expires_at_utc,
            allow=self.allow,
            transfer_from=self.transfer_from,
        )


def repair_authority_digest(
    *,
    authority_id: str,
    state_key: bytes,
    expected_checkpoint: ExpectedCheckpoint,
    scope_hash: str,
    reason: str,
    expires_at_utc: datetime,
    allow: RepairAllowance,
    transfer_from: TargetAuthorityTransfer | None = None,
) -> str:
    """Build the digest stored by the environment authority provisioner."""

    payload = {
        "allow": {
            "full_baseline": allow.full_baseline,
            "max_delete_ratio": allow.max_delete_ratio,
            "max_delete_rows": allow.max_delete_rows,
        },
        "authority_id": authority_id,
        "expected_checkpoint_or_absent": expected_checkpoint.canonical_value(),
        "expires_at_utc": _canonical_timestamp(expires_at_utc),
        "reason": reason,
        "scope_hash": scope_hash,
        "state_key": state_key.hex(),
        "transfer_from": transfer_from.canonical_value() if transfer_from is not None else None,
    }
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class RepairAuthorityUse:
    """Observed exceptional action bound to one load and commit receipt."""

    authority: RepairAuthority
    load_id: str
    receipt_id: str
    used_full_baseline: bool
    observed_delete_rows: int
    observed_delete_ratio: float


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(_UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _optional_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _transfer_from_record(row: Mapping[str, Any]) -> TargetAuthorityTransfer | None:
    raw_key = row.get("transfer_from_state_key")
    raw_xmin = row.get("transfer_from_xmin")
    raw_revision = row.get("transfer_from_revision")
    present = (raw_key is not None, raw_xmin is not None, raw_revision is not None)
    if not any(present):
        return None
    if not all(present):
        raise RepairAuthorityError("repair_authority.transfer_binding_invalid")
    assert raw_xmin is not None and raw_revision is not None
    if isinstance(raw_key, bytes):
        state_key = raw_key
    elif isinstance(raw_key, (bytearray, memoryview)):
        state_key = bytes(raw_key)
    else:
        raise RepairAuthorityError("repair_authority.transfer_binding_invalid")
    return TargetAuthorityTransfer(
        state_key=state_key,
        xmin=int(raw_xmin),
        revision=int(raw_revision),
    )


__all__ = [
    "ExpectedCheckpoint",
    "RepairAllowance",
    "RepairAuthority",
    "RepairAuthorityError",
    "RepairAuthorityUse",
    "TargetAuthorityTransfer",
    "repair_authority_digest",
]
