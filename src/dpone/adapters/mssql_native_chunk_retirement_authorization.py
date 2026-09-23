"""Read-only authorization projection from one exact native parent journal v4."""

from __future__ import annotations

from typing import Protocol

from dpone.adapters.mssql_native_v4_snapshot_validation import validate_native_v4_snapshot
from dpone.adapters.mssql_sqlclient_native_receipt_contracts import (
    native,
    validate_native_chunk_receipt,
)
from dpone.ports.mssql_native_chunk_retirement_authority import (
    NativeChunkRetirementAuthorization,
    _issue_native_chunk_retirement_authorization,
)


class NativeParentJournalV4(Protocol):
    @property
    def data(self) -> dict | None: ...


class NativeChunkRetirementJournalObserver:
    """Derive authority only from a validated, currently observed v4 record."""

    def __init__(self, journal: NativeParentJournalV4) -> None:
        self._journal = journal
        self._issued: dict[int, tuple[NativeChunkRetirementAuthorization, str]] = {}

    def observe(self, projection: object) -> NativeChunkRetirementAuthorization | None:
        if type(projection) is not native.SqlClientNativeChunkProjection:
            raise ValueError("mssql_native.chunk_retirement_journal_invalid")
        validated = self._validated(projection)
        if validated is None:
            return None
        data, authority, snapshot_sha256 = validated
        raw = data["chunks"].get(str(projection.attempt.ordinal))
        if not isinstance(raw, dict) or not isinstance(raw.get("receipt"), dict):
            return None
        try:
            receipt = native.NativeChunkReceipt(**raw["receipt"])
            evidence = validate_native_chunk_receipt(receipt, projection=projection)
            identity = data["identity"]
            if evidence.plan_sha256 != native.canonical_digest(identity) or evidence.window_fingerprint != identity.get(
                "window_fingerprint"
            ):
                return None
            authorization = _issue_native_chunk_retirement_authorization(
                authority=authority,
                parent_journal_sha256=snapshot_sha256,
                parent_identity_sha256=projection.attempt.plan_sha256,
                projection_sha256=projection.projection_sha256,
                chunk_receipt=receipt,
            )
            self._issued[id(authorization)] = (authorization, snapshot_sha256)
            return authorization
        except (TypeError, ValueError, KeyError, native.WindowContractError):
            return None

    def verify(self, projection: object, authorization: NativeChunkRetirementAuthorization) -> bool:
        """Re-read the journal and compare its exact current authority."""
        if (
            type(projection) is not native.SqlClientNativeChunkProjection
            or type(authorization) is not NativeChunkRetirementAuthorization
        ):
            return False
        issued = self._issued.get(id(authorization))
        validated = self._validated(projection)
        return (
            issued is not None
            and issued[0] is authorization
            and validated is not None
            and issued[1] == validated[2]
            and authorization.parent_journal_sha256 == validated[2]
            and authorization.authority == validated[1]
        )

    def _validated(
        self, projection: native.SqlClientNativeChunkProjection
    ) -> tuple[dict, native.NativeParentAuthority, str] | None:
        data = self._journal.data
        if not isinstance(data, dict) or data.get("version") != 4:
            return None
        try:
            if set(data) != {
                "version",
                "identity",
                "phase",
                "chunks",
                "complete",
                "observations",
                "publication",
                "completion_metadata",
                "rollback_history",
                "limits",
            }:
                return None
            identity = data["identity"]
            if not isinstance(identity, dict):
                return None
            validate_native_v4_snapshot(data, identity)
            publication = data["publication"]
            phase = publication["phase"]
            if phase not in {"published", "aborted", "retirement_required", "retiring"}:
                return None
            authority = native.NativeParentAuthority(**publication["authority"])
            if native.canonical_digest(data["identity"]) != projection.attempt.plan_sha256:
                return None
            return data, authority, native.canonical_digest(data)
        except (TypeError, ValueError, KeyError, native.WindowContractError):
            return None


__all__ = ("NativeChunkRetirementJournalObserver",)
