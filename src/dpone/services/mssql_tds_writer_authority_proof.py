"""Closed immutable lineage proof for the private SQLClient writer stages."""

from __future__ import annotations

from hashlib import sha256
from typing import Any, Never

from dpone.services.mssql_tds_original_continuation import PreparationTransition
from dpone.services.mssql_tds_permission_grant import PermissionGrantHeldOwner
from dpone.services.mssql_tds_permission_grant_association import PermissionGrantAssociation
from dpone.services.mssql_tds_permission_grant_release import PermissionGrantLocallyReleased
from dpone.services.mssql_tds_permission_grant_settlement import _SettlementOwner
from dpone.services.mssql_tds_restricted_writer_settlement import _Owner
from dpone.services.mssql_tds_restricted_writer_verification import _VerifyOwner
from dpone.services.mssql_tds_restricted_writer_verify_coordinator import RestrictedWriterVerifyRetained
from dpone.services.mssql_tds_writer_admission import RestrictedWriterVerified
from dpone.services.mssql_tds_writer_authority_proof_contract import _PROOF_TOKEN, _WriterAuthorityProof
from dpone.services.mssql_tds_writer_contracts import (
    RestrictedWriterSettlementReceipt,
    SqlClientObserverAdmission,
    canonical_json_bytes,
)
from dpone.services.mssql_tds_writer_grant_origin import capture_writer_grant_result

ERROR = "mssql_native.sqlclient_writer_authority_unknown"


def _invalid() -> Never:
    raise ValueError(ERROR)


def _lineage_digest(tokens: tuple[int, ...], grant_sha256: str) -> str:
    return sha256(canonical_json_bytes({"grant_sha256": grant_sha256, "object_tokens": tokens})).hexdigest()


def _capture_writer(owner: Any) -> SqlClientObserverAdmission:
    try:
        completion = object.__getattribute__(owner, "_completion")
        writer = object.__getattribute__(
            object.__getattribute__(object.__getattribute__(completion, "request"), "plan"), "writer_admission"
        )
    except (AttributeError, TypeError):
        _invalid()
    if type(writer) is not SqlClientObserverAdmission:
        _invalid()
    writer.__post_init__()
    return writer


def mint_writer_authority_proof(
    terminal: Any,
    owner: Any,
    verify: Any,
    transition: Any,
    *,
    fence: object,
) -> _WriterAuthorityProof:
    """Seal the exact live P7-P9 graph once at its trusted producer."""
    retained, association = owner.retained, verify._association
    refs, capture = owner._refs, terminal._association_capture
    if (
        type(terminal) is not RestrictedWriterVerified
        or type(owner) is not _Owner
        or type(retained) is not RestrictedWriterVerifyRetained
        or type(verify) is not _VerifyOwner
        or type(association) is not PermissionGrantAssociation
        or type(transition) is not PreparationTransition
        or type(refs) is not tuple
        or type(capture) is not tuple
        or fence is None
    ):
        _invalid()
    grant_receipt, grant_sha256 = capture_writer_grant_result(association)
    tokens = tuple(id(value) for value in (terminal, owner, retained, verify, association, transition, *refs, *capture))
    proof = _WriterAuthorityProof(
        _PROOF_TOKEN,
        fence,
        terminal,
        owner,
        retained,
        verify,
        association,
        transition,
        refs,
        capture,
        grant_receipt,
        grant_sha256,
        tokens,
        _lineage_digest(tokens, grant_sha256),
    )
    assert_writer_authority(proof, terminal=terminal, owner=owner, transition=transition, fence=fence)
    return proof


def assert_writer_authority(
    proof: _WriterAuthorityProof,
    *,
    terminal: Any,
    owner: Any,
    transition: Any,
    fence: object,
    require_writer: bool = False,
) -> tuple[RestrictedWriterSettlementReceipt | None, SqlClientObserverAdmission | None, object, str]:
    """Fail closed when the sealed lineage is foreign, mutated or replayed."""
    if type(proof) is not _WriterAuthorityProof or proof._token is not _PROOF_TOKEN:
        _invalid()
    refs, capture = owner._refs, terminal._association_capture
    current_tokens = tuple(
        id(value)
        for value in (
            terminal,
            owner,
            owner.retained,
            owner.retained._owner,
            owner.retained._owner._association,
            transition,
            *refs,
            *capture,
        )
    )
    grant_receipt, grant_sha256 = capture_writer_grant_result(proof.association)
    receipt = object.__getattribute__(owner, "_receipt")
    grant_owner = object.__getattribute__(proof.association, "_verify_owner_ref")
    grant_local = object.__getattribute__(grant_owner, "local") if type(grant_owner) is _SettlementOwner else None
    grant_held = (
        object.__getattribute__(grant_local, "held_owner")
        if type(grant_local) is PermissionGrantLocallyReleased
        else None
    )
    if (
        fence is not proof.fence
        or terminal is not proof.terminal
        or owner is not proof.owner
        or transition is not proof.transition
        or owner.phase != "SETTLED"
        or owner.unknown
        or owner.busy
        or owner.retained is not proof.retained
        or proof.retained._owner is not proof.verify
        or proof.verify._association is not proof.association
        or type(grant_owner) is not _SettlementOwner
        or type(grant_local) is not PermissionGrantLocallyReleased
        or type(grant_held) is not PermissionGrantHeldOwner
        or len(refs) != 13
        or refs[0] is not owner.claim
        or refs[1] is not proof.retained
        or refs[2] is not proof.verify
        or refs[3] is not proof.association
        or refs[4] is not proof.verify._request
        or refs[5] is not proof.verify._result
        or refs[6] is not proof.verify._reservation
        or refs[7] is not proof.verify._registration
        or type(refs[8]) is not tuple
        or len(refs[8]) != len(proof.verify._receipts)
        or any(value is not exact for value, exact in zip(proof.verify._receipts, refs[8], strict=True))
        or refs[9] is not owner.origin
        or refs[10] is not owner.coordinator
        or refs[11] is not owner.evidence
        or refs[12] is not owner.operations
        or owner.origin is not proof.association
        or refs is not proof.owner_refs
        or capture is not proof.association_capture
        or proof.association._capture is not capture
        or len(capture) < 2
        or capture[0] is not transition.attempt
        or capture[1] is not transition
        or transition.attempt._permission_grant_owner is not proof.association
        or transition.attempt._prepared_origin is not transition
        or transition._origin is not transition._captured_origin
        or transition._origin is not terminal._preparation_origin
        or transition._origin._writer_inputs is not terminal._writer_inputs
        or owner._terminal is not terminal
        or terminal._owner is not owner
        or grant_receipt is not proof.grant_receipt
        or grant_sha256 != proof.grant_sha256
        or current_tokens != proof.object_tokens
        or _lineage_digest(current_tokens, grant_sha256) != proof.lineage_sha256
    ):
        _invalid()
    if not require_writer:
        return None, None, proof.grant_receipt, proof.grant_sha256
    if type(receipt) is not RestrictedWriterSettlementReceipt or terminal[1] is not receipt:
        _invalid()
    receipt.__post_init__()
    return receipt, _capture_writer(owner), proof.grant_receipt, proof.grant_sha256


__all__ = ()
