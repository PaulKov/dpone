"""Source-free v4 parent settlement for the explicit SqlClient route."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.ports.mssql_native_chunk_inspection import NativeChunkInspector
from dpone.ports.mssql_native_route_backend import (
    NativeCheckpointReceipt,
    NativeCheckpointRequest,
    NativeChunkRetirementReceipt,
    NativeInputCustodyReceipt,
    NativeInputCustodyRequest,
    NativeParentAuthority,
    NativeParentRetirementReceipt,
    NativeParentSettlementBinding,
    ParentCheckpointCas,
    ParentChunkRetirer,
    ParentInputCustody,
    ParentSettlementJournal,
    TdsDirectoryLimits,
    custody_request_digest,
)


@dataclass(frozen=True, slots=True)
class NativeParentSettlementResult:
    """Deterministic terminal result reconstructed entirely from durable evidence."""

    authority: NativeParentAuthority
    retirement: NativeParentRetirementReceipt
    checkpoint: NativeCheckpointReceipt


class SqlClientNativeParentSettlement:
    """Resume the first missing durable suffix; unknown states perform no effects."""

    def __init__(
        self,
        *,
        journal: ParentSettlementJournal,
        inspector: NativeChunkInspector,
        retirer: ParentChunkRetirer,
        custody: ParentInputCustody,
        checkpoint: ParentCheckpointCas,
        directory_limits: TdsDirectoryLimits,
    ) -> None:
        self._journal = journal
        self._inspector = inspector
        self._retirer = retirer
        self._custody = custody
        self._checkpoint = checkpoint
        self._limits = directory_limits

    def settle(self) -> NativeParentSettlementResult:
        state = self._state()
        phase = state["phase"]
        identity = self._identity()
        if phase in {"published", "aborted"}:
            authority = self._authority()
            self._journal.retirement_required(authority)
            phase = self._state()["phase"]
        if phase == "retirement_required":
            self._journal.retiring()
            phase = self._state()["phase"]
        retirement: NativeParentRetirementReceipt | None
        if phase == "retiring":
            self._retire_missing(self._authority())
            retirement = self._journal.retired()
            phase = self._state()["phase"]
        else:
            retirement = self._journal.retirement_receipt()
        if phase == "retired":
            if retirement is None:
                raise RuntimeError("mssql_native.parent_retirement_missing")
            custody_request = NativeInputCustodyRequest.bind(retirement, identity)
            custody_receipt = self._custody.release(custody_request)
            if type(
                custody_receipt
            ) is not NativeInputCustodyReceipt or custody_receipt.request_sha256 != custody_request_digest(
                custody_request
            ):
                raise ValueError("mssql_native.input_custody_receipt_invalid")
            self._journal.checkpoint_required()
            phase = self._state()["phase"]
        if phase == "checkpoint_required":
            if retirement is None:
                retirement = self._journal.retirement_receipt()
            if retirement is None:
                raise RuntimeError("mssql_native.parent_retirement_missing")
            checkpoint_request = NativeCheckpointRequest.bind(retirement, identity)
            checkpoint = self._checkpoint.advance(checkpoint_request)
            if (
                type(checkpoint) is not NativeCheckpointReceipt
                or checkpoint.parent_retirement_digest != retirement.digest
                or checkpoint.target_id != identity.target_id
                or checkpoint.window_fingerprint != identity.window_fingerprint
                or checkpoint.fence != identity.fence
            ):
                raise ValueError("mssql_native.checkpoint_receipt_invalid")
            self._journal.succeeded(checkpoint)
            phase = self._state()["phase"]
        if phase != "succeeded":
            raise RuntimeError("mssql_native.parent_settlement_unavailable")
        return self._terminal()

    def _retire_missing(self, authority: NativeParentAuthority) -> None:
        state = self._state()
        completed = state.get("chunk_retirements")
        if type(completed) is not list:
            raise ValueError("mssql_native.parent_retirement_prefix_invalid")
        total = self._identity().chunk_count
        if len(completed) > total:
            raise ValueError("mssql_native.parent_chunk_count_invalid")
        for ordinal in range(len(completed), total):
            # Inspection is mandatory even when the retirer can rehydrate internally:
            # it proves recovery is source-free and the exact terminal still exists.
            projection = self._inspector.inspect(ordinal, self._limits)
            receipt = self._retirer.retire(projection, authority)
            if (
                type(receipt) is not NativeChunkRetirementReceipt
                or receipt.ordinal != ordinal
                or receipt.parent_authority_digest != authority.digest
            ):
                raise ValueError("mssql_native.chunk_retirement_receipt_invalid")
            self._journal.chunk_retired(receipt)

    def _terminal(self) -> NativeParentSettlementResult:
        state = self._state()
        authority = self._authority()
        retirement = self._journal.retirement_receipt()
        raw = state.get("checkpoint_receipt")
        if retirement is None or not isinstance(raw, dict):
            raise RuntimeError("mssql_native.parent_terminal_evidence_missing")
        checkpoint = NativeCheckpointReceipt.from_dict(raw)
        return NativeParentSettlementResult(authority, retirement, checkpoint)

    def _authority(self) -> NativeParentAuthority:
        authority = self._journal.authority()
        if authority is None:
            raise RuntimeError("mssql_native.parent_authority_unsettled")
        return authority

    def _identity(self) -> NativeParentSettlementBinding:
        identity = self._journal.settlement_binding()
        authority = self._journal.authority()
        if type(identity) is not NativeParentSettlementBinding or (
            authority is not None and authority.fence != identity.fence
        ):
            raise ValueError("mssql_native.parent_settlement_binding_invalid")
        return identity

    def _state(self) -> dict[str, Any]:
        state = self._journal.state()
        if state is None or type(state.get("phase")) is not str:
            raise RuntimeError("mssql_native.parent_state_missing")
        return state


__all__ = (
    "NativeParentSettlementResult",
    "ParentCheckpointCas",
    "ParentChunkRetirer",
    "ParentInputCustody",
    "ParentSettlementJournal",
    "SqlClientNativeParentSettlement",
)
