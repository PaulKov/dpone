"""Bind a deployment checkpoint CAS to exact durable parent evidence."""

from __future__ import annotations

from typing import Protocol

from dpone.adapters.mssql_sqlclient_checkpoint import SqlClientCheckpointCas
from dpone.contracts.mssql_native_parent_journal import NativeParentRetirementReceipt
from dpone.ports.mssql_native_route_backend import (
    NativeCheckpointReceipt,
    NativeCheckpointRequest,
    NativeParentSettlementBinding,
)


class _CheckpointParentJournal(Protocol):
    """Read the durable facts that authorize one parent checkpoint effect."""

    def settlement_binding(self) -> NativeParentSettlementBinding: ...

    def retirement_receipt(self) -> NativeParentRetirementReceipt | None: ...


class SqlClientParentCheckpointBridge:
    """Re-read and bind retired parent evidence before invoking checkpoint CAS."""

    def __init__(self, *, journal: _CheckpointParentJournal, checkpoint: SqlClientCheckpointCas) -> None:
        if not callable(getattr(journal, "settlement_binding", None)) or not callable(
            getattr(journal, "retirement_receipt", None)
        ):
            raise ValueError("mssql_native.parent_checkpoint_bridge_invalid")
        if type(checkpoint) is not SqlClientCheckpointCas:
            raise ValueError("mssql_native.parent_checkpoint_bridge_invalid")
        self._journal = journal
        self._checkpoint = checkpoint

    def advance(self, request: NativeCheckpointRequest) -> NativeCheckpointReceipt:
        """Commit only while the durable parent facts still equal the request."""
        if type(request) is not NativeCheckpointRequest:
            raise ValueError("mssql_native.checkpoint_request_invalid")
        retirement = self._journal.retirement_receipt()
        binding = self._journal.settlement_binding()
        if type(retirement) is not NativeParentRetirementReceipt or type(binding) is not NativeParentSettlementBinding:
            raise RuntimeError("mssql_native.parent_checkpoint_evidence_changed")
        observed = (
            retirement.digest,
            binding.target_id,
            binding.window_fingerprint,
            binding.fence,
        )
        expected = (
            request.retirement_digest,
            request.target_id,
            request.window_fingerprint,
            request.fence,
        )
        if observed != expected:
            raise RuntimeError("mssql_native.parent_checkpoint_evidence_changed")
        return self._checkpoint.commit(
            retirement,
            target_id=request.target_id,
            window_fingerprint=request.window_fingerprint,
            fence=request.fence,
        )


__all__ = ("SqlClientParentCheckpointBridge",)
