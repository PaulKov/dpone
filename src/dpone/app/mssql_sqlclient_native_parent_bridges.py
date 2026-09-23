"""Strict bridges from native parent v4 evidence to settlement services."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from dpone.contracts.mssql_native_parent_journal import (
    NativeCheckpointReceipt,
    NativeParentAuthority,
    NativeParentRetirementReceipt,
    canonical_digest,
)
from dpone.contracts.mssql_sqlclient_native_chunk import SqlClientNativeChunkProjection
from dpone.ports.mssql_native_chunk_retirement_authority import NativeChunkRetirementRequest
from dpone.ports.mssql_native_route_backend import (
    NativeCheckpointRequest,
    NativeInputCustodyReceipt,
    NativeInputCustodyRequest,
    NativeParentSettlementBinding,
    custody_request_digest,
)
from dpone.services.mssql_native_chunk_retirement import NativeChunkRetirementService


class _PublicationJournalV4(Protocol):
    @property
    def data(self) -> dict[str, Any] | None: ...

    def state(self) -> dict[str, Any] | None: ...
    def authority(self) -> NativeParentAuthority | None: ...
    def abort_required(self) -> None: ...
    def abort_confirmed(self, receipt: dict[str, Any]) -> NativeParentAuthority: ...


class DurableInputCustody(Protocol):
    """Serialize one request-bound effect and its durable receipt.

    Implementations must persist the returned receipt before acknowledging it.
    Re-entry, including from another process, must return that byte-identical
    receipt or fail closed without invoking ``advance`` again.
    """

    def observe_or_advance(
        self,
        request_sha256: str,
        advance: Callable[[], NativeInputCustodyReceipt],
    ) -> NativeInputCustodyReceipt: ...


class SqlClientParentJournalBridge:
    """Expose exact v4 settlement identity and authoritative abort ordering."""

    def __init__(
        self,
        journal: _PublicationJournalV4,
        *,
        fence: Callable[[], int],
        rollback_no_commit: Callable[[], dict[str, Any]],
    ) -> None:
        if not callable(fence) or not callable(rollback_no_commit):
            raise ValueError("mssql_native.sqlclient_parent_bridge_invalid")
        self._journal = journal
        self._fence = fence
        self._rollback_no_commit = rollback_no_commit

    def settlement_binding(self) -> NativeParentSettlementBinding:
        data = self._journal.data
        if not isinstance(data, dict) or data.get("version") != 4:
            raise ValueError("mssql_native.parent_settlement_binding_invalid")
        identity = data.get("identity")
        chunks = data.get("chunks")
        fence = self._fence()
        if not isinstance(identity, dict) or not isinstance(chunks, dict):
            raise ValueError("mssql_native.parent_settlement_binding_invalid")
        target_id = identity.get("target_id")
        window = identity.get("window_fingerprint")
        if type(target_id) is not str or type(window) is not str:
            raise ValueError("mssql_native.parent_settlement_binding_invalid")
        return NativeParentSettlementBinding(
            target_id=target_id,
            window_fingerprint=window,
            fence=fence,
            chunk_count=len(chunks),
        )

    def abort(self) -> NativeParentAuthority:
        """Persist intent before the injected authoritative rollback observation."""
        state = self._journal.state()
        if state is None or state.get("phase") not in {"prepared", "publishing", "abort_required", "aborted"}:
            raise RuntimeError("mssql_native.sqlclient_parent_abort_unavailable")
        if state["phase"] == "aborted":
            authority = self._journal.authority()
            if authority is None or authority.kind != "aborted":
                raise RuntimeError("mssql_native.sqlclient_parent_abort_unavailable")
            return authority
        if state["phase"] != "abort_required":
            self._journal.abort_required()
        receipt = self._rollback_no_commit()
        if type(receipt) is not dict or not receipt:
            raise RuntimeError("mssql_native.sqlclient_parent_abort_unavailable")
        return self._journal.abort_confirmed(receipt)


class SqlClientParentChunkRetirer:
    """Issue a request only from currently observed parent authorization."""

    def __init__(
        self,
        authorizations: object,
        service: NativeChunkRetirementService,
        prepare: Callable[[NativeChunkRetirementRequest], None],
    ) -> None:
        if type(service) is not NativeChunkRetirementService or not callable(prepare):
            raise ValueError("mssql_native.sqlclient_parent_bridge_invalid")
        self._authorizations = authorizations
        self._service = service
        self._prepare = prepare

    def retire(self, projection: SqlClientNativeChunkProjection, authority: NativeParentAuthority):
        observe = getattr(self._authorizations, "observe", None)
        if not callable(observe):
            raise ValueError("mssql_native.sqlclient_parent_bridge_invalid")
        authorization = observe(projection)
        if authorization is None or authorization.authority != authority:
            raise RuntimeError("mssql_native.chunk_retirement_authorization_unavailable")
        request = NativeChunkRetirementRequest(projection, authorization)
        self._prepare(request)
        return self._service.retire(request)


class SqlClientParentInputCustody:
    """Release the ordered retired parent input with request-bound idempotence."""

    def __init__(
        self,
        retirement: Callable[[], NativeParentRetirementReceipt | None],
        release: Callable[[NativeParentRetirementReceipt], str],
        durable: DurableInputCustody,
    ) -> None:
        if (
            not callable(retirement)
            or not callable(release)
            or not callable(getattr(durable, "observe_or_advance", None))
        ):
            raise ValueError("mssql_native.sqlclient_parent_bridge_invalid")
        self._retirement = retirement
        self._release = release
        self._durable = durable

    def release(self, request: NativeInputCustodyRequest) -> NativeInputCustodyReceipt:
        if type(request) is not NativeInputCustodyRequest:
            raise ValueError("mssql_native.input_custody_request_invalid")
        request_sha = custody_request_digest(request)
        retirement = self._retirement()
        if retirement is None or retirement.digest != request.retirement_digest:
            raise RuntimeError("mssql_native.input_custody_retirement_unsettled")

        def advance() -> NativeInputCustodyReceipt:
            return NativeInputCustodyReceipt(request_sha, self._release(retirement))

        try:
            receipt = self._durable.observe_or_advance(request_sha, advance)
        except Exception as exc:
            raise RuntimeError("mssql_native.input_custody_outcome_unknown") from exc
        if type(receipt) is not NativeInputCustodyReceipt or receipt.request_sha256 != request_sha:
            raise ValueError("mssql_native.input_custody_receipt_invalid")
        return receipt


class SqlClientParentCheckpoint:
    """Validate a backend CAS receipt against the exact parent request."""

    def __init__(self, advance: Callable[[NativeCheckpointRequest], NativeCheckpointReceipt]) -> None:
        if not callable(advance):
            raise ValueError("mssql_native.sqlclient_parent_bridge_invalid")
        self._advance = advance

    def advance(self, request: NativeCheckpointRequest) -> NativeCheckpointReceipt:
        if type(request) is not NativeCheckpointRequest:
            raise ValueError("mssql_native.checkpoint_request_invalid")
        receipt = self._advance(request)
        if type(receipt) is not NativeCheckpointReceipt or (
            receipt.parent_retirement_digest,
            receipt.target_id,
            receipt.window_fingerprint,
            receipt.fence,
        ) != (request.retirement_digest, request.target_id, request.window_fingerprint, request.fence):
            raise ValueError("mssql_native.checkpoint_receipt_invalid")
        return receipt


def terminal_parent_result(state: object) -> NativeCheckpointReceipt:
    """Return only an exact durable succeeded checkpoint receipt."""
    if not isinstance(state, dict) or state.get("phase") != "succeeded":
        raise RuntimeError("mssql_native.parent_terminal_evidence_missing")
    raw = state.get("checkpoint_receipt")
    receipt = NativeCheckpointReceipt.from_dict(raw)
    if state.get("checkpoint_receipt_digest") != canonical_digest(receipt.to_dict()):
        raise RuntimeError("mssql_native.parent_terminal_evidence_missing")
    return receipt


__all__ = (
    "DurableInputCustody",
    "SqlClientParentCheckpoint",
    "SqlClientParentChunkRetirer",
    "SqlClientParentInputCustody",
    "SqlClientParentJournalBridge",
    "terminal_parent_result",
)
