"""One-shot process-local claim gate for SQLClient writer admission."""

import os
from threading import Lock, get_ident
from types import MappingProxyType
from typing import Any

ERROR = "mssql_native.sqlclient_writer_admission_unknown"
P10A_CLAIM_TOKEN = object()


def _is_exact_identity(witness: object, candidate: object) -> bool:
    """Validate the terminal's immutable process-local identity witness."""
    return (
        type(witness) is MappingProxyType
        and len(witness) == 1
        and type(witness.get("owner_id")) is int
        and witness["owner_id"] == id(candidate)
    )


class WriterAdmissionGate:
    __slots__ = ("lock", "state", "claim", "owner")

    def __init__(self, lock: Lock) -> None:
        self.lock, self.state = lock, "AVAILABLE"
        self.claim: object | None = None
        self.owner: object | None = None


class WriterAdmissionClaim(tuple):
    def __new__(cls, token: object, terminal: object, gate: object, lock: object, pid: int, thread_id: int):
        if token is not P10A_CLAIM_TOKEN or type(gate) is not WriterAdmissionGate:
            raise ValueError(ERROR)
        return tuple.__new__(cls, (terminal, gate, lock, pid, thread_id, token))


class SqlClientWriterAdmissionUnknown(RuntimeError):
    """The exact terminal was consumed and cannot safely authorize a retry."""

    def __init__(self) -> None:
        super().__init__(ERROR)


class WriterAdmissionClaimMixin:
    """Consume the exact terminal once on its creating process and thread."""

    def _claim_p10a_once(self: Any) -> object:
        gate, lock, pid, thread_id = self[5:9]
        if os.getpid() != pid or get_ident() != thread_id:
            raise ValueError(ERROR)
        with lock:
            if not _is_exact_identity(self[13], self) or gate.owner is not self or gate.lock is not lock:
                gate.state = "UNKNOWN"
                raise ValueError(ERROR)
            if gate.state != "AVAILABLE" or gate.claim is not None:
                raise ValueError(ERROR)
            claim = WriterAdmissionClaim(P10A_CLAIM_TOKEN, self, gate, lock, pid, thread_id)
            gate.claim, gate.state = claim, "CLAIMED"
            return claim

    def _assert_p10a_claim(self: Any, claim: object) -> Any:
        gate, lock, pid, thread_id = self[5:9]
        with lock:
            if (
                type(claim) is not WriterAdmissionClaim
                or claim is not gate.claim
                or claim[0] is not self
                or claim[1] is not gate
                or claim[2] is not lock
                or claim[3:6] != (pid, thread_id, P10A_CLAIM_TOKEN)
                or gate.owner is not self
                or not _is_exact_identity(self[13], self)
                or gate.lock is not lock
                or gate.state != "CLAIMED"
                or self._owner._terminal is not self
            ):
                raise ValueError(ERROR)
            return self._owner

    def _mark_p10a_admitted(self: Any, claim: object) -> None:
        self._assert_p10a_claim(claim)
        gate, lock = self[5:7]
        with lock:
            if gate.claim is not claim or gate.state != "CLAIMED":
                raise ValueError(ERROR)
            gate.state = "ADMITTED"

    def _mark_p10a_unknown(self: Any, claim: object) -> None:
        gate, lock = self[5:7]
        with lock:
            gate.state = "UNKNOWN"
