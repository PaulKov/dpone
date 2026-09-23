"""Opaque one-shot authority passed from P10a to the future P10b writer."""

from __future__ import annotations

import os
from collections import namedtuple
from collections.abc import Callable
from threading import Lock, get_ident
from types import MappingProxyType

ERROR = "mssql_native.sqlclient_writer_admission_unknown"
_ADMITTED_TOKEN = object()


def _make_identity_witness() -> tuple[Callable[[object], None], MappingProxyType]:
    identity: dict[str, int] = {}
    witness = MappingProxyType(identity)

    def bind(candidate: object) -> None:
        if identity:
            raise ValueError(ERROR)
        identity["owner_id"] = id(candidate)

    return bind, witness


def _is_exact_identity(witness: object, candidate: object) -> bool:
    return (
        type(witness) is MappingProxyType
        and len(witness) == 1
        and type(witness.get("owner_id")) is int
        and witness["owner_id"] == id(candidate)
    )


_AdmissionPlanBase = namedtuple(
    "_AdmissionPlanBase",
    "transition attempt attempt_value directory directory_value stage stage_snapshot "
    "input_descriptor input_snapshot input_binding_sha256 policy policy_snapshot installation "
    "build_sha256 implementation_sha256 startup_deadline operation_deadline operation_deadline_ns "
    "termination_timeout_seconds max_worker_address_space_bytes grant_result_receipt grant_result_sha256 "
    "content_expectation terminal p9_owner authority_fence authority_proof",
)


class _AdmissionPlan(_AdmissionPlanBase):
    __slots__ = ()

    def __repr__(self) -> str:
        return "_AdmissionPlan(<opaque>)"


class SqlClientWriterAdmitted:
    """Credential-free, non-reconstructible authority for future P10b."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "SqlClientWriterAdmitted(<opaque>)"

    def _claim_p10b_once(self) -> object:
        raise ValueError(ERROR)

    def _assert_p10b_claim(self, claim: object) -> _AdmissionPlan:
        raise ValueError(ERROR)

    def _mark_p10b_registered(self, claim: object) -> None:
        raise ValueError(ERROR)

    def _assert_p10b_registered(self, claim: object) -> _AdmissionPlan:
        raise ValueError(ERROR)

    def _mark_p10b_unknown(self, claim: object | None) -> _AdmissionPlan | None:
        raise ValueError(ERROR)


def _create_sqlclient_writer_admitted(token: object, plan: _AdmissionPlan) -> SqlClientWriterAdmitted:
    """Bind P10b authority to one empty-slotted instance and closure state."""
    if token is not _ADMITTED_TOKEN or type(plan) is not _AdmissionPlan:
        raise ValueError(ERROR)
    lock = Lock()
    pid, thread_id = os.getpid(), get_ident()
    state = "AVAILABLE"
    exact_claim: object | None = None
    exact: SqlClientWriterAdmitted

    class _ExactWriterAdmitted(SqlClientWriterAdmitted):
        __slots__ = ()

        def _claim_p10b_once(self) -> object:
            nonlocal state, exact_claim
            if self is not exact or os.getpid() != pid or get_ident() != thread_id:
                raise ValueError(ERROR)
            with lock:
                if state != "AVAILABLE" or exact_claim is not None:
                    raise ValueError(ERROR)
                exact_claim = object()
                state = "CLAIMED"
                return exact_claim

        def _assert_p10b_claim(self, claim: object) -> _AdmissionPlan:
            if self is not exact or os.getpid() != pid or get_ident() != thread_id:
                raise ValueError(ERROR)
            with lock:
                if state != "CLAIMED" or claim is not exact_claim:
                    raise ValueError(ERROR)
                return plan

        def _mark_p10b_registered(self, claim: object) -> None:
            nonlocal state
            self._assert_p10b_claim(claim)
            with lock:
                if state != "CLAIMED" or claim is not exact_claim:
                    raise ValueError(ERROR)
                state = "REGISTERED"

        def _assert_p10b_registered(self, claim: object) -> _AdmissionPlan:
            if self is not exact or os.getpid() != pid or get_ident() != thread_id:
                raise ValueError(ERROR)
            with lock:
                if state != "REGISTERED" or claim is not exact_claim:
                    raise ValueError(ERROR)
                return plan

        def _mark_p10b_unknown(self, claim: object | None) -> _AdmissionPlan | None:
            nonlocal state
            if self is not exact or os.getpid() != pid or get_ident() != thread_id:
                raise ValueError(ERROR)
            with lock:
                if state == "CLAIMED" and (claim is None or claim is exact_claim):
                    state = "UNKNOWN"
                    return plan
                return None

    exact = _ExactWriterAdmitted()
    return exact


__all__ = ("SqlClientWriterAdmitted",)
