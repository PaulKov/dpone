"""Public permission transcript codecs and state machine."""
# ruff: noqa: F403

from __future__ import annotations

from hashlib import sha256 as sha256

from dpone.contracts.mssql_sqlclient_permission_grant import (
    EVIDENCE_LIMIT,
    decode_permission_grant_evidence,
)
from dpone.contracts.mssql_sqlclient_permission_grant import (
    encode_permission_grant_evidence as encode_permission_grant_evidence,
)
from dpone.contracts.mssql_sqlclient_permission_grant import (
    validate_permission_binding as validate_permission_binding,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire_codec import *
from dpone.contracts.mssql_sqlclient_permission_grant_wire_codec import (
    CREDENTIAL_LIMIT,
    ERROR,
    K,
    PermissionWireBinding,
    PermissionWireKind,
    PermissionWireMessage,
    _require,
    checked_physical_total,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire_codec import _grant as _grant
from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest as coordinator_identity_digest
from dpone.contracts.mssql_tds_coordinator_authority import decode_authority
from dpone.contracts.mssql_tds_validation import _hash as _hash
from dpone.contracts.mssql_tds_validation import _integer
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


class PermissionWireState:
    """One bounded structural transcript; fail cannot renew or repair its state."""

    def __init__(self, binding: PermissionWireBinding) -> None:
        _require(type(binding) is PermissionWireBinding)
        self._binding, self._snapshot = binding, binding.snapshot()
        self.phase = "STARTUP"
        self.total = 0
        self._ordinal = 4
        self._request_hash = self._evidence_hash = ""
        self._authority = self._grant = self._evidence = b""
        self.failed = False

    def fail(self) -> None:
        self.failed = True

    def _begin(self) -> None:
        _require(not self.failed)
        self.failed = True
        _require(self._binding.snapshot() == self._snapshot)

    def consume_credentials(self, *, payload_size: int) -> None:
        try:
            self._begin()
            _require(self.phase == "CREDENTIALS")
            _integer(payload_size, 1, CREDENTIAL_LIMIT)
            self.total = checked_physical_total(self.total, payload_size)
            self.phase, self.failed = "AUTHORITY", False
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError, UnicodeError):
            raise ValueError(ERROR) from None

    def observe_eof(self) -> None:
        try:
            self._begin()
            _require(self.phase == "RELEASE_ACKNOWLEDGED")
            self.phase, self.failed = "CLOSED", False
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError, UnicodeError):
            raise ValueError(ERROR) from None

    def accept(self, payload: bytes, *, direction: str) -> PermissionWireMessage:
        try:
            self._begin()
            _require(type(payload) is bytes and 0 < len(payload) <= EVIDENCE_LIMIT)
            value = strict_json_object(payload)
            kind = K(value["kind"])
            expected = (
                K.RELEASE
                if self.phase == "HOLD" and kind is K.RELEASE
                else K.CHECK_HELD
                if self.phase == "HOLD"
                else K(self.phase)
            )
            _require(kind is expected and kind is not K.CREDENTIALS)
            ordinal = {
                K.STARTUP: 0,
                K.REQUEST: 1,
                K.REQUEST_ACCEPTED: 1,
                K.AUTHORITY: 2,
                K.EXECUTE: 3,
                K.PERMISSION_HELD: 3,
            }.get(kind, self._ordinal)
            sending = kind in (K.REQUEST, K.EXECUTE, K.CHECK_HELD, K.RELEASE)
            _require(type(direction) is str and direction == ("PARENT_TO_CHILD" if sending else "CHILD_TO_PARENT"))
            # Resolve through the compatibility facade so existing instrumentation
            # and fault injection continue to observe the public codec boundary.
            from dpone.contracts import mssql_sqlclient_permission_grant_wire as public_wire

            message = public_wire.decode_permission_message(payload, binding=self._binding, kind=kind, ordinal=ordinal)
            total = checked_physical_total(self.total, len(payload))
            body = strict_json_object(message.body)
            self._advance(kind, body, payload)
            self.total, self.failed = total, False
            return message
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError, UnicodeError):
            raise ValueError(ERROR) from None

    def _advance(self, kind: PermissionWireKind, body: dict, payload: bytes) -> None:
        binding = self._binding
        if kind is K.REQUEST:
            self._request_hash = sha256(payload).hexdigest()
        elif kind is K.REQUEST_ACCEPTED:
            _require(body["request_payload_sha256"] == self._request_hash)
        elif kind is K.AUTHORITY:
            self._authority = canonical_json_bytes(body["authority"])
        elif kind in (K.EXECUTE, K.PERMISSION_HELD):
            authority = decode_authority(self._authority)
            if kind is K.EXECUTE:
                grant = _grant(body["grant"])
                validate_permission_binding(binding.request, binding.operation, grant, authority)
                self._grant = canonical_json_bytes(body["grant"])
            else:
                evidence = decode_permission_grant_evidence(canonical_json_bytes(body["evidence"]))
                grant = _grant(strict_json_object(self._grant))
                validate_permission_binding(binding.request, binding.operation, grant, authority)
                _require(evidence.grant == grant and evidence.authority == authority)
                self._evidence = encode_permission_grant_evidence(evidence)
                self._evidence_hash = sha256(self._evidence).hexdigest()
        elif "evidence_sha256" in body:
            _require(body["evidence_sha256"] == self._evidence_hash)
            if kind is K.HELD:
                _require(canonical_json_bytes(body["authority"]) == self._authority)
                self._ordinal += 1
        self.phase = {
            K.STARTUP: "REQUEST",
            K.REQUEST: "REQUEST_ACCEPTED",
            K.REQUEST_ACCEPTED: "CREDENTIALS",
            K.AUTHORITY: "EXECUTE",
            K.EXECUTE: "PERMISSION_HELD",
            K.PERMISSION_HELD: "HOLD",
            K.CHECK_HELD: "HELD",
            K.HELD: "HOLD",
            K.RELEASE: "RELEASED",
            K.RELEASED: "RELEASE_ACKNOWLEDGED",
        }[kind]
