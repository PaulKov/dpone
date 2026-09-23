"""Finite typed worker evidence dispatch; validation precedes exact byte hashing."""

from dataclasses import dataclass, field
from hashlib import sha256

from dpone.contracts.mssql_sqlclient_evidence_types import (
    ERROR,
    SqlClientEvidenceKind,
    SqlClientEvidenceReceipt,
    evidence_name,
    require_payload,
)
from dpone.contracts.mssql_sqlclient_execution_evidence import SqlClientResultContext, decode_worker_local_exit
from dpone.contracts.mssql_sqlclient_registration import decode_credential_intent, decode_registration
from dpone.contracts.mssql_sqlclient_result import decode_sqlclient_result
from dpone.contracts.mssql_sqlclient_session_control import decode_bulk_grant, encode_bulk_grant
from dpone.contracts.mssql_sqlclient_writer_evidence import decode_writer_observation
from dpone.contracts.mssql_sqlclient_writer_settlement import decode_writer_settlement
from dpone.contracts.mssql_tds_validation import _hash


@dataclass(frozen=True, slots=True)
class SqlClientEvidenceRecord:
    """Bounded persistence request, not proof that supplied references were ACKed.

    Result context contains independently retained originals. Only valid RESULT
    JSON may retain noncanonical spacing/order, and its exact bytes are hashed.
    """

    attempt_sha256: str
    kind: SqlClientEvidenceKind
    payload: bytes = field(repr=False)
    result_context: SqlClientResultContext | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        try:
            _hash(self.attempt_sha256)
            require_payload(self.payload, self.kind)
            if self.kind is SqlClientEvidenceKind.RESULT:
                context = self.result_context
                if type(context) is not SqlClientResultContext:
                    raise ValueError(ERROR)
                context.__post_init__()
                result = decode_sqlclient_result(
                    self.payload,
                    launch=context.launch,
                    expected_input=context.expected_input,
                    expected_grant_id=context.expected_grant_id,
                )
                attempt = result.attempt_sha256
            else:
                if self.result_context is not None:
                    raise ValueError(ERROR)
                if self.kind is SqlClientEvidenceKind.GRANT_INTENT:
                    grant = decode_bulk_grant(self.payload)
                    if encode_bulk_grant(grant) != self.payload:
                        raise ValueError(ERROR)
                    attempt = grant.attempt_sha256
                elif self.kind is SqlClientEvidenceKind.REGISTRATION:
                    attempt = decode_registration(self.payload).binding.attempt_sha256
                elif self.kind is SqlClientEvidenceKind.CREDENTIAL_INTENT:
                    attempt = decode_credential_intent(self.payload).binding.attempt_sha256
                elif self.kind is SqlClientEvidenceKind.WRITER_OBSERVATION:
                    attempt = decode_writer_observation(self.payload).binding.attempt_sha256
                elif self.kind is SqlClientEvidenceKind.LOCAL_EXIT:
                    attempt = decode_worker_local_exit(self.payload).binding.attempt_sha256
                elif self.kind is SqlClientEvidenceKind.VERIFICATION:
                    attempt = decode_writer_settlement(self.payload).attempt_sha256
                else:
                    raise ValueError(ERROR)
            if attempt != self.attempt_sha256:
                raise ValueError(ERROR)
        except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
            raise ValueError(ERROR) from None

    @property
    def receipt(self) -> SqlClientEvidenceReceipt:
        """Revalidate even frozen input before computing an artifact identifier."""
        self.__post_init__()
        digest = sha256(self.payload).hexdigest()
        return SqlClientEvidenceReceipt(
            self.attempt_sha256,
            self.kind,
            evidence_name(self.attempt_sha256, self.kind, digest),
            digest,
            len(self.payload),
        )
