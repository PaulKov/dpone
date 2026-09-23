"""Ordered immutable evidence references within the existing attempt journal.

References identify separately persisted technical records, never credentials.
The supervisor must validate full original bindings in those records; a digest
alone does not prove a session observation, grant delivery or SQL settlement.
"""

from dataclasses import dataclass, replace

from dpone.contracts.mssql_tds_validation import _hash


@dataclass(frozen=True)
class SqlClientAttemptEvidence:
    launch_sha256: str
    credential_intent_sha256: str
    input_empty: bool
    writer_observation_sha256: str | None = None
    grant_intent_sha256: str | None = None

    def __post_init__(self) -> None:
        for value in (self.launch_sha256, self.credential_intent_sha256):
            _hash(value)
        for value in (self.writer_observation_sha256, self.grant_intent_sha256):
            if value is not None:
                _hash(value)
        if type(self.input_empty) is not bool:
            raise ValueError("mssql_native.sqlclient_empty_flag")
        if self.input_empty and (self.writer_observation_sha256 is not None or self.grant_intent_sha256 is not None):
            raise ValueError("mssql_native.sqlclient_empty_authority")
        if self.grant_intent_sha256 is not None and self.writer_observation_sha256 is None:
            raise ValueError("mssql_native.sqlclient_unobserved_grant")


@dataclass(frozen=True)
class SqlClientCredentialIntent:
    evidence: SqlClientAttemptEvidence


@dataclass(frozen=True)
class SqlClientWriterObserved:
    proof_sha256: str


@dataclass(frozen=True)
class SqlClientGrantIntent:
    proof_sha256: str


def advance_sqlclient_evidence(current: SqlClientAttemptEvidence | None, event: object) -> SqlClientAttemptEvidence:
    """Advance one prerequisite exactly once; journal CAS/ACK remains external."""
    if type(event) is SqlClientCredentialIntent:
        value = event.evidence
        if (
            current is not None
            or type(value) is not SqlClientAttemptEvidence
            or value.writer_observation_sha256 is not None
            or value.grant_intent_sha256 is not None
        ):
            raise ValueError("mssql_native.sqlclient_credential_intent")
        return value
    if current is None or current.input_empty:
        raise ValueError("mssql_native.sqlclient_missing_credential_intent")
    if type(event) is SqlClientWriterObserved and current.writer_observation_sha256 is None:
        return replace(current, writer_observation_sha256=event.proof_sha256)
    if (
        type(event) is SqlClientGrantIntent
        and current.writer_observation_sha256 is not None
        and current.grant_intent_sha256 is None
    ):
        return replace(current, grant_intent_sha256=event.proof_sha256)
    raise ValueError("mssql_native.sqlclient_repeated_or_unordered_intent")
