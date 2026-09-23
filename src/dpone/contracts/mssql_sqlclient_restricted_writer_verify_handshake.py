"""Nominal bounded frames for the pre-probe restricted-writer handshake."""

from dataclasses import dataclass

from dpone.contracts.mssql_tds_validation import _hash

ERROR = "mssql_native.sqlclient_restricted_writer_verify_invalid"


@dataclass(frozen=True, slots=True)
class RestrictedWriterVerifyOpening:
    """Child proof that one request-bound writer session is open."""

    request_sha256: str
    context: object
    schema: str = "dpone.sqlclient.restricted-writer-verify-opening.v1"

    def __post_init__(self) -> None:
        _hash(self.request_sha256)
        if self.schema != "dpone.sqlclient.restricted-writer-verify-opening.v1":
            raise ValueError(ERROR)


@dataclass(frozen=True, slots=True)
class RestrictedWriterProbeAuthorization:
    """Parent authorization bound to one exact canonical opening frame."""

    opening_sha256: str
    schema: str = "dpone.sqlclient.restricted-writer-probe-authorization.v1"

    def __post_init__(self) -> None:
        _hash(self.opening_sha256)
        if self.schema != "dpone.sqlclient.restricted-writer-probe-authorization.v1":
            raise ValueError(ERROR)
