"""Directory authority required before restricted-writer verification."""

from typing import Any

ERROR = "mssql_native.sqlclient_restricted_writer_verify_invalid"


class RestrictedWriterVerifyOrigin:
    def reserve_verify(self, request: object, *, request_sha256: str, deadline: float) -> object:
        raise NotImplementedError

    def assert_verify_reservation(self, reservation: object, request: object, *, deadline: float) -> None:
        raise NotImplementedError


class VerifyCustodySlotMixin:
    """One-shot custody slot shared by verification failure paths."""

    def _retain_custody(self: Any, value: object) -> None:
        if self._custody_tampered or self._coordinator_custody_ref is not None:
            raise ValueError(ERROR)
        object.__setattr__(self, "_coordinator_custody", value)
        object.__setattr__(self, "_coordinator_custody_ref", value)

    def _release_custody(self: Any) -> Any:
        value = self._coordinator_custody_ref
        object.__setattr__(self, "_coordinator_custody", None)
        object.__setattr__(self, "_coordinator_custody_ref", None)
        return value
