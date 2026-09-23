"""Bounded launch, validation, and private credential envelopes for P9a."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import asdict, dataclass
from hashlib import sha256
from traceback import clear_frames

from dpone.contracts.mssql_sqlclient_restricted_writer_verify import (
    ERROR,
    RestrictedWriterVerifyRegistration,
    SqlClientRestrictedWriterVerifyRequest,
)
from dpone.contracts.mssql_tds_api import (
    canonical_verify_json,
    decode_verify_material,
    decode_verify_process,
    decode_verify_request,
    encode_verify_request,
    strict_verify_object,
)
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial, TdsConnectionProfile


def discard_exception(error: BaseException) -> None:
    """Sever subordinate frames before translating a credential failure."""
    pending = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        pending.extend(value for value in (current.__cause__, current.__context__) if value is not None)
        traceback = current.__traceback__
        current.__traceback__ = current.__cause__ = current.__context__ = None
        if traceback is not None:
            clear_frames(traceback)


def validate_deadline(value: float) -> None:
    """Require a finite, strictly positive deadline value."""
    if type(value) is not float or not math.isfinite(value) or value <= 0:
        raise ValueError(ERROR)


LIMIT = 1048576
LAUNCH_KEYS = frozenset(
    {
        "schema",
        "request",
        "startup_deadline",
        "operation_deadline",
        "termination_timeout",
        "admission_sha256",
        "profile",
    }
)


@dataclass(frozen=True, slots=True, kw_only=True)
class RestrictedWriterVerifyLaunchRequest:
    request: SqlClientRestrictedWriterVerifyRequest
    startup_deadline: float
    operation_deadline: float
    termination_timeout: float
    admission_sha256: str
    profile: TdsConnectionProfile
    schema: str = "dpone.sqlclient.restricted-writer-verify-launch.v1"

    def __post_init__(self) -> None:
        if type(self.request) is not SqlClientRestrictedWriterVerifyRequest:
            raise ValueError(ERROR)
        self.request.__post_init__()
        validate_deadline(self.startup_deadline)
        validate_deadline(self.operation_deadline)
        if (
            self.schema != "dpone.sqlclient.restricted-writer-verify-launch.v1"
            or self.startup_deadline > self.operation_deadline
            or type(self.termination_timeout) is not float
            or (not math.isfinite(self.termination_timeout))
            or (self.termination_timeout <= 0)
            or (type(self.admission_sha256) is not str)
            or (len(self.admission_sha256) != 64)
            or any(char not in "0123456789abcdef" for char in self.admission_sha256)
            or (type(self.profile) is not TdsConnectionProfile)
        ):
            raise ValueError(ERROR)
        try:
            int(self.admission_sha256, 16)
        except ValueError:
            raise ValueError(ERROR) from None

    def public_payload(self) -> bytes:
        return encode_verify_launch_request(self)


class RestrictedWriterCredentialSupplier:
    """One-shot secret supplier consumed only after durable credential intent."""

    def __init__(self, factory: Callable[[], tuple[TdsConnectionMaterial, bytes]]) -> None:
        if not callable(factory):
            raise ValueError(ERROR)
        self._factory: Callable[[], tuple[TdsConnectionMaterial, bytes]] | None = factory
        self._attempted = False

    @property
    def consumed(self) -> bool:
        return self._attempted and self._factory is None

    def take(
        self,
        launch: RestrictedWriterVerifyLaunchRequest,
        registration: RestrictedWriterVerifyRegistration,
        *,
        public_payload: bytes,
    ) -> bytearray:
        if self._attempted or self._factory is None:
            raise ValueError(ERROR)
        self._attempted = True
        factory, self._factory = (self._factory, None)
        material = nonce = encoded = None
        failure: BaseException | None = None
        try:
            material, nonce = factory()
            if (
                type(material) is not TdsConnectionMaterial
                or material.database != launch.request.stage.database_name
                or type(nonce) is not bytes
                or (len(nonce) != 32)
                or (not any(nonce))
            ):
                raise ValueError
            encoded = encode_verify_credentials(
                launch, registration, public_payload=public_payload, material=material, session_nonce=nonce
            )
            return bytearray(encoded)
        except BaseException as error:
            failure = error
        finally:
            del factory, material, nonce, encoded
        assert failure is not None
        discard_exception(failure)
        del failure
        raise ValueError(ERROR) from None


def encode_verify_launch_request(value: RestrictedWriterVerifyLaunchRequest) -> bytes:
    try:
        if type(value) is not RestrictedWriterVerifyLaunchRequest:
            raise ValueError
        value.__post_init__()
        payload = canonical_verify_json(
            {
                "schema": value.schema,
                "request": strict_verify_object(encode_verify_request(value.request)),
                "startup_deadline": value.startup_deadline,
                "operation_deadline": value.operation_deadline,
                "termination_timeout": value.termination_timeout,
                "admission_sha256": value.admission_sha256,
                "profile": value.profile.value,
            }
        )
        if not 0 < len(payload) <= LIMIT:
            raise ValueError
        return payload
    except (ValueError, TypeError, AttributeError, UnicodeError, OverflowError, RecursionError):
        raise ValueError(ERROR) from None


def decode_verify_launch_request(payload: bytes) -> dict:
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= LIMIT:
            raise ValueError
        body = strict_verify_object(payload)
        if set(body) != LAUNCH_KEYS or body["schema"] != "dpone.sqlclient.restricted-writer-verify-launch.v1":
            raise ValueError
        result = dict(body)
        result["request"] = decode_verify_request(canonical_verify_json(body["request"]))
        result["profile"] = TdsConnectionProfile(body["profile"])
        validate_deadline(result["startup_deadline"])
        validate_deadline(result["operation_deadline"])
        timeout = result["termination_timeout"]
        digest = result["admission_sha256"]
        if (
            result["startup_deadline"] > result["operation_deadline"]
            or type(timeout) is not float
            or (not math.isfinite(timeout))
            or (timeout <= 0)
            or (type(digest) is not str)
            or (len(digest) != 64)
            or any(char not in "0123456789abcdef" for char in digest)
            or (canonical_verify_json(body) != payload)
        ):
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, OverflowError, RecursionError):
        raise ValueError(ERROR) from None


def encode_verify_credentials(
    value: RestrictedWriterVerifyLaunchRequest,
    registration: RestrictedWriterVerifyRegistration,
    *,
    public_payload: bytes,
    material: TdsConnectionMaterial,
    session_nonce: bytes,
) -> bytes:
    try:
        value.__post_init__()
        if type(registration) is not RestrictedWriterVerifyRegistration:
            raise ValueError
        registration.__post_init__()
        decoded_public = decode_verify_launch_request(public_payload)
        if registration.reservation.operation_id != value.request.operation_id:
            raise ValueError
        if (
            decoded_public["request"] != value.request
            or decoded_public["startup_deadline"] != value.startup_deadline
            or decoded_public["operation_deadline"] != value.operation_deadline
            or (decoded_public["termination_timeout"] != value.termination_timeout)
            or (decoded_public["admission_sha256"] != value.admission_sha256)
            or (decoded_public["profile"] is not value.profile)
            or (type(material) is not TdsConnectionMaterial)
            or (material.database != value.request.stage.database_name)
            or (type(session_nonce) is not bytes)
            or (len(session_nonce) != 32)
            or (not any(session_nonce))
        ):
            raise ValueError
        payload = canonical_verify_json(
            {
                "schema": "dpone.sqlclient.restricted-writer-verify-credentials.v1",
                "public_sha256": sha256(public_payload).hexdigest(),
                "process": asdict(registration.process),
                "session_nonce": session_nonce.hex(),
                "connection_material": asdict(material),
            }
        )
        if not 0 < len(payload) <= LIMIT:
            raise ValueError
        return payload
    except (ValueError, TypeError, AttributeError, UnicodeError, OverflowError, RecursionError):
        raise ValueError(ERROR) from None


def decode_verify_credentials(
    payload: bytes | bytearray, *, public_payload: bytes, process: object
) -> tuple[TdsConnectionMaterial, bytes]:
    try:
        if type(payload) not in (bytes, bytearray) or not 0 < len(payload) <= LIMIT:
            raise ValueError
        encoded = bytes(payload)
        body = strict_verify_object(encoded)
        if (
            set(body) != {"schema", "public_sha256", "process", "session_nonce", "connection_material"}
            or body["schema"] != "dpone.sqlclient.restricted-writer-verify-credentials.v1"
        ):
            raise ValueError
        if body["public_sha256"] != sha256(public_payload).hexdigest():
            raise ValueError
        decoded_process = decode_verify_process(body["process"])
        if decoded_process != process:
            raise ValueError
        nonce = bytes.fromhex(body["session_nonce"])
        material = decode_verify_material(body["connection_material"])
        if len(nonce) != 32 or not any(nonce) or canonical_verify_json(body) != encoded:
            raise ValueError
        return (material, nonce)
    except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, OverflowError, RecursionError):
        raise ValueError(ERROR) from None
