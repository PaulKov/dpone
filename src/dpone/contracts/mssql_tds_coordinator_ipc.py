"""Closed coordinator IPC bytes, separate from physical launch authentication.

Decoding a startup receipt proves only structural validity. The owning launcher
must compare every binding with its admitted source, pidfd and launch inputs,
after bounded framing through EOF. No filesystem or credential I/O occurs here.
"""

from dataclasses import asdict, dataclass, fields
from hashlib import sha256

from dpone.contracts.mssql_tds_worker import TdsProcessIdentity, _hash
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

_STARTUP_LIMIT = 16384
_STARTUP_SCHEMA = "dpone.tds.coordinator-startup.v1"
_INVALID = "mssql_native.tds_coordinator_startup_invalid"


@dataclass(frozen=True)
class TdsCoordinatorStartup:
    """Observed process/source/root and fresh launch nonce, never SQL authority."""

    process: TdsProcessIdentity
    implementation_sha256: str
    package_root: str
    launch_nonce: bytes

    def __post_init__(self) -> None:
        try:
            if type(self.process) is not TdsProcessIdentity:
                raise ValueError("process")
            _hash(self.implementation_sha256)
            if (
                type(self.package_root) is not str
                or not self.package_root.startswith("/")
                or len(self.package_root.encode("utf-8")) > 4096
                or any(ord(char) < 32 or ord(char) == 127 for char in self.package_root)
                or type(self.launch_nonce) is not bytes
                or len(self.launch_nonce) != 32
                or not any(self.launch_nonce)
            ):
                raise ValueError("launch_binding")
        except (ValueError, TypeError):
            raise ValueError(_INVALID) from None


def encode_startup(startup: TdsCoordinatorStartup) -> bytes:
    """Serialize only the fixed v1 startup fields within the phase byte budget."""
    if type(startup) is not TdsCoordinatorStartup:
        raise ValueError(_INVALID)
    payload = canonical_json_bytes(
        dict(asdict(startup), schema=_STARTUP_SCHEMA, launch_nonce=startup.launch_nonce.hex())
    )
    if len(payload) > _STARTUP_LIMIT:
        raise ValueError(_INVALID)
    return payload


def decode_startup(payload: bytes) -> TdsCoordinatorStartup:
    """Reject ambiguous fields and scalar aliases without exposing input bytes."""
    try:
        if type(payload) is not bytes or len(payload) > _STARTUP_LIMIT:
            raise ValueError("size")
        body = strict_json_object(payload)
        if set(body) != {"schema", "process", "implementation_sha256", "package_root", "launch_nonce"}:
            raise ValueError("fields")
        process = body["process"]
        if (
            body["schema"] != _STARTUP_SCHEMA
            or type(process) is not dict
            or set(process) != {field.name for field in fields(TdsProcessIdentity)}
            or type(body["launch_nonce"]) is not str
            or len(body["launch_nonce"]) != 64
        ):
            raise ValueError("fields")
        startup = TdsCoordinatorStartup(
            TdsProcessIdentity(**process),
            body["implementation_sha256"],
            body["package_root"],
            bytes.fromhex(body["launch_nonce"]),
        )
        if strict_json_object(encode_startup(startup)) != body:
            raise ValueError("noncanonical")
        return startup
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError(_INVALID) from None


def encode_registration(startup: TdsCoordinatorStartup, admission_sha256: str) -> bytes:
    """Bind authenticated startup to expected admission inputs for persistence.

    The supervisor calls this after authenticating startup and retains the full
    canonical admission descriptor alongside this non-secret receipt. Serialization
    cannot establish that a supplied startup was observed from a real process.
    """
    _hash(admission_sha256)
    return canonical_json_bytes(
        {
            "schema": "dpone.tds.coordinator-registration.v1",
            "startup": strict_json_object(encode_startup(startup)),
            "admission_sha256": admission_sha256,
        }
    )


def registration_digest(startup: TdsCoordinatorStartup, admission_sha256: str) -> str:
    """Hash the versioned structured receipt, never concatenated field strings."""
    return sha256(encode_registration(startup, admission_sha256)).hexdigest()
