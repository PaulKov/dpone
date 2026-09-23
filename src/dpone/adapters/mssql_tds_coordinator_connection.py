"""Dedicated admitted pyodbc connection; import and credentials stay in the child.

Expected hashes are immutable reviewed composition inputs, never learned from the
current executable. Normal-path admission is not fault/reconnect certification.
"""

from __future__ import annotations

import ctypes
import hashlib
import importlib
import importlib.util
import math
import os
import platform
import re as re
import stat
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass, fields
from pathlib import Path
from time import monotonic
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_tds_connection import (
    TdsConnectionMaterial as TdsConnectionMaterial,
)
from dpone.contracts.mssql_tds_connection import (
    TdsConnectionProfile as TdsConnectionProfile,
)
from dpone.contracts.mssql_tds_validation import _hash
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


class TdsConnectionError(RuntimeError):
    """Static diagnostic; driver text and credential material never escape."""


@dataclass(frozen=True)
class TdsBinaryPin:
    path: Path
    sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.path, Path) or not self.path.is_absolute():
            raise ValueError("mssql_native.tds_build_admission_invalid")
        _hash(self.sha256)


@dataclass(frozen=True)
class TdsCoordinatorBuild:
    interpreter: TdsBinaryPin
    pyodbc: TdsBinaryPin
    driver: TdsBinaryPin
    driver_manager: TdsBinaryPin

    def __post_init__(self) -> None:
        if any(
            type(pin) is not TdsBinaryPin for pin in (self.interpreter, self.pyodbc, self.driver, self.driver_manager)
        ):
            raise ValueError("mssql_native.tds_build_admission_required")


def _before(deadline: float, clock: Callable[[], float]) -> float:
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        raise TdsConnectionError("mssql_native.tds_sql_deadline")
    remaining = deadline - clock()
    if not math.isfinite(remaining) or remaining <= 0:
        raise TdsConnectionError("mssql_native.tds_sql_deadline")
    return remaining


def _verify(pin: TdsBinaryPin) -> Path:
    path = pin.path.resolve(strict=True)
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError
        hashed = hashlib.sha256()
        total = 0
        while chunk := stream.read(1024**2):
            total += len(chunk)
            if total > 256 * 1024**2:
                raise ValueError
            hashed.update(chunk)
        digest = hashed.hexdigest()
    if digest != pin.sha256:
        raise ValueError
    return path


def _admit(build: TdsCoordinatorBuild) -> Any:
    """Load only exact supplied binaries in a fresh dedicated Linux child."""
    if (
        type(build) is not TdsCoordinatorBuild
        or sys.platform != "linux"
        or platform.machine() != "aarch64"
        or sys.version_info[:2] != (3, 12)
        or "pyodbc" in sys.modules
    ):
        raise TdsConnectionError("mssql_native.tds_build_admission_required")
    try:
        interpreter, extension, driver, manager = tuple(
            _verify(pin) for pin in (build.interpreter, build.pyodbc, build.driver, build.driver_manager)
        )
        if Path(sys.executable).resolve() != interpreter:
            raise ValueError
        spec = importlib.util.find_spec("pyodbc")
        if spec is None or spec.origin is None or Path(spec.origin).resolve() != extension:
            raise ValueError
        # Preload pinned DSN-less driver/manager before any connection material.
        ctypes.CDLL(str(manager))
        ctypes.CDLL(str(driver))
        module = importlib.import_module("pyodbc")
        if module.version != "5.3.0" or Path(module.__file__).resolve() != extension:
            raise ValueError
        module.pooling = False
        module.native_uuid = True
        # Refuse an already-loaded alternative driver manager even with same SONAME.
        mapped = Path("/proc/self/maps").read_text().splitlines()
        managers = {Path(line.split()[-1]).resolve() for line in mapped if "/libodbc.so" in line}
        if managers != {manager}:
            raise ValueError
        return module
    except Exception:
        raise TdsConnectionError("mssql_native.tds_build_admission_failed") from None


def _escaped(value: str) -> str:
    return "{" + value.replace("}", "}}") + "}"


class TdsSqlConnection:
    """Exclusive cursor and connection cleanup, retained after partial failure."""

    def __init__(
        self,
        connection: Any,
        cursor: Any,
        *,
        client_connection_id: UUID | None = None,
        client_session_id: int | None = None,
        module: Any | None = None,
        driver_manager: Path | None = None,
    ) -> None:
        self._connection, self.cursor = connection, cursor
        self.client_connection_id = client_connection_id
        self.client_session_id = client_session_id
        self._module = module
        self._driver_manager = driver_manager
        self._owner = (os.getpid(), threading.current_thread())
        self._closed = False

    def check_owner(self) -> None:
        if self._closed or self._owner != (os.getpid(), threading.current_thread()):
            raise TdsConnectionError("mssql_native.tds_sql_owner_invalid")

    def require_client_identity(self) -> tuple[UUID, int]:
        self.check_owner()
        if self.client_connection_id is None or self.client_session_id is None:
            if self._module is None or self._driver_manager is None:
                raise TdsConnectionError("mssql_native.tds_client_identity_unavailable")
            self.client_connection_id, self.client_session_id = _client_identity(
                self._connection,
                module=self._module,
                driver_manager=self._driver_manager,
            )
        return self.client_connection_id, self.client_session_id

    def set_query_deadline(self, *, deadline: float, clock: Callable[[], float]) -> None:
        """Bind future SQL statements to the remaining parent-owned deadline.

        pyodbc copies ``Connection.timeout`` into an ODBC statement when the
        cursor is created.  Replace the still-unused admission cursor so the
        settlement statement cannot retain the short catalog-probe timeout.
        """
        self.check_owner()
        replacement = None
        try:
            seconds = max(1, min(2**31 - 1, math.ceil(_before(deadline, clock))))
            self._connection.timeout = seconds
            replacement = self._connection.cursor()
            previous, self.cursor = self.cursor, None
            previous.close()
            self.cursor = replacement
        except TdsConnectionError:
            raise
        except BaseException:
            if replacement is not None:
                try:
                    replacement.close()
                except BaseException:
                    pass
            raise TdsConnectionError("mssql_native.tds_sql_timeout_unknown") from None

    def close(self) -> None:
        self.check_owner()
        self._closed = True
        failed = False
        for resource in (self.cursor, self._connection):
            if resource is None:
                continue
            try:
                resource.close()
            except BaseException:
                failed = True
        if failed:
            raise TdsConnectionError("mssql_native.tds_sql_close_unknown")


class TdsCoordinatorConnection:
    """One explicit admitted build/profile, one connection attempt, never fallback."""

    def __init__(
        self, build: TdsCoordinatorBuild, profile: TdsConnectionProfile, *, clock: Callable[[], float] = monotonic
    ) -> None:
        if type(profile) is not TdsConnectionProfile:
            raise ValueError("mssql_native.tds_connection_profile_invalid")
        self._module = _admit(build)
        self._build, self._profile, self._clock = build, profile, clock
        self._owner = (os.getpid(), threading.current_thread())
        self._attempted = False
        self.connection: TdsSqlConnection | None = None

    def connect(self, material: TdsConnectionMaterial, *, deadline: float) -> TdsSqlConnection:
        if self._owner != (os.getpid(), threading.current_thread()) or self._attempted:
            raise TdsConnectionError("mssql_native.tds_connection_reuse_forbidden")
        if type(material) is not TdsConnectionMaterial:
            raise ValueError("mssql_native.tds_connection_material_invalid")
        remaining = _before(deadline, self._clock)
        self._attempted = True
        try:
            trust = "yes" if self._profile is TdsConnectionProfile.SYNTHETIC_LOCAL else "no"
            text = (
                ";".join(
                    (
                        "DRIVER=" + _escaped(str(self._build.driver.path.resolve())),
                        "SERVER=" + _escaped(f"tcp:{material.host},{material.port}"),
                        "DATABASE=" + _escaped(material.database),
                        "UID=" + _escaped(material.username),
                        "PWD=" + _escaped(material.password),
                        "Encrypt=yes",
                        "TrustServerCertificate=" + trust,
                        "MARS_Connection=no",
                        "ConnectRetryCount=0",
                    )
                )
                + ";"
            )
            try:
                connection = self._module.connect(text, autocommit=True, timeout=max(1, min(5, math.ceil(remaining))))
            finally:
                del text
            self.connection = TdsSqlConnection(
                connection,
                None,
                module=self._module,
                driver_manager=self._build.driver_manager.path,
            )
            connection.timeout = max(1, min(3, math.ceil(_before(deadline, self._clock))))
            self.connection.cursor = connection.cursor()
            _before(deadline, self._clock)
            return self.connection
        except BaseException:
            # Cleanup is child-owned and may block; the parent retains pidfd authority.
            if self.connection is not None:
                try:
                    self.connection.close()
                except BaseException:
                    pass
            raise TdsConnectionError("mssql_native.tds_connection_unknown") from None


def _client_identity(connection: Any, *, module: Any, driver_manager: Path) -> tuple[UUID, int]:
    """Read immutable Microsoft ODBC connection facts without server DMV grants.

    The layout access is admitted only for the already pinned CPython 3.12 and
    pyodbc 5.3.0 build. Any ABI, attribute, length, or scalar drift fails the
    connection before a cursor or credential-bearing operation is exposed.
    """
    try:
        if type(connection) is not module.Connection or module.version != "5.3.0":
            raise ValueError
        pointer_size = ctypes.sizeof(ctypes.c_void_p)
        hdbc = ctypes.c_void_p.from_address(id(connection) + 2 * pointer_size).value
        if type(hdbc) is not int or hdbc <= 0:
            raise ValueError
        manager = ctypes.CDLL(str(driver_manager.resolve(strict=True)))
        get_attribute = manager.SQLGetConnectAttr
        get_attribute.argtypes = (
            ctypes.c_void_p,
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_int),
        )
        get_attribute.restype = ctypes.c_short
        identifier_buffer = (ctypes.c_ubyte * 74)()
        identifier_length = ctypes.c_int()
        if get_attribute(hdbc, 1233, identifier_buffer, len(identifier_buffer), ctypes.byref(identifier_length)) != 0:
            raise ValueError
        if identifier_length.value != 72:
            raise ValueError
        identifier_text = bytes(identifier_buffer[:72]).decode("utf-16-le")
        identifier = UUID(identifier_text)
        if not identifier.int or str(identifier) != identifier_text.lower():
            raise ValueError
        session_id = ctypes.c_uint32()
        session_length = ctypes.c_int()
        if get_attribute(hdbc, 1401, ctypes.byref(session_id), 4, ctypes.byref(session_length)) != 0:
            raise ValueError
        if session_length.value not in (0, 4) or not 1 <= session_id.value <= 32767:
            raise ValueError
        return identifier, session_id.value
    except (ValueError, TypeError, AttributeError, OSError, UnicodeError):
        raise TdsConnectionError("mssql_native.tds_client_identity_unavailable") from None


def encode_connection_admission(build: TdsCoordinatorBuild, profile: TdsConnectionProfile) -> bytes:
    """Pure non-secret launch descriptor, authenticated by the parent startup binding."""
    if type(build) is not TdsCoordinatorBuild or type(profile) is not TdsConnectionProfile:
        raise ValueError("mssql_native.tds_build_descriptor_invalid")
    binaries = {
        item.name: {"path": str(getattr(build, item.name).path), "sha256": getattr(build, item.name).sha256}
        for item in fields(build)
    }
    payload = canonical_json_bytes(
        {"schema": "dpone.tds.coordinator-build.v1", "build": binaries, "profile": profile.value}
    )
    if len(payload) > 16384:
        raise ValueError("mssql_native.tds_build_descriptor_invalid")
    return payload


def decode_connection_admission(payload: bytes) -> tuple[TdsCoordinatorBuild, TdsConnectionProfile]:
    """Decode pins only; observing or decoding a build never grants admission."""
    try:
        if type(payload) is not bytes or len(payload) > 16384:
            raise ValueError
        value = strict_json_object(payload)
        if set(value) != {"schema", "build", "profile"} or value["schema"] != "dpone.tds.coordinator-build.v1":
            raise ValueError
        if (
            type(value["build"]) is not dict
            or set(value["build"]) != {item.name for item in fields(TdsCoordinatorBuild)}
            or type(value["profile"]) is not str
        ):
            raise ValueError
        pins = {}
        for name, pin in value["build"].items():
            if type(pin) is not dict or set(pin) != {"path", "sha256"} or type(pin["path"]) is not str:
                raise ValueError
            pins[name] = TdsBinaryPin(Path(pin["path"]), pin["sha256"])
        build, profile = TdsCoordinatorBuild(**pins), TdsConnectionProfile(value["profile"])
        if strict_json_object(encode_connection_admission(build, profile)) != value:
            raise ValueError
        return build, profile
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError("mssql_native.tds_build_descriptor_invalid") from None
