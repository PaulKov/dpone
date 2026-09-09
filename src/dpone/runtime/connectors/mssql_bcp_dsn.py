"""Descriptor-owned ODBC DSN lifecycle for Microsoft BCP connections."""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

_DSN_NAME = "dpone_bcp_connection"
_FORBIDDEN_DSN_VALUE_CHARS = frozenset({"\n", "\r", "\x00", ";", "[", "]"})


@dataclass(frozen=True, slots=True)
class BcpDsnOptions:
    """Finite ODBC options required by BCP for an AG listener."""

    driver: str = "ODBC Driver 18 for SQL Server"
    encrypt: str = "Yes"
    trust_server_certificate: str = "No"
    multi_subnet_failover: bool = True
    login_timeout_seconds: int = 60

    def __post_init__(self) -> None:
        _require_safe_value(self.driver, label="ODBC driver")
        _require_safe_value(self.encrypt, label="BCP DSN Encrypt option")
        trust = _require_safe_value(
            self.trust_server_certificate,
            label="BCP DSN TrustServerCertificate option",
        )
        if trust.lower() not in {"yes", "no"}:
            raise ValueError("BCP DSN TrustServerCertificate must be Yes or No")
        if not isinstance(self.multi_subnet_failover, bool):
            raise TypeError("BCP DSN MultiSubnetFailover must be boolean")
        if not 1 <= int(self.login_timeout_seconds) <= 3_600:
            raise ValueError("BCP DSN login timeout must be between 1 and 3600 seconds")


class BcpDsnLease:
    """Own one private DSN file until its BCP process reaches a terminal state."""

    def __init__(self, directory: Path, environment: dict[str, str]) -> None:
        self.directory = directory
        self.environment = environment
        self.dsn_name = _DSN_NAME
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        shutil.rmtree(self.directory)


def issue_bcp_dsn(
    *,
    host: str,
    port: int,
    options: BcpDsnOptions,
) -> BcpDsnLease:
    """Create a private, credential-free DSN and return its owned lifecycle."""

    safe_host = _require_safe_value(host, label="SQL Server host")
    safe_driver = _require_safe_value(options.driver, label="ODBC driver")
    if not 1 <= int(port) <= 65_535:
        raise ValueError("SQL Server port must be between 1 and 65535")
    directory = Path(tempfile.mkdtemp(prefix="dpone-bcp-dsn-"))
    try:
        os.chmod(directory, 0o700)
        path = directory / "odbc.ini"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(
                f"[{_DSN_NAME}]\n"
                f"Driver={safe_driver}\n"
                f"Server=tcp:{safe_host},{int(port)}\n"
                f"MultiSubnetFailover={'Yes' if options.multi_subnet_failover else 'No'}\n"
                f"LoginTimeout={int(options.login_timeout_seconds)}\n"
                f"Encrypt={str(options.encrypt).strip()}\n"
                "TrustServerCertificate="
                f"{str(options.trust_server_certificate).strip()}\n"
            )
        environment = dict(os.environ)
        environment["ODBCINI"] = str(path)
        environment.setdefault("ODBCSYSINI", "/etc")
        return BcpDsnLease(directory, environment)
    except BaseException:
        shutil.rmtree(directory)
        raise


def _require_safe_value(value: object, *, label: str) -> str:
    text = str(value or "").strip()
    if not text or any(character in text for character in _FORBIDDEN_DSN_VALUE_CHARS):
        raise ValueError(f"{label} is invalid for an ODBC DSN")
    return text


__all__ = ["BcpDsnLease", "BcpDsnOptions", "issue_bcp_dsn"]
