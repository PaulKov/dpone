"""Memory-only credentials for the explicitly selected SqlClient backend.

Never persist or log this record, its fields, serialized jobs, or password hashes.
The disposable TLS profile is representable here but must be authorized by the
trusted synthetic-environment composition before delivery. This DTO grants no
permission to connect and performs no I/O. Python does not guarantee zeroization.
"""

import re
from dataclasses import dataclass
from typing import Literal

from dpone.contracts.mssql_tds_validation import _integer, _text

_ERROR = "mssql_native.sqlclient_credentials_invalid"


@dataclass(frozen=True, repr=False)
class SqlClientCredentials:
    """Closed typed connection material; TLS selection is explicit, never inferred."""

    host: str
    port: int
    database: str
    username: str
    password: str
    tls_profile: Literal["verified", "disposable_test"]

    def __post_init__(self) -> None:
        try:
            if type(self.host) is not str or re.fullmatch(r"[A-Za-z0-9.\-:\[\]]{1,255}", self.host) is None:
                raise ValueError(_ERROR)
            _integer(self.port, 1, 65535)
            for identifier in (self.database, self.username):
                _text(identifier, 128)
                if len(identifier.encode("utf-16le")) > 256:
                    raise ValueError(_ERROR)
            if type(self.password) is not str or not self.password or "\0" in self.password:
                raise ValueError(_ERROR)
            if len(self.password.encode("utf-8", errors="strict")) > 16384:
                raise ValueError(_ERROR)
            if type(self.tls_profile) is not str or self.tls_profile not in ("verified", "disposable_test"):
                raise ValueError(_ERROR)
        except (ValueError, TypeError, UnicodeError):
            raise ValueError(_ERROR) from None
