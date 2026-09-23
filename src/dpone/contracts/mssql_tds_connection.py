"""Pure admitted connection material and profile, without driver imports or I/O.

The original coordinator adapter re-exports these exact classes. Passwords stay
repr-hidden; validation and limits are unchanged for existing callers.
"""

import re
from dataclasses import dataclass, field
from enum import StrEnum

from dpone.contracts.mssql_tds_coordinator_authority import identifier
from dpone.contracts.mssql_tds_validation import _integer


class TdsConnectionProfile(StrEnum):
    VERIFIED_TLS = "pyodbc-raw-verified-tls-v1"
    SYNTHETIC_LOCAL = "pyodbc-raw-synthetic-local-v1"


@dataclass(frozen=True)
class TdsConnectionMaterial:
    host: str = field(repr=False)
    port: int = field(repr=False)
    database: str = field(repr=False)
    username: str = field(repr=False)
    password: str = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.host) is not str or re.fullmatch(r"[A-Za-z0-9.\-:\[\]]{1,255}", self.host) is None:
            raise ValueError("mssql_native.tds_connection_material_invalid")
        _integer(self.port, 1, 65535)
        identifier(self.database)
        identifier(self.username)
        try:
            if (
                type(self.password) is not str
                or not self.password
                or "\0" in self.password
                or len(self.password.encode("utf-8")) > 16384
            ):
                raise ValueError
        except (ValueError, UnicodeError):
            raise ValueError("mssql_native.tds_connection_material_invalid") from None
