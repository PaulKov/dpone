"""Internal source-free admission port for MSSQL target authority V2."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.contracts.mssql_target_authority_v2_pre_source import (
        MssqlPreSourceAdmissionV2,
        MssqlPreSourceAuthorityRequestV2,
    )


class MssqlPreSourceAuthorityPort(Protocol):
    """Replay or exclusively admit one effect before source business I/O."""

    def admit(self, request: MssqlPreSourceAuthorityRequestV2) -> MssqlPreSourceAdmissionV2:
        """Return a committed target-only decision or fail closed."""


__all__ = ["MssqlPreSourceAuthorityPort"]
