"""Application service for atomic SQL Server semantic-refresh admission."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql import (
        MssqlAdmissionReceipt,
        MssqlAdmissionRequest,
        SemanticRefreshMssqlAdmissionPort,
    )


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlAdmissionService:
    """Pass one already-validated full admission unit to its authority."""

    admission: SemanticRefreshMssqlAdmissionPort

    def admit(self, request: MssqlAdmissionRequest) -> MssqlAdmissionReceipt:
        """Admit all guards/journals atomically through one port invocation."""

        return self.admission.admit(request)


__all__ = ["SemanticRefreshMssqlAdmissionService"]
