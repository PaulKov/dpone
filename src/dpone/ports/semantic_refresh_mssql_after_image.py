"""Protected committed-after-image boundary for semantic-refresh sealing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from dpone.ports.semantic_refresh_mssql_authority_records import (
    MssqlProtectedWritableColumn,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql_authority import (
        MssqlProtectedOperationAuthority,
    )


@dataclass(frozen=True, slots=True)
class MssqlCommittedAfterImageSnapshot:
    """Rows and identity proven together in one SQL Server transaction."""

    relation_id: str
    image_sha256: str
    row_count: int
    columns: tuple[MssqlProtectedWritableColumn, ...]
    rows: tuple[tuple[object, ...], ...]

    def __post_init__(self) -> None:
        if not self.relation_id.strip():
            raise ValueError("after-image relation identity must be non-empty")
        if (
            not self.image_sha256.startswith("sha256:")
            or len(self.image_sha256) != 71
            or any(character not in "0123456789abcdef" for character in self.image_sha256[7:])
        ):
            raise ValueError("after-image digest must be canonical lowercase sha256")
        if isinstance(self.row_count, bool) or self.row_count < 0:
            raise ValueError("after-image row count must be non-negative")
        if not self.columns or any(not isinstance(item, MssqlProtectedWritableColumn) for item in self.columns):
            raise TypeError("after-image columns must be a non-empty protected closure")
        if self.row_count != len(self.rows) or any(len(row) != len(self.columns) for row in self.rows):
            raise ValueError("after-image rows differ from the protected schema or count")


class SemanticRefreshMssqlAfterImagePort(Protocol):
    """Read one immutable committed image using protected operation authority."""

    def read(self, operation: MssqlProtectedOperationAuthority) -> MssqlCommittedAfterImageSnapshot:
        """Return exact rows only after schema, count and digest verification."""


__all__ = [
    "MssqlCommittedAfterImageSnapshot",
    "SemanticRefreshMssqlAfterImagePort",
]
