"""Create-only MSSQL persistence boundary for complete baseline receipts."""

from __future__ import annotations

from typing import Protocol

from dpone.contracts.semantic_refresh_baseline_receipt import (
    SemanticRefreshBaselineAdoptionReceipt,
)


class SemanticRefreshMssqlBaselineReceiptStorePort(Protocol):
    """Persist and resolve canonical baseline receipts without issuing evidence."""

    def persist_exact(
        self,
        receipt: SemanticRefreshBaselineAdoptionReceipt,
    ) -> SemanticRefreshBaselineAdoptionReceipt:
        """Create or exact-replay one COMPLETE receipt."""

    def load_exact(
        self,
        *,
        mssql_connection_authority_id: str,
        mssql_relation_id: str,
    ) -> SemanticRefreshBaselineAdoptionReceipt:
        """Return one current COMPLETE receipt after canonical revalidation."""


def validate_mssql_baseline_receipt_identity(
    receipt: SemanticRefreshBaselineAdoptionReceipt,
) -> None:
    """Require the canonical receipt's MSSQL relation and physical authority to agree."""

    if not isinstance(receipt, SemanticRefreshBaselineAdoptionReceipt):
        raise TypeError("receipt must be a canonical baseline adoption receipt")
    relation_parts = receipt.mssql_relation_id.split(".")
    if len(relation_parts) != 3 or any(not part for part in relation_parts):
        raise ValueError("baseline MSSQL relation must be database.schema.table")
    database, schema, table = relation_parts
    expected = f"mssql://{receipt.mssql_connection_authority_id}/{database}/{schema}.{table}"
    if receipt.mssql_target_authority_id != expected:
        raise ValueError("baseline MSSQL target authority differs from relation identity")


__all__ = [
    "SemanticRefreshMssqlBaselineReceiptStorePort",
    "validate_mssql_baseline_receipt_identity",
]
