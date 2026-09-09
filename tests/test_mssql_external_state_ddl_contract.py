from __future__ import annotations

from pathlib import Path

from dpone.runtime.state.mssql_contract import COMMIT_RECEIPT_CONTRACT

ROOT = Path(__file__).resolve().parents[1]
EXTERNAL_STATE_DDL = ROOT / "tests/integration/postgres/sql/postgres_xmin_mssql_external_state.sql"


def test_publication_receipt_ddl_matches_exact_mssql_catalog_shape() -> None:
    """Keep SQL character width aligned with ``sys.columns.max_length`` bytes."""

    shape = next(item for item in COMMIT_RECEIPT_CONTRACT.shapes if item.name == "publication_receipt_id")
    ddl = EXTERNAL_STATE_DDL.read_text(encoding="utf-8")

    assert shape.type_name == "nvarchar"
    assert shape.max_length == 2 * 128
    assert "[publication_receipt_id] nvarchar(128) NULL" in ddl
    assert "[publication_receipt_id] nvarchar(256)" not in ddl
