"""Backend-neutral state factory input contracts."""

from __future__ import annotations

import pytest

from dpone.runtime.credentials.config import require_connection_id


@pytest.mark.parametrize("value", [None, "", "   "])
def test_connection_id_is_required_before_backend_connector_creation(value: str | None) -> None:
    with pytest.raises(ValueError, match="MSSQL state connector requires a non-empty connection_id"):
        require_connection_id(value, backend="MSSQL")


def test_connection_id_is_normalized_once_for_every_backend_factory() -> None:
    assert require_connection_id("  logical_state  ", backend="PostgreSQL") == "logical_state"
