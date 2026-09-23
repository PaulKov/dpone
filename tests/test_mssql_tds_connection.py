"""Pure connection definitions preserve original adapter class identities."""

import pytest

from dpone.adapters import mssql_tds_coordinator_connection as adapter
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial, TdsConnectionProfile


def test_original_imports_are_exact_canonical_classes():
    assert adapter.TdsConnectionMaterial is TdsConnectionMaterial
    assert adapter.TdsConnectionProfile is TdsConnectionProfile
    material = TdsConnectionMaterial("localhost", 1433, "database", "user", "secret")
    assert repr(material) == "TdsConnectionMaterial()"


@pytest.mark.parametrize(
    "field,value",
    [("host", "bad;host"), ("port", True), ("password", ""), ("password", "a\0b"), ("password", "a" * 16385)],
)
def test_pure_material_keeps_original_rejections(field, value):
    values = dict(host="localhost", port=1433, database="database", username="user", password="secret")
    values[field] = value
    with pytest.raises(ValueError):
        TdsConnectionMaterial(**values)
