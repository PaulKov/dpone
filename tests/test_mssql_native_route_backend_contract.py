from types import SimpleNamespace

import pytest

from dpone.ports.mssql_native_route_backend import (
    BcpNativeRouteBackend,
    NativeActorCapacity,
    NativeMssqlBackend,
    SqlClientNativeRouteBackend,
    select_native_route_backend,
)


def _sqlclient(*, available: int = 11) -> SqlClientNativeRouteBackend:
    value = SimpleNamespace(
        import_file=lambda *args: None,
        inspect=lambda *args: None,
        settle=lambda *args: None,
        allocated_bytes=lambda: 0,
    )
    return SqlClientNativeRouteBackend(
        importer=value,
        settlement=value,
        capacity=NativeActorCapacity(available, 2, 3, 1),
        implementation_sha256="a" * 64,
    )


def test_capacity_formula_is_admitted_before_bundle_is_returned():
    bcp = BcpNativeRouteBackend(SimpleNamespace())
    selected = select_native_route_backend(NativeMssqlBackend.SQLCLIENT, bcp=bcp, sqlclient=_sqlclient(), parallelism=2)
    assert isinstance(selected, SqlClientNativeRouteBackend)
    assert selected.capacity.required(2) == 11


def test_explicit_sqlclient_never_falls_back_to_bcp():
    bcp = BcpNativeRouteBackend(SimpleNamespace())
    with pytest.raises(ValueError, match="authored_backend_unavailable"):
        select_native_route_backend(NativeMssqlBackend.SQLCLIENT, bcp=bcp, sqlclient=None, parallelism=1)
    with pytest.raises(ValueError, match="backend_capacity_insufficient"):
        select_native_route_backend(
            NativeMssqlBackend.SQLCLIENT, bcp=bcp, sqlclient=_sqlclient(available=10), parallelism=2
        )


def test_bcp_selection_preserves_legacy_settlement_without_sqlclient_admission():
    legacy = SimpleNamespace()
    bcp = BcpNativeRouteBackend(legacy)
    assert select_native_route_backend(NativeMssqlBackend.BCP, bcp=bcp, sqlclient=None, parallelism=64) is bcp


@pytest.mark.parametrize("parallelism", (0, True, -1))
def test_capacity_rejects_invalid_parallelism(parallelism):
    with pytest.raises(ValueError, match="import_parallelism_invalid"):
        NativeActorCapacity(10, 1, 1, 1).required(parallelism)
