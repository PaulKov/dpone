from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.runtime.state.mssql import MSSQLXMinStateStorage
from dpone.runtime.state.mssql_generic_transaction_storage import (
    MssqlGenericTransactionStateStorage,
)
from dpone.runtime.state.mssql_route_preflight import (
    MssqlAtomicRoutePreflightError,
    require_atomic_mssql_route,
)


def _identity(*, replica: str = "SQLNODE01", principal: str = "svc_dpone") -> dict[str, str]:
    return {
        "server_name": "DWH-LISTENER",
        "machine_name": "DWHCLUSTER",
        "instance_name": "MSSQLSERVER",
        "replica_name": replica,
        "effective_principal": principal,
        "original_login": principal,
    }


class _Session:
    def __init__(self, identity: dict[str, str], *, host: str, deny_probe: bool = False) -> None:
        self.identity = identity
        self.host = host
        self.deny_probe = deny_probe
        self.calls: list[str] = []
        self.rolled_back = False

    def get_records(self, query, params=None, as_dict=False):
        del params, as_dict
        self.calls.append(str(query))
        return [self.identity]

    def begin(self) -> None:
        self.calls.append("BEGIN")

    def execute_query(self, query, params=None) -> int:
        del params
        self.calls.append(str(query))
        if self.deny_probe:
            raise PermissionError("denied")
        return 0

    def rollback(self) -> None:
        self.calls.append("ROLLBACK")
        self.rolled_back = True

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    @staticmethod
    def qualified_name(schema: str, table: str, *, database: str | None = None) -> str:
        return ".".join(f"[{value}]" for value in (database, schema, table) if value)


def _storage(session: _Session):
    storage = SimpleNamespace(
        atomicity="target_atomic",
        connector=session,
        database="Example_System",
        schema="dbo",
        table="dpone_source_state",
        receipt_table="dpone_commit_receipt",
        repair_authority_table="dpone_repair_authority",
        repair_consumption_table="dpone_repair_authority_consumption",
        run_table="dpone_run_state",
        audit_table="dpone_load_audit",
        preflight_count=0,
        catalog_preflight_count=0,
    )

    def ensure() -> None:
        storage.preflight_count += 1

    storage.create_state_table = ensure

    def preflight_catalog() -> None:
        storage.catalog_preflight_count += 1

    storage.preflight_atomic_catalog = preflight_catalog
    return storage


def test_listener_aliases_are_accepted_when_sql_identity_and_replica_match() -> None:
    target = _Session(_identity(), host="dwh-write.internal")
    state_session = _Session(_identity(), host="192.0.2.42")
    storage = _storage(state_session)

    require_atomic_mssql_route(target, storage)

    assert storage.preflight_count == 1
    assert storage.catalog_preflight_count == 1
    assert target.rolled_back is True
    probe = next(call for call in target.calls if "dpone_source_state" in call)
    assert "[Example_System].[dbo].[dpone_source_state]" in probe
    assert "WHERE 1 = 0" in probe
    operational_probe = next(call for call in state_session.calls if "dpone_run_state" in call)
    assert "[Example_System].[dbo].[dpone_load_audit]" in operational_probe
    assert state_session.rolled_back is True


def test_missing_atomic_catalog_preflight_fails_before_identity_or_permission_probes() -> None:
    target = _Session(_identity(), host="listener")
    state_session = _Session(_identity(), host="listener")
    storage = _storage(state_session)
    del storage.preflight_atomic_catalog

    with pytest.raises(MssqlAtomicRoutePreflightError, match="state_catalog_preflight_missing"):
        require_atomic_mssql_route(target, storage)

    assert storage.preflight_count == 0
    assert not target.calls
    assert not state_session.calls


def test_ag_listener_sessions_on_different_physical_replicas_fail_closed() -> None:
    target = _Session(_identity(replica="SQLNODE01"), host="listener")
    storage = _storage(_Session(_identity(replica="SQLNODE02"), host="listener"))

    with pytest.raises(MssqlAtomicRoutePreflightError, match="instance_or_replica_mismatch"):
        require_atomic_mssql_route(target, storage)

    assert "BEGIN" not in target.calls


def test_different_effective_principals_fail_before_permission_probe() -> None:
    target = _Session(_identity(principal="svc_target"), host="listener")
    storage = _storage(_Session(_identity(principal="svc_state"), host="listener"))

    with pytest.raises(MssqlAtomicRoutePreflightError, match="effective_principal_mismatch"):
        require_atomic_mssql_route(target, storage)

    assert "BEGIN" not in target.calls


def test_cross_database_permission_denial_always_rolls_back_probe() -> None:
    target = _Session(_identity(), host="listener", deny_probe=True)
    storage = _storage(_Session(_identity(), host="listener"))

    with pytest.raises(MssqlAtomicRoutePreflightError, match="cross_database_permission_denied"):
        require_atomic_mssql_route(target, storage)

    assert target.rolled_back is True
    assert target.calls[-1] == "ROLLBACK"


def test_operational_state_permission_denial_rolls_back_state_session() -> None:
    target = _Session(_identity(), host="listener")
    state_session = _Session(_identity(), host="listener", deny_probe=True)
    storage = _storage(state_session)

    with pytest.raises(MssqlAtomicRoutePreflightError, match="operational_state_permission_denied"):
        require_atomic_mssql_route(target, storage)

    assert target.rolled_back is True
    assert state_session.rolled_back is True


def test_generic_transaction_storage_requires_only_four_object_preflight(monkeypatch) -> None:
    target = _Session(_identity(), host="listener")
    state_session = _Session(_identity(), host="listener")
    storage = MssqlGenericTransactionStateStorage(
        state_session,
        database="Example_System",
        schema="dbo",
    )
    storage.bind_database_authority(SimpleNamespace(verify=lambda **_kwargs: None))
    captured: list[tuple[object, object]] = []

    def preflight(self, target_connector) -> None:
        captured.append((self.connector, target_connector))

    monkeypatch.setattr(
        "dpone.runtime.state.mssql_generic_transaction.MssqlGenericTransactionState.preflight",
        preflight,
    )

    require_atomic_mssql_route(target, storage)

    assert captured == [(state_session, target)]
    assert not hasattr(storage, "create_state_table")
    assert all("dpone_source_state" not in call for call in (*target.calls, *state_session.calls))


def test_generic_transaction_storage_rejects_unbound_database_authority_before_io() -> None:
    target = _Session(_identity(), host="listener")
    state_session = _Session(_identity(), host="listener")
    storage = MssqlGenericTransactionStateStorage(
        state_session,
        database="Example_System",
        schema="dbo",
    )

    with pytest.raises(RuntimeError, match="database_authority_verifier_required"):
        require_atomic_mssql_route(target, storage)

    assert target.calls == []
    assert state_session.calls == []


def test_xmin_storage_rejects_unbound_database_authority_before_io() -> None:
    target = _Session(_identity(), host="listener")
    state_session = _Session(_identity(), host="listener")
    storage = MSSQLXMinStateStorage(
        state_session,
        database="Example_System",
        schema="dbo",
        atomicity="target_atomic",
        provisioning="external",
    )

    with pytest.raises(RuntimeError, match="database_authority_verifier_required"):
        require_atomic_mssql_route(target, storage)

    assert target.calls == []
    assert state_session.calls == []
