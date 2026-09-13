from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.mssql_database_authority import (
    MssqlDatabaseAuthorityContractError,
    MssqlDatabaseAuthoritySet,
)
from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
)
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.etl.mssql_transaction_identity import invocation_route_fingerprint
from dpone.runtime.sinks.staging_managers.mssql import MSSQLStagingManager
from dpone.runtime.state.mssql import MSSQLXMinStateStorage
from dpone.runtime.state.mssql_database_authority import (
    MSSQL_DATABASE_AUTHORITY_SHA256_OPTION,
    MssqlDatabaseAuthorityVerificationError,
    MssqlDatabaseAuthorityVerifier,
)
from dpone.runtime.state.mssql_route_preflight import require_atomic_mssql_route


def _guid(token: str) -> str:
    return f"{token * 8}-{token * 4}-{token * 4}-{token * 4}-{token * 12}"


def _pin(database_id: int, token: str) -> dict[str, Any]:
    return {
        "database_id": database_id,
        "create_token": f"2026-08-16T00:00:0{token}.0000000",
        "database_guid": _guid(token),
    }


@pytest.mark.parametrize("capability", ["source", "target", "staging", "state"])
def test_database_authority_set_is_closed_canonical_and_case_alias_aware(capability: str) -> None:
    properties = _connection_properties("DWH", {"DWH": _pin(7, "1")})

    authorities = MssqlDatabaseAuthoritySet.from_connection_properties(
        properties,
        capability=capability,
    )

    assert authorities.require("dwh", capability=capability).database_name == "DWH"


@pytest.mark.parametrize(
    ("authorities", "code"),
    (
        ({}, "database_authorities_required"),
        ({"DWH": {**_pin(7, "1"), "extra": True}}, "database_authority_fields_invalid"),
        ({"DWH": {**_pin(7, "1"), "database_id": True}}, "database_authority_id_invalid"),
        ({"DWH": {**_pin(7, "1"), "create_token": "today"}}, "database_authority_create_token_invalid"),
        (
            {"DWH": {**_pin(7, "1"), "database_guid": "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"}},
            "database_authority_guid_not_canonical",
        ),
    ),
)
def test_database_authority_set_rejects_open_or_noncanonical_documents(
    authorities: dict[str, Any],
    code: str,
) -> None:
    properties = {"database": "DWH", "database_authorities": authorities}

    with pytest.raises(MssqlDatabaseAuthorityContractError, match=code):
        MssqlDatabaseAuthoritySet.from_connection_properties(properties, capability="target")


def test_cross_database_staging_requires_its_own_signed_pin() -> None:
    authorities = MssqlDatabaseAuthoritySet.from_connection_properties(
        _connection_properties("DWH", {"DWH": _pin(7, "1")}),
        capability="target",
    )

    with pytest.raises(MssqlDatabaseAuthorityContractError, match="staging_database_authority_required"):
        authorities.require("DWH_Stage", capability="staging")


def test_verifier_checks_target_staging_state_and_current_sessions() -> None:
    target_connection = _connection(
        "DWH",
        {"DWH": _pin(7, "1"), "DWH_Stage": _pin(8, "2")},
    )
    state_connection = _connection("Example_System", {"Example_System": _pin(9, "3")})
    databases = _database_rows()
    masters: list[_Connector] = []

    def master_factory(_connection: ResolvedBindingConnection) -> _Connector:
        connector = _Connector("master", databases)
        masters.append(connector)
        return connector

    verifier = MssqlDatabaseAuthorityVerifier.from_connections(
        target_connection=target_connection,
        state_connection=state_connection,
        target_database="DWH",
        staging_database="DWH_Stage",
        state_database="Example_System",
        master_connector_factory=master_factory,
    )

    verifier.verify(
        target_connector=_Connector("DWH", databases),
        state_connector=_Connector("Example_System", databases),
    )

    assert len(masters) == 2
    assert all(connector.closed for connector in masters)


def test_verifier_propagates_parent_timeout_to_ephemeral_master_sessions() -> None:
    databases = _database_rows()
    masters: list[_Connector] = []

    def master_factory(_connection: ResolvedBindingConnection) -> _Connector:
        connector = _Connector("master", databases)
        masters.append(connector)
        return connector

    verifier = MssqlDatabaseAuthorityVerifier.from_connections(
        target_connection=_connection("DWH", {"DWH": _pin(7, "1")}),
        state_connection=_connection("Example_System", {"Example_System": _pin(9, "3")}),
        target_database="DWH",
        staging_database="DWH",
        state_database="Example_System",
        master_connector_factory=master_factory,
    )

    with verifier.bounded_query_timeout(5):
        verifier.verify(
            target_connector=_Connector("DWH", databases),
            state_connector=_Connector("Example_System", databases),
        )

    assert len(masters) == 2
    assert all(connector.timeout_scopes == [5] for connector in masters)
    assert all(
        connector.query_timeouts_observed and set(connector.query_timeouts_observed) == {5} for connector in masters
    )
    assert all(connector.query_timeout == 0 for connector in masters)


def test_verify_pins_uses_nested_active_timeout_without_widening() -> None:
    databases = _database_rows()
    masters: list[_Connector] = []

    def master_factory(_connection: ResolvedBindingConnection) -> _Connector:
        connector = _Connector("master", databases)
        masters.append(connector)
        return connector

    verifier = MssqlDatabaseAuthorityVerifier.from_connections(
        target_connection=_connection("DWH", {"DWH": _pin(7, "1")}),
        state_connection=_connection("Example_System", {"Example_System": _pin(9, "3")}),
        target_database="DWH",
        staging_database="DWH",
        state_database="Example_System",
        master_connector_factory=master_factory,
    )

    with verifier.bounded_query_timeout(2):
        with verifier.bounded_query_timeout(5):
            verifier.verify_pins()
        verifier.verify_pins()
    verifier.verify_pins()

    assert len(masters) == 6
    assert all(connector.timeout_scopes == [2] for connector in masters[:4])
    assert all(connector.timeout_scopes == [] for connector in masters[4:])
    assert all(set(connector.query_timeouts_observed) == {2} for connector in masters[:4])
    assert all(set(connector.query_timeouts_observed) == {0} for connector in masters[4:])
    assert all(connector.query_timeout == 0 for connector in masters)
    assert all(connector.closed for connector in masters)


def test_staging_use_boundary_rechecks_fresh_master_after_early_preflight() -> None:
    target_connection = _connection(
        "DWH",
        {"DWH": _pin(7, "1"), "DWH_Stage": _pin(8, "2")},
    )
    state_connection = _connection("Example_System", {"Example_System": _pin(9, "3")})
    databases = _database_rows()
    masters: list[_Connector] = []

    def master_factory(_connection: ResolvedBindingConnection) -> _Connector:
        connector = _Connector("master", databases)
        masters.append(connector)
        return connector

    verifier = MssqlDatabaseAuthorityVerifier.from_connections(
        target_connection=target_connection,
        state_connection=state_connection,
        target_database="DWH",
        staging_database="DWH_Stage",
        state_database="Example_System",
        master_connector_factory=master_factory,
        staging_connector_factory=lambda _connection, database: _Connector(
            database,
            databases,
        ),
    )
    verifier.verify_pins()
    databases["DWH_Stage"] = {
        **databases["DWH_Stage"],
        "database_guid": _guid("f"),
    }

    with pytest.raises(
        MssqlDatabaseAuthorityVerificationError,
        match="staging_database_identity_mismatch",
    ):
        verifier.acquire_staging_lease(
            target_connector=_Connector("DWH", databases),
        )

    assert len(masters) == 3
    assert all(connector.closed for connector in masters)


def test_staging_manager_checks_authority_before_create_sql() -> None:
    class RejectingAuthority:
        def acquire_staging_database_authority_lease(self, connector: Any) -> None:
            assert connector is target
            raise MssqlDatabaseAuthorityVerificationError("mssql_transaction.staging_database_identity_mismatch")

    target = _Connector("DWH", _database_rows())
    manager = MSSQLStagingManager(
        target,
        database_authority=RejectingAuthority(),
    )
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="events",
        target_schema="dbo",
        target_table="events",
        staging_database="DWH_Stage",
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
    )

    with pytest.raises(
        MssqlDatabaseAuthorityVerificationError,
        match="staging_database_identity_mismatch",
    ):
        manager.create(config, [("id", "int")])

    assert target.queries == []


def test_staging_manager_releases_authority_without_dropping_preserved_table() -> None:
    class Lease:
        closed = False

        def close(self) -> None:
            self.closed = True

    target = _Connector("DWH", _database_rows())
    manager = MSSQLStagingManager(target)
    artifact = SimpleNamespace()
    lease = Lease()
    manager._authority_leases[id(artifact)] = lease

    manager.release_authority_lease(artifact)
    manager.release_authority_lease(artifact)

    assert lease.closed is True
    assert id(artifact) not in manager._authority_leases
    assert not any("DROP TABLE" in query for query, _params in target.queries)


def test_verifier_closes_target_master_when_state_master_creation_fails() -> None:
    target_connection = _connection("DWH", {"DWH": _pin(7, "1")})
    state_connection = _connection("Example_System", {"Example_System": _pin(9, "3")})
    target_master = _Connector("master", _database_rows())
    calls = 0

    def master_factory(_connection: ResolvedBindingConnection) -> _Connector:
        nonlocal calls
        calls += 1
        if calls == 1:
            return target_master
        raise PermissionError("state master unavailable")

    verifier = MssqlDatabaseAuthorityVerifier.from_connections(
        target_connection=target_connection,
        state_connection=state_connection,
        target_database="DWH",
        staging_database="DWH",
        state_database="Example_System",
        master_connector_factory=master_factory,
    )

    with verifier.bounded_query_timeout(5):
        with pytest.raises(PermissionError, match="state master unavailable"):
            verifier.verify(
                target_connector=_Connector("DWH", _database_rows()),
                state_connector=_Connector("Example_System", _database_rows()),
            )

    assert target_master.closed is True
    assert target_master.timeout_scopes == [5]
    assert target_master.query_timeout == 0


def test_verifier_rejects_reused_database_id_with_changed_guid() -> None:
    target_connection = _connection("DWH", {"DWH": _pin(7, "1")})
    state_connection = _connection("Example_System", {"Example_System": _pin(9, "3")})
    databases = _database_rows()
    databases["DWH"] = {**databases["DWH"], "database_guid": _guid("f")}
    verifier = MssqlDatabaseAuthorityVerifier.from_connections(
        target_connection=target_connection,
        state_connection=state_connection,
        target_database="DWH",
        staging_database="DWH",
        state_database="Example_System",
        master_connector_factory=lambda _connection: _Connector("master", databases),
    )

    with pytest.raises(MssqlDatabaseAuthorityVerificationError, match="target_database_identity_mismatch"):
        verifier.verify(
            target_connector=_Connector("DWH", databases),
            state_connector=_Connector("Example_System", databases),
        )


def test_verifier_requires_explicit_database_metadata_permission_before_catalog() -> None:
    target_connection = _connection("DWH", {"DWH": _pin(7, "1")})
    state_connection = _connection("Example_System", {"Example_System": _pin(9, "3")})
    databases = _database_rows()

    def master_factory(connection: ResolvedBindingConnection) -> _Connector:
        return _Connector(
            "master",
            databases,
            view_any_database=connection is state_connection,
        )

    verifier = MssqlDatabaseAuthorityVerifier.from_connections(
        target_connection=target_connection,
        state_connection=state_connection,
        target_database="DWH",
        staging_database="DWH",
        state_database="Example_System",
        master_connector_factory=master_factory,
    )

    with pytest.raises(
        MssqlDatabaseAuthorityVerificationError,
        match="target_database_metadata_permission_denied",
    ):
        verifier.verify(
            target_connector=_Connector("DWH", databases),
            state_connector=_Connector("Example_System", databases),
        )


def test_verifier_accepts_sysadmin_when_permission_scalar_is_null() -> None:
    databases = _database_rows()
    verifier = MssqlDatabaseAuthorityVerifier.from_connections(
        target_connection=_connection("DWH", {"DWH": _pin(7, "1")}),
        state_connection=_connection("Example_System", {"Example_System": _pin(9, "3")}),
        target_database="DWH",
        staging_database="DWH",
        state_database="Example_System",
        master_connector_factory=lambda _connection: _Connector(
            "master",
            databases,
            view_any_database=None,
            is_sysadmin=True,
        ),
    )

    verifier.verify_pins()


def test_verifier_normalizes_permission_query_failure_without_catalog_lookup() -> None:
    databases = _database_rows()
    verifier = MssqlDatabaseAuthorityVerifier.from_connections(
        target_connection=_connection("DWH", {"DWH": _pin(7, "1")}),
        state_connection=_connection("Example_System", {"Example_System": _pin(9, "3")}),
        target_database="DWH",
        staging_database="DWH",
        state_database="Example_System",
        master_connector_factory=lambda _connection: _Connector(
            "master",
            databases,
            permission_query_error=True,
        ),
    )

    with pytest.raises(
        MssqlDatabaseAuthorityVerificationError,
        match="target_database_authority_query_failed",
    ):
        verifier.verify_pins()


def test_xmin_route_rejects_same_name_recreate_before_catalog_or_source() -> None:
    target_connection = _connection("DWH", {"DWH": _pin(7, "1")})
    state_connection = _connection("Example_System", {"Example_System": _pin(9, "3")})
    databases = _database_rows()
    databases["DWH"] = {**databases["DWH"], "database_guid": _guid("f")}
    verifier = MssqlDatabaseAuthorityVerifier.from_connections(
        target_connection=target_connection,
        state_connection=state_connection,
        target_database="DWH",
        staging_database="DWH",
        state_database="Example_System",
        master_connector_factory=lambda _connection: _Connector("master", databases),
    )
    state_session = _Connector("Example_System", databases)
    storage = MSSQLXMinStateStorage(
        state_session,
        database="Example_System",
        schema="governance",
        atomicity="target_atomic",
        provisioning="external",
    ).bind_database_authority(verifier)
    target_session = _Connector("DWH", databases)

    with pytest.raises(
        MssqlDatabaseAuthorityVerificationError,
        match="target_database_identity_mismatch",
    ):
        require_atomic_mssql_route(target_session, storage)

    assert storage._table_created is False
    assert not any("dpone_source_state" in query for query in target_session.queries)


def test_database_authority_rotation_changes_same_invocation_route_fingerprint() -> None:
    first = _verifier(target_pin=_pin(7, "1"))
    second = _verifier(target_pin={**_pin(7, "1"), "database_guid": _guid("f")})
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_database="source",
        target_database="DWH",
        source_schema="public",
        source_table="events",
        target_schema="dbo",
        target_table="events",
        staging_database="DWH",
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={MSSQL_DATABASE_AUTHORITY_SHA256_OPTION: first.authority_sha256},
    )
    source = SourcePhysicalIdentity(
        dialect="postgres",
        cluster_identifier="cluster",
        database="source",
        effective_principal="reader",
        session_principal="reader",
    )
    first_fingerprint = invocation_route_fingerprint(
        config,
        target_identity=b"t" * 32,
        source_identity=source,
    )
    config.options[MSSQL_DATABASE_AUTHORITY_SHA256_OPTION] = second.authority_sha256

    assert first.authority_sha256 != second.authority_sha256
    assert (
        invocation_route_fingerprint(
            config,
            target_identity=b"t" * 32,
            source_identity=source,
        )
        != first_fingerprint
    )


def _verifier(*, target_pin: dict[str, Any]) -> MssqlDatabaseAuthorityVerifier:
    return MssqlDatabaseAuthorityVerifier.from_connections(
        target_connection=_connection("DWH", {"DWH": target_pin}),
        state_connection=_connection("Example_System", {"Example_System": _pin(9, "3")}),
        target_database="DWH",
        staging_database="DWH",
        state_database="Example_System",
        master_connector_factory=lambda _connection: pytest.fail("digest must not perform I/O"),
    )


def _connection(
    database: str,
    authorities: dict[str, dict[str, Any]],
) -> ResolvedBindingConnection:
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(database=database),
        safe_metadata={},
        descriptor=ResolvedConnectionDescriptor(
            connection_type="mssql",
            properties=_connection_properties(database, authorities),
        ),
    )


def _connection_properties(
    database: str,
    authorities: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return {"database": database, "database_authorities": authorities}


def _database_rows() -> dict[str, dict[str, Any]]:
    return {
        name: {
            "database_id": database_id,
            "database_name": name,
            "state_desc": "ONLINE",
            "user_access_desc": "MULTI_USER",
            "has_dbaccess": 1,
            "create_token": pin["create_token"],
            "database_guid": pin["database_guid"],
        }
        for name, database_id, pin in (
            ("DWH", 7, _pin(7, "1")),
            ("DWH_Stage", 8, _pin(8, "2")),
            ("Example_System", 9, _pin(9, "3")),
        )
    }


class _Connector:
    def __init__(
        self,
        current_database: str,
        databases: dict[str, dict[str, Any]],
        *,
        view_any_database: bool | None = True,
        is_sysadmin: bool = False,
        permission_query_error: bool = False,
        query_timeout: int = 0,
    ) -> None:
        self.current_database = current_database
        self.databases = databases
        self.view_any_database = view_any_database
        self.is_sysadmin = is_sysadmin
        self.permission_query_error = permission_query_error
        self.closed = False
        self.queries: list[str] = []
        self.query_timeout = query_timeout
        self.timeout_scopes: list[int] = []
        self.query_timeouts_observed: list[int] = []

    @contextmanager
    def bounded_query_timeout(self, seconds: int) -> Iterator[None]:
        previous = self.query_timeout
        self.query_timeout = min(previous, seconds) if previous > 0 else seconds
        self.timeout_scopes.append(self.query_timeout)
        try:
            yield
        finally:
            self.query_timeout = previous

    def get_records(
        self,
        query: str,
        params: tuple[Any, ...] = (),
        *,
        as_dict: bool,
    ) -> list[dict[str, Any]]:
        assert as_dict
        self.queries.append(query)
        self.query_timeouts_observed.append(self.query_timeout)
        if "HAS_PERMS_BY_NAME" in query:
            if self.permission_query_error:
                raise PermissionError("metadata permission query denied")
            return [
                {
                    "permitted": (None if self.view_any_database is None else int(self.view_any_database)),
                    "is_sysadmin": int(self.is_sysadmin),
                }
            ]
        if "SERVERPROPERTY" in query:
            return [
                {
                    "server_name": "SQLNODE",
                    "machine_name": "SQLNODE",
                    "instance_name": "MSSQLSERVER",
                    "replica_name": "SQLNODE",
                    "effective_principal": "svc_dpone",
                    "original_login": "svc_dpone",
                }
            ]
        if "database_recovery_status" in query:
            database_id, database_name = params
            return [
                dict(row)
                for row in self.databases.values()
                if row is not None and (row["database_id"] == database_id or row["database_name"] == database_name)
            ]
        if "DB_ID() AS database_id" in query:
            return [dict(self.databases[self.current_database])]
        raise AssertionError(f"unexpected SQL: {query}")

    def close(self) -> None:
        self.closed = True
