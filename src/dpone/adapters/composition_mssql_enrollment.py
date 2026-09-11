"""Read protected SQL Server database enrollment before physical observation."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING
from uuid import UUID

from dpone.adapters.composition_mssql_database_policy import require_database_policy
from dpone.adapters.composition_mssql_issuance import require_gate_policy
from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.dbapi_lifecycle import row
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_physical import CompositionPhysicalDomain
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin, MssqlDatabaseAuthoritySet

if TYPE_CHECKING:
    from dpone.contracts.composition_activation import CompositionOccurrenceContext
    from dpone.contracts.dbt_relation_writes import DbtRelationWrite
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.ports.sql_connection import SqlControlConnection


def mssql_physical_domain(service_id: str, pin: MssqlDatabaseAuthorityPin) -> CompositionPhysicalDomain:
    """Central stable database preimage shared with external provisioning.

    Database name, endpoint spelling, principal, catalog and engine version are
    excluded. The pin's physical ID, recovery GUID and create token must first
    be compared with both protected enrollment and the actual server catalog.
    """
    try:
        if str(UUID(service_id)) != service_id or UUID(service_id).int == 0 or pin.database_guid.int == 0:
            raise ValueError("uuid")
        checked = MssqlDatabaseAuthorityPin.from_raw(
            pin.database_name,
            {
                "database_id": pin.database_id,
                "database_guid": str(pin.database_guid),
                "create_token": pin.create_token,
            },
            capability="target",
        )
        if checked != pin:
            raise ValueError("pin")
    except (ValueError, TypeError, AttributeError):
        raise CompositionAdmissionError("mssql_physical_identity") from None
    return CompositionPhysicalDomain(
        "mssql",
        service_id,
        canonical_fingerprint(
            {
                "schema": "dpone.composition-mssql-database.v1",
                "service_id": service_id,
                "database_id": pin.database_id,
                "database_guid": str(pin.database_guid),
                "create_token": pin.create_token,
            }
        ),
    )


def mssql_target_pin(connection: ResolvedBindingConnection, write: DbtRelationWrite) -> MssqlDatabaseAuthorityPin:
    """Select signed authority without learning missing pins from the endpoint."""
    try:
        descriptor = connection.descriptor
        if descriptor is None or descriptor.connection_type != "mssql" or write.connector != "mssql":
            raise ValueError("connector")
        authorities = MssqlDatabaseAuthoritySet.from_connection_properties(descriptor.properties, capability="target")
        database = connection.credentials.database
        if not isinstance(database, str) or not database:
            raise ValueError("database")
        default = authorities.require(database, capability="target")
        target = authorities.require(write.database or database, capability="target")
        if default != target:
            raise ValueError("cross database")
        return target
    except Exception:
        raise CompositionAdmissionError("mssql_database_authority") from None


class MssqlCompositionEnrollmentReader:
    """Inspect protected service/database enrollment and actual exclusive policy.

    The injected factory opens a bounded, dedicated controller connection from
    the sealed context. The mandatory target-service reader must independently inspect the protected
    service marker through the actual workload endpoint; matching database GUIDs
    across a cloned server is insufficient. Ordinary credentials never assert
    protected enrollment. No database, role, guard or login is created here.
    A successful read is not a lease; admission and issuance recheck authority.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[CompositionOccurrenceContext], SqlControlConnection],
        require_target_service: Callable[[ResolvedBindingConnection, str, CompositionOccurrenceContext], None],
        control_schema: str = "dpone_control",
    ) -> None:
        self._factory = connection_factory
        self._require_target_service = require_target_service
        self._schema = require_control_schema(control_schema)

    def resolve(
        self, connection: ResolvedBindingConnection, write: DbtRelationWrite, context: CompositionOccurrenceContext
    ) -> CompositionPhysicalDomain:
        """Verify exact signed incarnation, managed schema and physical continuity."""
        pin = mssql_target_pin(connection, write)
        assert connection.descriptor is not None
        service_id = connection.descriptor.properties.get("composition_service_id")
        if not isinstance(service_id, str):
            raise CompositionAdmissionError("mssql_service_identity")
        domain = mssql_physical_domain(service_id, pin)
        control = cursor = None
        failure = None
        try:
            self._require_target_service(connection, domain.service_id, context)
            control = self._factory(context)
            control.autocommit = False
            cursor = control.cursor()
            ledger = CompositionMssqlLedger(cursor, self._schema)
            transaction = ledger.begin(domain.service_id)
            cursor.execute("SELECT DB_NAME();")
            control_row = row(cursor)
            if control_row is None or len(control_row) != 1 or type(control_row[0]) is not str:
                raise CompositionAdmissionError("mssql_control_database")
            require_gate_policy(ledger, control_row[0])
            self._require_database(ledger, domain, pin, write.schema)
            ledger.require_transaction(transaction)
            self._require_target_service(connection, domain.service_id, context)
        except CompositionAdmissionError as exc:
            failure = exc
        except Exception:
            failure = CompositionAdmissionError("mssql_enrollment")
        finally:
            operations = []
            if control is not None:
                operations.append(control.rollback)
            if cursor is not None:
                operations.append(cursor.close)
            if control is not None:
                operations.append(control.close)
            for operation in operations:
                try:
                    operation()
                except Exception:
                    failure = CompositionAdmissionError("mssql_enrollment_cleanup")
        if failure is not None:
            raise failure from None
        return domain

    @staticmethod
    def _require_database(ledger, domain, pin, schema):
        cursor = ledger.cursor
        cursor.execute(
            "SELECT TOP (2) e.database_name,e.database_id,LOWER(CONVERT(char(36),e.database_guid)), "
            "e.database_create_token,e.writer_role,d.database_id,LOWER(CONVERT(char(36),r.database_guid)), "
            "CONVERT(nvarchar(33),d.create_date,126),d.is_trustworthy_on,d.is_db_chaining_on,d.containment,d.state, "
            "DB_ID(),d.owner_sid,SUSER_SID(ORIGINAL_LOGIN()), "
            "g.connector,LOWER(CONVERT(char(36),g.service_id)),g.physical_subject_sha256 "
            f"FROM {ledger.table('domains')} g WITH (HOLDLOCK) JOIN {ledger.table('mssql_enrollments')} e WITH (HOLDLOCK) "
            "ON e.guard_id=g.guard_id LEFT JOIN sys.databases d ON d.database_id=e.database_id "
            "LEFT JOIN sys.database_recovery_status r ON r.database_id=d.database_id WHERE g.guard_id=?;",
            domain.guard_id,
        )
        rows = tuple(tuple(value) for value in cursor.fetchall())
        if len(rows) != 1 or len(rows[0]) != 18 or None in rows[0]:
            raise CompositionAdmissionError("mssql_database_enrollment")
        record = rows[0]
        name, dbid, guid, token, writer_role = record[:5]
        if (
            (name, dbid, guid, token) != (pin.database_name, pin.database_id, str(pin.database_guid), pin.create_token)
            or record[5:8] != (dbid, guid, token)
            or type(dbid) is not int
            or dbid <= 4
            or dbid == record[12]
            or record[8:12] != (0, 0, 0, 0)
            or record[13] != record[14]
            or record[15:] != (domain.connector, domain.service_id, domain.physical_subject_sha256)
        ):
            raise CompositionAdmissionError("mssql_database_enrollment")
        require_control_schema(name)
        require_control_schema(writer_role)
        cursor.execute(
            f"SELECT TOP (8193) schema_name FROM {ledger.table('mssql_managed_schemas')} WITH (HOLDLOCK) "
            "WHERE guard_id=? ORDER BY schema_name;",
            domain.guard_id,
        )
        schemas = tuple(value[0] for value in cursor.fetchall())
        if not 1 <= len(schemas) <= 8192 or len(set(schemas)) != len(schemas) or schema not in schemas:
            raise CompositionAdmissionError("mssql_managed_schema")
        for value in schemas:
            require_control_schema(value)
        require_database_policy(
            ledger, database=name, writer_role=writer_role, schemas=schemas, guard_id=domain.guard_id
        )
        # Cascading FK effects are outside the initial modeled writer footprint.
        cursor.execute(
            f"SELECT COUNT(*) FROM [{name}].sys.foreign_keys WHERE delete_referential_action<>0 OR update_referential_action<>0;"
        )
        if row(cursor) != (0,):
            raise CompositionAdmissionError("mssql_cascade_effects")
