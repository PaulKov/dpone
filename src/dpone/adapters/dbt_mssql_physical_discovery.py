"""Fresh METADATA-only P discovery with bounded acknowledged read settlement."""

from collections.abc import Callable
from uuid import UUID

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.dbt_mssql_physical_catalog_fetch import CatalogReadBudget, require_final_rowset
from dpone.adapters.dbt_mssql_physical_discovery_queries import ENTRY
from dpone.adapters.dbt_mssql_physical_source import _bytes, _uuid
from dpone.contracts.dbt_mssql_physical_catalog_binding import (
    CatalogRegistrationBinding,
    require_catalog_binding_registration,
)
from dpone.contracts.dbt_mssql_physical_discovery import (
    PhysicalDiscoveryObservation,
    PhysicalDiscoveryRequest,
    decode_discovery_observation,
    encode_discovery_request,
    require_discovery_observation,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_values import DedicatedObserver
from dpone.contracts.dbt_mssql_physical_validation import require_sql_positive_integer
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.ports.physical_catalog_connection import PhysicalCatalogConnection, PhysicalCatalogCursor


class PhysicalDiscoveryReadError(RuntimeError):
    """No accepted observation, durable absence or permission to reserve/launch."""


def _binary(value: object, maximum: int) -> bytes:
    if type(value) not in (bytes, bytearray, memoryview):
        raise ValueError("discovery binary fact has an unsupported driver representation")
    size = value.nbytes if isinstance(value, memoryview) else len(value)  # type: ignore[arg-type]
    if not 0 < size <= maximum:
        raise ValueError("discovery binary fact exceeds its bound")
    return _bytes(value, maximum)


def _row(cursor: PhysicalCatalogCursor, budget: CatalogReadBudget) -> tuple[object, ...]:
    budget.seconds()
    raw = cursor.fetchone()
    if raw is None or len(raw) != 18:
        raise ValueError("discovery requires one closed scalar row")
    value = list(raw)
    value[1] = _uuid(value[1])
    value[5] = UUID(_uuid(value[5]))
    for index in (2, 3):
        value[index] = _binary(value[index], 71).decode("ascii")
    for index in (15, 17):
        value[index] = _binary(value[index], 85).hex()
    result = tuple(value)
    budget.charge(result, 1)
    budget.seconds()
    if cursor.fetchone() is not None:
        raise ValueError("discovery returned extra facts")
    require_final_rowset(cursor)
    budget.seconds()
    return result


class MssqlPhysicalDiscoveryReader:
    """Observe supplied absence/FG selection using previously provisioned P.

    The factory supplies a fresh ordinary METADATA login with a finite connection
    timeout and qualified nextset/statement-timeout support. The upstream caller
    authenticates selected policy and exact registration/binding/module inventory;
    constructing request/binding objects does not establish those authorities.
    There is no G, executor, original-store I/O, mutation or automatic retry.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[], PhysicalCatalogConnection],
        registration: MssqlPhysicalRuntimeRegistration,
        binding: CatalogRegistrationBinding,
        operation_timeout_seconds: int,
        clock: Callable[[], float],
    ) -> None:
        require_catalog_binding_registration(binding, registration)
        if type(registration.principals.observer) is not DedicatedObserver:
            raise ValueError("discovery first cell requires a dedicated observer")
        require_sql_positive_integer(operation_timeout_seconds, "operation_timeout_seconds")
        self._connect, self._registration, self._binding = connection_factory, registration, binding
        self._timeout, self._clock = operation_timeout_seconds, clock
        self._schema = native_control_schema(registration.local_schema)

    def read(self, request: PhysicalDiscoveryRequest) -> PhysicalDiscoveryObservation:
        """Return temporal facts after acknowledged commit; cleanup is best effort."""
        registration = self._registration
        payload = encode_discovery_request(
            request, max_bytes=registration.limits.max_metadata_bytes, max_objects=registration.limits.max_catalog_rows
        )
        if request.subject != registration.platform_subject:
            raise ValueError("discovery request differs from registered PLATFORM subject")
        budget = CatalogReadBudget(self._clock() + self._timeout, registration.limits.max_metadata_bytes, self._clock)
        connection = None
        cursor = None
        try:
            connection = self._connect()
            connection.autocommit = False
            connection.timeout = budget.seconds()
            cursor = connection.cursor()
            cursor.execute(
                "IF @@TRANCOUNT=0 BEGIN TRANSACTION; IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 "
                "THROW 51500, 'DPONE_DISCOVERY_TRANSACTION_INVALID', 1;"
            )
            require_final_rowset(cursor)
            cursor.close()
            cursor = None
            connection.timeout = budget.seconds()
            cursor = connection.cursor()
            cursor.execute(
                f"EXEC [{self._schema}].[{ENTRY}] @registration_id=?, @request=?", registration.registration_id, payload
            )
            observation = decode_discovery_observation(_row(cursor, budget))
            require_discovery_observation(observation, registration, self._binding, request, payload)
            budget.seconds()
            connection.commit()
            budget.seconds()
            return observation
        except Exception as error:
            dbapi_lifecycle.rollback(connection)
            raise PhysicalDiscoveryReadError("P-only discovery could not be observed") from error
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)
