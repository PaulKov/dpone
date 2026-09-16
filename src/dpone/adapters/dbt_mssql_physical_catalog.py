"""Owned-transaction SQL2022 catalog acquisition and exact physical comparison."""

from collections.abc import Callable
from types import MappingProxyType
from typing import cast

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.dbt_mssql_physical_catalog_fetch import (
    CatalogReadBudget,
    fetch_catalog_result,
    require_final_rowset,
)
from dpone.adapters.dbt_mssql_physical_source import _facts
from dpone.adapters.dbt_mssql_physical_source_queries import ENTRY as SOURCE_ENTRY
from dpone.contracts.dbt_mssql_physical import PhysicalModelPlan
from dpone.contracts.dbt_mssql_physical_catalog_comparison import require_catalog_match
from dpone.contracts.dbt_mssql_physical_catalog_observation import PhysicalCatalogObservation
from dpone.contracts.dbt_mssql_physical_catalog_rows import CatalogRow, HeaderRow
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_source_identity import require_source_identity
from dpone.contracts.dbt_mssql_physical_validation import (
    require_physical_identifier,
    require_physical_timestamp,
    require_physical_uuid,
    require_sql_positive_integer,
)
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.ports.physical_catalog_connection import PhysicalCatalogConnection, PhysicalCatalogCursor

_KINDS = (
    "COUNT",
    "HEADER",
    "TABLE",
    "COLUMN",
    "INDEX",
    "INDEX_COLUMN",
    "PARTITION",
    "DEPENDENCY",
    "FORBIDDEN_PROPERTY",
)


class PhysicalCatalogReadError(RuntimeError):
    """No accepted observation; never implies absence, rollback proof or replay."""


class MssqlPhysicalCatalogReader:
    """Read an existing table through a separately provisioned signed module.

    Upstream must authenticate registration/profile/schema binding and verify
    exact installed module/certificate/grant inventory. DTO construction is not
    authentication. The factory supplies a fresh ordinary METADATA/BUILD login,
    qualified finite connect timeout and nextset/statement-timeout capability.
    No observer, administrator or impersonated connection is admitted.
    The default version 1 preserves existing deployments. Explicit version 2
    requires the new protected binding and selected profile resource reference.

    The supplied local plan is compared, not authenticated for membership or
    resource authority. This post-G reader cannot prepare the P-only plan that
    precedes generation reservation. It grants no enrollment or mutation.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[], PhysicalCatalogConnection],
        registration: MssqlPhysicalRuntimeRegistration,
        operation_timeout_seconds: int,
        clock: Callable[[], float],
        catalog_version: int = 1,
    ) -> None:
        if type(registration) is not MssqlPhysicalRuntimeRegistration:
            raise ValueError("catalog requires an exact registration")
        registration.__post_init__()
        require_sql_positive_integer(operation_timeout_seconds, "operation_timeout_seconds")
        if type(catalog_version) is not int or catalog_version not in (1, 2):
            raise ValueError("catalog version must explicitly select 1 or 2")
        self._entry = f"physical_catalog_v{catalog_version}"
        self._version = catalog_version
        self._connect, self._registration = connection_factory, registration
        self._timeout, self._clock = operation_timeout_seconds, clock
        self._schema = native_control_schema(registration.local_schema)

    def read(
        self,
        *,
        plan: PhysicalModelPlan,
        executor_invocation_id: str,
        object_id: int,
        expected_object_name: str,
        expected_object_create_time: str,
    ) -> PhysicalCatalogObservation:
        """Return detached matching facts only after successful read settlement.

        Source guard and all catalog calls share one transaction. The first COUNT request rejects RLS and takes
        COUNT_BIG TABLOCK,HOLDLOCK before the first
        HEADER, retaining table/source locks through settlement. Failures and
        deadline expiry return no result and trigger best-effort cleanup; an
        uncertain commit is not silently retried or interpreted as rollback.
        """
        if type(plan) is not PhysicalModelPlan:
            raise ValueError("catalog comparison requires an exact physical plan")
        require_physical_uuid(plan.generation_id, "generation_id")
        require_physical_uuid(executor_invocation_id, "executor_invocation_id")
        require_sql_positive_integer(object_id, "object_id")
        require_physical_identifier(expected_object_name, "expected_object_name")
        require_physical_timestamp(expected_object_create_time, "expected_object_create_time")
        if plan.spec.relation.database != self._registration.model_database.database_name:
            raise ValueError("plan database differs from registered database")
        if self._version == 2 and plan.spec.resource_bounds != self._registration.trusted_profile.reference:
            raise ValueError("catalog resource bounds differ from the selected profile projection")
        limits = self._registration.limits
        budget = CatalogReadBudget(self._clock() + self._timeout, limits.max_metadata_bytes, self._clock)
        connection: PhysicalCatalogConnection | None = None
        cursor: PhysicalCatalogCursor | None = None
        try:
            connection = self._connect()
            connection.timeout = budget.seconds()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute(
                "SET NOCOUNT ON; SET LOCK_TIMEOUT " + str(min(self._timeout * 1000, 2147483647)) + "; "
                "IF @@TRANCOUNT=0 BEGIN TRANSACTION; IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 "
                "THROW 51430, 'DPONE_PHYSICAL_CATALOG_TRANSACTION_INVALID', 1;"
            )
            require_final_rowset(cursor)
            cursor.close()
            cursor = None
            connection.timeout = budget.seconds()
            cursor = connection.cursor()
            cursor.execute(
                f"EXEC [{self._schema}].[{SOURCE_ENTRY}] @registration_id=?, @generation=?, @expected_invocation=?",
                self._registration.registration_id,
                plan.generation_id,
                executor_invocation_id,
            )
            source = _facts(dbapi_lifecycle.row(cursor))
            if dbapi_lifecycle.row(cursor) is not None:
                raise ValueError("source read returned extra facts")
            require_final_rowset(cursor)
            require_source_identity(source, self._registration, plan.generation_id, executor_invocation_id)
            results: dict[str, tuple[CatalogRow, ...]] = {}
            for kind in (*_KINDS, "HEADER"):
                cursor.close()
                cursor = None
                connection.timeout = budget.seconds()
                cursor = connection.cursor()
                cursor.execute(
                    f"EXEC [{self._schema}].[{self._entry}] @registration_id=?, @generation=?, "
                    "@expected_invocation=?, @object_id=?, @kind=?",
                    self._registration.registration_id,
                    plan.generation_id,
                    executor_invocation_id,
                    object_id,
                    kind,
                )
                row_limit = limits.max_catalog_rows
                if kind in {"HEADER", "TABLE", "COUNT"}:
                    row_limit = 1
                elif kind in {"COLUMN", "INDEX_COLUMN"}:
                    row_limit = min(row_limit, limits.max_columns)
                elif kind == "DEPENDENCY":
                    row_limit = min(row_limit, limits.max_dependency_rows)
                result = fetch_catalog_result(
                    cursor,
                    kind=kind,
                    object_id=object_id,
                    row_limit=row_limit,
                    definition_limit=limits.max_definition_utf16_bytes,
                    budget=budget,
                    header_count_limit=limits.max_catalog_rows,
                )
                if kind == "HEADER":
                    header = cast(HeaderRow, result[0])
                    pin = self._registration.model_database
                    if (header.database_id, header.database_guid) != (pin.database_id, pin.database_guid):
                        raise ValueError("catalog database identity differs")
                    if "HEADER" in results and result != results["HEADER"]:
                        raise ValueError("catalog identity changed during acquisition")
                results[kind] = result
            require_catalog_match(
                results,
                plan=plan,
                expected_object_name=expected_object_name,
                expected_object_create_time=expected_object_create_time,
            )
            connection.timeout = budget.seconds()
            connection.commit()
            budget.seconds()
            return PhysicalCatalogObservation(source, MappingProxyType(results))
        except Exception as error:
            dbapi_lifecycle.rollback(connection)
            raise PhysicalCatalogReadError("catalog observation could not be verified") from error
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)
