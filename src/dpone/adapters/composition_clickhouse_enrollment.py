"""Read-only ClickHouse incarnation and complete catalog admission.

The protected enrollment capability independently reopens shared SQL ownership,
exclusive supervisor policy and catalog-reader visibility. SQL observations here
cannot replace that authority or issue a publisher. No semantic-refresh lease is
reused. Unsupported or unobservable server capabilities reject the whole parent.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any
from uuid import UUID

from dpone.adapters.semantic_refresh_clickhouse_http_queries import literal
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_physical import CompositionDomainObservation, CompositionPhysicalDomain
from dpone.contracts.dbt_workspace_activation import dbt_relation_write_subject

if TYPE_CHECKING:
    from dpone.contracts.composition_activation import CompositionOccurrenceContext
    from dpone.contracts.dbt_relation_writes import DbtRelationWrite
    from dpone.contracts.runtime_connection import ResolvedBindingConnection


def _uuid(value: object) -> str:
    try:
        if type(value) is not str or str(UUID(value)) != value or UUID(value).int == 0:
            raise ValueError("uuid")
        return value
    except (ValueError, TypeError, AttributeError):
        raise CompositionAdmissionError("physical_uuid") from None


def clickhouse_physical_domain(service_id: str, database_uuid: str) -> CompositionPhysicalDomain:
    """Shared stable preimage for protected provisioning and runtime admission.

    Only persistent server/database incarnation participates. Endpoint aliases,
    login identities, table inventories and server versions do not rotate guards.
    Construction is not proof that either identifier was independently observed.
    """
    service_id, database_uuid = _uuid(service_id), _uuid(database_uuid)
    return CompositionPhysicalDomain(
        "clickhouse",
        service_id,
        canonical_fingerprint(
            {
                "schema": "dpone.composition-clickhouse-database.v1",
                "service_id": service_id,
                "database_uuid": database_uuid,
            }
        ),
    )


class ClickHouseCompositionEnrollmentReader:
    """Read actual serverUUID/Atomic database UUID and all visible user tables.

    ``query`` must use the protected observer credential for the sealed binding's
    endpoint, enforce transport byte/time limits, and return detached driver rows.
    ``require_enrollment`` must verify complete metadata visibility for that same
    observer and independently enrolled exclusive supervisor/writer policy. An
    arbitrary principal's filtered system.tables result is insufficient proof.
    """

    def __init__(
        self,
        *,
        query: Callable[[ResolvedBindingConnection, str], Sequence[Sequence[Any]]],
        require_enrollment: Callable[[CompositionPhysicalDomain, CompositionOccurrenceContext], None],
    ) -> None:
        self._query = query
        self._require_enrollment = require_enrollment

    def resolve(
        self, connection: ResolvedBindingConnection, write: DbtRelationWrite, context: CompositionOccurrenceContext
    ) -> CompositionPhysicalDomain:
        """Match signed closed pins to protected enrollment and actual server facts."""
        try:
            domain, database, database_uuid = self._pins(connection, write)
            self._require_enrollment(domain, context)
            self._identity(connection, domain, database, database_uuid)
            return domain
        except CompositionAdmissionError:
            raise
        except Exception:
            raise CompositionAdmissionError("clickhouse_enrollment") from None

    def observe(
        self,
        domain: CompositionPhysicalDomain,
        writes: tuple[DbtRelationWrite, ...],
        connection: ResolvedBindingConnection,
        context: CompositionOccurrenceContext,
    ) -> CompositionDomainObservation:
        """Bind one global catalog rowset and all targets, including absent names.

        Initial admission allows local plain MergeTree tables only. Views anywhere
        in the observed user catalog, dependencies, background mutations, row
        policies, computed columns, TTL and auxiliary physical effects reject.
        A protected database guard also owns dynamic staging/publication names;
        their bounded generation is verified by the injected execution capability.
        """
        try:
            if not writes or len(writes) > 8192:
                raise CompositionAdmissionError("physical_write_budget")
            expected, database, database_uuid = self._pins(connection, writes[0])
            if domain != expected or any(self._database(write) != database for write in writes):
                raise CompositionAdmissionError("physical_binding_drift")
            self._require_enrollment(domain, context)
            header = self._identity(connection, domain, database, database_uuid)
            tables = self._rows(
                connection,
                "SELECT database,name,toString(uuid),engine,create_table_query,dependencies_database,dependencies_table "
                "FROM system.tables WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema') "
                "ORDER BY database,name LIMIT 8193",
            )
            self._require_tables(tables)
            checks = (
                "SELECT count() FROM system.clusters WHERE NOT is_local OR shard_num != 1 OR replica_num != 1",
                "SELECT count() FROM system.mutations WHERE NOT is_done",
                "SELECT count() FROM system.row_policies",
                f"SELECT count() FROM system.columns WHERE database={literal(database)} AND default_kind != ''",
                f"SELECT count() FROM system.data_skipping_indices WHERE database={literal(database)}",
            )
            for statement in checks:
                result = self._rows(connection, statement)
                if result != ((0,),) or type(result[0][0]) is not int:
                    raise CompositionAdmissionError("clickhouse_catalog_effects")
            if self._identity(connection, domain, database, database_uuid) != header:
                raise CompositionAdmissionError("physical_binding_drift")
            self._require_enrollment(domain, context)
            names = tuple((self._database(write), write.relation) for write in writes)
            # ClickHouse identifiers compare exactly; no SQL Server case folding.
            equivalences = {name: index for index, name in enumerate(sorted(set(names)))}
            return CompositionDomainObservation(
                domain,
                tuple((dbt_relation_write_subject(write), equivalences[name]) for write, name in zip(writes, names)),
                canonical_fingerprint(
                    {
                        "schema": "dpone.composition-clickhouse-catalog.v1",
                        "header": header,
                        "tables": tables,
                        "checks": list(checks),
                        "targets": names,
                    }
                ),
            )
        except CompositionAdmissionError:
            raise
        except Exception:
            raise CompositionAdmissionError("clickhouse_catalog") from None

    def _identity(self, connection, domain, database, database_uuid):
        rows = self._rows(
            connection,
            "SELECT toString(serverUUID()),name,toString(uuid),engine FROM system.databases "
            f"WHERE name={literal(database)} LIMIT 2",
        )
        if rows != ((domain.service_id, database, database_uuid, "Atomic"),):
            raise CompositionAdmissionError("clickhouse_physical_identity")
        return rows

    def _rows(self, connection, statement):
        rows = self._query(
            connection,
            statement
            + " SETTINGS max_execution_time=10,max_result_rows=8193,max_result_bytes=8388608,result_overflow_mode='throw'",
        )
        if (
            not isinstance(rows, (tuple, list))
            or len(rows) > 8192
            or any(not isinstance(row, (tuple, list)) for row in rows)
        ):
            raise CompositionAdmissionError("clickhouse_catalog_budget")
        result = tuple(tuple(row) for row in rows)
        if len(json.dumps(result, allow_nan=False).encode()) > 8 * 1024 * 1024:
            raise CompositionAdmissionError("clickhouse_catalog_budget")
        return result

    @staticmethod
    def _require_tables(rows):
        names = set()
        for row in rows:
            if len(row) != 7 or any(type(row[i]) is not str for i in range(5)):
                raise CompositionAdmissionError("clickhouse_catalog_shape")
            database, name, identity, engine, sql, dependencies_db, dependencies_table = row
            _uuid(identity)
            if not isinstance(dependencies_db, (tuple, list)) or not isinstance(dependencies_table, (tuple, list)):
                raise CompositionAdmissionError("clickhouse_catalog_shape")
            if (database, name) in names:
                raise CompositionAdmissionError("clickhouse_catalog_shape")
            names.add((database, name))
            if (
                engine != "MergeTree"
                or dependencies_db
                or dependencies_table
                or re.search(
                    r"\b(TTL|PROJECTION|INDEX|CONSTRAINT|CODEC|DEFAULT|MATERIALIZED|ALIAS|ON\s+CLUSTER)\b",
                    sql,
                    re.IGNORECASE,
                )
            ):
                raise CompositionAdmissionError("clickhouse_catalog_effects")

    @staticmethod
    def _database(write):
        # The existing ClickHouse target renderer uses manifest schema as DB.
        if write.connector != "clickhouse" or write.kind != "transfer" or write.database not in (None, write.schema):
            raise CompositionAdmissionError("clickhouse_target_coordinates")
        if not write.schema or "\x00" in write.schema:
            raise CompositionAdmissionError("clickhouse_target_coordinates")
        return write.schema

    @classmethod
    def _pins(cls, connection, write):
        database = cls._database(write)
        descriptor = connection.descriptor
        if descriptor is None or descriptor.connection_type != "clickhouse":
            raise CompositionAdmissionError("physical_binding_connector")
        properties = descriptor.properties
        authorities = properties.get("database_authorities")

        if (
            not isinstance(authorities, Mapping)
            or not authorities
            or properties.get("database") != connection.credentials.database
        ):
            raise CompositionAdmissionError("clickhouse_database_authorities")
        for name, value in authorities.items():
            if type(name) is not str or not name or not isinstance(value, Mapping) or set(value) != {"database_uuid"}:
                raise CompositionAdmissionError("clickhouse_database_authorities")
            _uuid(value["database_uuid"])
        if properties["database"] not in authorities or database not in authorities:
            raise CompositionAdmissionError("clickhouse_database_authorities")
        identity = authorities[database]["database_uuid"]
        return clickhouse_physical_domain(properties.get("composition_service_id"), identity), database, identity
