"""Map one complete SQL Server observation to global physical guard identities."""

from __future__ import annotations

from dpone.contracts.dbt_workspace_control import (
    DbtWorkspaceActivationError,
    DbtWorkspacePhysicalResource,
    MssqlWorkspaceObservation,
    canonical_fingerprint,
)


class MssqlDbtWorkspacePhysicalAuthority:
    """Map every write to a conservative guard for the observed physical DB.

    A logical connection alias and the login identity are deliberately absent
    from the guard identity: different aliases or credentials can still mutate
    the same physical database.  SQL Server does not expose a stable canonical
    key for a relation that does not exist yet under every supported collation,
    so database-level serialization is the safe cross-request boundary.
    """

    def authorize_mssql(
        self,
        *,
        connection_ref: str,
        observation: MssqlWorkspaceObservation,
        write_subjects: tuple[str, ...],
    ) -> tuple[DbtWorkspacePhysicalResource, ...]:
        """Return one physical database guard containing every write subject."""

        if not isinstance(observation, MssqlWorkspaceObservation):
            raise DbtWorkspaceActivationError("physical_observation")
        if not isinstance(connection_ref, str) or not connection_ref.strip():
            raise DbtWorkspaceActivationError("physical_connection_ref")
        observation.request.__post_init__()
        if len(observation.slots) != len(write_subjects) or len(observation.request.writes) != len(write_subjects):
            raise DbtWorkspaceActivationError("physical_partition")
        service_authority = canonical_fingerprint(
            {
                "schema": "dpone.mssql-service-authority.v1",
                "server_facts_sha256": observation.header.server_facts_sha256,
                "engine_version": observation.header.engine_version,
                "server_collation": observation.header.server_collation,
            }
        )
        pin = observation.request.pin
        target_authority = canonical_fingerprint(
            {
                "schema": "dpone.mssql-physical-database-authority.v1",
                "service_authority_sha256": service_authority,
                "database_id": observation.header.database_id,
                "database_name": observation.header.database_name,
                "database_guid": str(pin.database_guid),
                "database_create_token": pin.create_token,
                "database_principal_sid_sha256": observation.header.database_principal_sid_sha256,
                "database_collation": observation.header.database_collation,
                "catalog_collation": observation.header.catalog_collation,
            }
        )
        return (
            DbtWorkspacePhysicalResource(
                guard_id=f"mssql:{service_authority}:{target_authority}",
                connector="mssql",
                service_authority_sha256=service_authority,
                target_authority_sha256=target_authority,
                observation_sha256=observation.observation_sha256,
                write_subjects=tuple(sorted(write_subjects)),
            ),
        )


__all__ = ["MssqlDbtWorkspacePhysicalAuthority"]
