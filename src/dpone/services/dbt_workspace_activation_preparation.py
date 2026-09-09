"""Assemble physical workspace admission only from verified runtime authorities."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any, Protocol

from dpone.contracts.dbt_workspace_control import (
    DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256,
    DbtRelationWrite,
    DbtReleaseSources,
    DbtWorkspaceActivationError,
    DbtWorkspaceActivationRequest,
    DbtWorkspacePhysicalResource,
    MssqlDatabaseAuthorityContractError,
    MssqlDatabaseAuthoritySet,
    MssqlWorkspaceObservationRequest,
    ResolvedBindingConnection,
    canonical_fingerprint,
    dbt_relation_write_subject,
)

if TYPE_CHECKING:
    from dpone.ports.dbt_workspace_activation import (
        DbtWorkspaceMssqlObservationPort,
        DbtWorkspacePhysicalAuthorityPort,
    )


class _Resolver(Protocol):
    def resolve(self, connection_ref: str) -> Any: ...


class _RuntimeContext(Protocol):
    @property
    def environment(self) -> str: ...

    @property
    def release_id(self) -> str | None: ...

    @property
    def deployment_id(self) -> str | None: ...

    @property
    def authority_subject_sha256(self) -> str | None: ...

    @property
    def resolver(self) -> _Resolver: ...


class DbtWorkspaceActivationPreparation:
    """Join complete source writes to exact runtime connections and observations."""

    def __init__(
        self,
        *,
        physical_authority: DbtWorkspacePhysicalAuthorityPort,
        mssql_observer: DbtWorkspaceMssqlObservationPort,
    ) -> None:
        self._physical_authority = physical_authority
        self._mssql_observer = mssql_observer

    def prepare(
        self,
        *,
        activation_id: str,
        previous_deployment_id: str | None,
        sources: DbtReleaseSources,
        runtime_context: _RuntimeContext,
    ) -> DbtWorkspaceActivationRequest:
        """Observe every emitted write or fail without producing a request."""

        release_id, deployment_id, context_subject = _require_runtime_subjects(sources, runtime_context)
        grouped: dict[str, list[DbtRelationWrite]] = defaultdict(list)
        for write in sources.relation_writes:
            grouped[write.connection_ref].append(write)
        if not grouped:
            raise DbtWorkspaceActivationError("write_closure")
        invocation_databases = _invocation_databases(sources)
        resources: list[DbtWorkspacePhysicalResource] = []
        all_subjects: list[str] = []
        for connection_ref in sorted(grouped):
            writes = tuple(grouped[connection_ref])
            if any(write.connector != "mssql" for write in writes):
                raise DbtWorkspaceActivationError("connector_authority_unavailable")
            connection = runtime_context.resolver.resolve(connection_ref)
            if not isinstance(connection, ResolvedBindingConnection):
                raise DbtWorkspaceActivationError("connection_resolution")
            descriptor = connection.descriptor
            if descriptor is None or descriptor.connection_type != "mssql":
                raise DbtWorkspaceActivationError("connector")
            default_database = str(connection.credentials.database or "")
            try:
                authority_set = MssqlDatabaseAuthoritySet.from_connection_properties(
                    descriptor.properties,
                    capability="target",
                )
                database_names = (
                    default_database,
                    *invocation_databases.get(connection_ref, ()),
                    *(write.database or default_database for write in writes),
                )
                pins = tuple(authority_set.require(name, capability="target") for name in database_names)
            except (AttributeError, MssqlDatabaseAuthorityContractError):
                raise DbtWorkspaceActivationError("database_authority") from None
            if not pins or any(pin != pins[0] for pin in pins[1:]):
                raise DbtWorkspaceActivationError("cross_database_unsupported")
            subjects = tuple(dbt_relation_write_subject(write) for write in writes)
            observation = self._mssql_observer.observe(
                MssqlWorkspaceObservationRequest(
                    release_id=release_id,
                    runtime_context_sha256=context_subject,
                    macro_authority_sha256=DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256,
                    pin=pins[0],
                    default_database=default_database,
                    invocation_databases=tuple(
                        dict.fromkeys(invocation_databases.get(connection_ref, (default_database,)))
                    ),
                    writes=writes,
                ),
                connection,
            )
            resources.extend(
                self._physical_authority.authorize_mssql(
                    connection_ref=connection_ref,
                    observation=observation,
                    write_subjects=subjects,
                )
            )
            all_subjects.extend(subjects)
        resources = list(_merge_physical_resources(tuple(resources)))
        return DbtWorkspaceActivationRequest.build(
            activation_id=activation_id,
            environment=runtime_context.environment,
            release_id=release_id,
            deployment_id=deployment_id,
            previous_deployment_id=previous_deployment_id,
            source_inventory_sha256=sources.inventory.snapshot_sha256,
            runtime_context_sha256=context_subject,
            write_subjects=tuple(sorted(all_subjects)),
            resources=tuple(sorted(resources)),
        )


def _require_runtime_subjects(
    sources: DbtReleaseSources,
    context: _RuntimeContext,
) -> tuple[str, str, str]:
    values = (context.release_id, context.deployment_id, context.authority_subject_sha256)
    if any(not isinstance(value, str) for value in values) or context.release_id != sources.release_id:
        raise DbtWorkspaceActivationError("runtime_context")
    assert isinstance(context.release_id, str)
    assert isinstance(context.deployment_id, str)
    assert isinstance(context.authority_subject_sha256, str)
    return context.release_id, context.deployment_id, context.authority_subject_sha256


def _invocation_databases(sources: DbtReleaseSources) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = defaultdict(list)
    for workflow in sources.workflows:
        profile = workflow.execution.invocation_profile()
        grouped[profile.connection_ref].append(profile.database)
    return {key: tuple(dict.fromkeys(values)) for key, values in grouped.items()}


def _merge_physical_resources(
    resources: tuple[DbtWorkspacePhysicalResource, ...],
) -> tuple[DbtWorkspacePhysicalResource, ...]:
    """Coalesce aliases that SQL observation mapped to one physical guard."""

    grouped: dict[str, list[DbtWorkspacePhysicalResource]] = defaultdict(list)
    for resource in resources:
        grouped[resource.guard_id].append(resource)
    merged = []
    for guard_id, matches in sorted(grouped.items()):
        first = matches[0]
        if any(
            (item.connector, item.service_authority_sha256, item.target_authority_sha256)
            != (first.connector, first.service_authority_sha256, first.target_authority_sha256)
            for item in matches[1:]
        ):
            raise DbtWorkspaceActivationError("physical_authority_collision")
        observations = tuple(sorted({item.observation_sha256 for item in matches}))
        observation_sha256 = (
            observations[0]
            if len(observations) == 1
            else canonical_fingerprint(
                {
                    "schema": "dpone.dbt-workspace-observation-set.v1",
                    "observations": list(observations),
                }
            )
        )
        merged.append(
            DbtWorkspacePhysicalResource(
                guard_id=guard_id,
                connector=first.connector,
                service_authority_sha256=first.service_authority_sha256,
                target_authority_sha256=first.target_authority_sha256,
                observation_sha256=observation_sha256,
                write_subjects=tuple(sorted(subject for item in matches for subject in item.write_subjects)),
            )
        )
    return tuple(merged)


__all__ = ["DbtWorkspaceActivationPreparation"]
