"""One-use same-lock permission inventory and original-journal point resolution.

The caller owns the still-open OBSERVE SQL handle and external hard containment.
Raw collection resolves locators only; explicit authenticated collection also
reads sealed historical CREATE evidence. Neither admits deployment permissions,
produces Prepared, releases the lock or enables a writer route.
"""

import os
from dataclasses import replace
from threading import current_thread
from typing import Any
from uuid import UUID

from dpone.adapters.mssql_permission_preparation_capabilities import (
    PinnedEvidenceReadFactory,
    SqlClientGrantCatalog,
    TdsActorPool,
    TdsCoordinatorSql,
    TdsJournalActorUnknown,
)
from dpone.app.mssql_sqlclient_create_evidence_composition import (
    acquire_authenticated_create,
    recheck_authenticated_create,
)
from dpone.app.mssql_sqlclient_grant_inventory_snapshot import GrantInventorySnapshotMixin
from dpone.app.mssql_sqlclient_stage_locator_composition import (
    _AdmittedSqlClientStoreFactory,
    read_sqlclient_stage_locator,
)
from dpone.contracts.mssql_permission_preparation_capabilities import (
    SqlClientObserverAdmission,
    TdsCoordinatorIdentity,
    coordinator_identity_digest,
    deadline_nanoseconds,
)
from dpone.contracts.mssql_sqlclient_grant_inventory import (
    SqlClientGrantInventory,
    SqlClientGrantInventoryLimits,
    SqlClientGrantMember,
    SqlClientGrantPrincipal,
    SqlClientPermissionRow,
    encode_grant_inventory,
    inventory_member_bytes,
    snapshot_inventory_authority,
    validate_inventory_original,
)
from dpone.contracts.mssql_tds_api import (
    SqlClientAuthenticatedCreate,
    SqlClientAuthenticatedInventory,
    SqlClientDatabasePrincipal,
    SqlClientObserverIncarnation,
    SqlClientStageIdentity,
    SqlClientStageLookup,
    TdsCoordinatorCommand,
    WindowOutcomeUnknown,
    authenticated_create_bytes,
    canonical_json_bytes,
    encode_authenticated_inventory,
    snapshot_stage_admission,
    strict_json_object,
)
from dpone.ports.mssql_sqlclient_grant_catalog import SqlClientGrantCatalogPort
from dpone.services.mssql_tds_attempt import _ShutdownCapability
from dpone.services.mssql_tds_attempt_continuation import TdsObserveContinuation

_ERROR = "mssql_native.sqlclient_grant_inventory_unknown"
_CatalogObservation = tuple[
    SqlClientObserverIncarnation,
    SqlClientGrantPrincipal,
    SqlClientGrantPrincipal,
    tuple[SqlClientPermissionRow, ...],
    tuple[SqlClientStageIdentity, ...],
]


class SqlClientGrantInventoryUnknown(WindowOutcomeUnknown):
    """Retain actual resources, not a successful inventory or retry permission.

    SQL teardown may block: the caller's external supervisor must contain that
    resource. Only the failed local actor exposes bounded close here.
    """

    def __init__(
        self, collector: "SqlClientGrantInventoryCollector", gateway: _ShutdownCapability | None = None
    ) -> None:
        self.sql, self.pool, self.gateway = collector.sql, collector.pool, gateway
        self.deadline = collector.deadline
        self._gateway_closed = False
        super().__init__(_ERROR)

    def close_locator(self, *, deadline: float) -> None:
        """Compatibility alias for closing the actual failed local actor."""
        self.close_gateway(deadline=deadline)

    def close_gateway(self, *, deadline: float) -> None:
        deadline_nanoseconds(deadline)
        if self.gateway is not None and not self._gateway_closed:
            self.gateway.close(deadline=min(deadline, self.deadline))
            self._gateway_closed = True


class SqlClientGrantInventoryCollector(GrantInventorySnapshotMixin):
    """One read sequence, one original handle/domain/pool/deadline; never retries."""

    def __init__(
        self,
        sql: TdsCoordinatorSql,
        *,
        management_admission: SqlClientObserverAdmission,
        writer_admission: SqlClientObserverAdmission,
        writer_principal: SqlClientDatabasePrincipal,
        admitted_factory: _AdmittedSqlClientStoreFactory,
        pool: TdsActorPool,
        limits: SqlClientGrantInventoryLimits,
        deadline: float,
    ) -> None:
        if type(sql) is not TdsCoordinatorSql:
            raise ValueError(_ERROR)
        self._initialize(
            SqlClientGrantCatalog(sql, deadline=deadline),
            management_admission=management_admission,
            writer_admission=writer_admission,
            writer_principal=writer_principal,
            admitted_factory=admitted_factory,
            pool=pool,
            limits=limits,
            deadline=deadline,
            continuation=None,
        )
        self.sql: TdsCoordinatorSql | SqlClientGrantCatalogPort = sql

    @classmethod
    def from_catalog(
        cls,
        catalog: SqlClientGrantCatalogPort,
        *,
        management_admission: SqlClientObserverAdmission,
        writer_admission: SqlClientObserverAdmission,
        writer_principal: SqlClientDatabasePrincipal,
        admitted_factory: _AdmittedSqlClientStoreFactory,
        pool: TdsActorPool,
        limits: SqlClientGrantInventoryLimits,
        deadline: float,
        continuation: TdsObserveContinuation,
    ) -> "SqlClientGrantInventoryCollector":
        if type(continuation) is not TdsObserveContinuation:
            raise ValueError(_ERROR)
        continuation.assert_current()
        instance = cls.__new__(cls)
        instance._initialize(
            catalog,
            management_admission=management_admission,
            writer_admission=writer_admission,
            writer_principal=writer_principal,
            admitted_factory=admitted_factory,
            pool=pool,
            limits=limits,
            deadline=deadline,
            continuation=continuation,
        )
        return instance

    def _initialize(
        self,
        catalog: SqlClientGrantCatalogPort,
        *,
        management_admission: SqlClientObserverAdmission,
        writer_admission: SqlClientObserverAdmission,
        writer_principal: SqlClientDatabasePrincipal,
        admitted_factory: _AdmittedSqlClientStoreFactory,
        pool: TdsActorPool,
        limits: SqlClientGrantInventoryLimits,
        deadline: float,
        continuation: TdsObserveContinuation | None,
    ) -> None:
        sql = catalog
        deadline_nanoseconds(deadline)
        validate_inventory_original(limits, SqlClientGrantInventoryLimits)
        validate_inventory_original(writer_principal, SqlClientDatabasePrincipal)
        if type(admitted_factory) is not _AdmittedSqlClientStoreFactory:
            raise ValueError(_ERROR)
        validate_inventory_original(sql.identity, TdsCoordinatorIdentity)
        if sql.authority is None:
            raise ValueError(_ERROR)
        self.authority = snapshot_inventory_authority(sql.authority)
        self.management = snapshot_stage_admission(management_admission)
        self.writer_admission = snapshot_stage_admission(writer_admission)
        self.writer_principal, self.limits = replace(writer_principal), replace(limits)
        self.sql, self.pool, self.deadline = sql, pool, deadline
        self.factory = admitted_factory
        self.domain = admitted_factory.domain_id
        # The existing lookup validator checks exact UUID integer fields before copying.
        SqlClientStageLookup(self.domain, self.management.server, self.authority.database, "a" * 64, UUID(int=1))
        self.domain = UUID(int=self.domain.int)
        mgmt, writer, db = self.management, self.writer_admission, self.authority.database
        if (
            sql.identity.command is not TdsCoordinatorCommand.OBSERVE
            or coordinator_identity_digest(sql.identity) != self.authority.operation_sha256
            or self.authority.execution_owner != sql.execution_owner
            or self.authority.process != sql.process
            or mgmt.login.is_sysadmin is not True
            or writer.login.is_sysadmin is not False
            or writer.login.sid == writer.database.owner_sid
            or (mgmt.server, mgmt.database) != (writer.server, writer.database)
            or (db.name, db.database_id, str(db.database_guid))
            != (mgmt.database.database_name, mgmt.database.database_id, mgmt.database.database_guid)
            or writer_principal.principal_id <= 4
            or writer_principal.sid != writer.login.sid
        ):
            raise ValueError(_ERROR)
        self.catalog = catalog
        self.continuation = continuation
        self._owner, self._used = (os.getpid(), current_thread()), False
        self._faulted = False

    def _guard(self) -> None:
        if self._faulted or self._owner != (os.getpid(), current_thread()):
            raise ValueError(_ERROR)
        if self.continuation is not None:
            self.continuation.assert_current()
        if snapshot_inventory_authority(self.catalog.guard()) != self.authority:
            raise ValueError(_ERROR)
        self.pool.assert_deadline(deadline=self.deadline)
        if self._faulted:
            raise ValueError(_ERROR)

    def _own_rows(self, statement: str) -> list[Any]:
        rows = self.catalog.own_incarnation(statement)
        validate_inventory_original(tuple(rows), tuple)
        return rows

    def collect(self) -> SqlClientGrantInventory:
        """Preserve the original raw observation and one-use contract."""
        result = self._collect(None)
        assert type(result) is SqlClientGrantInventory
        return result

    def collect_authenticated(self, *, reader_factory: PinnedEvidenceReadFactory) -> SqlClientAuthenticatedInventory:
        """Acquire mandatory historical CREATE proof using independent root custody."""
        result = self._collect(reader_factory, authenticate=True)
        assert type(result) is SqlClientAuthenticatedInventory
        return result

    def _collect(
        self, reader_factory: PinnedEvidenceReadFactory | None, *, authenticate: bool = False
    ) -> SqlClientGrantInventory | SqlClientAuthenticatedInventory:
        if self._used or self._owner != (os.getpid(), current_thread()):
            self._used = self._faulted = True
            raise SqlClientGrantInventoryUnknown(self)
        self._used = True
        try:
            if authenticate and type(reader_factory) is not PinnedEvidenceReadFactory:
                raise ValueError(_ERROR)
            opening = self._snapshot()
            management, writer, public, permissions, stages, excluded = opening
            members, size = [], 0
            for stage in stages:
                self._guard()
                lookup = SqlClientStageLookup(
                    self.domain,
                    management.authority.server,
                    self.authority.database,
                    stage.owner_binding,
                    stage.object_nonce,
                )
                snapshot = read_sqlclient_stage_locator(
                    admitted_factory=self.factory, lookup=lookup, pool=self.pool, deadline=self.deadline
                )
                self._guard()
                member = SqlClientGrantMember(stage, snapshot)
                size += len(inventory_member_bytes(member))
                if size > self.limits.observation_bytes:
                    raise ValueError(_ERROR)
                members.append(member)
            proofs: list[SqlClientAuthenticatedCreate] = []
            if reader_factory is not None:
                if type(reader_factory) is not PinnedEvidenceReadFactory:
                    raise ValueError(_ERROR)
                raw = SqlClientGrantInventory(
                    self.authority,
                    management,
                    management,
                    writer,
                    public,
                    permissions,
                    tuple(members),
                    self.limits,
                    excluded,
                )
                # Exact canonical enclosing size before appending any proof.
                size = len(
                    canonical_json_bytes(
                        dict(
                            creates=[],
                            inventory=strict_json_object(encode_grant_inventory(raw)),
                            schema="dpone.sqlclient.authenticated-inventory.v1",
                        )
                    )
                )
                if size > self.limits.observation_bytes:
                    raise ValueError(_ERROR)
                for member in members:
                    self._guard()
                    proof = acquire_authenticated_create(
                        admitted_factory=self.factory,
                        member=member,
                        reader_factory=reader_factory,
                        continuation=self.continuation,
                        pool=self.pool,
                        deadline=self.deadline,
                    )
                    self._guard()
                    size += len(authenticated_create_bytes(proof)) + int(bool(proofs))
                    if size > self.limits.observation_bytes:
                        raise ValueError(_ERROR)
                    proofs.append(proof)
                for member, proof in zip(members, proofs, strict=True):
                    self._guard()
                    recheck_authenticated_create(
                        admitted_factory=self.factory,
                        member=member,
                        proof=proof,
                        pool=self.pool,
                        deadline=self.deadline,
                    )
                    self._guard()
            closing = self._snapshot()
            if closing != opening:
                raise ValueError(_ERROR)
            result = SqlClientGrantInventory(
                self.authority,
                management,
                closing[0],
                writer,
                public,
                permissions,
                tuple(members),
                self.limits,
                excluded,
            )
            encode_grant_inventory(result)
            self._guard()
            if reader_factory is not None:
                authenticated = SqlClientAuthenticatedInventory(result, tuple(proofs))
                encode_authenticated_inventory(authenticated)
                self._guard()
                return authenticated
            return result
        except BaseException as error:
            self._faulted = True
            gateway = error.gateway if isinstance(error, TdsJournalActorUnknown) else None
            raise SqlClientGrantInventoryUnknown(self, gateway) from None
