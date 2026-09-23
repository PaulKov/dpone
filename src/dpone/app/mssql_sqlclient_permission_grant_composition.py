"""Concrete composition for one retained SQL permission through HELD_READY."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from time import monotonic
from uuid import uuid4

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_permission_grant_evidence_actor import SqlClientPermissionGrantEvidenceActor
from dpone.app.mssql_sqlclient_permission_grant_request import SqlClientPermissionGrantLaunchRequest
from dpone.app.mssql_tds_coordinator_composition import TdsActorPoolCapability, create_tds_coordinator
from dpone.contracts.mssql_tds_api import TdsDirectoryLimits, WindowLease
from dpone.ports.bounded_window import WindowStore
from dpone.services.mssql_tds_permission_grant import (
    PermissionGrantHeldOwner,
    hold_permission_grant,
    permission_evidence_subject,
)

StoreFactory = Callable[[], AbstractContextManager[WindowStore]]


def hold_mssql_sqlclient_permission(
    pool: TdsActorPoolCapability,
    store_factory: StoreFactory,
    association,
    limits: TdsDirectoryLimits,
    lease: WindowLease,
    launcher,
    launch_request: SqlClientPermissionGrantLaunchRequest,
    admission: bytes,
    evidence_root: Path,
    *,
    supervisor_token: str,
    deadline: float,
) -> PermissionGrantHeldOwner:
    """Persist INTENT, launch once, and retain all owners at HELD_READY."""
    association.assert_ready()
    identity = association.identity
    if identity != launch_request.operation or not evidence_root.is_absolute():
        raise ValueError("mssql_native.sqlclient_permission_composition_invalid")
    coordinator = create_tds_coordinator(
        pool,
        store_factory,
        identity,
        limits,
        lease,
        supervisor_token=supervisor_token,
        deadline=deadline,
    )

    def evidence_factory(binding):
        subject = permission_evidence_subject(binding)

        @contextmanager
        def writer() -> Iterator[DescriptorPinnedCreateOnlyEvidenceWriter]:
            yield DescriptorPinnedCreateOnlyEvidenceWriter(evidence_root)

        return pool.open(
            lambda actor_deadline, actor_clock: SqlClientPermissionGrantEvidenceActor(
                writer, subject, binding, actor_deadline, actor_clock
            ),
            deadline=deadline,
        )

    return hold_permission_grant(
        association,
        coordinator,
        launcher,
        launch_request,
        admission,
        launch_request.credential_payload,
        evidence_factory,
        grant_id=uuid4,
        deadline=deadline,
        clock=monotonic,
    )
