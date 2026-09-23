"""Compose the exact SQLClient GRANT and restricted-writer authority chain."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Any, NoReturn, cast
from uuid import UUID

from dpone.app.mssql_sqlclient_departure_infrastructure_composition import admitted_departure_launcher as _admission
from dpone.app.mssql_sqlclient_permission_grant_release_composition import (
    release_mssql_sqlclient_permission_locally,
)
from dpone.app.mssql_sqlclient_permission_grant_settlement_composition import (
    settle_mssql_sqlclient_permission_grant,
)
from dpone.app.mssql_sqlclient_permission_reservation_composition import (
    SqlClientPermissionReservationInputs,
    _validate_launcher,
    reserve_and_hold_mssql_sqlclient_permission,
)
from dpone.app.mssql_sqlclient_restricted_writer_settlement_composition import (
    settle_mssql_sqlclient_restricted_writer_managed,
)
from dpone.app.mssql_sqlclient_restricted_writer_verify_composition import (
    bind_mssql_sqlclient_restricted_writer_verify,
    run_mssql_sqlclient_restricted_writer_verify,
)
from dpone.app.mssql_sqlclient_restricted_writer_verify_request import (
    RestrictedWriterCredentialSupplier,
    RestrictedWriterVerifyLaunchRequest,
    build_origin_request,
)
from dpone.app.mssql_tds_coordinator_composition import create_tds_coordinator
from dpone.contracts.mssql_tds_api import RestrictedWriterSettlementOperations
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial, TdsConnectionProfile
from dpone.services.mssql_tds_attempt import TdsAttempt
from dpone.services.mssql_tds_restricted_writer_settlement import RestrictedWriterVerified

ERROR = "mssql_native.sqlclient_authorization_composition_invalid"


def _fail() -> NoReturn:
    raise ValueError(ERROR) from None


def _root(value: object) -> None:
    if type(value) is not type(Path()) or not cast(Path, value).is_absolute():
        _fail()


def _positive(value: object) -> float:
    if type(value) is not float or not math.isfinite(value) or value <= 0:
        _fail()
    return value


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientAuthorizationInputs:
    """Immutable runtime dependencies for the exact P8/P9 composition."""

    permission: SqlClientPermissionReservationInputs
    grant_release_evidence_root: Path
    grant_settlement_evidence_root: Path
    grant_departure_launcher: object
    grant_management_credentials: Callable[[], TdsConnectionMaterial]
    containment_deadline: float | None
    grant_helper_startup_timeout: float
    grant_cleanup_deadline: float
    verify_operation_id: UUID
    verify_launcher: object
    verify_profile: TdsConnectionProfile
    verify_admission_sha256: str
    verify_startup_deadline: float
    verify_termination_timeout: float
    verify_credentials: Callable[[], tuple[TdsConnectionMaterial, bytes]]
    verify_evidence_root: Path
    verify_supervisor_token: str
    restricted_departure_launcher: object
    restricted_management_credentials: Callable[[], TdsConnectionMaterial]
    restricted_evidence_root: Path
    restricted_helper_startup_timeout: float
    restricted_cleanup_deadline: float
    settlement_operations: RestrictedWriterSettlementOperations

    def __post_init__(self) -> None:
        try:
            if (
                type(self.permission) is not SqlClientPermissionReservationInputs
                or type(self.verify_operation_id) is not UUID
                or not self.verify_operation_id.int
                or type(self.verify_profile) is not TdsConnectionProfile
                or type(self.verify_supervisor_token) is not str
                or not self.verify_supervisor_token
                or type(self.settlement_operations) is not RestrictedWriterSettlementOperations
                or not callable(getattr(self.grant_departure_launcher, "spawn", None))
                or not callable(getattr(self.verify_launcher, "launch", None))
                or not callable(getattr(self.restricted_departure_launcher, "spawn", None))
                or not all(
                    callable(getattr(self.settlement_operations, name, None))
                    for name in ("encode_request", "validate_result", "settlement_record")
                )
                or not all(
                    callable(value)
                    for value in (
                        self.grant_management_credentials,
                        self.verify_credentials,
                        self.restricted_management_credentials,
                    )
                )
            ):
                raise ValueError
            self.permission.__post_init__()
            for root in (
                self.grant_release_evidence_root,
                self.grant_settlement_evidence_root,
                self.verify_evidence_root,
                self.restricted_evidence_root,
            ):
                _root(root)
            for value in (
                self.grant_helper_startup_timeout,
                self.grant_cleanup_deadline,
                self.verify_startup_deadline,
                self.verify_termination_timeout,
                self.restricted_helper_startup_timeout,
                self.restricted_cleanup_deadline,
            ):
                _positive(value)
            if self.containment_deadline is not None:
                _positive(self.containment_deadline)
            if (
                self.verify_startup_deadline > self.permission.operation_deadline
                or monotonic() >= self.verify_startup_deadline
            ):
                raise ValueError
            # These reads validate canonical deployment facts without starting a
            # process, opening a store, or invoking a credential callback.
            _admission(cast(Any, self.grant_departure_launcher))
            _admission(cast(Any, self.restricted_departure_launcher))
            _validate_launcher(
                self.verify_launcher,
                self.verify_profile,
                self.verify_admission_sha256,
                cast(Any, self.verify_launcher).implementation_sha256,
            )
        except (AttributeError, TypeError, ValueError, OverflowError):
            _fail()


def authorize_mssql_sqlclient_writer(
    attempt: TdsAttempt, inputs: SqlClientAuthorizationInputs
) -> RestrictedWriterVerified:
    """Return only the settlement-created terminal from the original attempt."""
    if type(attempt) is not TdsAttempt or type(inputs) is not SqlClientAuthorizationInputs:
        _fail()
    inputs.__post_init__()
    origin = attempt._prepared_origin
    if origin is None:
        _fail()
    observed = origin.handle.request
    raw_association, held = reserve_and_hold_mssql_sqlclient_permission(attempt, inputs.permission)
    association = cast(Any, raw_association)
    local = release_mssql_sqlclient_permission_locally(
        cast(Any, inputs.permission.pool),
        held,
        inputs.grant_release_evidence_root,
        deadline=inputs.permission.operation_deadline,
        containment_deadline=inputs.containment_deadline,
    )
    settled = settle_mssql_sqlclient_permission_grant(
        inputs.permission.pool,
        local,
        inputs.grant_settlement_evidence_root,
        cast(Any, inputs.grant_departure_launcher),
        observed.management_admission,
        observed.writer_admission,
        inputs.grant_management_credentials,
        deadline=inputs.permission.operation_deadline,
        helper_startup_timeout=inputs.grant_helper_startup_timeout,
        cleanup_deadline=inputs.grant_cleanup_deadline,
    )
    verify_origin = bind_mssql_sqlclient_restricted_writer_verify(
        attempt, settled, deadline=inputs.permission.operation_deadline
    )
    verify_request = build_origin_request(
        association._verify_grant_request_ref,
        association._verify_effective,
        operation_id=inputs.verify_operation_id,
        implementation_sha256=cast(Any, inputs.verify_launcher).implementation_sha256,
    )
    verify_launch = RestrictedWriterVerifyLaunchRequest(
        request=verify_request,
        startup_deadline=inputs.verify_startup_deadline,
        operation_deadline=inputs.permission.operation_deadline,
        termination_timeout=inputs.verify_termination_timeout,
        admission_sha256=inputs.verify_admission_sha256,
        profile=inputs.verify_profile,
    )
    coordinator_calls = 0

    def coordinator_factory() -> object:
        nonlocal coordinator_calls
        if coordinator_calls or getattr(association, "_verify_phase", None) != "ready":
            _fail()
        coordinator_calls += 1
        return create_tds_coordinator(
            cast(Any, inputs.permission.pool),
            cast(Any, inputs.permission.store_factory),
            association._verify_identity,
            inputs.permission.limits,
            inputs.permission.lease,
            supervisor_token=inputs.verify_supervisor_token,
            deadline=inputs.permission.operation_deadline,
        )

    retained = run_mssql_sqlclient_restricted_writer_verify(
        cast(Any, inputs.permission.pool),
        verify_origin,
        inputs.verify_launcher,
        verify_launch,
        RestrictedWriterCredentialSupplier(inputs.verify_credentials),
        inputs.verify_evidence_root,
        deadline=inputs.permission.operation_deadline,
        coordinator_factory=cast(Any, coordinator_factory),
    )
    terminal = settle_mssql_sqlclient_restricted_writer_managed(
        inputs.permission.pool,
        inputs.permission.store_factory,
        retained,
        verify_origin,
        inputs.permission.limits,
        inputs.permission.lease,
        inputs.restricted_departure_launcher,
        observed.management_admission,
        observed.writer_admission,
        inputs.restricted_management_credentials,
        inputs.restricted_evidence_root,
        supervisor_token=inputs.verify_supervisor_token,
        deadline=inputs.permission.operation_deadline,
        helper_startup_timeout=inputs.restricted_helper_startup_timeout,
        cleanup_deadline=inputs.restricted_cleanup_deadline,
        operations=inputs.settlement_operations,
    )
    if not isinstance(terminal, RestrictedWriterVerified):
        _fail()
    return terminal
