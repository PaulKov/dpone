"""Exact PREPARED-to-GRANT reservation and immediate HELD transfer."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from time import monotonic
from traceback import clear_frames
from typing import Any, NoReturn, cast
from uuid import UUID

from dpone.adapters.mssql_tds_coordinator_connection import (
    decode_connection_admission,
    encode_connection_admission,
)
from dpone.app.mssql_sqlclient_permission_grant_composition import hold_mssql_sqlclient_permission
from dpone.app.mssql_sqlclient_permission_grant_request import SqlClientPermissionGrantLaunchRequest
from dpone.contracts.mssql_tds_api import (
    SqlClientGrantInventory,
    SqlClientPermissionGrantRequest,
    SqlClientPrincipalResolution,
    TdsCoordinatorCommand,
    TdsCoordinatorIdentity,
    TdsDirectoryLimits,
    WindowLease,
    permission_grant_digest,
)
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial, TdsConnectionProfile
from dpone.services.mssql_tds_attempt import TdsAttempt
from dpone.services.mssql_tds_original_continuation import PreparationTransition
from dpone.services.mssql_tds_permission_grant import PermissionGrantHeldUnknown

ERROR = "mssql_native.sqlclient_permission_reservation_composition_invalid"


def _fail(error: BaseException | None = None) -> NoReturn:
    if error is not None:
        traceback = error.__traceback__
        error.__traceback__ = error.__cause__ = error.__context__ = None
        if traceback is not None:
            clear_frames(traceback)
    raise ValueError(ERROR) from None


def _deadline(value: object) -> float:
    if type(value) is not float or not math.isfinite(value) or value <= 0:
        _fail()
    return value


def _hash(value: object) -> str:
    if type(value) is not str or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        _fail()
    return value


def _validate_launcher(launcher: object, profile: TdsConnectionProfile, digest: str, implementation: str) -> None:
    try:
        digest = _hash(digest)
        implementation = _hash(implementation)
        admission = cast(Any, launcher).admission
        canonical = encode_connection_admission(*decode_connection_admission(admission))
        if (
            canonical != admission
            or sha256(canonical).hexdigest() != digest
            or cast(Any, launcher).implementation_sha256 != implementation
            or profile is not decode_connection_admission(canonical)[1]
        ):
            raise ValueError
    except (AttributeError, TypeError, ValueError, OverflowError) as error:
        _fail(error)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientPermissionReservationInputs:
    """Immutable inputs for one exact GRANT reservation and custody transfer."""

    pool: object
    store_factory: Callable[[], object]
    limits: TdsDirectoryLimits
    lease: WindowLease
    launcher: object
    admission: bytes = field(repr=False)
    evidence_root: Path
    supervisor_token: str
    operation_id: UUID
    profile: TdsConnectionProfile
    admission_sha256: str
    startup_deadline: float
    operation_deadline: float
    termination_timeout: float
    session_nonce: bytes = field(repr=False)
    material_supplier: Callable[[], TdsConnectionMaterial] = field(repr=False)

    def __post_init__(self) -> None:
        try:
            if (
                not callable(self.store_factory)
                or not callable(getattr(self.pool, "open", None))
                or not callable(getattr(self.pool, "assert_deadline", None))
                or not callable(getattr(self.launcher, "launch", None))
                or type(self.limits) is not TdsDirectoryLimits
                or type(self.lease) is not WindowLease
                or type(self.lease.target_id) is not str
                or not self.lease.target_id
                or type(self.lease.owner) is not str
                or not self.lease.owner
                or type(self.lease.fence) is not int
                or self.lease.fence <= 0
                or type(self.evidence_root) is not type(Path())
                or not self.evidence_root.is_absolute()
                or type(self.supervisor_token) is not str
                or not self.supervisor_token
                or type(self.operation_id) is not UUID
                or not self.operation_id.int
                or type(self.profile) is not TdsConnectionProfile
                or type(self.admission) is not bytes
                or type(self.session_nonce) is not bytes
                or len(self.session_nonce) != 32
                or not any(self.session_nonce)
                or not callable(self.material_supplier)
            ):
                raise ValueError
            _hash(self.admission_sha256)
            start, operation = _deadline(self.startup_deadline), _deadline(self.operation_deadline)
            if (
                start > operation
                or monotonic() >= start
                or not math.isfinite(self.termination_timeout)
                or self.termination_timeout <= 0
            ):
                raise ValueError
            self.limits.__post_init__()
        except (AttributeError, TypeError, ValueError, OverflowError) as error:
            _fail(error)


class _GrantMaterialCustody:
    """One-shot local reference; values never enter repr or returned state."""

    __slots__ = ("_material", "_taken")

    def __init__(self, material: TdsConnectionMaterial) -> None:
        if type(material) is not TdsConnectionMaterial:
            _fail()
        material.__post_init__()
        self._material: TdsConnectionMaterial | None = material
        self._taken = False

    def __repr__(self) -> str:
        return "_GrantMaterialCustody(<opaque>)"

    def take(self) -> TdsConnectionMaterial:
        material = self._material
        if self._taken or type(material) is not TdsConnectionMaterial:
            _fail()
        self._taken, self._material = True, None
        return material

    def close(self) -> None:
        self._taken, self._material = True, None


def _project_request(origin: PreparationTransition) -> SqlClientPermissionGrantRequest:
    try:
        origin.validate_origin()
        expected, receipt, opening = origin.expected, origin.evidence_receipt, origin.opening
        observed = origin.handle.request
        inventory = opening.inventory
        if type(inventory) is not SqlClientGrantInventory or expected is None or receipt is None:
            raise ValueError
        writer = observed.writer_principal
        login = observed.writer_admission.login
        if inventory.writer != writer or inventory.writer.sid != login.sid:
            raise ValueError
        management = inventory.management_before.authority
        request = SqlClientPermissionGrantRequest(
            parent=expected.state.identity,
            stage=observed.selected_stage,
            writer=SqlClientPrincipalResolution(
                "mapped_user", inventory.writer.principal_id, inventory.writer.name, inventory.writer.sid
            ),
            writer_login=login,
            management=management.principal_resolution,
            management_login=management.login,
            preparation_sha256=receipt.payload_sha256,
        )
        request.__post_init__()
        origin.validate_origin()
        return request
    except (AttributeError, TypeError, ValueError) as error:
        _fail(error)


def reserve_and_hold_mssql_sqlclient_permission(
    attempt: TdsAttempt,
    inputs: SqlClientPermissionReservationInputs,
) -> tuple[object, object]:
    """Preflight fully, reserve the original GRANT once, and transfer material."""
    if type(attempt) is not TdsAttempt or type(inputs) is not SqlClientPermissionReservationInputs:
        _fail()
    inputs.__post_init__()
    # Reject replay, re-entry, foreign-thread use and a poisoned attempt before
    # the only caller-controlled callback is invoked.
    attempt._owned()
    attempt._require_unpoisoned()
    if attempt._permission_grant_owner is not None:
        _fail()
    origin = attempt._prepared_origin
    if type(origin) is not PreparationTransition:
        _fail()
    request = _project_request(origin)
    implementation = origin.identity.implementation_sha256
    digest = permission_grant_digest(request)
    _validate_launcher(inputs.launcher, inputs.profile, inputs.admission_sha256, implementation)
    if cast(Any, inputs.launcher).admission != inputs.admission:
        _fail()

    custody: _GrantMaterialCustody | None = None
    try:
        try:
            material = inputs.material_supplier()
        except BaseException as error:
            _fail(error)
        custody = _GrantMaterialCustody(material)
        del material
        directory = attempt.directory
        predicted_identity = TdsCoordinatorIdentity(
            request.parent,
            len(directory.state.slots),
            inputs.operation_id,
            TdsCoordinatorCommand.GRANT,
            digest,
            directory.ownership.fence,
            implementation,
        )
        dry_material = custody.take()
        dry = SqlClientPermissionGrantLaunchRequest(
            request=request,
            operation=predicted_identity,
            execution_owner=directory.ownership,
            connection_material=dry_material,
            profile=inputs.profile,
            admission_sha256=inputs.admission_sha256,
            startup_deadline=inputs.startup_deadline,
            operation_deadline=inputs.operation_deadline,
            termination_timeout=inputs.termination_timeout,
            session_nonce=inputs.session_nonce,
        )
        dry.__post_init__()
        # Retain the already validated value in a fresh one-shot custody, then
        # discard the dry envelope before the durable reservation.
        custody = _GrantMaterialCustody(dry_material)
        del dry, dry_material
        origin.validate_origin()
        association = attempt._permission_grant(
            inputs.operation_id,
            digest,
            implementation,
            deadline=inputs.operation_deadline,
        )
        association.reserve()
        material = custody.take()
        try:
            try:
                launch = SqlClientPermissionGrantLaunchRequest(
                    request=request,
                    operation=association.identity,
                    execution_owner=association.reservation.ownership,
                    connection_material=material,
                    profile=inputs.profile,
                    admission_sha256=inputs.admission_sha256,
                    startup_deadline=inputs.startup_deadline,
                    operation_deadline=inputs.operation_deadline,
                    termination_timeout=inputs.termination_timeout,
                    session_nonce=inputs.session_nonce,
                )
            except BaseException:
                association._unknown()
            try:
                held = hold_mssql_sqlclient_permission(
                    cast(Any, inputs.pool),
                    cast(Any, inputs.store_factory),
                    association,
                    inputs.limits,
                    inputs.lease,
                    inputs.launcher,
                    launch,
                    inputs.admission,
                    inputs.evidence_root,
                    supervisor_token=inputs.supervisor_token,
                    deadline=inputs.operation_deadline,
                )
            except PermissionGrantHeldUnknown:
                raise
            except BaseException:
                # Coordinator/evidence allocation can fail before the held
                # service installs custody. The durable GRANT reservation is
                # then ambiguous and must become sticky UNKNOWN.
                association._unknown()
        finally:
            del material
        return association, held
    finally:
        if custody is not None:
            custody.close()
