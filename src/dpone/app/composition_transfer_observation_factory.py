"""Construct transfer proof from protected SQL originals and retained source bytes.

No caller booleans or cached executor receipts are inputs. The binding decoder
roundtrips canonical originals, enabling observation after process loss as long
as the supervisor-owned payload directory remains available.
"""

from __future__ import annotations

from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Any

from dpone.adapters.composition_mssql_connection_identity import require_mssql_connection_identity
from dpone.adapters.composition_mssql_enrollment import mssql_target_pin
from dpone.adapters.composition_mssql_transaction_binding import MssqlCompositionTransactionBindings
from dpone.app.composition_transfer_observation import CompositionTransferCommittedObserver
from dpone.config.state import resolve_mssql_state_location
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_mssql_binding import CompositionMssqlOperationBinding
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthoritySet
from dpone.runtime.composition_transfer_payload import (
    CompositionTransferCaptureLifecycle,
    CompositionTransferPayloadStore,
)
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.state.mssql_generic_operation_state import MssqlGenericOperationState


def build_composition_transfer_observation(
    *,
    control: Any,
    sink_target: Any,
    state_target: Any,
    state_config: Any,
    read_plan: Any,
    verify_operation: Any,
    payload_root: Path,
) -> tuple[CompositionTransferCommittedObserver, Any]:
    """Build an independent reader and the capture hook for the same supervisor root.

    The caller supplies the private, preprovisioned supervisor directory. All
    connection bindings and state configuration come from the verified parent.
    Constructing this factory neither connects nor grants database permissions.
    """
    location = resolve_mssql_state_location(state_config, state_target)
    if location.atomicity != "target_atomic" or location.provisioning != "external":
        raise CompositionAdmissionError("external_target_atomic_state_required")
    for target in (sink_target, state_target):
        if (
            target.descriptor is None
            or target.descriptor.properties.get("composition_service_id") != control.expected_service_id
        ):
            raise CompositionAdmissionError("transfer_observation_service")
    payloads = CompositionTransferPayloadStore(payload_root)

    bindings = MssqlCompositionTransactionBindings(
        control.connection_factory,
        expected_service_id=control.expected_service_id,
        control_database=control.control_database,
        control_schema=control.control_schema,
        read_plan=read_plan,
        verify_operation=verify_operation,
    )

    @contextmanager
    def transaction():
        connector = ResolvedConnectorFactory.create(sink_target, autocommit=False)
        try:
            yield connector.connection
        finally:
            connector.close()

    def read_receipt(connection: Any, bound: CompositionMssqlOperationBinding) -> Any:
        return MssqlGenericOperationState(
            _ReadSession(connection),
            database=location.location.database,
            schema=location.location.schema,
        ).receipt_by_key(bound.operation.operation_key)

    def require_target(connection: Any, bound: CompositionMssqlOperationBinding) -> None:
        _require_target(connection, bound, control, sink_target, state_target, location.location.database)

    observer = CompositionTransferCommittedObserver(
        read_binding=bindings.read_closed,
        transaction=transaction,
        read_receipt=read_receipt,
        require_target=require_target,
        read_payload=payloads.read,
    )
    return observer, partial(CompositionTransferCaptureLifecycle, payloads)


decode_transfer_binding = CompositionMssqlOperationBinding.from_bytes


class _ReadSession:
    """Adapt one DB-API cursor read to the existing generic immutable receipt reader."""

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def get_records(self, sql: str, params: Any = (), *, as_dict: bool = False) -> Any:
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, *params)
            rows = tuple(tuple(row) for row in cursor.fetchall())
            if not as_dict:
                return rows
            names = tuple(column[0] for column in cursor.description)
            if len(names) != len(set(names)):
                raise CompositionAdmissionError("transfer_receipt_columns")
            return [dict(zip(names, row, strict=True)) for row in rows]
        finally:
            cursor.close()


def _require_target(
    connection: Any, bound: CompositionMssqlOperationBinding, control: Any, target: Any, state: Any, state_database: str
) -> None:
    target_pin = mssql_target_pin(target, bound.write)
    state_pins = MssqlDatabaseAuthoritySet.from_connection_properties(state.descriptor.properties, capability="state")
    state_pin = state_pins.require(state_database, capability="state")
    # Both endpoint descriptors must pin the same control database continuity;
    # the marker is observed on this exact business read connection.
    target_pins = MssqlDatabaseAuthoritySet.from_connection_properties(
        target.descriptor.properties, capability="target"
    )
    control_pin = target_pins.require(control.control_database, capability="target")
    if state_pins.require(control.control_database, capability="state") != control_pin:
        raise CompositionAdmissionError("transfer_observation_service")
    require_mssql_connection_identity(
        connection,
        pins=(target_pin, state_pin, control_pin),
        control_database=control.control_database,
        control_schema=control.control_schema,
        service_id=control.expected_service_id,
    )
