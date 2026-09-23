"""One immutable seal over admitted original journals; no read-side repair."""

from dataclasses import dataclass

from dpone.ports.bounded_window import WindowStore
from dpone.ports.mssql_tds_coordinator import TdsCoordinatorObserver
from dpone.services.mssql_sqlclient_stage_locator import SqlClientStageLocatorJournal, _assert, _lease, _read, _save
from dpone.services.mssql_tds_writer_contracts import (
    MAX_CREATE_SEAL_BYTES,
    SqlClientCreateSeal,
    SqlClientStageLocator,
    SqlClientStageLocatorSnapshot,
    TdsCoordinatorSnapshot,
    WindowContractError,
    WindowLease,
    WindowRecord,
    _integer,
    create_seal_key,
    decode_create_seal,
    encode_create_seal,
    snapshot_create_state,
    validate_create_lineage,
)


@dataclass(frozen=True)
class SqlClientCreateSealObservation:
    locator: SqlClientStageLocatorSnapshot
    seal: SqlClientCreateSeal
    record: WindowRecord
    current: TdsCoordinatorSnapshot


class SqlClientCreateEvidenceJournal:
    """Store decisions over injected domain-checked locator and coordinator views."""

    def __init__(
        self, store: WindowStore, locators: SqlClientStageLocatorJournal, coordinators: TdsCoordinatorObserver
    ) -> None:
        self.store, self.locators, self.coordinators = store, locators, coordinators

    def read(self, locator: SqlClientStageLocator) -> SqlClientCreateSealObservation:
        observed = self.locators.read(locator.lookup())
        if observed.locator != locator:
            raise WindowContractError("mssql_native.sqlclient_create_locator_changed")
        record = _read(self.store, create_seal_key(locator))
        if type(record) is not WindowRecord:
            raise WindowContractError("mssql_native.sqlclient_create_seal_missing")
        # Validate actual revision independently of seal payload.
        if (
            type(record.revision) is not int
            or record.revision < 1
            or type(record.payload) is not str
            or len(record.payload) > MAX_CREATE_SEAL_BYTES
        ):
            raise WindowContractError("mssql_native.sqlclient_create_seal_invalid")
        _integer(record.revision, 1)
        seal = decode_create_seal(record.payload.encode("utf-8"), locator)
        current = self.coordinators.read(locator.create_operation)
        if current is None:
            raise WindowContractError("mssql_native.sqlclient_create_original_missing")
        current = snapshot_create_state(current)
        validate_create_lineage(seal.original, current)
        if current.state.error is not None:
            raise WindowContractError("mssql_native.sqlclient_create_error_present")
        return SqlClientCreateSealObservation(observed, seal, record, current)

    def create(
        self, locator: SqlClientStageLocator, seal: SqlClientCreateSeal, lease: WindowLease
    ) -> SqlClientCreateSealObservation:
        _lease(lease)
        payload = encode_create_seal(seal)
        seal = decode_create_seal(payload, locator)
        owner = locator.execution_owner
        if (lease.target_id, lease.owner, lease.fence) != (
            locator.create_operation.parent.target_key,
            owner.owner,
            owner.fence,
        ):
            raise WindowContractError("mssql_native.sqlclient_create_original_lease_required")
        _assert(self.store, lease)
        observed = self.locators.read(locator.lookup())
        current = self.coordinators.read(locator.create_operation)
        if observed.locator != locator or current is None or snapshot_create_state(current) != seal.original:
            raise WindowContractError("mssql_native.sqlclient_create_original_changed")
        record = _save(self.store, create_seal_key(locator), payload.decode("utf-8"), lease)
        _assert(self.store, lease)
        return SqlClientCreateSealObservation(observed, seal, record, current)
