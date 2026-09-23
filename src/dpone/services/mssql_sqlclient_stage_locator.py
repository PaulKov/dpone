"""Domain admission and immutable reverse lookup policy over injected ports.

The composition root supplies the same actor-owned WindowStore and original
journal observers. These cross-journal decisions create no clients or threads.
Fenced CAS remains single-key; unknown outcomes never imply rollback or replay.
"""

from uuid import uuid4

from dpone.ports.bounded_window import WindowStore
from dpone.ports.mssql_tds_coordinator import TdsCoordinatorObserver
from dpone.ports.mssql_tds_directory import TdsDirectoryObserver
from dpone.ports.mssql_tds_journal import TdsAttemptObserver
from dpone.services.mssql_tds_writer_contracts import (
    STATE_DOMAIN_KEY,
    SqlClientStageLocator,
    SqlClientStageLocatorSnapshot,
    SqlClientStageLookup,
    TdsAttemptOwnership,
    TdsCreateRequest,
    WindowContractError,
    WindowLease,
    WindowOutcomeUnknown,
    WindowRecord,
    _integer,
    _text,
    decode_stage_locator_record,
    encode_stage_locator,
    encode_state_domain,
    initial_coordinator_state,
    initial_state,
    stage_locator_key,
    validate_locator_request,
    validate_state_domain_record,
    window_record_ack_matches,
)


def _lease(lease: WindowLease) -> None:
    if type(lease) is not WindowLease:
        raise WindowContractError("mssql_native.sqlclient_locator_lease_invalid")
    _text(lease.target_id)
    _text(lease.owner)
    _integer(lease.fence, 1)


def _read(store: WindowStore, key: str) -> WindowRecord | None:
    try:
        return store.load(key)
    except WindowContractError:
        raise
    except Exception:
        raise WindowOutcomeUnknown("mssql_native.sqlclient_locator_read_unknown") from None


def _assert(store: WindowStore, lease: WindowLease) -> None:
    try:
        store.assert_lease(lease)
    except WindowContractError:
        raise
    except Exception:
        raise WindowOutcomeUnknown("mssql_native.sqlclient_locator_lease_unknown") from None


def _save(store: WindowStore, key: str, payload: str, lease: WindowLease) -> WindowRecord:
    try:
        record = store.save(key, None, payload, lease)
    except WindowContractError:
        raise
    except Exception:
        raise WindowOutcomeUnknown("mssql_native.sqlclient_locator_write_unknown") from None
    if not window_record_ack_matches(record, revision=None, payload=payload):
        raise WindowOutcomeUnknown("mssql_native.sqlclient_locator_ack_unknown")
    return record


def require_state_domain(store: WindowStore, expected: WindowRecord) -> None:
    """Recheck the exact marker in the same store context, without repairing it."""
    try:
        validate_state_domain_record(expected)
        current = _read(store, STATE_DOMAIN_KEY)
        if current is None:
            raise ValueError
        validate_state_domain_record(current)
        if current != expected:
            raise ValueError
    except ValueError:
        raise WindowContractError("mssql_native.sqlclient_state_domain_changed") from None


def admit_state_domain(store: WindowStore, lease: WindowLease) -> WindowRecord:
    """Read or initialize exactly once; a racing CAS loser aborts this admission."""
    _lease(lease)
    _assert(store, lease)
    record = _read(store, STATE_DOMAIN_KEY)
    if record is None:
        record = _save(store, STATE_DOMAIN_KEY, encode_state_domain(uuid4()).decode("utf-8"), lease)
    try:
        validate_state_domain_record(record)
    except ValueError:
        raise WindowContractError("mssql_native.sqlclient_state_domain_invalid") from None
    require_state_domain(store, record)
    _assert(store, lease)
    return record


class SqlClientStageLocatorJournal:
    """Create once from original journals or read original recovery coordinates.

    Creation is serialized by the existing attempt composition. These reads and
    the locator CAS are not a cross-record transaction. Successor recovery may
    inspect an unchanged locator, but cannot acquire a new CREATE capability.
    """

    def __init__(
        self,
        store: WindowStore,
        domain: WindowRecord,
        *,
        parent_observer: TdsAttemptObserver,
        directory_observer: TdsDirectoryObserver,
        coordinator_observer: TdsCoordinatorObserver,
    ) -> None:
        self._parents = parent_observer
        self._directories = directory_observer
        self._coordinators = coordinator_observer
        self._domain_id = validate_state_domain_record(domain)
        self._store = store
        self._domain = WindowRecord(domain.revision, domain.payload)

    def _originals(self, locator: SqlClientStageLocator, *, creating: bool) -> None:
        operation, owner = locator.create_operation, locator.execution_owner
        parent = self._parents.read(operation.parent)
        directory = self._directories.read(operation.parent, locator.directory_limits)
        coordinator = self._coordinators.read(operation)
        if (
            parent is None
            or directory is None
            or coordinator is None
            or parent.state.identity != operation.parent
            or directory.state.parent != operation.parent
            or coordinator.state.identity != operation
            or parent.state.schema_version != 2
            or parent.state.backend != "mssql_sqlclient"
            or directory.state.schema_version != 2
            or directory.state.limits != locator.directory_limits
            or coordinator.state.execution_owner != owner
            or operation.slot_index >= len(directory.state.slots)
        ):
            raise WindowContractError("mssql_native.sqlclient_locator_originals_invalid")
        slot = directory.state.slots[operation.slot_index]
        if (slot.operation_id, slot.command, slot.command_sha256, slot.owner_fence) != (
            operation.operation_id,
            operation.command,
            operation.command_sha256,
            operation.original_fence,
        ):
            raise WindowContractError("mssql_native.sqlclient_locator_reservation_changed")
        if creating:
            if (
                parent.state != initial_state(operation.parent, owner, backend="mssql_sqlclient")
                or directory.ownership != owner
                or directory.state.work_sealed
                or coordinator.state != initial_coordinator_state(operation, directory.state, owner)
            ):
                raise WindowContractError("mssql_native.sqlclient_locator_creation_rejected")
        elif any(
            current.fence < owner.fence or (current.fence == owner.fence and current != owner)
            for current in (parent.state.ownership, directory.ownership)
        ):
            raise WindowContractError("mssql_native.sqlclient_locator_original_fence_changed")

    def create(
        self, locator: SqlClientStageLocator, request: TdsCreateRequest, lease: WindowLease
    ) -> SqlClientStageLocatorSnapshot:
        """Acknowledge immutable discovery only after exact original CREATE intent."""
        validate_locator_request(locator, request)
        _lease(lease)
        if (
            locator.state_domain_id != self._domain_id
            or lease.target_id != locator.create_operation.parent.target_key
            or TdsAttemptOwnership(lease.owner, lease.fence, locator.execution_owner.supervisor_id)
            != locator.execution_owner
        ):
            raise WindowContractError("mssql_native.sqlclient_locator_owner_mismatch")
        payload = encode_stage_locator(locator).decode("utf-8")
        require_state_domain(self._store, self._domain)
        _assert(self._store, lease)
        self._originals(locator, creating=True)
        # Existing identical bytes are not permission for another producer run.
        key = stage_locator_key(locator.lookup())
        if _read(self._store, key) is not None:
            raise WindowContractError("mssql_native.sqlclient_locator_already_exists")
        record = _save(self._store, key, payload, lease)
        _assert(self._store, lease)
        return decode_stage_locator_record(record)

    def read(self, lookup: SqlClientStageLookup) -> SqlClientStageLocatorSnapshot:
        """Resolve exactly one catalog member; absence or orphaned state rejects."""
        key = stage_locator_key(lookup)
        if lookup.state_domain_id != self._domain_id:
            raise WindowContractError("mssql_native.sqlclient_locator_domain_mismatch")
        require_state_domain(self._store, self._domain)
        record = _read(self._store, key)
        try:
            if record is None:
                raise ValueError
            snapshot = decode_stage_locator_record(record)
            if snapshot.locator.lookup() != lookup:
                raise ValueError
        except ValueError:
            raise WindowContractError("mssql_native.sqlclient_locator_record_invalid") from None
        self._originals(snapshot.locator, creating=False)
        return snapshot
