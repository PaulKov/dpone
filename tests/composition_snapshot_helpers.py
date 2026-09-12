"""Synthetic fixtures and later offline doubles; never live ClickHouse proof."""

from dataclasses import replace
from hashlib import sha256
from threading import Lock

from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionActivationReceipt,
    CompositionAdmissionError,
    CompositionPhysicalResource,
)
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_proof import CompositionProofAuthority
from dpone.contracts.composition_snapshot import (
    SnapshotCatalogObservation,
    SnapshotGeneration,
    SnapshotLimits,
    SnapshotPublicationIntent,
    SnapshotPublicationRecord,
    SnapshotPublisherClosure,
    SnapshotTarget,
)
from tests.test_composition_activation_contract import request

SERVICE = "10000000-0000-4000-8000-000000000010"
DATABASE = "10000000-0000-4000-8000-000000000011"
OLD = "10000000-0000-4000-8000-000000000012"
NEW = "10000000-0000-4000-8000-000000000013"


def digest(value):
    return "sha256:" + sha256(value.encode()).hexdigest()


def target():
    return SnapshotTarget(
        SERVICE,
        digest("physical"),
        DATABASE,
        "synthetic",
        "target",
        "generation",
        digest("native write"),
        digest("enrollment"),
    )


def occurrence(state="ACTIVE"):
    parent = request()
    physical = target()
    native = replace(
        parent.workloads[0],
        execution_cell="mssql_clickhouse_full_refresh_v1",
        write_subjects=(physical.write_subject_sha256,),
    )
    old_resource = replace(parent.resources[0], write_subjects=parent.workloads[1].write_subjects)
    new_resource = CompositionPhysicalResource(
        physical.guard_id,
        "clickhouse",
        SERVICE,
        physical.physical_subject_sha256,
        digest("catalog"),
        native.write_subjects,
    )
    parent = replace(
        parent,
        workloads=(native, parent.workloads[1]),
        resources=tuple(sorted((old_resource, new_resource), key=lambda row: row.guard_id)),
    )
    return CompositionActivationOccurrence(
        parent,
        CompositionActivationReceipt(
            parent.request_sha256, state, tuple((row.guard_id, 1) for row in parent.resources)
        ),
    )


def intent(*, rows=2, try_number=1):
    parent = occurrence()
    workload = parent.request.workloads[0]
    attempt = CompositionAttemptIdentity(
        parent.request.request_sha256,
        workload.workload_id,
        workload.constituent_id,
        workload.pack_sha256,
        digest("plan"),
        "run",
        "execute",
        try_number,
        -1,
        ((target().guard_id, 1),),
    )
    generation = SnapshotGeneration(
        digest("generation record"),
        digest("source snapshot"),
        digest("content"),
        digest("schema"),
        digest("design"),
        OLD,
        NEW,
        rows,
        rows * 10,
        rows * 12,
        50,
        40,
        10,
    )

    def principal(suffix):
        return CompositionProofAuthority(
            "clickhouse",
            SERVICE,
            "clickhouse-user:10000000-0000-4000-8000-0000000000" + suffix,
        )

    return SnapshotPublicationIntent(
        attempt,
        target(),
        generation,
        SnapshotLimits(100, 1000, 1200, 200, 100, 500),
        principal("21"),
        principal("22"),
        digest("closed ingest"),
    )


def observation(value, *, published=False):
    generation = value.generation
    pair = (NEW, OLD) if published else (OLD, NEW)
    return SnapshotCatalogObservation(
        value.target,
        *pair,
        "Atomic",
        ("MergeTree", "MergeTree"),
        1,
        1,
        (generation.schema_sha256,) * 2,
        (generation.physical_sha256,) * 2,
        (),
        generation.content_sha256,
        generation.rows,
        50,
        40,
        10,
        digest("fresh catalog"),
    )


class MemoryStore:
    """Thread-safe offline CAS model; not SQL durability/permission evidence."""

    def __init__(self):
        self.records = {}
        self.lock = Lock()
        self.events = []
        self.claim_error = False
        self.read_error = False
        self.resolution_error = False

    def prepare(self, value):
        with self.lock:
            self.events.append("prepare")
            existing = self.records.get(value.intent_sha256)
            if existing is not None:
                assert existing.state == "PREPARED" and existing.intent.to_bytes() == value.to_bytes()
                return existing
            assert not any(
                row.intent.attempt == value.attempt
                and row.intent.target.write_subject_sha256 == value.target.write_subject_sha256
                for row in self.records.values()
            )
            row = SnapshotPublicationRecord(value)
            self.records[value.intent_sha256] = row
            return row

    def read(self, key):
        self.events.append("read")
        if self.read_error:
            raise RuntimeError("secret read connection")
        with self.lock:
            row = self.records.get(key)
            return SnapshotPublicationRecord.from_bytes(row.to_bytes(), row.record_sha256) if row else None

    def claim_exchange(self, expected):
        with self.lock:
            self.events.append("claim")
            if self.records[expected.intent.intent_sha256] != expected:
                return None
            row = expected.transition("EXCHANGE_INTENT")
            self.records[expected.intent.intent_sha256] = row
            if self.claim_error:
                raise RuntimeError("secret commit ACK lost")
            return row

    def resolve(self, expected, *, state, closure, observation):
        with self.lock:
            self.events.append("resolve")
            assert self.records[expected.intent.intent_sha256] == expected
            row = expected.transition(state, closure=closure, observation=observation)
            self.records[expected.intent.intent_sha256] = row
            if self.resolution_error:
                raise RuntimeError("secret outcome ACK lost")
            return row


class Authority:
    """Offline authority stub: booleans simulate unavailable dependencies only."""

    def __init__(self, value):
        self.value = value
        self.parent = occurrence()
        self.closed = False
        self.close_error = False
        self.current_error = False
        self.calls = []
        self.enrollment_calls = []
        self.enrollments = {"original": b"supervisor-enrollment"}

    def load_prepared(self, attempt, generation_ref):
        assert self.value.attempt == attempt and self.value.generation.record_sha256 == generation_ref
        return self.value

    def require_current(self, value, *, recovery):
        self.calls.append(recovery)
        if self.current_error:
            raise RuntimeError("secret authority unavailable")
        if not recovery and self.closed:
            raise RuntimeError("closed gate")
        return self.parent

    def require_enrollment(self, attempt, target):
        self.enrollment_calls.append((attempt, target))
        if not self.enrollments:
            raise CompositionAdmissionError("enrollment_missing")

    def close_publisher(self, value):
        self.closed = True
        if self.close_error:
            raise RuntimeError("secret quiescence unavailable")
        return SnapshotPublisherClosure(value.intent_sha256, digest("closed publisher"), digest("quiescence"))


class Catalog:
    def __init__(self, value):
        self.value = observation(value)
        self.error = False

    def inspect(self, intent):
        if self.error:
            raise RuntimeError("secret catalog unavailable")
        return self.value


class Executor:
    def __init__(self, value, authority, catalog, store):
        self.value, self.authority, self.catalog, self.store = value, authority, catalog, store
        self.calls = []
        self.error = False
        self.no_effect = False

    def exchange_once(self, value):
        assert not self.authority.closed
        assert self.store.records[value.intent_sha256].state == "EXCHANGE_INTENT"
        self.calls.append(value.exchange_query_id)
        if not self.no_effect:
            self.catalog.value = observation(value, published=True)
        if self.error:
            raise RuntimeError("secret exchange ACK lost")


def rig(value=None):
    from dpone.runtime.composition_snapshot import ClickHouseAtomicSnapshotPublisher

    value = value or intent()
    store, authority, catalog = MemoryStore(), Authority(value), Catalog(value)
    executor = Executor(value, authority, catalog, store)
    publisher = ClickHouseAtomicSnapshotPublisher(authority=authority, store=store, catalog=catalog, executor=executor)
    return publisher, store, authority, catalog, executor
