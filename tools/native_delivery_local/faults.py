"""Narrow real delegate faults; no fabricated receipt or no-op SQL verifier."""

from __future__ import annotations

import json
from dataclasses import asdict

from .inventory import canonical
from .provisioning import json_value


class Faults:
    def __init__(self):
        self.armed = None
        self.events = []
        self.source_queries = 0
        self.publications = 0
        self.probes = 0
        self.stage_reads = 0
        self.known = True
        self.active_finalizer = False
        self.rollback_acknowledged = False
        self.commit_attempted = False

    def begin_finalizer(self):
        self.rollback_acknowledged = False
        self.commit_attempted = False
        self.active_finalizer = True

    def arm(self, fault):
        if fault not in {"after_eof", "before_commit", "lost_ack", "unknown_commit"}:
            raise ValueError("local_fixture.unsupported_fault")
        if self.armed is not None:
            raise ValueError("local_fixture.fault_already_armed")
        self.armed = fault

    def fire(self, fault):
        if self.armed == fault:
            self.events.append(fault)
            if fault != "unknown_commit":
                self.armed = None
            raise RuntimeError("local_fixture.injected:" + fault)


class FaultStore:
    """Throw only after SQLite acknowledged durable EOF including metadata."""

    def __init__(self, delegate, faults):
        self.delegate, self.faults = delegate, faults

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def save(self, key, revision, payload, lease):
        record = self.delegate.save(key, revision, payload, lease)
        if key.startswith("mssql-native-chunks-v1/"):
            state = json.loads(payload)
            if state.get("phase") == "stage_complete" and state.get("completion_metadata"):
                self.faults.fire("after_eof")
        return record


class TargetDelegate:
    """Only the finalizer's real COMMIT acknowledgement can inject lost ACK."""

    def __init__(self, connector, faults):
        self.connector, self.faults = connector, faults

    def __getattr__(self, name):
        return getattr(self.connector, name)

    def get_records_iterator(self, query, *args, **kwargs):
        result = self.connector.get_records_iterator(query, *args, **kwargs)
        if "[business]" not in query:
            self.faults.stage_reads += 1
        return result

    def commit_transaction(self):
        if self.faults.active_finalizer:
            self.faults.commit_attempted = True
            self.faults.known = False
        result = self.connector.commit_transaction()
        if self.faults.active_finalizer:
            self.faults.publications += 1
            self.faults.fire("lost_ack")
            self.faults.fire("unknown_commit")
            self.faults.known = True
        return result

    def rollback(self):
        result = self.connector.rollback()
        if self.faults.active_finalizer and not self.faults.commit_attempted:
            self.faults.rollback_acknowledged = True
        return result


class ReceiptDelegate:
    """Persist expectations from actual precommit receipt producer, then probe independently."""

    def __init__(self, state, faults, store, lease, key):
        self.state, self.faults, self.store, self.lease, self.key = state, faults, store, lease, key

    def __getattr__(self, name):
        return getattr(self.state, name)

    def insert_receipt(self, *args, **kwargs):
        receipt = self.state.insert_receipt(*args, **kwargs)
        prior = self.store.load(self.key)
        payload = canonical(json_value(asdict(receipt))).decode()
        if prior is not None and prior.payload != payload:
            raise ValueError("local_fixture.expected_receipt_changed")
        if prior is None:
            self.store.save(self.key, None, payload, self.lease)
        self.faults.fire("before_commit")
        return receipt

    def probe_receipt_fresh(self, *args, **kwargs):
        self.faults.probes += 1
        if self.faults.armed == "unknown_commit":
            raise RuntimeError("local_fixture.receipt_probe_unavailable")
        receipt = self.state.probe_receipt_fresh(*args, **kwargs)
        if receipt is not None:
            self.faults.known = True
        return receipt
