"""Scripted DB-API responses for offline gate failure-boundary tests."""

from collections import deque
from dataclasses import replace

import pytest

from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_SCHEMA_VERSION
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_activation import CompositionActivationOccurrence, CompositionActivationReceipt
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from tests.composition_mssql_catalog_helpers import catalog_observation, install_offline_catalog_references
from tests.test_composition_activation_contract import digest, request

SERVICE = "10000000-0000-4000-8000-000000000002"


def occurrence(state="ACTIVE"):
    parent = request()
    resource = parent.resources[0]
    resource = replace(
        resource,
        service_id=SERVICE,
        guard_id=canonical_fingerprint(
            {
                "schema": "dpone.composition-physical-domain.v1",
                "connector": "mssql",
                "service_id": SERVICE,
                "physical_subject_sha256": resource.physical_subject_sha256,
            }
        ),
    )
    parent = replace(parent, resources=(resource,))
    return CompositionActivationOccurrence(
        parent, CompositionActivationReceipt(parent.request_sha256, state, ((resource.guard_id, 1),))
    )


def attempt(try_number=1):
    parent = occurrence()
    workload = parent.request.workloads[0]
    return CompositionAttemptIdentity(
        parent.request.request_sha256,
        workload.workload_id,
        workload.constituent_id,
        workload.pack_sha256,
        digest("plan"),
        "run",
        "execute",
        try_number,
        -1,
        parent.receipt.guard_epochs,
    )


class Cursor:
    def __init__(self, steps=()):
        self.steps = deque(steps)
        self.calls = []
        self.rows = []

    def execute(self, sql, *parameters):
        self.calls.append((sql, parameters))
        if "composition_login_gates]" in sql and "UPDATE " in sql and "OUTPUT inserted.gate_state" in sql:
            assert "OUTPUT inserted.gate_state INTO @transition" in sql, (
                "SQL334: OUTPUT requires INTO with AFTER trigger"
            )
        projected = catalog_observation(sql, parameters)
        if sql.startswith("DECLARE @count"):
            self.rows = [(1, 1, "Exclusive", 7)]
        elif sql.startswith("SELECT TOP (2) singleton,schema_version,"):
            self.rows = [(1, COMPOSITION_MSSQL_SCHEMA_VERSION, SERVICE)]
        elif projected is not None:
            self.rows = projected
        elif self.steps:
            contains, result = self.steps.popleft()
            assert contains in sql, (contains, sql)
            if isinstance(result, Exception):
                raise result
            self.rows = list(result)
        return self

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        result, self.rows = self.rows, []
        return result

    def close(self):
        pass


class Connection:
    autocommit = False

    def __init__(self, cursor, *, commit_error=None):
        self.value = cursor
        self.commit_error = commit_error
        self.committed = self.rolled_back = False

    def cursor(self):
        return self.value

    def commit(self):
        if self.commit_error:
            raise self.commit_error
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def begin_steps():
    return [
        ("SET XACT_ABORT", []),
        ("sp_getapplock", [(0,)]),
    ]


def parent_steps(parent=None):
    from dpone.adapters.composition_mssql_store_queries import resource_document
    from dpone.contracts.composition_persistence import encode_activation_request

    parent = parent or occurrence()
    resource = parent.request.resources[0]
    return [
        ("SELECT LOWER(CONVERT(char(36), activation_id))", [(parent.request.activation_id,)]),
        (
            "SELECT request_sha256, request_document, state",
            [(parent.request.request_sha256, encode_activation_request(parent.request), parent.receipt.state)],
        ),
        ("SELECT guard_id, resource_document, fencing_epoch", [(resource.guard_id, resource_document(resource), 1)]),
        (
            "SELECT connector",
            [
                (
                    resource.connector,
                    resource.service_id,
                    resource.physical_subject_sha256,
                    1,
                    parent.request.activation_id,
                )
            ],
        ),
    ]


def attempt_record(value=None, state="RUNNING", proofs=(None, None, None)):
    from dpone.contracts.composition_persistence import encode_attempt_identity
    from dpone.contracts.composition_proof import composition_attempt_epoch_subject

    value = value or attempt()
    return (
        value.attempt_sha256,
        value.activation_request_sha256,
        composition_attempt_epoch_subject(value),
        encode_attempt_identity(value),
        state,
        *proofs,
        occurrence().request.activation_id,
    )


def receipt_steps(value=None, state="RUNNING", proofs=(None, None, None)):
    value = value or attempt()
    return [("SELECT attempt_sha256", [attempt_record(value, state, proofs)]), *partition_steps(value)]


def partition_steps(value=None):
    value = value or attempt()
    return [
        ("SELECT request_sha256 FROM", [(value.activation_request_sha256,)]),
        ("SELECT guard_id, fencing_epoch", list(value.guard_epochs)),
    ]


@pytest.fixture(autouse=True)
def offline_catalog(monkeypatch):
    install_offline_catalog_references(monkeypatch)


@pytest.fixture
def issuing(monkeypatch):
    from dpone.adapters import composition_mssql_login_gate as gate_module
    from dpone.adapters.composition_mssql_issuance import MssqlEnrollment, MssqlIssuedCredentials
    from dpone.adapters.composition_mssql_login_gate import MssqlCompositionLoginGate
    from dpone.contracts.composition_attempt import CompositionAttemptReceipt

    value = attempt()
    # This gate-boundary fixture supplies an independently observed existing
    # operation. Kernel/history and actual SQL are separate validation layers.
    monkeypatch.setattr(gate_module, "read_attempt", lambda *_: CompositionAttemptReceipt(value, "RUNNING"))
    credentials = MssqlIssuedCredentials("dpone_v3_" + value.attempt_sha256[7:], b"a" * 16, "test-password-secret")
    enrollment = MssqlEnrollment(
        value.guard_epochs[0][0],
        "SyntheticTarget",
        8,
        "10000000-0000-4000-8000-000000000009",
        "2026-09-10T00:00:00",
        "bounded_writer",
        ("dbo",),
    )
    monkeypatch.setattr(gate_module, "new_credentials", lambda _: credentials)
    monkeypatch.setattr(gate_module, "require_gate_policy", lambda *_: None)
    monkeypatch.setattr(gate_module, "require_enrollments", lambda *_: (enrollment,))
    monkeypatch.setattr(MssqlCompositionLoginGate, "_require_running", staticmethod(lambda *_: None))
    return value, credentials, enrollment
