"""Workspace attempts are fenced before mutation and terminalized durably."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dpone.adapters.dbt_workspace_mssql_attempt_admission import MssqlDbtWorkspaceAttemptAdmission
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationError
from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptRequest

ACTIVATION_ID = "164a3c74-cf85-4a4a-a087-07c9b07050ff"
GUARD_ID = "mssql://service/warehouse/mart/orders"


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _request(character: str = "a") -> DbtWorkspaceAttemptRequest:
    return DbtWorkspaceAttemptRequest.build(
        activation_id=ACTIVATION_ID,
        attempt_id=_digest(character),
        workflow_id="orders",
        write_subjects=(_digest("1"),),
    )


@dataclass
class _Store:
    activation_state: str = "ACTIVE"
    subjects: dict[str, str] = field(default_factory=lambda: {_digest("1"): GUARD_ID})
    guards: dict[str, tuple[Any, ...]] = field(
        default_factory=lambda: {GUARD_ID: (7, f"dbt-workspace:{ACTIVATION_ID}", ACTIVATION_ID, None, "HELD")}
    )
    attempts: dict[str, tuple[Any, ...]] = field(default_factory=dict)
    attempt_guards: dict[str, dict[str, int]] = field(default_factory=dict)


class _Cursor:
    def __init__(self, store: _Store) -> None:
        self.store = store
        self.one: tuple[Any, ...] | None = None
        self.many: list[tuple[Any, ...]] = []

    def execute(self, sql: str, *parameters: object):
        self.one, self.many = None, []
        normalized = " ".join(sql.split())
        if normalized.startswith("SET XACT_ABORT"):
            return self
        if normalized.startswith("SELECT LOWER(CONVERT(char(36), activation_id))"):
            self.one = self.store.attempts.get(str(parameters[0]))
            return self
        if normalized.startswith("SELECT state FROM [dpone_control].[dbt_workspace_activations]"):
            self.one = (self.store.activation_state,)
            return self
        if normalized.startswith("SELECT subject.write_subject_sha256"):
            activation_id, *subjects = map(str, parameters)
            if activation_id != ACTIVATION_ID:
                return self
            for subject in sorted(subjects):
                guard_id = self.store.subjects.get(subject)
                if guard_id is None:
                    continue
                epoch, owner, workflow_id, operation_id, status = self.store.guards[guard_id]
                self.many.append((subject, guard_id, epoch, owner, workflow_id, operation_id, status, epoch))
            return self
        if normalized.startswith("SELECT TOP (1) attempt.attempt_id"):
            *guard_ids, attempt_id = map(str, parameters)
            for other_id, attempt in self.store.attempts.items():
                if other_id == attempt_id or attempt[3] != "RUNNING":
                    continue
                if set(guard_ids).intersection(self.store.attempt_guards.get(other_id, {})):
                    self.one = (other_id,)
                    break
            return self
        if normalized.startswith("INSERT INTO [dpone_control].[dbt_workspace_attempts]"):
            attempt_id, activation_id, request_sha256, workflow_id = map(str, parameters)
            self.store.attempts[attempt_id] = (activation_id, request_sha256, workflow_id, "RUNNING", None)
            return self
        if normalized.startswith("INSERT INTO [dpone_control].[dbt_workspace_attempt_guards]"):
            attempt_id, guard_id, epoch = parameters
            self.store.attempt_guards.setdefault(str(attempt_id), {})[str(guard_id)] = int(epoch)
            return self
        if normalized.startswith("SELECT owned.guard_id, owned.fencing_epoch"):
            self.many = sorted(self.store.attempt_guards.get(str(parameters[0]), {}).items())
            return self
        if normalized.startswith("UPDATE [dpone_control].[dbt_workspace_attempts]"):
            state, receipt_sha256, attempt_id, request_sha256 = map(str, parameters)
            current = self.store.attempts.get(attempt_id)
            if current is not None and current[1] == request_sha256 and current[3:] == ("RUNNING", None):
                self.store.attempts[attempt_id] = (*current[:3], state, receipt_sha256)
                self.one = (state, receipt_sha256)
            return self
        raise AssertionError(normalized)

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.many

    def close(self) -> None:
        pass


class _Connection:
    def __init__(self, store: _Store, *, fail_commit: bool = False) -> None:
        self.store = store
        self.fail_commit = fail_commit
        self.autocommit = True

    def cursor(self) -> _Cursor:
        return _Cursor(self.store)

    def commit(self) -> None:
        if self.fail_commit:
            raise RuntimeError("ack lost")

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


class _Factory:
    def __init__(self, store: _Store, *, fail_first_commit: bool = False) -> None:
        self.store = store
        self.fail_first_commit = fail_first_commit
        self.calls = 0

    def __call__(self) -> _Connection:
        self.calls += 1
        return _Connection(self.store, fail_commit=self.fail_first_commit and self.calls == 1)


def test_attempt_admission_and_terminal_replay_preserve_activation_epoch() -> None:
    store, request = _Store(), _request()
    admission = MssqlDbtWorkspaceAttemptAdmission(_Factory(store))

    running = admission.admit(request)
    replay = admission.admit(request)
    succeeded = admission.terminalize(request, state="SUCCEEDED")
    terminal_replay = admission.terminalize(request, state="SUCCEEDED")

    assert running == replay
    assert succeeded == terminal_replay
    assert running.guard_epochs[0].fencing_epoch == 7
    assert succeeded.receipt_sha256 == store.attempts[request.attempt_id][4]


def test_overlapping_running_attempt_is_rejected_but_terminal_retry_is_admitted() -> None:
    store = _Store()
    admission = MssqlDbtWorkspaceAttemptAdmission(_Factory(store))
    first, retry = _request("a"), _request("b")

    admission.admit(first)
    with pytest.raises(DbtWorkspaceActivationError, match="attempt_guard_conflict"):
        admission.admit(retry)

    admission.terminalize(first, state="FAILED")
    assert admission.admit(retry).state == "RUNNING"


def test_inactive_activation_or_stale_epoch_fails_closed() -> None:
    store, request = _Store(activation_state="RETIRING"), _request()
    admission = MssqlDbtWorkspaceAttemptAdmission(_Factory(store))

    with pytest.raises(DbtWorkspaceActivationError, match="attempt_activation_not_active"):
        admission.admit(request)

    store.activation_state = "ACTIVE"
    store.guards[GUARD_ID] = (8, f"dbt-workspace:{ACTIVATION_ID}", ACTIVATION_ID, None, "HELD")
    # The ownership row remains at epoch 7 in production; model the joined mismatch explicitly.
    store.guards[GUARD_ID] = (7, "foreign", ACTIVATION_ID, None, "HELD")
    with pytest.raises(DbtWorkspaceActivationError, match="attempt_guard_stale"):
        admission.admit(request)


def test_attempt_commit_unknown_reconciles_from_fresh_connection() -> None:
    store, request = _Store(), _request()
    factory = _Factory(store, fail_first_commit=True)

    receipt = MssqlDbtWorkspaceAttemptAdmission(factory).admit(request)

    assert receipt.state == "RUNNING"
    assert factory.calls == 2


def test_attempt_request_fingerprint_is_closed_over_write_subset() -> None:
    request = _request()

    assert request.request_sha256 == canonical_fingerprint(
        {
            "schema": "dpone.dbt-workspace-attempt-request.v1",
            "activation_id": ACTIVATION_ID,
            "attempt_id": request.attempt_id,
            "workflow_id": "orders",
            "write_subjects": [_digest("1")],
        }
    )
