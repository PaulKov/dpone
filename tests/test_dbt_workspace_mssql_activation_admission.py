"""Durable workspace admission owns exact epochs in the shared guard table."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from dpone.adapters.dbt_workspace_mssql_activation_admission import MssqlDbtWorkspaceActivationAdmission
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.dbt_workspace_activation import (
    DbtWorkspaceActivationError,
    DbtWorkspaceActivationRequest,
    DbtWorkspacePhysicalResource,
)

ACTIVATION_ID = "164a3c74-cf85-4a4a-a087-07c9b07050ff"


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _request() -> DbtWorkspaceActivationRequest:
    resource = DbtWorkspacePhysicalResource(
        guard_id="mssql://service/warehouse/mart/orders",
        connector="mssql",
        service_authority_sha256=_digest("1"),
        target_authority_sha256=_digest("2"),
        observation_sha256=_digest("3"),
        write_subjects=(_digest("4"),),
    )
    return DbtWorkspaceActivationRequest.build(
        activation_id=ACTIVATION_ID,
        environment="prod",
        release_id=_digest("a"),
        deployment_id=_digest("b"),
        previous_deployment_id=None,
        source_inventory_sha256=_digest("c"),
        runtime_context_sha256=_digest("d"),
        write_subjects=resource.write_subjects,
        resources=(resource,),
    )


@dataclass
class _Store:
    activations: dict[str, tuple[Any, ...]] = field(default_factory=dict)
    guards: dict[str, tuple[Any, ...]] = field(default_factory=dict)
    ownership: dict[tuple[str, str], tuple[str, int]] = field(default_factory=dict)
    subjects: dict[tuple[str, str], str] = field(default_factory=dict)
    attempts: dict[str, tuple[str, str | None]] = field(default_factory=dict)


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
        if "sys.sp_getapplock" in normalized:
            self.one = (0,)
            return self
        if normalized.startswith("SELECT request_sha256"):
            self.one = self.store.activations.get(str(parameters[0]))
            return self
        if normalized.startswith("INSERT INTO [dpone_control].[dbt_workspace_activations]"):
            activation_id = str(parameters[0])
            self.store.activations[activation_id] = (*parameters[1:], "PREPARED")
            return self
        if normalized.startswith("SELECT fencing_epoch"):
            self.one = self.store.guards.get(str(parameters[0]))
            return self
        if normalized.startswith("INSERT INTO [dpone_control].[semantic_refresh_guards]"):
            guard_id, epoch, owner, workflow_id = parameters
            self.store.guards[str(guard_id)] = (epoch, owner, workflow_id, None, "HELD")
            return self
        if (
            normalized.startswith("UPDATE [dpone_control].[semantic_refresh_guards]")
            and "SET owner_id = NULL" in normalized
        ):
            guard_id, epoch, owner, workflow_id = parameters
            existing = self.store.guards.get(str(guard_id))
            if existing == (epoch, owner, workflow_id, None, "HELD"):
                self.store.guards[str(guard_id)] = (epoch, None, None, None, "AVAILABLE")
                self.one = (guard_id,)
            return self
        if normalized.startswith("UPDATE [dpone_control].[semantic_refresh_guards]"):
            next_epoch, owner, workflow_id, guard_id, previous_epoch = parameters
            existing = self.store.guards.get(str(guard_id))
            if existing == (previous_epoch, None, None, None, "AVAILABLE"):
                self.store.guards[str(guard_id)] = (next_epoch, owner, workflow_id, None, "HELD")
                self.one = (guard_id,)
            return self
        if normalized.startswith("INSERT INTO [dpone_control].[dbt_workspace_activation_guards]"):
            activation_id, guard_id, resource_sha256, epoch = parameters
            self.store.ownership[(str(activation_id), str(guard_id))] = (str(resource_sha256), int(epoch))
            return self
        if normalized.startswith("INSERT INTO [dpone_control].[dbt_workspace_activation_write_subjects]"):
            activation_id, write_subject, guard_id = map(str, parameters)
            self.store.subjects[(activation_id, write_subject)] = guard_id
            return self
        if normalized.startswith("SELECT ownership.guard_id"):
            activation_id = str(parameters[0])
            for (owner_activation, guard_id), (resource_sha256, epoch) in sorted(self.store.ownership.items()):
                if owner_activation != activation_id:
                    continue
                guard = self.store.guards[guard_id]
                self.many.append((guard_id, resource_sha256, epoch, *guard[1:], guard[0]))
            return self
        if normalized.startswith("SELECT TOP (1) attempt_id, state, terminal_receipt_sha256"):
            for attempt_id, (state, receipt) in self.store.attempts.items():
                if state in {"RUNNING", "COMMIT_UNKNOWN"} or receipt is None:
                    self.one = (attempt_id, state, receipt)
                    break
            return self
        if normalized.startswith("SELECT guard_id, fencing_epoch"):
            activation_id = str(parameters[0])
            self.many = sorted(
                (guard_id, epoch)
                for (owner_activation, guard_id), (_resource, epoch) in self.store.ownership.items()
                if owner_activation == activation_id
            )
            return self
        if normalized.startswith("UPDATE [dpone_control].[dbt_workspace_activations]"):
            activation_id, request_sha256 = map(str, parameters)
            existing = self.store.activations.get(activation_id)
            transitions = {"N'PREPARED'": "ACTIVE", "N'ACTIVE'": "RETIRING", "N'RETIRING'": "RETIRED"}
            previous = normalized.rpartition("state = ")[2].partition(";")[0]
            next_state = transitions.get(previous)
            if existing is not None and next_state is not None and existing[0] == request_sha256:
                expected_previous = {"ACTIVE": "PREPARED", "RETIRING": "ACTIVE", "RETIRED": "RETIRING"}[next_state]
                if existing[-1] == expected_previous:
                    self.store.activations[activation_id] = (*existing[:-1], next_state)
                    self.one = (next_state,)
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
        self.autocommit = True
        self.fail_commit = fail_commit
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _Cursor:
        return _Cursor(self.store)

    def commit(self) -> None:
        self.commits += 1
        if self.fail_commit:
            raise RuntimeError("ack lost")

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        pass


class _Factory:
    def __init__(self, store: _Store, *, fail_first_commit: bool = False) -> None:
        self.store = store
        self.fail_first_commit = fail_first_commit
        self.connections: list[_Connection] = []

    def __call__(self) -> _Connection:
        connection = _Connection(self.store, fail_commit=self.fail_first_commit and not self.connections)
        self.connections.append(connection)
        return connection


def test_prepare_activate_and_active_readback_preserve_exact_shared_epoch() -> None:
    store, request = _Store(), _request()
    admission = MssqlDbtWorkspaceActivationAdmission(_Factory(store))

    prepared = admission.prepare(request)
    replay = admission.prepare(request)
    active = admission.activate(request)
    readback = admission.require_active(request)

    assert prepared == replay
    assert active == readback
    assert prepared.guard_epochs[0].fencing_epoch == 1
    assert store.guards[request.resources[0].guard_id] == (
        1,
        f"dbt-workspace:{ACTIVATION_ID}",
        ACTIVATION_ID,
        None,
        "HELD",
    )
    assert store.subjects[(request.activation_id, request.write_subjects[0])] == request.resources[0].guard_id


def test_existing_available_guard_advances_one_epoch_under_shared_lock() -> None:
    store, request = _Store(), _request()
    guard_id = request.resources[0].guard_id
    store.guards[guard_id] = (7, None, None, None, "AVAILABLE")

    receipt = MssqlDbtWorkspaceActivationAdmission(_Factory(store)).prepare(request)

    assert receipt.guard_epochs[0].fencing_epoch == 8


def test_semantic_refresh_owned_guard_blocks_workspace_reservation() -> None:
    store, request = _Store(), _request()
    store.guards[request.resources[0].guard_id] = (7, "semantic-owner", "workflow", "operation", "HELD")

    with pytest.raises(DbtWorkspaceActivationError, match="guard_conflict"):
        MssqlDbtWorkspaceActivationAdmission(_Factory(store)).prepare(request)


def test_commit_unknown_is_reconciled_on_a_fresh_connection() -> None:
    store, request = _Store(), _request()
    factory = _Factory(store, fail_first_commit=True)

    receipt = MssqlDbtWorkspaceActivationAdmission(factory).prepare(request)

    assert receipt.state == "PREPARED"
    assert len(factory.connections) == 2


def test_stale_resource_readback_fails_closed() -> None:
    store, request = _Store(), _request()
    admission = MssqlDbtWorkspaceActivationAdmission(_Factory(store))
    admission.prepare(request)
    key = (request.activation_id, request.resources[0].guard_id)
    store.ownership[key] = (canonical_fingerprint({"foreign": True}), 1)

    with pytest.raises(DbtWorkspaceActivationError, match="guard_readback"):
        admission.activate(request)


def test_retirement_closes_admission_then_releases_only_after_terminal_quiescence() -> None:
    store, request = _Store(), _request()
    admission = MssqlDbtWorkspaceActivationAdmission(_Factory(store))
    admission.prepare(request)
    admission.activate(request)
    store.attempts["attempt"] = ("RUNNING", None)

    retiring = admission.begin_retirement(request)
    with pytest.raises(DbtWorkspaceActivationError, match="terminal_quiescence_unavailable"):
        admission.finalize_retirement(request)

    store.attempts["attempt"] = ("SUCCEEDED", _digest("8"))
    retired = admission.finalize_retirement(request)
    replay = admission.finalize_retirement(request)

    assert retiring.state == "RETIRING"
    assert retired == replay
    assert store.guards[request.resources[0].guard_id] == (1, None, None, None, "AVAILABLE")
