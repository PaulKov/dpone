"""Offline gateway DDL/SQL contracts; live transactional proof is separate."""

import json
from dataclasses import asdict, replace

import pytest

from dpone.contracts.dbt_workspace_channel import WorkspaceHandoverError
from dpone.contracts.dbt_workspace_lifecycle import workspace_request_payload
from dpone.contracts.dbt_workspace_registration import (
    WorkspaceRegistrationActor,
    WorkspaceRegistrationInventory,
    WorkspaceRegistrationInventoryItem,
    WorkspaceRegistrationReceipt,
)
from tests.test_dbt_workspace_handover_state_machine import _prepared
from tests.test_dbt_workspace_registration_contracts import _registration


def _empty_snapshot():
    receipt = WorkspaceRegistrationReceipt(
        _registration(), WorkspaceRegistrationActor("certification", "certification", 7), "2026-09-23T00:00:00.000000Z"
    )
    return {
        "channel_json": json.dumps(receipt.input.channel.to_dict()),
        "revision": 0,
        "current_activation_id": None,
        "pending_activation_id": None,
        "registration_json": json.dumps(receipt.to_dict()),
        "current_claim_json": None,
        "current_request_json": None,
        "current_completed": None,
        "pending_claim_json": None,
        "pending_request_json": None,
        "pending_completed": None,
        "current_lifecycle": None,
        "pending_lifecycle": None,
    }


def _lifecycle_json(occurrence):
    value = asdict(occurrence.lifecycle.identity)
    del value["write_subjects"]
    value.update(request_sha256=occurrence.lifecycle.request_sha256, state=occurrence.lifecycle.state)
    value["guards"] = [
        {
            **asdict(guard),
            "live_epoch": guard.fencing_epoch,
            "owner_id": f"dbt-workspace:{occurrence.request.activation_id}",
            "workflow_id": occurrence.request.activation_id,
            "operation_id": None,
            "status": "HELD",
            "write_subjects": [{"write_subject_sha256": subject} for subject in guard.write_subjects],
        }
        for guard in occurrence.lifecycle.guards
    ]
    return value


def _prepared_snapshot():
    readback = _prepared()
    current = readback.current
    registration = replace(
        _registration(),
        channel=readback.channel,
        mode="adopt_active",
        empty_attestation=None,
        adopted_current=current.snapshot,
        inventory=WorkspaceRegistrationInventory(
            (
                WorkspaceRegistrationInventoryItem(
                    current.request.activation_id, current.request.request_sha256, "ACTIVE", None
                ),
            )
        ),
    )
    receipt = WorkspaceRegistrationReceipt(
        registration, WorkspaceRegistrationActor("certification", "certification", 7), "2026-09-23T00:00:00.000000Z"
    )
    return readback, {
        **_empty_snapshot(),
        "channel_json": json.dumps(readback.channel.to_dict()),
        "revision": readback.revision,
        "current_activation_id": current.request.activation_id,
        "pending_activation_id": readback.pending.successor_activation_id,
        "registration_json": json.dumps(receipt.to_dict()),
        "pending_claim_json": json.dumps(readback.pending.to_dict()),
        "pending_request_json": json.dumps(workspace_request_payload(readback.pending_occurrence.request)),
        "pending_completed": False,
        "current_lifecycle": _lifecycle_json(current),
        "pending_lifecycle": _lifecycle_json(readback.pending_occurrence),
    }


class _GatewayConnection:
    def __init__(self, factory):
        self.factory = factory
        self.autocommit = False
        self.returned = False

    def cursor(self):
        return self

    def execute(self, sql, *parameters):
        assert self.autocommit is True
        self.factory.calls.append((sql, parameters))
        if isinstance(self.factory.failure, Exception):
            raise self.factory.failure
        if self.factory.failure:
            raise OSError("injected failure; do not expose connection diagnostics")
        return self

    def fetchone(self):
        if self.returned:
            return None
        self.returned = True
        return (json.dumps(self.factory.snapshot),)

    def close(self):
        pass


class _GatewayFactory:
    def __init__(self):
        self.snapshot = _empty_snapshot()
        self.calls = []
        self.failure = False
        self.connections = []

    def __call__(self):
        connection = _GatewayConnection(self)
        self.connections.append(connection)
        return connection


def test_adapter_reads_registered_empty_channel_only_through_fixed_gateway():
    from dpone.adapters.dbt_workspace_mssql_handover import MssqlWorkspaceHandoverStore

    factory = _GatewayFactory()
    channel = _registration().channel
    result = MssqlWorkspaceHandoverStore(factory).read_channel(channel)
    assert result.channel == channel and result.revision == 0 and result.current is None and result.pending is None
    assert factory.calls[0][0] == "EXEC [dpone_control].[workspace_channel_read] ?;"
    assert json.loads(factory.calls[0][1][0]) == channel.to_dict()


@pytest.mark.parametrize("mutation", ["extra", "foreign", "missing_registration", "orphan_current", "bool_revision"])
def test_adapter_rejects_partial_or_foreign_snapshot(mutation):
    from dpone.adapters.dbt_workspace_mssql_handover import MssqlWorkspaceHandoverStore

    factory = _GatewayFactory()
    if mutation == "extra":
        factory.snapshot["unrecognized"] = "value"
    elif mutation == "foreign":
        factory.snapshot["channel_json"] = json.dumps(replace(_registration().channel, source_ref="other").to_dict())
    elif mutation == "missing_registration":
        factory.snapshot["registration_json"] = None
    elif mutation == "orphan_current":
        factory.snapshot["current_activation_id"] = "123e4567-e89b-42d3-a456-426614174000"
    else:
        factory.snapshot["revision"] = True
    with pytest.raises(WorkspaceHandoverError):
        MssqlWorkspaceHandoverStore(factory).read_channel(_registration().channel)


def test_read_failure_is_content_free_and_does_not_run_mutation():
    from dpone.adapters.dbt_workspace_mssql_handover import MssqlWorkspaceHandoverStore

    factory = _GatewayFactory()
    factory.failure = True
    with pytest.raises(WorkspaceHandoverError) as caught:
        MssqlWorkspaceHandoverStore(factory).read_channel(_registration().channel)
    assert "injected" not in str(caught.value)
    assert len(factory.calls) == 1


@pytest.mark.parametrize(
    ("native", "code"),
    [(51005, "DPONE_WORKSPACE_CHANNEL_UNREGISTERED"), (51004, "DPONE_WORKSPACE_CHANNEL_CAS_CONFLICT")],
)
def test_adapter_preserves_safe_gateway_error_code_without_driver_details(native, code):
    from dpone.adapters.dbt_workspace_mssql_handover import MssqlWorkspaceHandoverStore

    factory = _GatewayFactory()
    factory.failure = OSError("42000", f"private diagnostics ({native}) (SQLExecDirectW)")
    with pytest.raises(WorkspaceHandoverError) as caught:
        MssqlWorkspaceHandoverStore(factory).read_channel(_registration().channel)
    assert caught.value.code == code
    assert "private" not in str(caught.value)


def test_cold_readback_uses_exact_request_and_historical_epochs_after_physical_reownership():
    from dpone.adapters.dbt_workspace_mssql_handover import MssqlWorkspaceHandoverStore

    expected, snapshot = _prepared_snapshot()
    # The old occurrence is RETIRED, so current physical ownership legitimately
    # belongs to the successor; its original history must remain unchanged.
    snapshot["current_lifecycle"]["guards"][0].update(
        live_epoch=2,
        owner_id=f"dbt-workspace:{expected.pending.successor_activation_id}",
        workflow_id=expected.pending.successor_activation_id,
    )
    factory = _GatewayFactory()
    factory.snapshot = snapshot
    assert MssqlWorkspaceHandoverStore(factory).read_channel(expected.channel) == expected


@pytest.mark.parametrize(
    "mutation", ["epoch", "foreign_owner", "request", "missing_guard", "resource", "write_subject", "extra_coordinate"]
)
def test_adapter_requires_complete_original_and_live_ownership(mutation):
    from dpone.adapters.dbt_workspace_mssql_handover import MssqlWorkspaceHandoverStore

    expected, snapshot = _prepared_snapshot()
    pending = snapshot["pending_lifecycle"]
    if mutation == "epoch":
        pending["guards"][0]["live_epoch"] += 1
    elif mutation == "foreign_owner":
        pending["guards"][0]["owner_id"] = "foreign"
    elif mutation == "request":
        snapshot["pending_request_json"] = snapshot["registration_json"]
    elif mutation == "missing_guard":
        pending["guards"] = []
    elif mutation == "resource":
        pending["guards"][0]["resource_sha256"] = "sha256:" + "f" * 64
    elif mutation == "write_subject":
        pending["guards"][0]["write_subjects"] = []
    else:
        pending["extra"] = "unexpected"
    factory = _GatewayFactory()
    factory.snapshot = snapshot
    with pytest.raises(WorkspaceHandoverError):
        MssqlWorkspaceHandoverStore(factory).read_channel(expected.channel)


def test_channel_read_uses_protected_barrier_and_exact_document_before_snapshot():
    from dpone.adapters.dbt_workspace_mssql_channel_procedures import render_workspace_channel_read

    sql = render_workspace_channel_read("dpone_control")
    assert sql.startswith("CREATE OR ALTER PROCEDURE [dpone_control].[workspace_channel_read]")
    assert sql.index("dpone:workspace-inventory:") < sql.index("dpone:workspace-channel:")
    assert sql.index("dpone:workspace-channel:") < sql.index("FROM [dpone_control].[dbt_workspace_channels]")
    assert "@LockOwner = N'Transaction'" in sql and "@LockTimeout = 0" in sql
    assert "@LockMode = N'Shared'" in sql
    assert "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE" in sql
    assert "SET NUMERIC_ROUNDABORT OFF" in sql and "SET ARITHABORT ON" in sql
    assert "SET LOCK_TIMEOUT 0" in sql
    assert "CONVERT(varbinary(max), channel_json)" in sql
    assert "request_json" in sql and "registration_json" in sql
    assert "UPDATE " not in sql and "DELETE " not in sql
    assert "@entry_trancount <> 0" in sql


def test_closed_document_fragment_checks_fields_types_schema_and_exact_digest():
    from dpone.adapters.dbt_workspace_mssql_gateway_validation import workspace_document_validation

    sql = workspace_document_validation(
        "dpone_control",
        variable="document",
        fields={"schema": (1,), "value": (0, 1), "document_sha256": (1,)},
        schema_name="dpone.example.v1",
        digest_field="document_sha256",
        maximum_bytes=1024,
    )
    assert "OPENJSON(@document)" in sql
    assert "FULL JOIN" in sql and "actual.type NOT IN" not in sql
    assert "workspace_json_canonical" in sql and "workspace_json_utf8_sha256" in sql
    assert "DATALENGTH(@document) > 2048" in sql
    assert "JSON_VALUE(@document, N'$.schema') IS NULL" in sql
    assert "CONVERT(varbinary(max), @document_claimed_digest)" in sql
    assert "JSON_MODIFY(@document, N'$.document_sha256', NULL)" in sql


@pytest.mark.parametrize(
    "change",
    [
        {"variable": "x;--"},
        {"schema_name": "x'"},
        {"digest_field": "missing"},
        {"fields": {"bad-name": (1,)}},
        {"fields": {"schema": (99,)}},
        {"maximum_bytes": True},
    ],
)
def test_closed_document_fragment_has_no_sql_interpolation_escape(change):
    from dpone.adapters.dbt_workspace_mssql_gateway_validation import workspace_document_validation

    arguments = dict(
        variable="document",
        fields={"schema": (1,), "document_sha256": (1,)},
        schema_name="dpone.example.v1",
        digest_field="document_sha256",
        maximum_bytes=1024,
    )
    arguments.update(change)
    with pytest.raises(ValueError):
        workspace_document_validation("dpone_control", **arguments)


def test_handover_schema_bounds_and_unique_pending_are_explicit():
    from dpone.adapters.dbt_workspace_mssql_handover_schema import render_workspace_handover_schema

    sql = render_workspace_handover_schema("dpone_control")
    for table in ("dbt_workspace_channels", "dbt_workspace_handover_claims"):
        assert f"CREATE TABLE [dpone_control].[{table}]" in sql
    assert "WHERE completed = 0" in sql
    assert "UNIQUE (activation_id)" in sql
    assert "UNIQUE (channel_sha256, claim_revision)" in sql
    assert "registration_json nvarchar(max) NOT NULL" in sql
    assert "request_sha256 IS NULL AND request_json IS NULL" in sql
    assert "DATALENGTH(claim_json) <= 524288" in sql
    assert "DATALENGTH(request_json) <= 33554432" in sql
    assert "COLLATE Latin1_General_100_BIN2" in sql
    assert "DROP " not in sql and "GRANT " not in sql
    assert "SET ANSI_NULLS ON" in sql and "SET QUOTED_IDENTIFIER ON" in sql


@pytest.mark.parametrize("schema", ["", "a.b", "x]", "x" * 129])
def test_handover_schema_rejects_unsafe_identifiers(schema):
    from dpone.adapters.dbt_workspace_mssql_handover_schema import render_workspace_handover_schema

    with pytest.raises(ValueError):
        render_workspace_handover_schema(schema)
