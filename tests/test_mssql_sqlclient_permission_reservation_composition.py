"""Exact request projection and pre-effect rejection for GRANT composition."""

import hashlib
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dpone.adapters.mssql_tds_coordinator_connection import (
    TdsBinaryPin,
    TdsCoordinatorBuild,
    encode_connection_admission,
)
from dpone.app import mssql_sqlclient_permission_reservation_composition as module
from dpone.contracts.bounded_window import WindowLease
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial, TdsConnectionProfile
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits
from dpone.services.mssql_tds_permission_grant import PermissionGrantHeldUnknown
from tests.test_mssql_sqlclient_observe_departure_composition import prepared as prepared
from tests.test_mssql_sqlclient_preparation_composition import composed_preparation as composed_preparation
from tests.test_mssql_tds_permission_grant_association import _settled


def _inputs(prepared, tmp_path, supplier):
    origin = prepared.attempt._prepared_origin
    pin = TdsBinaryPin(Path(sys.executable), "b" * 64)
    admission = encode_connection_admission(
        TdsCoordinatorBuild(pin, pin, pin, pin), TdsConnectionProfile.SYNTHETIC_LOCAL
    )
    launcher = SimpleNamespace(
        admission=admission,
        implementation_sha256=origin.identity.implementation_sha256,
        launch=lambda value: None,
    )
    return module.SqlClientPermissionReservationInputs(
        pool=prepared.pool,
        store_factory=lambda: object(),
        limits=TdsDirectoryLimits(10, 1, 1_000_000, 100),
        lease=WindowLease("target", "owner", 1),
        launcher=launcher,
        admission=admission,
        evidence_root=tmp_path,
        supervisor_token="supervisor",
        operation_id=uuid4(),
        profile=TdsConnectionProfile.SYNTHETIC_LOCAL,
        admission_sha256=hashlib.sha256(admission).hexdigest(),
        startup_deadline=origin.deadline - 1.0,
        operation_deadline=origin.deadline,
        termination_timeout=1.0,
        session_nonce=b"n" * 32,
        material_supplier=supplier,
    )


def test_projects_original_request_and_reserves_once(prepared, tmp_path, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    origin = attempt._prepared_origin
    material = TdsConnectionMaterial(
        "localhost",
        1433,
        origin.handle.request.selected_stage.database_name,
        origin.handle.request.writer_admission.login.name,
        "secret",
    )
    calls = []
    inputs = _inputs(prepared, tmp_path, lambda: calls.append("supplier") or material)
    captured = {}

    def hold(*args, **kwargs):
        captured["association"] = args[2]
        captured["launch"] = args[6]
        calls.append("hold")
        return "held"

    monkeypatch.setattr(module, "hold_mssql_sqlclient_permission", hold)
    association, held = module.reserve_and_hold_mssql_sqlclient_permission(attempt, inputs)
    request = captured["launch"].request
    inventory = origin.opening.inventory
    assert held == "held" and captured["association"] is association
    assert calls == ["supplier", "hold"]
    assert request.parent == origin.expected.state.identity
    assert request.stage is origin.handle.request.selected_stage
    assert request.writer.principal_id == inventory.writer.principal_id
    assert request.management is inventory.management_before.authority.principal_resolution
    assert request.preparation_sha256 == origin.evidence_receipt.payload_sha256
    assert attempt._permission_grant_owner is association


def test_invalid_material_rejects_before_grant_owner(prepared, tmp_path, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    calls = []
    inputs = _inputs(prepared, tmp_path, lambda: calls.append("supplier") or object())
    monkeypatch.setattr(
        module,
        "hold_mssql_sqlclient_permission",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("hold entered")),
    )
    with pytest.raises(ValueError, match="permission_reservation_composition_invalid"):
        module.reserve_and_hold_mssql_sqlclient_permission(attempt, inputs)
    assert calls == ["supplier"]
    assert attempt._permission_grant_owner is None


def test_replay_rejects_before_second_supplier(prepared, tmp_path, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    origin = attempt._prepared_origin
    material = TdsConnectionMaterial(
        "localhost",
        1433,
        origin.handle.request.selected_stage.database_name,
        origin.handle.request.writer_admission.login.name,
        "secret",
    )
    first = _inputs(prepared, tmp_path, lambda: material)
    monkeypatch.setattr(module, "hold_mssql_sqlclient_permission", lambda *args, **kwargs: object())
    module.reserve_and_hold_mssql_sqlclient_permission(attempt, first)
    calls = []
    replay = _inputs(prepared, tmp_path, lambda: calls.append("supplier") or material)
    with pytest.raises(ValueError, match="permission_reservation_composition_invalid"):
        module.reserve_and_hold_mssql_sqlclient_permission(attempt, replay)
    assert calls == []


def test_post_reservation_pre_held_failure_poisoned_without_retry(prepared, tmp_path, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    origin = attempt._prepared_origin
    material = TdsConnectionMaterial(
        "localhost",
        1433,
        origin.handle.request.selected_stage.database_name,
        origin.handle.request.writer_admission.login.name,
        "secret",
    )
    calls = []
    inputs = _inputs(prepared, tmp_path, lambda: calls.append("supplier") or material)
    failure = RuntimeError("coordinator allocation failed")
    monkeypatch.setattr(
        module,
        "hold_mssql_sqlclient_permission",
        lambda *args, **kwargs: (_ for _ in ()).throw(failure),
    )
    with pytest.raises(Exception) as caught:
        module.reserve_and_hold_mssql_sqlclient_permission(attempt, inputs)
    assert caught.value is not failure
    assert calls == ["supplier"]
    assert attempt._poisoned
    assert attempt._permission_grant_owner._phase == "unknown"


def test_held_unknown_preserves_exact_phase_custody(prepared, tmp_path, monkeypatch):
    attempt = _settled(prepared, monkeypatch)
    origin = attempt._prepared_origin
    material = TdsConnectionMaterial(
        "localhost",
        1433,
        origin.handle.request.selected_stage.database_name,
        origin.handle.request.writer_admission.login.name,
        "secret",
    )
    inputs = _inputs(prepared, tmp_path, lambda: material)
    owner = object()
    failure = PermissionGrantHeldUnknown(owner)  # type: ignore[arg-type]
    monkeypatch.setattr(
        module,
        "hold_mssql_sqlclient_permission",
        lambda *args, **kwargs: (_ for _ in ()).throw(failure),
    )
    with pytest.raises(PermissionGrantHeldUnknown) as caught:
        module.reserve_and_hold_mssql_sqlclient_permission(attempt, inputs)
    assert caught.value is failure and caught.value.owner is owner
    assert attempt._permission_grant_owner._phase == "ready"


def test_record_and_custody_repr_do_not_expose_material(prepared, tmp_path):
    material = TdsConnectionMaterial("localhost", 1433, "db", "writer", "private-canary")
    inputs = _inputs(prepared, tmp_path, lambda: material)
    custody = module._GrantMaterialCustody(material)
    assert "private-canary" not in repr(inputs)
    assert repr(custody) == "_GrantMaterialCustody(<opaque>)"
    assert custody.take() is material
    with pytest.raises(ValueError):
        custody.take()
