"""Fixture composition checks with doubles; no live ownership qualification."""

from types import SimpleNamespace

import pytest

from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceGuardEpoch
from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptReceipt
from tests.support import dbt_mssql_physical_source_authority as source
from tests.support.dbt_mssql_physical_registration import registration_inputs


def setup_owner(monkeypatch):
    authority = source.FixtureAuthority()
    registration = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    authority.authority = registration.platform_subject.authority
    events = []
    activation = []

    def migration(name):
        return lambda *args, **kwargs: SimpleNamespace(apply=lambda: events.append(name))

    def activation_admission(*args, **kwargs):
        def prepare(request):
            events.append("prepare")
            activation.append(request)

        def activate(request):
            assert request is activation[0]
            events.append("activate")

        return SimpleNamespace(prepare=prepare, activate=activate)

    def attempt_admission(*args, **kwargs):
        def admit(request):
            events.append("attempt")
            assert request.activation_id == activation[0].activation_id
            return DbtWorkspaceAttemptReceipt.build(
                request=request,
                state="RUNNING",
                guard_epochs=(DbtWorkspaceGuardEpoch(activation[0].resources[0].guard_id, 17),),
            )

        return SimpleNamespace(admit=admit)

    def forbidden(*args, **kwargs):
        pytest.fail("P-only fixture entered the generation path")

    monkeypatch.setattr(source, "MssqlNativeOriginalSchemaMigration", migration("originals"))
    monkeypatch.setattr(source, "MssqlSemanticRefreshSchemaMigration", migration("workspace"))
    monkeypatch.setattr(source, "MssqlDbtWorkspaceActivationAdmission", activation_admission)
    monkeypatch.setattr(source, "MssqlDbtWorkspaceAttemptAdmission", attempt_admission)
    for name in ("MssqlNativeGenerationSchemaMigration", "MssqlNativeOriginalBindings", "MssqlNativeGenerationControl"):
        monkeypatch.setattr(source, name, forbidden)
    return authority, registration, events


def test_fixture_can_stop_after_exact_workspace_owner_without_generation(monkeypatch):
    authority, registration, events = setup_owner(monkeypatch)
    owner = authority.admit_physical_owner(registration, object(), "metadata")
    assert events == ["originals", "workspace", "prepare", "activate", "attempt"]
    assert owner.attempt.write_subjects == owner.resource.write_subjects
    assert owner.receipt.request_sha256 == owner.attempt.request_sha256
    assert owner.receipt.guard_epochs == (DbtWorkspaceGuardEpoch(owner.resource.guard_id, 17),)
    assert owner.command == authority.get("command")
    assert "reservation" not in authority.references


def test_existing_generation_path_reuses_completed_workspace_owner_first(monkeypatch):
    authority, registration, events = setup_owner(monkeypatch)

    class AtGenerationBoundary(Exception):
        pass

    def generation_migration(**kwargs):
        assert events == ["originals", "workspace", "prepare", "activate", "attempt"]
        assert kwargs["control_authority"] == registration.control_authority
        assert kwargs["trusted_profile"] == registration.trusted_profile.reference
        events.append("generation")
        raise AtGenerationBoundary

    monkeypatch.setattr(source, "MssqlNativeGenerationSchemaMigration", generation_migration)
    with pytest.raises(AtGenerationBoundary):
        authority.admit(registration, object(), object(), "metadata")
    assert events[-1] == "generation"


def test_archive_backed_p_only_admission_preserves_actual_activation_identity(tmp_path, monkeypatch):
    from tests.support.dbt_mssql_physical_catalog_v2_live import ArchiveAuthority

    base, registration, events = setup_owner(monkeypatch)
    authority = ArchiveAuthority(tmp_path, monkeypatch, 1)
    authority.authority = base.authority
    expected_activation = "10000000-0000-0000-0000-000000000009"
    expected_inventory = "sha256:" + "e" * 64
    authority.archive = SimpleNamespace(identity=SimpleNamespace(activation_id=expected_activation))
    authority.sources = SimpleNamespace(inventory=SimpleNamespace(snapshot_sha256=expected_inventory))
    original = source.DbtWorkspaceActivationRequest
    owner = authority.admit_physical_owner(registration, object(), "metadata")
    assert owner.attempt.activation_id == expected_activation
    assert authority.activation.source_inventory_sha256 == expected_inventory
    assert authority.activation.environment == authority.authority.environment
    assert source.DbtWorkspaceActivationRequest is original
    assert events[-1] == "attempt"
    assert "reservation" not in authority.references


def test_source_fixture_default_admission_hook_preserves_existing_results(monkeypatch):
    from tests.support.dbt_mssql_physical_source_fixture import SourceFixture

    fixture = SourceFixture(object())
    fixture.registration = object()
    calls = []
    expected = tuple(object() for _ in range(4))

    def admit(registration, admin, runtime, metadata_name):
        assert registration is fixture.registration
        assert callable(admin) and callable(runtime)
        assert metadata_name == fixture.credentials["metadata"][0]
        calls.append("admit")
        return expected

    monkeypatch.setattr(fixture, "authority", SimpleNamespace(admit=admit))
    fixture._admit()
    assert calls == ["admit"]
    assert (fixture.request, fixture.executor, fixture.provider, fixture.reserved) == expected
