"""Pure prerequisite checks and opt-in real-role SQL enrollment qualification."""

import pytest


def test_retained_original_rejects_wrong_version_and_bound():
    from dataclasses import replace

    from dpone.ports.semantic_refresh_artifact_store import ArtifactObjectRef
    from tests.support.dbt_mssql_physical_enrollment import components
    from tests.support.dbt_mssql_physical_enrollment_live import RetainedOriginals

    subject, reference, payload, _, _ = components()
    original = ArtifactObjectRef(
        reference.locator,
        "fixture-owner-v1",
        len(payload),
        reference.sha256,
        "fixture-memory-only",
        "2099-01-01T00:00:00Z",
    )
    store = RetainedOriginals()
    store.retain(original, "mssql_physical_plan_set_v1", subject, payload)
    args = dict(expected_kind="mssql_physical_plan_set_v1", expected_subject=subject, max_bytes=len(payload))
    assert store.read(original, **args) == payload
    with pytest.raises(ValueError):
        store.read(replace(original, version="other"), **args)
    with pytest.raises(ValueError):
        store.read(original, **(args | {"max_bytes": len(payload) - 1}))


def test_synthetic_producer_retains_exact_vars_and_complete_plan():
    from types import SimpleNamespace
    from uuid import UUID

    from dpone.contracts.dbt_mssql_physical_enrollment import PhysicalPlanEnrollment
    from dpone.contracts.dbt_mssql_physical_invocation import PhysicalManagedInvocation, require_managed_command
    from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
    from dpone.contracts.dbt_mssql_physical_wire import decode_physical_plan_set
    from dpone.contracts.native_delivery_json import decode_native_delivery_json
    from tests.support.dbt_mssql_physical_enrollment import components
    from tests.support.dbt_mssql_physical_enrollment_originals_live import plan_and_command
    from tests.support.dbt_mssql_physical_registration import registration_inputs
    from tests.support.dbt_mssql_physical_source_authority import FixtureAuthority

    subject, old_ref, plan_bytes, writer, command_bytes = components()
    value = PhysicalPlanEnrollment(
        subject,
        decode_native_delivery_json(plan_bytes)["runtime_registration_id"],
        old_ref.sha256,
        old_ref.sha256,
        old_ref,
        plan_bytes,
        writer,
        command_bytes,
    )
    owner = SimpleNamespace(
        attempt=value.plan.workspace_attempt, receipt=SimpleNamespace(guard_epochs=(value.plan.guard,))
    )
    authority = FixtureAuthority()
    graph = authority.retain("graph", {"provenance": "SYNTHETIC"})
    registration = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    plan_ref, command_ref = plan_and_command(
        registration, owner, UUID(value.plan.generation_id), writer.invocation_id, graph, authority.retain
    )
    observed = decode_physical_plan_set(authority.payloads[plan_ref.locator])
    assert observed.workspace_attempt == owner.attempt
    assert observed.models[0].spec.source_graph_sha256 == graph.sha256
    require_managed_command(
        authority.payloads[command_ref.locator],
        expected=PhysicalManagedInvocation(
            observed.generation_id, str(writer.invocation_id), plan_ref, registration.registration_id
        ),
    )


@pytest.fixture(params=("same_database", "two_database"))
def enrollment(tmp_path, monkeypatch, request, record_property):
    import importlib
    import json
    import os

    from tests.support.dbt_mssql_physical_catalog_v2_live import selected_profile
    from tests.support.dbt_mssql_physical_discovery_live import DiscoverySourceFixture
    from tests.support.dbt_mssql_physical_enrollment_live import EnrollmentFixture
    from tests.support.dbt_mssql_physical_enrollment_originals_live import EnrollmentArchiveAuthority

    if os.environ.get("DPONE_RUN_PHYSICAL_ENROLLMENT_LIVE") != "1":
        pytest.skip("isolated enrollment SQL qualification disabled")
    for name in ("DPONE_NATIVE_SQL_TEST_HOST", "DPONE_NATIVE_SQL_TEST_PASSWORD"):
        if not os.environ.get(name):
            pytest.skip("isolated SQL fixture input unavailable: " + name)
    source = DiscoverySourceFixture(importlib.import_module("pyodbc"), selected_profile(), layout=request.param)
    source.authority = EnrollmentArchiveAuthority(tmp_path / "archive", monkeypatch, 1)
    fixture = EnrollmentFixture(source)
    try:
        source.install()
        fixture.install()
        record_property("enrollment_environment", json.dumps(fixture.evidence(), sort_keys=True))
        yield fixture
    finally:
        try:
            fixture.cleanup()
        finally:
            source.cleanup()


def rejected(fixture, action, *, number):
    """Require SQL diagnostic and unchanged P/G/capacity/original/session custody."""
    import re

    before = fixture.snapshot()
    with pytest.raises(fixture.source.driver.Error) as caught:
        action()
    symbols = {
        51301: "DPONE_NATIVE_GENERATION_PHYSICAL_OWNER_CHANGED",
        51426: "DPONE_PHYSICAL_SOURCE_EXECUTOR_MISMATCH",
        51480: "DPONE_DISCOVERY_NAMESPACE_COLLISION",
        51600: "DPONE_ENROLLMENT_INPUT_INVALID",
        51601: "DPONE_ENROLLMENT_BYTES_MISMATCH",
        51603: "DPONE_ENROLLMENT_ORIGINAL_UNBOUND",
        51607: "DPONE_ENROLLMENT_UNAVAILABLE",
        51608: "DPONE_ENROLLMENT_SOURCE_CLOSED",
        51610: "DPONE_SESSION_ALREADY_REGISTERED",
        51620: "DPONE_SESSION_CONNECTION_UNOBSERVABLE",
        51632: "DPONE_ENROLLMENT_SIGNATURE_INVENTORY_MISMATCH",
    }
    pattern = rf"\({number}\)" if number == 229 else rf"\b{symbols[number]}\b\s+\({number}\)"
    assert any(re.search(pattern, str(arg)) for arg in caught.value.args), str(caught.value)
    assert fixture.snapshot() == before


@pytest.mark.integration_live
def test_real_sql_roles_replay_drift_closed_authority_and_attach(enrollment):
    from dataclasses import replace

    from dpone.adapters.dbt_mssql_physical_enrollment_permissions import ATTACH, ENROLL, OBSERVER, READ, C
    from dpone.adapters.dbt_workspace_mssql_attempt_admission import MssqlDbtWorkspaceAttemptAdmission
    from dpone.contracts.native_delivery_json import encode_native_delivery_json
    from dpone.contracts.native_source_custody import encode_source_executor_binding
    from tests.support.dbt_mssql_physical_discovery_live import require_ddl_transition
    from tests.support.dbt_mssql_physical_source_authority import LOCAL, SCHEMA

    f, source = enrollment, enrollment.source
    baseline = f.rows()
    assert len(baseline[0]) == 1 and not baseline[1]
    before_replay = f.snapshot()
    assert f.acquire() == f.expected and f.read() == f.enroll()
    assert f.snapshot() == before_replay
    row = f.attach()
    assert row[:8] == (
        1,
        *f.identity()[:1],
        f.expected.registration_sha256,
        *f.identity()[1:],
        f.plan_ref.sha256,
        f.expected.plan.models[0].spec.model_unique_id,
        f.expected.plan.models[0].model_plan_sha256,
    )
    assert row[10] == source.executor.guard_epoch and row[11] == 2
    assert row[13].encode() == encode_source_executor_binding(source.executor)
    assert row[14].encode() == encode_native_delivery_json(f.expected.plan.models[0].to_dict())
    after_attach = f.snapshot()
    assert after_attach["control"] == before_replay["control"]
    assert after_attach["model"][0] == before_replay["model"][0]
    assert len(f.rows()[1]) == 1  # the BUILD connection has already disconnected
    # The complete successful BUILD baseline precedes every negative drill.
    for role in ("build", "observer"):
        for operation in (f.enroll, f.read):
            rejected(f, lambda operation=operation, role=role: operation(role=role), number=229)
    for role in ("metadata", "observer"):
        rejected(f, lambda role=role: f.attach(role=role), number=229)
    for role in ("metadata", "build", "observer"):
        rejected(f, lambda role=role: f.execute(OBSERVER, (None, None, None, None), role=role), number=229)
        before_denials = f.snapshot()
        with source.connection("model", role, autocommit=True) as runtime:
            for table in (
                "physical_plan_enrollments_v1",
                "physical_model_sessions_v1",
                "physical_runtime_registrations_v1",
            ):
                with pytest.raises(source.driver.Error, match=r"\(229\)"):
                    runtime.execute(f"SELECT * FROM [{LOCAL}].[{table}]")
            assert (
                runtime.execute(
                    "SELECT ISNULL(HAS_PERMS_BY_NAME(NULL,'SERVER','VIEW SERVER PERFORMANCE STATE'),0)"
                ).fetchone()[0]
                == 0
            )
        assert f.snapshot() == before_denials
    rejected(f, lambda: f.read(digest=b"sha256:" + b"0" * 64), number=51607)
    rejected(f, lambda: f.enroll(payload=f.payload + b" "), number=51601)
    rejected(f, lambda: f.attach(plan_ref=replace(f.plan_ref, locator=f.plan_ref.locator + "-other")), number=51601)
    rejected(f, lambda: f.attach(model="model.alpha.other"), number=51600)
    before_create = f.snapshot()
    with source.connection(autocommit=True) as admin:
        admin.execute("CREATE VIEW [catalog_models].[orders] AS SELECT 1 AS id")
    try:
        require_ddl_transition(before_create, f.snapshot(), layout=source.layout, event="CREATE_VIEW")
        rejected(f, f.enroll, number=51480)
        rejected(f, f.attach, number=51480)
    finally:
        before_drop = f.snapshot()
        with source.connection(autocommit=True) as admin:
            admin.execute("DROP VIEW [catalog_models].[orders]")
        require_ddl_transition(before_drop, f.snapshot(), layout=source.layout, event="DROP_VIEW")
    # Privileged tamper drills restore exact prior bytes; no DTO can hide drift.
    with source.connection(autocommit=True) as admin:
        admin.execute(f"UPDATE [{LOCAL}].[physical_plan_enrollments_v1] SET payload=?", f.payload + b" ")
    try:
        rejected(f, f.read, number=51601)
    finally:
        with source.connection(autocommit=True) as admin:
            admin.execute(f"UPDATE [{LOCAL}].[physical_plan_enrollments_v1] SET payload=?", f.payload)
    before_replay = f.snapshot()
    assert f.read() == f.enroll()
    assert f.snapshot() == before_replay
    # Original binding projections must still match the actual retained object.
    with source.connection("control", autocommit=True) as admin:
        admin.execute(
            f"UPDATE [{SCHEMA}].[native_original_bindings_v1] SET payload_digest=? WHERE locator=?",
            b"sha256:" + b"0" * 64,
            f.plan_ref.locator.encode(),
        )
    try:
        rejected(f, f.enroll, number=51603)
        rejected(f, f.read, number=51603)
        rejected(f, f.attach, number=51603)
    finally:
        with source.connection("control", autocommit=True) as admin:
            admin.execute(
                f"UPDATE [{SCHEMA}].[native_original_bindings_v1] SET payload_digest=? WHERE locator=?",
                f.plan_ref.sha256.encode(),
                f.plan_ref.locator.encode(),
            )
    # Missing C signature must fail attachment, and provisioning must detect drift.
    with source.connection(autocommit=True) as admin:
        admin.execute(f"DROP SIGNATURE FROM OBJECT::[{LOCAL}].[{OBSERVER}] BY CERTIFICATE [{C}]")
    try:
        rejected(f, lambda: f.provisioner.apply(source.registration, f.binding), number=51632)
        rejected(f, f.attach, number=51620)
    finally:
        with source.connection(autocommit=True) as admin:
            admin.execute(f"ADD SIGNATURE TO OBJECT::[{LOCAL}].[{OBSERVER}] BY CERTIFICATE [{C}]")
    assert f.provisioner.apply(source.registration, f.binding) == f.deployment
    rejected(f, f.attach, number=51610)
    from uuid import uuid4

    rejected(
        f,
        lambda: f.execute(
            READ, (f.identity()[0], f.identity()[1], str(uuid4()), f.read().payload_sha256.encode()), columns=8
        ),
        number=51426,
    )
    source.provider.close_writer_admission(source.reserved, expected_revision=2)
    for operation in (f.enroll, f.read, f.attach):
        rejected(f, operation, number=51608)
    MssqlDbtWorkspaceAttemptAdmission(lambda: source.connect("control"), control_schema=SCHEMA).terminalize(
        source.owner.attempt, state="FAILED"
    )
    for operation in (f.enroll, f.read, f.attach):
        rejected(f, operation, number=51301)
    assert source.provisioner.apply(source.registration) == source.registration
    f.require_old_inventory()
    assert {ENROLL, READ, ATTACH} <= dict(f.deployment.module_sha256).keys()


def test_rejection_cannot_hide_native_custody_mutation():
    """Pure adversarial fixture check; no SQL or admission qualification."""
    from copy import deepcopy
    from types import SimpleNamespace

    from tests.support.dbt_mssql_physical_enrollment_live import EnrollmentFixture

    class SqlDiagnostic(Exception):
        pass

    custody = {"native_generations_v1": (("G", "OPEN", 2),), "native_capacity_v1": ((60,),)}
    source = SimpleNamespace(driver=SimpleNamespace(Error=SqlDiagnostic), snapshot=lambda: deepcopy(custody))

    class CustodyFixture(EnrollmentFixture):
        def rows(self):
            return ((), ())

    fixture = CustodyFixture(source)

    def mutation():
        custody["native_capacity_v1"] = ((0,),)
        raise SqlDiagnostic("DPONE_ENROLLMENT_ORIGINAL_UNBOUND (51603)")

    with pytest.raises(AssertionError):
        rejected(fixture, mutation, number=51603)


def test_rejection_requires_symbol_not_only_shared_sql_number():
    """A same-number unrelated SQL failure is not the intended rejection."""
    from types import SimpleNamespace

    class SqlDiagnostic(Exception):
        pass

    fixture = SimpleNamespace(source=SimpleNamespace(driver=SimpleNamespace(Error=SqlDiagnostic)), snapshot=lambda: {})

    def wrong_symbol():
        raise SqlDiagnostic("UNRELATED_FAILURE (51603)")

    with pytest.raises(AssertionError):
        rejected(fixture, wrong_symbol, number=51603)


def test_attach_description_matches_actual_managed_wire_and_rejects_positional_only():
    """Pure wire regression; actual live calls must supply cursor.description."""
    from tests.support.dbt_mssql_physical_enrollment_live import ATTACH_COLUMNS, require_attach_description
    from tests.test_dbt_mssql_managed_admission_macros import harness

    context, calls = harness()
    assert ATTACH_COLUMNS == tuple(context["dpone_managed_columns"]("attach"))
    assert calls == []
    description = tuple((name, None, None, None, None, None, None) for name in ATTACH_COLUMNS)
    require_attach_description(description)
    for malformed in (
        None,
        description[::-1],
        tuple(("", *field[1:]) for field in description),
        (("WIRE_VERSION", *description[0][1:]), *description[1:]),
    ):
        with pytest.raises(AssertionError):
            require_attach_description(malformed)
