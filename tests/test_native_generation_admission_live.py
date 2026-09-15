"""Isolated SQL reserve/bind/read acceptance using real existing P admission.

Synthetic original bindings qualify the ledger boundary only. They do not
certify artifact storage, source catalog qualification or an actual dbt launch.
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from hashlib import sha256
from uuid import uuid4

import pytest

from dpone.adapters.dbt_workspace_mssql_activation_admission import MssqlDbtWorkspaceActivationAdmission
from dpone.adapters.dbt_workspace_mssql_attempt_admission import MssqlDbtWorkspaceAttemptAdmission
from dpone.adapters.native_generation_mssql import (
    MssqlNativeGenerationControl,
    NativeGenerationAdmissionError,
    NativeWriterAdmissionUncertain,
)
from dpone.adapters.native_generation_mssql_schema import MssqlNativeGenerationSchemaMigration
from dpone.adapters.semantic_refresh_mssql_schema import MssqlSemanticRefreshSchemaMigration
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationRequest, DbtWorkspacePhysicalResource
from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptRequest
from dpone.contracts.native_delivery_json import decode_native_delivery_json, encode_native_delivery_json
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativeGenerationOriginalSubject
from dpone.contracts.native_source_custody import SourceExecutorBinding
from dpone.contracts.native_source_custody_codec import encode_source_executor_binding
from tests.test_native_generation_admission import generation_request
from tests.test_native_original_bindings import D, binding
from tests.test_native_original_subjects import authority
from tests.test_native_originals_mssql_live import CommitFault
from tests.test_native_originals_mssql_live import ledger as ledger

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(
        os.environ.get("DPONE_RUN_NATIVE_ORIGINAL_MSSQL_LIVE") != "1", reason="isolated SQL acceptance disabled"
    ),
]


@pytest.fixture
def generations(ledger):
    originals, runtime, admin, _ = ledger
    MssqlSemanticRefreshSchemaMigration(admin, control_schema="dpone_control").apply()
    resource = DbtWorkspacePhysicalResource(
        "mssql://source/orders",
        "mssql",
        D,
        D,
        D,
        ("sha256:" + "b" * 64, "sha256:" + "c" * 64),
    )
    activation = DbtWorkspaceActivationRequest.build(
        activation_id=str(uuid4()),
        environment="dev",
        release_id=D,
        deployment_id=D,
        previous_deployment_id=None,
        source_inventory_sha256=D,
        runtime_context_sha256=D,
        write_subjects=resource.write_subjects,
        resources=(resource,),
    )
    admission = MssqlDbtWorkspaceActivationAdmission(admin, control_schema="dpone_control")
    admission.prepare(activation)
    admission.activate(activation)
    attempt = DbtWorkspaceAttemptRequest.build(
        activation_id=activation.activation_id,
        attempt_id=D,
        workflow_id="orders",
        write_subjects=resource.write_subjects,
    )
    receipt = MssqlDbtWorkspaceAttemptAdmission(admin, control_schema="dpone_control").admit(attempt)
    connection = runtime()
    try:
        principal = connection.execute("SELECT USER_NAME()").fetchone()[0]
    finally:
        connection.close()
    profile = OriginalRef("x" * 4001 + "café/東京/😀", generation_request().profile.sha256)

    def install_profile(selected=profile, capacity=100):
        migration = MssqlNativeGenerationSchemaMigration(
            connection_factory=admin,
            control_schema="dpone_control",
            control_authority=OriginalRef("control/authority", D),
            runtime_database_principal=principal,
            physical_guard=resource.guard_id,
            resource_authority=OriginalRef("resource/authority", D),
            capacity_bytes=capacity,
            trusted_profile=selected,
            max_generation_bytes=100,
        )
        migration.apply()
        return migration

    migration = install_profile()

    def request(*, size=60, guard=None, selected_profile=profile):
        value = generation_request(
            subject=NativeGenerationOriginalSubject(authority(), uuid4()),
            attempt=attempt,
            guard=guard or receipt.guard_epochs[0],
            requested_bytes=size,
            profile=selected_profile,
        )
        originals().bind(
            replace(
                binding(),
                subject=value.subject,
                locator=value.reservation.locator,
                payload_sha256=value.reservation.sha256,
            )
        )
        return value

    def provider(factory=runtime):
        return MssqlNativeGenerationControl(
            connection_factory=factory,
            control_schema="dpone_control",
            control_authority=OriginalRef("control/authority", D),
        )

    return request, provider, runtime, admin, migration, originals, install_profile


def writer(request):
    return SourceExecutorBinding(
        request.subject.generation_id,
        request.guard.fencing_epoch,
        uuid4(),
        request.reservation,
        request.profile,
        request.command,
    )


def test_live_reserve_bind_read_and_duplicate_never_grants_dispatch(generations):
    request, provider, _, _, migration, _, _ = generations
    migration.apply()
    value = request()
    first = provider().reserve(value)
    assert provider().reserve(value) == first
    executor = writer(value)
    bound = provider().bind_writer_invocation(first, executor, expected_revision=first.revision)
    assert bound.state == "BUILDING" and bound.revision == 2
    assert provider().read_custody(value.subject.generation_id) == bound
    with pytest.raises(NativeWriterAdmissionUncertain) as failure:
        provider().bind_writer_invocation(first, executor, expected_revision=first.revision)
    assert failure.value.observed == bound


def test_live_competing_generations_share_capacity(generations):
    request, provider, _, admin, _, _, _ = generations
    values = [request(), request()]

    def reserve(value):
        try:
            return provider().reserve(value)
        except NativeGenerationAdmissionError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reserve, values))
    assert sum(result is not None for result in results) == 1
    connection = admin()
    try:
        assert (
            connection.execute("SELECT charged_bytes FROM dpone_control.native_generation_capacity_v1").fetchone()[0]
            == 60
        )
    finally:
        connection.close()


@pytest.mark.parametrize("committed", [True, False])
def test_live_writer_lost_ack_never_returns_dispatch_or_repeats_bind(generations, committed):
    request, provider, runtime, _, _, _, _ = generations
    value = request()
    reservation = provider().reserve(value)
    calls = []

    def fault():
        calls.append(1)
        connection = runtime()
        return CommitFault(connection, committed) if len(calls) == 1 else connection

    with pytest.raises(NativeWriterAdmissionUncertain) as failure:
        provider(fault).bind_writer_invocation(reservation, writer(value), expected_revision=reservation.revision)
    assert len(calls) == 2
    assert failure.value.observed.state == ("BUILDING" if committed else "RESERVED")
    assert failure.value.observed.revision == (2 if committed else 1)


def test_live_stale_epoch_cannot_charge_capacity(generations):
    request, provider, _, admin, _, _, _ = generations
    value = request()
    connection = admin()
    try:
        connection.execute("UPDATE dpone_control.semantic_refresh_guards SET fencing_epoch=fencing_epoch+1")
        connection.commit()
        with pytest.raises(NativeGenerationAdmissionError):
            provider().reserve(value)
        assert (
            connection.execute("SELECT charged_bytes FROM dpone_control.native_generation_capacity_v1").fetchone()[0]
            == 0
        )
    finally:
        connection.close()


@pytest.mark.parametrize("mutation", ["missing", "long_schema", "uuid_suffix", "uuid_upper", "fractional_epoch"])
def test_live_direct_executor_malformed_json_cannot_change_custody(generations, mutation):
    request, provider, runtime, _, _, _, _ = generations
    value = request()
    provider().reserve(value)
    payload = decode_native_delivery_json(encode_source_executor_binding(writer(value)))
    if mutation == "missing":
        del payload["profile"]
    elif mutation == "long_schema":
        payload["schema"] = "x" * 4001
    elif mutation == "uuid_suffix":
        payload["generation_id"] += "-suffix"
    elif mutation == "uuid_upper":
        payload["invocation_id"] = "ABCDEFAB-1234-5678-1234-567812345678"
    else:
        payload["guard_epoch"] = 1.0
    connection = runtime()
    try:
        with pytest.raises(Exception, match="DPONE_NATIVE_GENERATION"):
            connection.execute(
                "EXEC dpone_control.native_source_writer_bind_v1 ?,?,?,?,?",
                b"control/authority",
                D.encode(),
                str(value.subject.generation_id),
                1,
                json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8"),
            )
        connection.rollback()
    finally:
        connection.close()
    assert provider().read_custody(value.subject.generation_id).state == "RESERVED"


def test_live_same_guard_incomplete_subjects_cannot_reuse_attempt_hash(generations):
    request, _, runtime, _, _, originals, _ = generations
    value = request()
    payload = decode_native_delivery_json(value.request_bytes())
    payload["workspace_attempt"]["write_subjects"] = payload["workspace_attempt"]["write_subjects"][:1]
    body = encode_native_delivery_json(payload)
    reference = OriginalRef(value.reservation.locator + "/incomplete", "sha256:" + sha256(body).hexdigest())
    originals().bind(
        replace(binding(), subject=value.subject, locator=reference.locator, payload_sha256=reference.sha256)
    )
    connection = runtime()
    try:
        with pytest.raises(Exception, match="ATTEMPT_HASH_INVALID"):
            connection.execute(
                "EXEC dpone_control.native_generation_reserve_v1 ?,?,?,?,?",
                b"control/authority",
                D.encode(),
                body,
                reference.locator.encode(),
                reference.sha256.encode(),
            )
        connection.rollback()
    finally:
        connection.close()


@pytest.mark.parametrize("change", ["disabled_check", "trigger", "nullable"])
def test_live_installer_rejects_weakened_retained_tables(generations, change):
    _, _, _, admin, migration, _, _ = generations
    connection = admin()
    try:
        if change == "disabled_check":
            connection.execute("ALTER TABLE dpone_control.native_generation_capacity_v1 NOCHECK CONSTRAINT ALL")
        elif change == "trigger":
            connection.execute(
                "CREATE TRIGGER dpone_control.generation_evil ON dpone_control.native_generations_v1 AFTER INSERT AS RETURN"
            )
        else:
            connection.execute(
                "ALTER TABLE dpone_control.native_generation_capacity_v1 ALTER COLUMN resource_locator varbinary(max) NULL"
            )
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(Exception, match="EXISTING_SCHEMA_MISMATCH"):
        migration.apply()


def test_live_profile_versions_share_one_capacity_account(generations):
    request, provider, _, _, _, _, install_profile = generations
    provider().reserve(request())
    alternate = OriginalRef("profiles/alternate", "sha256:" + "e" * 64)
    install_profile(alternate)
    with pytest.raises(NativeGenerationAdmissionError):
        provider().reserve(request(selected_profile=alternate))
    with pytest.raises(Exception, match="CAPACITY_REGISTRATION_CONFLICT"):
        install_profile(alternate, capacity=200)


def test_live_runtime_cannot_mutate_generation_tables_or_alias_digest(generations):
    request, provider, runtime, _, _, _, _ = generations
    value = request()
    provider().reserve(value)
    connection = runtime()
    try:
        for table in ("native_generations_v1", "native_generation_capacity_v1", "native_generation_profiles_v1"):
            with pytest.raises(Exception):
                connection.execute(f"DELETE FROM dpone_control.{table}")
            connection.rollback()
        with pytest.raises(Exception, match="AUTHORITY_INVALID"):
            connection.execute(
                "EXEC dpone_control.native_source_custody_read_v1 ?,?,?",
                b"control/authority",
                D.encode() + b"suffix",
                str(value.subject.generation_id),
            )
        connection.rollback()
    finally:
        connection.close()


def test_live_conflicting_binding_cannot_replace_reserved_command(generations):
    request, provider, _, _, _, _, _ = generations
    value = request()
    reservation = provider().reserve(value)
    changed = replace(writer(value), command=OriginalRef("other/command", D))
    with pytest.raises(NativeWriterAdmissionUncertain):
        provider().bind_writer_invocation(reservation, changed, expected_revision=1)
    assert provider().read_custody(value.subject.generation_id).state == "RESERVED"


@pytest.mark.parametrize("committed", [True, False])
def test_live_reservation_lost_ack_retains_charge_without_claiming_admission(generations, committed):
    request, provider, runtime, admin, _, _, _ = generations
    value = request()
    calls = []

    def fault():
        calls.append(1)
        connection = runtime()
        return CommitFault(connection, committed) if len(calls) == 1 else connection

    with pytest.raises(NativeGenerationAdmissionError):
        provider(fault).reserve(value)
    assert len(calls) == 2
    connection = admin()
    try:
        assert connection.execute("SELECT charged_bytes FROM dpone_control.native_generation_capacity_v1").fetchone()[
            0
        ] == (60 if committed else 0)
    finally:
        connection.close()


@pytest.mark.parametrize("mutation", ["space", "reorder", "nested_space"])
def test_live_noncanonical_executor_cannot_poison_retained_custody(generations, mutation):
    request, provider, runtime, _, _, _, _ = generations
    value = request()
    provider().reserve(value)
    canonical = encode_source_executor_binding(writer(value))
    if mutation == "space":
        payload = b" " + canonical
    elif mutation == "nested_space":
        payload = canonical.replace(b'"reservation":{', b'"reservation":{ ')
    else:
        fields = json.loads(canonical)
        payload = json.dumps(dict(reversed(list(fields.items()))), ensure_ascii=False, separators=(",", ":")).encode()
    connection = runtime()
    try:
        with pytest.raises(Exception, match="EXECUTOR_NONCANONICAL"):
            connection.execute(
                "EXEC dpone_control.native_source_writer_bind_v1 ?,?,?,?,?",
                b"control/authority",
                D.encode(),
                str(value.subject.generation_id),
                1,
                payload,
            )
        connection.rollback()
    finally:
        connection.close()
    snapshot = provider().read_custody(value.subject.generation_id)
    assert snapshot.state == "RESERVED" and snapshot.revision == 1 and snapshot.executor is None
