"""Full enrollment identity and actual original acquisition contracts."""

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_mssql_physical_enrollment import build_enrollment, decode_enrollment, encode_enrollment
from tests.support.dbt_mssql_physical_enrollment import components


def enrollment():
    subject, ref, plan, executor, command = components()
    return build_enrollment(
        subject=subject,
        registration_sha256="sha256:" + "b" * 64,
        catalog_binding_sha256="sha256:" + "c" * 64,
        plan_reference=ref,
        plan_payload=plan,
        executor=executor,
        command_payload=command,
        max_bytes=1048576,
    )


def test_full_enrollment_canonical_roundtrip_and_aggregate_bound():
    value = enrollment()
    encoded = encode_enrollment(value, max_bytes=1048576)
    assert decode_enrollment(encoded, max_bytes=len(encoded)) == value
    with pytest.raises(ValueError):
        encode_enrollment(value, max_bytes=len(encoded) - 1)


@pytest.mark.parametrize(
    "field", ["reservation", "registration", "plan_set", "executor", "command", "catalog_binding_sha256", "subject"]
)
def test_closed_component_substitution_never_repaired(field):
    from dpone.contracts.native_delivery_json import encode_native_delivery_json

    raw = enrollment().to_dict()
    raw[field] = None
    with pytest.raises((ValueError, DbtPublishingError)):
        decode_enrollment(encode_native_delivery_json(raw), max_bytes=1048576)


def test_readback_requires_all_eight_facts_and_complete_canonical_bytes():
    from dpone.contracts.dbt_mssql_physical_enrollment import require_enrollment_row

    value = enrollment()
    payload = encode_enrollment(value, max_bytes=1048576)
    from dpone.contracts.dbt_mssql_physical_enrollment import enrollment_digest

    row = (
        1,
        value.registration_id,
        value.registration_sha256,
        value.plan.generation_id,
        str(value.executor.invocation_id),
        value.executor.guard_epoch,
        enrollment_digest(payload),
        payload,
    )
    assert require_enrollment_row(row, expected=value, max_bytes=1048576).payload == payload
    for index in range(8):
        changed = list(row)
        changed[index] = None
        with pytest.raises((ValueError, DbtPublishingError)):
            require_enrollment_row(tuple(changed), expected=value, max_bytes=1048576)
    for changed in (row + (None,), row[:-1], (True,) + row[1:]):
        with pytest.raises(ValueError):
            require_enrollment_row(changed, expected=value, max_bytes=1048576)
    changed = row[:-1] + (payload + b" ",)
    with pytest.raises(ValueError):
        require_enrollment_row(changed, expected=value, max_bytes=1048576)


def test_actual_original_reader_rejects_substituted_binding_and_bytes():
    from dataclasses import replace

    from dpone.adapters.dbt_mssql_physical_catalog_fetch import CatalogReadBudget
    from dpone.adapters.dbt_mssql_physical_enrollment import acquire_enrollment_originals
    from tests.native_trusted_dbt_fixtures import SyntheticOriginals

    value = enrollment()
    store = SyntheticOriginals()
    store.subject = value.subject
    store.add("mssql_physical_plan_set_v1", value.plan_reference.locator, value.plan_payload)
    store.add("trusted_dbt_command_plan_v1", value.executor.command.locator, value.command_payload)

    from tests.support.dbt_mssql_physical_enrollment import reservation_payload

    store.add("generation_stored_file_v1", value.executor.reservation.locator, reservation_payload(value))

    def acquire():
        return acquire_enrollment_originals(
            originals=store,
            bindings=store,
            subject=value.subject,
            plan_reference=value.plan_reference,
            executor=value.executor,
            registration_sha256=value.registration_sha256,
            catalog_binding_sha256=value.catalog_binding_sha256,
            max_bytes=1048576,
            budget=CatalogReadBudget(10, 1048576, lambda: 0),
        )

    assert acquire() == value
    original = store.bound[value.plan_reference.locator]
    store.bound[value.plan_reference.locator] = replace(original, kind="generation_stored_file_v1")
    with pytest.raises(ValueError):
        acquire()
    store.bound[value.plan_reference.locator] = original
    store.documents[value.plan_reference.locator] += b" "
    with pytest.raises(ValueError):
        acquire()


def test_original_acquisition_stops_on_expired_deadline_before_provider_io():
    from dpone.adapters.dbt_mssql_physical_catalog_fetch import CatalogReadBudget
    from dpone.adapters.dbt_mssql_physical_enrollment import acquire_enrollment_originals

    value = enrollment()
    with pytest.raises(ValueError, match="deadline"):
        acquire_enrollment_originals(
            originals=None,
            bindings=None,
            subject=value.subject,
            plan_reference=value.plan_reference,
            executor=value.executor,
            registration_sha256=value.registration_sha256,
            catalog_binding_sha256=value.catalog_binding_sha256,
            max_bytes=1048576,
            budget=CatalogReadBudget(10, 1048576, lambda: 10),
        )


def test_generation_is_sole_enrollment_key_and_child_is_durable_once_only():
    from pathlib import Path

    from dpone.adapters.dbt_mssql_physical_enrollment_queries import enrollment_tables

    sql = enrollment_tables(
        enrollment_sql=Path("packages/dbt-dpone/control/sqlserver/physical-v1/enrollment.sql").read_bytes()
    )
    assert "PRIMARY KEY(generation_id)" in sql
    assert "UNIQUE(generation_id,model_unique_id_hash)" in sql
    assert "connection_id uniqueidentifier NOT NULL" in sql
    assert "connect_time datetime2(7) NOT NULL" in sql
    assert "login_time datetime2(7) NOT NULL" in sql
    assert "DATALENGTH(payload) BETWEEN 1 AND 1048576" in sql
    for forbidden in ("FOREIGN KEY", "transaction_id", "status", "UPDATE ", "DELETE ", "GRANT "):
        assert forbidden not in sql


def test_enrollment_sql_validates_native_primitive_limits_before_shape_consumption():
    from pathlib import Path

    from dpone.adapters.dbt_mssql_physical_enrollment_queries import enrollment_document_sql

    sql = enrollment_document_sql(
        enrollment_sql=Path("packages/dbt-dpone/control/sqlserver/physical-v1/enrollment.sql").read_bytes()
    )
    assert "65536" in sql and "4096" in sql and "32" in sql
    assert "COUNT_BIG(*)" in sql
    assert "DPONE_ENROLLMENT_INPUT_INVALID" in sql
    assert "DPONE_ENROLLMENT_BYTES_MISMATCH" in sql
    assert "JSON_QUERY(@enrollment" in sql
    assert "IS NULL" in sql
    for field in (
        "registration",
        "catalog_binding_sha256",
        "subject",
        "executor",
        "plan_set",
        "reservation",
        "command",
    ):
        assert field in sql


def test_retained_reservation_activation_mismatch_rejects_before_membership():
    from dpone.contracts.dbt_mssql_physical_enrollment import require_enrollment_reservation
    from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptRequest
    from dpone.contracts.native_generation_admission import generation_admission_request_bytes
    from tests.support.dbt_mssql_physical_enrollment import reservation_payload

    value = enrollment()
    require_enrollment_reservation(value, reservation_payload(value))
    attempt = value.plan.workspace_attempt
    changed = DbtWorkspaceAttemptRequest.build(
        activation_id="90000000-0000-0000-0000-000000000009",
        attempt_id=attempt.attempt_id,
        workflow_id=attempt.workflow_id,
        write_subjects=attempt.write_subjects,
    )
    payload = generation_admission_request_bytes(
        subject=value.subject,
        workspace_attempt=changed,
        guard=value.plan.guard,
        profile=value.executor.profile,
        command=value.executor.command,
        requested_bytes=100,
    )
    from dataclasses import replace

    from dpone.contracts.dbt_mssql_physical_enrollment import enrollment_digest
    from dpone.contracts.native_identity import OriginalRef

    value = replace(
        value,
        executor=replace(
            value.executor, reservation=OriginalRef(value.executor.reservation.locator, enrollment_digest(payload))
        ),
    )
    with pytest.raises(ValueError):
        require_enrollment_reservation(value, payload)


def test_real_enrollment_endpoints_lock_source_before_immutable_key_and_commit_before_result():
    from pathlib import Path

    from dpone.adapters.dbt_mssql_physical_enrollment_queries import CONTROL, ENROLL, READ, enrollment_procedures
    from tests.support.dbt_mssql_physical_registration import registration_inputs

    root = Path("packages/dbt-dpone/control/sqlserver/physical-v1")
    modules = enrollment_procedures(
        enrollment_sql=(root / "enrollment.sql").read_bytes(),
        discovery_sql=(root / "discovery.sql").read_bytes(),
        model_database=registration_inputs()["model_database"],
        model_schema="models",
        model_schema_id=10,
        control_database="example",
        control_schema="runtime_control",
        enrollment_certificate_thumbprint=b"e" * 20,
    )
    assert set(modules) == {ENROLL, READ, CONTROL}
    for name in (ENROLL, READ):
        sql = modules[name]
        assert sql.index("INSERT @source EXEC") < sql.index("FROM [dpone_physical].[physical_plan_enrollments_v1]")
        assert sql.index("COMMIT TRANSACTION;") < sql.index("SELECT CONVERT(smallint,1)")
        assert "@@TRANCOUNT<>0" in sql
        assert "{{" not in sql
    assert "WITH(UPDLOCK,HOLDLOCK)" in modules[ENROLL]
    assert "writer_admission='OPEN'" in modules[CONTROL]
    assert "native_original_bindings_v1" in modules[CONTROL]
    assert "$.plan_set.payload.workspace_attempt" in modules[CONTROL]
    assert "DPONE_DISCOVERY_NAMESPACE_COLLISION" in modules[ENROLL]
    assert "DPONE_DISCOVERY_NAMESPACE_COLLISION" not in modules[READ]


def test_bounded_enrollment_transport_ack_and_timeout_are_not_retried():
    from dpone.adapters.dbt_mssql_physical_catalog_fetch import CatalogReadBudget
    from dpone.adapters.dbt_mssql_physical_enrollment import _observe_enrollment
    from dpone.contracts.dbt_mssql_physical_enrollment import enrollment_digest

    value = enrollment()
    payload = encode_enrollment(value, max_bytes=1048576)
    row = (
        1,
        value.registration_id,
        value.registration_sha256.encode(),
        value.plan.generation_id,
        str(value.executor.invocation_id),
        value.executor.guard_epoch,
        enrollment_digest(payload).encode(),
        payload,
    )
    events = []

    class Cursor:
        def __init__(self, timeout):
            self.timeout = timeout
            self.rows = [row, None]

        def execute(self, sql, *parameters):
            events.append((sql, self.timeout, parameters))

        def fetchone(self):
            return self.rows.pop(0)

        def nextset(self):
            return None

        def close(self):
            pass

    class Connection:
        timeout = 0
        autocommit = False

        def cursor(self):
            assert self.autocommit is True
            return Cursor(self.timeout)

        def close(self):
            pass

    def connect(timeout):
        events.append(timeout)
        return Connection()

    result = _observe_enrollment(
        connection_factory=connect,
        expected=value,
        budget=CatalogReadBudget(10, 1048576, lambda: 1),
        max_bytes=1048576,
        enroll=True,
    )
    assert result.payload == payload
    assert events[0] == 9 and events[1][1] == 9
    assert "physical_enroll_plan_set_v1" in events[1][0]

    class LostCursor(Cursor):
        def execute(self, sql, *parameters):
            super().execute(sql, *parameters)
            raise OSError("synthetic lost acknowledgement")

    class LostAcknowledgement(Connection):
        def cursor(self):
            return LostCursor(self.timeout)

    calls = []

    def unavailable(timeout):
        calls.append(timeout)
        return LostAcknowledgement()

    with pytest.raises(OSError):
        _observe_enrollment(
            connection_factory=unavailable,
            expected=value,
            budget=CatalogReadBudget(10, 1048576, lambda: 1),
            max_bytes=1048576,
            enroll=True,
        )
    assert len(calls) == 1

    from dpone.adapters.dbt_mssql_physical_enrollment import _settle_enrollment

    calls.clear()

    def lost_readback(timeout):
        calls.append(timeout)
        return Connection() if len(calls) == 1 else LostAcknowledgement()

    with pytest.raises(OSError):
        _settle_enrollment(
            connection_factory=lost_readback,
            expected=value,
            budget=CatalogReadBudget(10, 1048576, lambda: 1),
            max_bytes=1048576,
        )
    assert len(calls) == 2
    assert "physical_read_plan_enrollment_v1" in events[-1][0]
