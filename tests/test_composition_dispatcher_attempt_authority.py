"""Offline protected-ledger tests; endpoint observations are explicit doubles."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore
from dpone.adapters.composition_mssql_store import MssqlCompositionActivationStore
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.app import composition_dispatcher_attempt_authority as module
from dpone.app.composition_clickhouse_execution import build_composition_clickhouse_attempt
from dpone.app.composition_dispatcher_context import StagedDispatcherAttempt
from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowArtifactIdentity, AirflowRunIdentity
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import (
    CompositionAttemptProof,
    composition_attempt_epoch_subject,
    encode_attempt_proof,
)
from dpone.contracts.strict_json import canonical_json_bytes
from tests.composition_mssql_catalog_helpers import install_offline_catalog_references
from tests.composition_snapshot_helpers import digest, intent, occurrence
from tests.test_composition_authority_connections import bound
from tests.test_composition_clickhouse_execution_root import _manifest
from tests.test_composition_clickhouse_gate import SQL_SERVICE, GateDatabase


@pytest.fixture
def case(monkeypatch, request):
    install_offline_catalog_references(monkeypatch)
    parent = occurrence()
    if getattr(request, "param", None) == "predecessor":
        retained = replace(
            parent.request,
            context=replace(parent.request.context, previous_deployment_id=digest("retained predecessor")),
        )
        parent = replace(
            parent, request=retained, receipt=replace(parent.receipt, request_sha256=retained.request_sha256)
        )
    run = AirflowRunIdentity(
        parent.request.release_id,
        parent.request.deployment_id,
        AirflowArtifactIdentity("a_native", parent.request.workloads[0].pack_sha256),
    )
    correlation = AirflowAttemptCorrelation(dag_id="dag", run_id="run", task_id="execute", try_number=1, map_index=-1)
    attempt = build_composition_clickhouse_attempt(
        parent,
        manifest=_manifest(),
        plan_sha256=parent.request.source_subject_sha256,
        run_identity=run,
        airflow_attempt=correlation,
    )
    resolved = bound()
    resolved = replace(
        resolved,
        descriptor=replace(
            resolved.descriptor, properties={**resolved.descriptor.properties, "composition_service_id": SQL_SERVICE}
        ),
    )
    resolutions = []

    def resolve(reference):
        resolutions.append(reference)
        assert reference == "control"
        return resolved

    runtime = SimpleNamespace(
        resolver=SimpleNamespace(resolve=resolve),
        binding_set={
            "bindings": {ref: {"connection_ref": "registry-" + ref} for ref in ("control", "mssql-source", "ch-sink")}
        },
    )
    context = SimpleNamespace(
        occurrence=replace(parent.request.context, previous_deployment_id=None),
        runtime=runtime,
        plan=SimpleNamespace(
            sources=SimpleNamespace(subject_sha256=parent.request.source_subject_sha256),
            workloads=parent.request.workloads,
        ),
        target_binding_ref="ch-sink",
    )
    selected = StagedDispatcherAttempt(
        context, attempt, SimpleNamespace(connection_ref="ch-sink"), canonical_json_bytes(_manifest())
    )
    db = GateDatabase(parent.request, SQL_SERVICE)
    activation = MssqlCompositionActivationStore(db.connect, expected_service_id=SQL_SERVICE)
    activation.prepare(parent.request)
    activation.activate(parent.request)
    verified = []

    def verify(authority, actual_context):
        assert actual_context == context.occurrence
        assert authority._inputs.resolve_connection(actual_context, "control") is resolved
        verified.append(actual_context)
        return db.connect()

    monkeypatch.setattr(module.CompositionAuthorityConnections, "control_connection", verify)
    return SimpleNamespace(
        parent=parent,
        selected=selected,
        db=db,
        run=run,
        correlation=correlation,
        resolutions=resolutions,
        verified=verified,
        factory=lambda *args, **kwargs: pytest.fail("endpoint verification is doubled"),
    )


def authority(case, **kwargs):
    return module.DispatcherAttemptAuthority(
        case.selected,
        control_connection_ref="control",
        expected_control_service_id=SQL_SERVICE,
        control_schema="dpone_control",
        connector_factory=case.factory,
        **kwargs,
    )


def test_constructor_resolves_control_only_and_fresh_inspection_never_admits(case):
    before = deepcopy(case.db.data)
    reader = authority(case)
    assert case.resolutions == ["control"]
    assert not case.verified
    assert reader.inspect(case.run, case.correlation) is None
    assert reader.read_active() == case.parent
    assert len(case.verified) == 2
    assert case.db.data == before
    assert reader.control.expected_service_id == SQL_SERVICE


def test_existing_running_operation_is_read_before_active_builder(case, monkeypatch):
    expected = MssqlCompositionAttemptStore(case.db.connect, expected_service_id=SQL_SERVICE).admit_once(
        case.selected.attempt
    )
    monkeypatch.setattr(
        module,
        "build_composition_clickhouse_attempt",
        lambda *args, **kwargs: pytest.fail("historical ACTIVE derivation"),
    )
    assert authority(case).inspect(case.run, case.correlation) == expected


@pytest.mark.parametrize("field", ["task_id", "run_id", "try_number", "map_index"])
def test_scheduler_substitution_rejected_before_sql(case, field):
    value = getattr(case.correlation, field)
    changed = replace(case.correlation, **{field: value + 1 if isinstance(value, int) else value + "changed"})
    with pytest.raises(CompositionAdmissionError):
        authority(case).inspect(case.run, changed)
    assert not case.verified


@pytest.mark.parametrize("method", ["inspect", "inspect_existing"])
@pytest.mark.parametrize("state", ["ACTIVE", "RETIRING", "RETIRED"])
@pytest.mark.parametrize("terminal", ["SUCCEEDED", "FAILED"])
def test_historical_terminal_is_independently_validated_and_never_requires_active(
    case, state, terminal, monkeypatch, method
):
    attempt = case.selected.attempt
    MssqlCompositionAttemptStore(case.db.connect, expected_service_id=SQL_SERVICE).admit_once(attempt)
    record = case.db.data["operations"][attempt.attempt_sha256]
    principal = intent().ingest_principal
    proofs = tuple(
        CompositionAttemptProof(
            kind,
            attempt.attempt_sha256,
            attempt.activation_request_sha256,
            composition_attempt_epoch_subject(attempt),
            (principal,),
            digest(kind),
            outcome_state=terminal if kind == "OUTCOME" else None,
        )
        for kind in ("CLOSED_GATES", "QUIESCENCE", "OUTCOME")
    )
    case.db.data["operations"][attempt.attempt_sha256] = (
        *record[:6],
        terminal,
        *(proof.proof_sha256 for proof in proofs),
    )
    case.db.data["issued_authorities"][attempt.attempt_sha256] = (
        (principal.connector, principal.service_id, principal.principal_id),
    )
    for proof in proofs:
        case.db.data["proofs"][attempt.attempt_sha256, proof.kind, proof.proof_sha256] = (
            "execution",
            encode_attempt_proof(proof),
        )
    owner = case.db.data["owners"][record[2]]
    case.db.data["owners"][record[2]] = (*owner[:5], state)
    if state == "RETIRED":
        case.db.data["domains"] = {guard: (*row[:4], None) for guard, row in case.db.data["domains"].items()}
    monkeypatch.setattr(
        module,
        "build_composition_clickhouse_attempt",
        lambda *args, **kwargs: pytest.fail("historical ACTIVE derivation"),
    )
    reader = authority(case)
    assert getattr(reader, method)(case.run, case.correlation).state == terminal
    case.db.data["proofs"].clear()
    with pytest.raises(CompositionAdmissionError):
        getattr(reader, method)(case.run, case.correlation)


@pytest.mark.parametrize("field", ["environment", "release_id", "runtime_context_sha256"])
def test_staged_parent_mismatch_rejected(case, field):
    changed = replace(
        case.selected.context.occurrence, **{field: "other" if field == "environment" else digest("other")}
    )
    case.selected.context.occurrence = changed
    with pytest.raises(CompositionAdmissionError):
        authority(case).inspect(case.run, case.correlation)


def test_source_plan_and_full_workload_partition_are_checked(case):
    case.selected.context.plan.workloads = case.selected.context.plan.workloads[:1]
    with pytest.raises(CompositionAdmissionError):
        authority(case).read_active()


def test_control_cannot_alias_source_registry_entry(case):
    bindings = case.selected.context.runtime.binding_set["bindings"]
    bindings["control"] = bindings["mssql-source"]
    with pytest.raises(CompositionAdmissionError):
        authority(case)
    assert not case.resolutions


@pytest.mark.parametrize("method", ["inspect", "inspect_existing"])
def test_sql_read_failure_never_becomes_absence(case, monkeypatch, method):
    def fail(*args, **kwargs):
        raise CompositionAdmissionError("actual_control_failure")

    monkeypatch.setattr(module, "read_shared_operation_in", fail)
    monkeypatch.setattr(
        module, "build_composition_clickhouse_attempt", lambda *args, **kwargs: pytest.fail("failure became absence")
    )
    with pytest.raises(CompositionAdmissionError, match="actual_control_failure"):
        getattr(authority(case), method)(case.run, case.correlation)


def test_source_subject_mismatch_rejected(case):
    case.selected.context.plan.sources.subject_sha256 = digest("substitute source")
    with pytest.raises(CompositionAdmissionError):
        authority(case).read_active()


def test_control_service_mismatch_rejected_before_sql(case):
    with pytest.raises(CompositionAdmissionError):
        module.DispatcherAttemptAuthority(
            case.selected,
            control_connection_ref="control",
            expected_control_service_id="10000000-0000-4000-8000-000000000099",
            control_schema="dpone_control",
            connector_factory=case.factory,
        )
    assert not case.verified


def test_pinned_inputs_reject_foreign_reference_and_context(case, monkeypatch):
    observed = []

    def verify(authority, context):
        for candidate, reference in ((context, "mssql-source"), (replace(context, environment="other"), "control")):
            with pytest.raises(CompositionAdmissionError):
                authority._inputs.resolve_connection(candidate, reference)
        observed.append(context)
        return case.db.connect()

    monkeypatch.setattr(module.CompositionAuthorityConnections, "control_connection", verify)
    assert authority(case).inspect(case.run, case.correlation) is None
    assert len(observed) == 1


@pytest.mark.parametrize("method", ["inspect", "inspect_existing"])
def test_actual_transaction_change_is_rejected(case, monkeypatch, method):
    original = module.read_shared_operation_in

    def changed(ledger, *args, **kwargs):
        result = original(ledger, *args, **kwargs)
        ledger.cursor.connection.transaction_id += 1
        return result

    monkeypatch.setattr(module, "read_shared_operation_in", changed)
    with pytest.raises(CompositionAdmissionError):
        getattr(authority(case), method)(case.run, case.correlation)


@pytest.mark.parametrize("case", ["predecessor"], indirect=True)
def test_unprovided_predecessor_preserves_exact_retained_request(case):
    assert case.selected.context.occurrence.previous_deployment_id is None
    assert case.parent.request.previous_deployment_id is not None
    reader = authority(case)
    assert reader.inspect(case.run, case.correlation) is None
    assert reader.read_active().request == case.parent.request


@pytest.mark.parametrize("method", ["inspect", "inspect_existing"])
@pytest.mark.parametrize("existing", [False, True])
def test_dag_spec_mismatch_rejected_before_sql_for_fresh_and_existing(case, existing, method):
    if existing:
        MssqlCompositionAttemptStore(case.db.connect, expected_service_id=SQL_SERVICE).admit_once(case.selected.attempt)
    run = replace(case.run, dag_spec=AirflowArtifactIdentity("foreign-dag", digest("dag")))
    with pytest.raises(CompositionAdmissionError, match="dispatcher_scheduler_identity"):
        getattr(authority(case), method)(run, case.correlation)
    assert not case.verified


def test_status_absence_never_reads_parent_or_derives_candidate(case, monkeypatch):
    before = deepcopy(case.db.data)
    monkeypatch.setattr(CompositionMssqlLedger, "read", lambda *args: pytest.fail("status read ACTIVE parent"))
    monkeypatch.setattr(
        module, "build_composition_clickhouse_attempt", lambda *args, **kwargs: pytest.fail("status derived candidate")
    )
    assert authority(case).inspect_existing(case.run, case.correlation) is None
    assert case.db.data == before and len(case.verified) == 1


def test_status_retains_running_receipt_without_admission(case):
    expected = MssqlCompositionAttemptStore(case.db.connect, expected_service_id=SQL_SERVICE).admit_once(
        case.selected.attempt
    )
    before = deepcopy(case.db.data)
    assert authority(case).inspect_existing(case.run, case.correlation) == expected
    assert case.db.data == before
