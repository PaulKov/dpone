"""Actual publication authority over offline SQL/gate boundaries; no live proof."""

from contextlib import contextmanager
from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace

import pytest

from dpone.adapters import composition_clickhouse_gate_queries as gate_queries
from dpone.app import composition_clickhouse_publication as publication
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import CompositionAttemptProof, composition_attempt_epoch_subject
from dpone.contracts.composition_snapshot import SnapshotPublisherClosure
from dpone.contracts.strict_json import canonical_json_bytes
from tests.composition_snapshot_helpers import digest, intent, occurrence

SQL_SERVICE = "10000000-0000-4000-8000-000000000002"


def document_digest(document):
    return "sha256:" + sha256(document).hexdigest()


class Cursor:
    """Return retained binding originals to the real ClickHouseGateQueries reader."""

    def __init__(self, value):
        self.value, self.documents, self.identifiers = value, {}, {}
        self.issued_attempt = value.attempt.attempt_sha256
        for purpose in ("ingest", "publisher"):
            binding = gate_queries.ClickHouseGateBinding(value.attempt, value.target, purpose)
            principal = getattr(value, purpose + "_principal")
            identifier = principal.principal_id.removeprefix("clickhouse-user:")
            self.identifiers[binding.key] = identifier
            self.documents[binding.key] = canonical_json_bytes({"gate_key": binding.key, "gate_id": identifier})

    def execute(self, sql, *parameters):
        if "evidence_document" in sql:
            document = self.documents.get(parameters[0])
            self.result = [] if document is None else [(document_digest(document), document)]
        elif "LOWER(CONVERT(char(36),gate_id))" in sql:
            self.result = [(self.identifiers[parameters[0]],)]
        elif "issued_authorities" in sql:
            assert parameters[0] == self.value.target.service_id
            assert parameters[1] in (
                self.value.ingest_principal.principal_id,
                self.value.publisher_principal.principal_id,
            )
            self.result = [(self.issued_attempt,)]
        else:
            raise AssertionError("unexpected SQL boundary")

    def fetchall(self):
        return self.result


class Gate:
    """External gate proof producer double; proofs remain canonical domain records."""

    def __init__(self, value, purpose, ledger):
        self.value, self.purpose, self.ledger = value, purpose, ledger
        self.calls, self.ready = [], True
        principal = getattr(value, purpose + "_principal")
        self.proofs = tuple(
            CompositionAttemptProof(
                kind,
                value.attempt.attempt_sha256,
                value.attempt.activation_request_sha256,
                composition_attempt_epoch_subject(value.attempt),
                (principal,),
                digest(purpose + kind),
            )
            for kind in ("CLOSED_GATES", "QUIESCENCE")
        )

    def observe_closed(self, ledger, *, attempt, target, gate_id):
        assert ledger is self.ledger
        assert (attempt, target) == (self.value.attempt, self.value.target)
        assert "clickhouse-user:" + gate_id == getattr(self.value, self.purpose + "_principal").principal_id
        self.calls.append("observe_closed")
        return self.proofs

    def require_dispatch(self, ledger, dispatch):
        assert ledger is self.ledger and dispatch.intent == self.value
        self.calls.append("require_dispatch")
        if not self.ready:
            raise CompositionAdmissionError("test_publisher_closed")

    def close(self, attempt):
        assert attempt == self.value.attempt
        self.calls.append("close")
        return self.proofs[0]

    def prove_quiescence(self, attempt):
        assert attempt == self.value.attempt
        self.calls.append("prove_quiescence")
        return self.proofs[1]


@pytest.fixture
def case(monkeypatch):
    value = intent()
    cursor = Cursor(value)
    ledger = SimpleNamespace(
        cursor=cursor,
        schema="control",
        expected_service_id=SQL_SERVICE,
        require_transaction=lambda expected=None: 1,
        table=lambda name: "composition_" + name,
    )
    ingest, publisher = Gate(value, "ingest", ledger), Gate(value, "publisher", ledger)
    value = replace(value, closed_ingest_sha256=ingest.proofs[0].proof_sha256)
    ingest.value = publisher.value = value
    state = SimpleNamespace(
        value=value,
        cursor=cursor,
        ledger=ledger,
        ingest=ingest,
        publisher=publisher,
        parent=occurrence(),
        generation=value.generation,
        transactions=[],
        enrollments=[],
    )
    factory = object()

    @contextmanager
    def transaction(connection_factory, schema, service):
        assert connection_factory is factory and schema == "control"
        assert service == SQL_SERVICE and service != value.target.service_id
        state.transactions.append(service)
        yield ledger

    def enrollment(context, reference, attempt, target):
        assert context is ledger
        assert (reference, attempt, target) == (digest("enrollment original"), value.attempt, value.target)
        state.enrollments.append(reference)

    monkeypatch.setattr(publication, "composition_control_transaction", transaction)
    monkeypatch.setattr(publication, "require_attempt_enrollment_original", enrollment)
    monkeypatch.setattr(gate_queries, "require_clickhouse_gate_schema", lambda *args: None)
    state.authority = publication.ObservingSnapshotPublicationAuthority(
        read_active=lambda: state.parent,
        connection_factory=factory,
        control_schema="control",
        control_service_id=SQL_SERVICE,
        target=value.target,
        limits=value.limits,
        enrollment_sha256=digest("enrollment original"),
        load_generation=lambda *args: state.generation,
        publisher_gate=publisher,
        ingest_gate=ingest,
    )
    return state


def test_sql_service_pin_is_distinct_from_clickhouse_and_prepared_hash_selects_full_proof(case):
    authority, value = case.authority, case.value
    assert authority.ingest_authority(value.attempt) == value.ingest_principal
    authority.require_enrollment(value.attempt, value.target)
    prepared = authority.load_prepared(value.attempt, value.generation.record_sha256)
    assert prepared.to_bytes() == value.to_bytes()
    assert prepared.closed_ingest_sha256 == case.ingest.proofs[0].proof_sha256
    assert prepared.closed_ingest_sha256 != case.ingest.proofs[0].evidence_sha256
    assert case.transactions == [SQL_SERVICE] * 3
    assert case.ingest.calls == ["observe_closed"]


def test_ready_and_closure_use_supplied_ledger_and_exact_proof_hashes(case):
    authority, value = case.authority, case.value
    authority.require_ready(case.ledger, value)
    closure = authority.close_publisher(value)
    assert closure == SnapshotPublisherClosure(value.intent_sha256, *(p.proof_sha256 for p in case.publisher.proofs))
    authority.require_closure(case.ledger, value, closure)
    assert case.transactions == []
    assert case.publisher.calls == ["require_dispatch", "close", "prove_quiescence", "observe_closed"]
    assert len(case.enrollments) == 2


@pytest.mark.parametrize("field", ["intent_sha256", "closed_gates_sha256", "quiescence_sha256"])
def test_wrong_closure_original_is_rejected(case, field):
    correct = case.authority.close_publisher(case.value)
    with pytest.raises(CompositionAdmissionError, match="publisher_closure"):
        case.authority.require_closure(case.ledger, case.value, replace(correct, **{field: digest("foreign")}))


@pytest.mark.parametrize("change", ["limits", "generation", "ingest_proof", "principal"])
def test_ready_rejects_changed_prepared_originals_before_publisher_authorization(case, change):
    value = case.value
    if change == "limits":
        value = replace(value, limits=replace(value.limits, max_rows=value.limits.max_rows + 1))
    elif change == "generation":
        case.generation = replace(value.generation, content_sha256=digest("different content"))
    elif change == "ingest_proof":
        value = replace(value, closed_ingest_sha256=digest("unbacked"))
    else:
        value = replace(
            value,
            publisher_principal=replace(
                value.publisher_principal, principal_id="clickhouse-user:10000000-0000-4000-8000-000000000099"
            ),
        )
    with pytest.raises(CompositionAdmissionError):
        case.authority.require_ready(case.ledger, value)
    assert case.publisher.calls == []


@pytest.mark.parametrize("corruption", ["missing", "noncanonical", "index", "issued_attempt"])
def test_real_gate_binding_reader_rejects_corrupted_originals(case, corruption):
    key = gate_queries.ClickHouseGateBinding(case.value.attempt, case.value.target, "ingest").key
    if corruption == "missing":
        del case.cursor.documents[key]
    elif corruption == "noncanonical":
        case.cursor.documents[key] += b" "
    elif corruption == "index":
        case.cursor.identifiers[key] = "10000000-0000-4000-8000-000000000099"
    else:
        case.cursor.issued_attempt = digest("another attempt")
    with pytest.raises(CompositionAdmissionError):
        case.authority.ingest_authority(case.value.attempt)


def test_current_parent_allows_retiring_only_for_recovery_and_rejects_stale_epochs(case):
    assert case.authority.require_current(case.value, recovery=False) == case.parent
    case.parent = occurrence("RETIRING")
    with pytest.raises(CompositionAdmissionError):
        case.authority.require_current(case.value, recovery=False)
    assert case.authority.require_current(case.value, recovery=True) == case.parent
    case.parent = replace(
        case.parent,
        receipt=replace(
            case.parent.receipt,
            guard_epochs=tuple((guard, epoch + 1) for guard, epoch in case.parent.receipt.guard_epochs),
        ),
    )
    with pytest.raises(CompositionAdmissionError):
        case.authority.require_current(case.value, recovery=True)


def test_closed_publisher_blocks_ready_and_foreign_target_blocks_enrollment(case):
    case.publisher.ready = False
    with pytest.raises(CompositionAdmissionError, match="publisher_closed"):
        case.authority.require_ready(case.ledger, case.value)
    with pytest.raises(CompositionAdmissionError, match="enrollment"):
        case.authority.require_enrollment(case.value.attempt, replace(case.value.target, target_table="other"))
    assert case.transactions == []


def test_closure_rejects_evidence_hashes_instead_of_canonical_proof_hashes(case):
    wrong = SnapshotPublisherClosure(case.value.intent_sha256, *(p.evidence_sha256 for p in case.publisher.proofs))
    with pytest.raises(CompositionAdmissionError, match="publisher_closure"):
        case.authority.require_closure(case.ledger, case.value, wrong)


def test_closure_reopens_generation_and_load_prepared_rejects_foreign_reference(case):
    closure = case.authority.close_publisher(case.value)
    case.generation = replace(case.value.generation, rows=case.value.generation.rows + 1)
    with pytest.raises(CompositionAdmissionError, match="prepared_subject"):
        case.authority.require_closure(case.ledger, case.value, closure)
    assert "observe_closed" not in case.publisher.calls
    with pytest.raises(CompositionAdmissionError, match="prepared_subject"):
        case.authority.load_prepared(case.value.attempt, digest("foreign generation reference"))
