import hashlib
import json
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from uuid import UUID

import pytest

from dpone.adapters.mssql_native_chunk_retirement_authorization import NativeChunkRetirementJournalObserver
from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy, NativeChunkPlan
from dpone.contracts.mssql_native_parent_journal import (
    NativeParentAuthority,
    canonical_digest,
    settled_parent_authority,
)
from dpone.contracts.mssql_sqlclient_evidence_types import (
    SqlClientEvidenceKind,
    SqlClientEvidenceReceipt,
    evidence_name,
)
from dpone.contracts.mssql_sqlclient_native_chunk import SqlClientDirectoryCoordinate, bind_sqlclient_native_chunk
from dpone.contracts.mssql_sqlclient_native_chunk_receipt import (
    SqlClientInputCustody,
    bind_native_chunk_receipt,
    validate_native_chunk_receipt,
)
from dpone.contracts.mssql_sqlclient_stage_identity import SqlClientStageIdentity, stage_object_identity
from dpone.contracts.mssql_tds_create import TdsCreateObservedColumn, TdsCreateType
from dpone.contracts.mssql_tds_directory import directory_key
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity
from dpone.ports.mssql_native_chunk_retirement import (
    NativeChunkDropIntent,
    NativeChunkDropObservation,
    NativeChunkDropOutcome,
    NativeChunkLifecyclePhase,
    NativeChunkRetirementAuthorization,
    NativeChunkRetirementProgress,
    NativeChunkRetirementRequest,
    NativeChunkRetirementReservation,
    bind_native_chunk_absence,
    bind_native_chunk_capacity,
    bind_native_chunk_closed_directory,
    bind_native_chunk_containment,
    bind_native_chunk_drop,
    bind_native_chunk_lifecycle,
    bind_native_chunk_retired_terminal,
    bind_native_chunk_settlement,
    native_chunk_object_incarnation_digest,
)
from dpone.ports.mssql_native_chunk_retirement_authority import (
    _issue_native_chunk_retirement_authorization,
)
from dpone.services.mssql_native_chunk_retirement import NativeChunkRetirementService

PARENT_IDENTITY = NativeChunkPlan(
    "run",
    "target",
    "query",
    "window",
    "schema",
    "wire",
    transport=NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30),
).to_dict()
_AUTHORIZATION_OBSERVERS = {}
_DIRECTORY_DIGESTS = {}


class _Publication:
    def __init__(self, authority):
        self._authority = authority

    def authority(self):
        return self._authority


class _Journal:
    def __init__(self, data, authority):
        self.data = data
        self.publication = _Publication(authority)


def _life(request, phase, lifecycle_revision, directory_revision):
    return bind_native_chunk_lifecycle(
        phase=phase,
        projection_sha256=request.projection.projection_sha256,
        authorization_sha256=request.authorization.authorization_sha256,
        directory_key=request.projection.directory_key,
        directory_revision=directory_revision,
        authority_digest=request.authority.digest,
        authority_fence=request.authority.fence,
        lifecycle_revision=lifecycle_revision,
    )


def _projection():
    attempt = TdsAttemptIdentity(
        target_key="target",
        run_id="run",
        ordinal=0,
        attempt=0,
        database="db",
        schema="stage",
        table="chunk",
        owner_binding="a" * 64,
        file_sha256="b" * 64,
        implementation_sha256="c" * 64,
        plan_sha256=canonical_digest(PARENT_IDENTITY),
        policy_sha256="4" * 64,
    )
    column = TdsCreateObservedColumn(1, "id", TdsCreateType.BIGINT, False, 8, 19, 0, None)
    stage = SqlClientStageIdentity(
        database_id=5,
        database_name="db",
        database_guid=UUID(int=1),
        schema_id=6,
        schema_name="stage",
        object_id=7,
        table_name="chunk",
        create_date=datetime(2026, 1, 1),
        object_nonce=UUID(int=2),
        owner_binding="a" * 64,
        columns=(column,),
    )
    attempt_sha = attempt_identity_digest(attempt)
    receipt = SqlClientEvidenceReceipt(
        attempt_sha256=attempt_sha,
        kind=SqlClientEvidenceKind.VERIFICATION,
        relative_name=evidence_name(attempt_sha, SqlClientEvidenceKind.VERIFICATION, "d" * 64),
        payload_sha256="d" * 64,
        byte_count=10,
    )
    registration_receipt = SqlClientEvidenceReceipt(
        attempt_sha256=attempt_sha,
        kind=SqlClientEvidenceKind.REGISTRATION,
        relative_name=evidence_name(attempt_sha, SqlClientEvidenceKind.REGISTRATION, "e" * 64),
        payload_sha256="e" * 64,
        byte_count=10,
    )
    return bind_sqlclient_native_chunk(
        attempt=attempt,
        attempt_sha256=attempt_identity_digest(attempt),
        stage=stage,
        object_identity=stage_object_identity(stage),
        rows=1,
        encoded_bytes=8,
        file_sha256="b" * 64,
        typed_digest="f" * 64,
        typed_sum=0,
        registration_receipt=registration_receipt,
        verification_receipt=receipt,
        lifecycle_verification_sha256="d" * 64,
        lifecycle_revision=9,
        worker_build_sha256="1" * 64,
        implementation_sha256="c" * 64,
        helper_implementation_sha256="2" * 64,
        directory_key=directory_key(attempt),
        directory_coordinate=SqlClientDirectoryCoordinate(target_key="target", run_id="run", ordinal=0, attempt=0),
    )


@dataclass
class _State:
    progress: NativeChunkRetirementProgress
    calls: list[str]
    unknown_at: str | None = None

    def observe(self, request):
        self.calls.append("observe")
        return self.progress

    def seal_work(self, request):
        self.calls.append("seal")
        self.progress = replace(self.progress, work_sealed=True)
        return self.progress

    def require_containment(self, request):
        self.calls.append("require_containment")
        proof = _life(request, NativeChunkLifecyclePhase.CONTAINMENT_REQUIRED, 10, 2)
        self.progress = replace(self.progress, lifecycle=self.progress.lifecycle + (proof,))
        return self.progress

    def record_containment(self, request, proof):
        self.calls.append("contain")
        lifecycle = _life(request, NativeChunkLifecyclePhase.CONTAINED, 11, 3)
        self.progress = replace(self.progress, containment=proof, lifecycle=self.progress.lifecycle + (lifecycle,))
        return self.progress

    def reserve_retirement(self, request, reservation):
        self.calls.append("reserve")
        lifecycle = _life(request, NativeChunkLifecyclePhase.RETIREMENT_REQUIRED, 12, 4)
        self.progress = replace(
            self.progress, reservation=reservation, lifecycle=self.progress.lifecycle + (lifecycle,)
        )
        return self.progress

    def record_drop_intent(self, request, intent):
        self.calls.append("drop_intent")
        self.progress = replace(self.progress, drop_intent=intent, drop=None)
        return self.progress

    def record_settlement(self, request, proof):
        self.calls.append("settle")
        self.progress = replace(self.progress, settlement=proof)
        return self.progress

    def record_drop_outcome(self, request, proof):
        self.calls.append("drop_outcome")
        self.progress = replace(self.progress, drop=proof)
        return self.progress

    def record_retired(self, request, absence):
        self.calls.append("retired")
        reservation = self.progress.reservation
        lifecycle = _life(request, NativeChunkLifecyclePhase.RETIRED, 13, 5)
        terminal = bind_native_chunk_retired_terminal(
            projection_sha256=request.projection.projection_sha256,
            authorization_sha256=request.authorization.authorization_sha256,
            parent_authority_digest=request.authority.digest,
            operation_sha256=reservation.operation_sha256,
            absence_sha256=absence.proof_sha256,
            directory_revision=lifecycle.directory_revision,
            lifecycle_revision=lifecycle.lifecycle_revision,
            lifecycle_proof_sha256=lifecycle.proof_sha256,
        )
        self.progress = replace(
            self.progress, absence=absence, terminal=terminal, lifecycle=self.progress.lifecycle + (lifecycle,)
        )
        return self.progress

    def close_admission(self, request):
        self.calls.append("close")
        reservation, terminal = self.progress.reservation, self.progress.terminal
        directory = bind_native_chunk_closed_directory(
            projection_sha256=request.projection.projection_sha256,
            parent_authority_digest=request.authority.digest,
            operation_sha256=reservation.operation_sha256,
            terminal_sha256=terminal.proof_sha256,
        )
        self.progress = replace(self.progress, admission_closed=True, directory=directory)
        _DIRECTORY_DIGESTS[request.projection.projection_sha256] = directory.proof_sha256
        return self.progress

    def record_capacity(self, request, proof):
        self.calls.append("capacity")
        self.progress = replace(self.progress, capacity=proof)
        return self.progress


class _Effects:
    def __init__(
        self,
        *,
        drop=NativeChunkDropOutcome.SUCCEEDED,
        reconciled=NativeChunkDropOutcome.UNKNOWN,
        absent=True,
    ):
        self.calls = []
        self.drop_outcome = drop
        self.reconciled_outcome = reconciled
        self.absent = absent
        self.settlement_sha256 = "0" * 64

    def prove_containment(self, request):
        self.calls.append("contain")
        return bind_native_chunk_containment(
            projection_sha256=request.projection.projection_sha256,
            authorization_sha256=request.authorization.authorization_sha256,
            directory_key=request.projection.directory_key,
            directory_revision=3,
            authority_digest=request.authority.digest,
            authority_fence=request.authority.fence,
            lifecycle_revision=11,
            process_absence_sha256="3" * 64,
            credential_channel_absence_sha256="4" * 64,
        )

    def observe_exact_incarnation(self, request):
        self.calls.append("incarnation")
        return native_chunk_object_incarnation_digest(request)

    def drop_exact(self, request, reservation, intent):
        self.calls.append("drop")
        return bind_native_chunk_drop(
            operation_sha256=reservation.operation_sha256,
            effect_attempt=intent.effect_attempt,
            observation=NativeChunkDropObservation.INITIAL,
            outcome=self.drop_outcome,
        )

    def reconcile_drop(self, request, reservation, intent, prior):
        self.calls.append("reconcile")
        return bind_native_chunk_drop(
            operation_sha256=reservation.operation_sha256,
            effect_attempt=intent.effect_attempt,
            observation=NativeChunkDropObservation.RECONCILED,
            outcome=self.reconciled_outcome,
        )

    def settle_drop(self, request, reservation, drop, object_incarnation_sha256):
        self.calls.append("settlement")
        proof = bind_native_chunk_settlement(
            projection_sha256=request.projection.projection_sha256,
            authorization_sha256=request.authorization.authorization_sha256,
            operation_id=reservation.operation_id,
            operation_sha256=reservation.operation_sha256,
            effect_attempt=drop.effect_attempt,
            drop_proof_sha256=drop.proof_sha256,
            object_incarnation_sha256=object_incarnation_sha256,
            directory_key=request.projection.directory_key,
            directory_revision=4,
            authority_digest=request.authority.digest,
            authority_fence=request.authority.fence,
            local_settlement_sha256="a" * 64,
            remote_settlement_sha256="e" * 64,
            outcome=drop.outcome,
        )
        self.settlement_sha256 = proof.proof_sha256
        return proof

    def observe_absence(self, request):
        self.calls.append("absence")
        settlement_sha256 = self.settlement_sha256
        return bind_native_chunk_absence(
            object_incarnation_sha256=native_chunk_object_incarnation_digest(request),
            settlement_sha256=settlement_sha256,
            directory_revision=4,
            lifecycle_revision=12,
            absent=self.absent,
        )

    def observe_capacity(self, request):
        self.calls.append("capacity")
        return bind_native_chunk_capacity(
            directory_sha256=_DIRECTORY_DIGESTS[request.projection.projection_sha256], released=True
        )


def _journal_observer(projection, receipt, authority, *, identity=PARENT_IDENTITY):
    receipt_record = asdict(receipt)

    def digest(value):
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    data = {
        "version": 4,
        "identity": identity,
        "chunks": {
            "0": {
                "attempt": 0,
                "phase": "verified",
                "file": None,
                "receipt": receipt_record,
            }
        },
        "phase": "stage_complete",
        "complete": {
            "rows": receipt.rows,
            "receipt_digest": digest([receipt_record]),
            "metadata_digest": digest({}),
        },
        "observations": [],
        "completion_metadata": {},
        "rollback_history": [],
        "limits": None,
    }
    publication_receipt = {"generation": "receipt"}
    data["publication"] = {"prepared": {"stage": "bound"}}
    settled = settled_parent_authority(data, authority.kind, publication_receipt, authority.fence)
    data["publication"] = {
        "phase": authority.kind,
        "prepared": {"stage": "bound"},
        "receipt": publication_receipt,
        "authority": {
            "kind": settled.kind,
            "digest": settled.digest,
            "fence": settled.fence,
        },
        "chunk_retirements": [],
        "retirement_receipt": None,
        "checkpoint_receipt": None,
        "checkpoint_receipt_digest": None,
    }
    return NativeChunkRetirementJournalObserver(_Journal(data, settled))


def _request():
    projection = _projection()
    authority = NativeParentAuthority("published", "0" * 64, 3)
    custody = SqlClientInputCustody.bind(
        plan_sha256=projection.attempt.plan_sha256,
        target_id="target",
        run_id="run",
        window_fingerprint="window",
        attempt_id="run-0-0",
        ordinal=0,
        rows=1,
        encoded_bytes=8,
        file_sha256="b" * 64,
        typed_digest="f" * 64,
        durable_object_id="object",
        durable_location_sha256="9" * 64,
    )
    receipt = bind_native_chunk_receipt(
        projection=projection,
        plan_sha256=projection.attempt.plan_sha256,
        window_fingerprint="window",
        attempt_id="run-0-0",
        custody=custody,
    )
    observer = _journal_observer(projection, receipt, authority)
    authorization = observer.observe(projection)
    assert authorization is not None
    _AUTHORIZATION_OBSERVERS[projection.projection_sha256] = observer
    return NativeChunkRetirementRequest(projection, authorization)


def _service(state, effects):
    observer = _AUTHORIZATION_OBSERVERS[state.progress.projection_sha256]
    return NativeChunkRetirementService(state, effects, observer)


def _progress(request):
    return NativeChunkRetirementProgress(
        projection_sha256=request.projection.projection_sha256,
        parent_authority_digest=request.authority.digest,
        authorization_sha256=request.authorization.authorization_sha256,
        lifecycle=(_life(request, NativeChunkLifecyclePhase.VERIFIED, 9, 1),),
    )


def test_exact_order_produces_parent_compatible_receipt():
    request = _request()
    state = _State(_progress(request), [])
    effects = _Effects()
    receipt = _service(state, effects).retire(request)
    assert state.calls == [
        "observe",
        "seal",
        "require_containment",
        "contain",
        "reserve",
        "drop_intent",
        "drop_outcome",
        "settle",
        "retired",
        "close",
        "capacity",
    ]
    assert effects.calls == ["contain", "incarnation", "drop", "settlement", "absence", "capacity"]
    assert receipt.parent_authority_digest == request.authority.digest
    assert receipt.verification_receipt_sha256 == request.parent_verification_sha256
    assert receipt.attempt_id == "run-0-0"


@pytest.mark.parametrize("outcome", [NativeChunkDropOutcome.FAILED, NativeChunkDropOutcome.UNKNOWN])
def test_failed_or_unknown_drop_never_becomes_absence(outcome):
    request = _request()
    state = _State(_progress(request), [])
    effects = _Effects(drop=outcome)
    with pytest.raises(RuntimeError, match="drop_not_succeeded"):
        _service(state, effects).retire(request)
    expected_effects = ["contain", "incarnation", "drop"]
    if outcome is NativeChunkDropOutcome.FAILED:
        expected_effects.append("settlement")
    assert effects.calls == expected_effects
    assert state.progress.absence is None
    assert state.progress.reservation is not None
    assert (state.progress.settlement is not None) is (outcome is NativeChunkDropOutcome.FAILED)
    if outcome is NativeChunkDropOutcome.UNKNOWN:
        effects.calls.clear()
        with pytest.raises(RuntimeError, match="drop_not_succeeded"):
            _service(state, effects).retire(request)
        assert effects.calls == ["reconcile"]


def test_non_absence_retains_custody_and_blocks_terminal_transitions():
    request = _request()
    state = _State(_progress(request), [])
    effects = _Effects(absent=False)
    with pytest.raises(RuntimeError, match="absence_unproven"):
        _service(state, effects).retire(request)
    assert "retired" not in state.calls
    assert "close" not in state.calls


def test_suffix_replay_uses_persisted_proofs_and_performs_no_duplicate_drop():
    request = _request()
    completed = _State(_progress(request), [])
    _service(completed, _Effects()).retire(request)
    state = _State(completed.progress, [])
    effects = _Effects()
    _service(state, effects).retire(request)
    assert effects.calls == []
    assert state.calls == ["observe"]


def test_changed_parent_or_projection_is_rejected_before_effect():
    request = _request()
    state = _State(replace(_progress(request), parent_authority_digest="f" * 64), [])
    effects = _Effects()
    with pytest.raises(ValueError, match="authority_mismatch"):
        _service(state, effects).retire(request)
    assert effects.calls == []


def test_changed_parent_receipt_is_rejected_when_request_is_built():
    projection = _projection()
    bad = replace(_request().parent_receipt, rows=2)
    authorization = _journal_observer(projection, bad, NativeParentAuthority("aborted", "0" * 64, 4)).observe(
        projection
    )
    assert authorization is None


@pytest.mark.parametrize("field", ["stage", "evidence"])
def test_parent_stage_and_consumed_evidence_are_exact(field):
    request = _request()
    receipt = request.parent_receipt
    changed = (
        replace(receipt, stage_id="sqlclient-stage-v1:" + "f" * 64)
        if field == "stage"
        else replace(receipt, consumed_part_evidence={"sqlclient_projection_sha256": "f" * 64})
    )
    authorization = _journal_observer(request.projection, changed, request.authority).observe(request.projection)
    assert authorization is None


@pytest.mark.parametrize("replay", ("plan", "window"))
def test_retirement_rejects_self_consistent_cross_parent_replay(replay):
    request = _request()
    receipt = request.parent_receipt
    evidence = validate_native_chunk_receipt(receipt)
    custody_facts = asdict(evidence.input_custody)
    custody_facts.pop("custody_sha256")
    replacement = "f" * 64 if replay == "plan" else "other-window"
    custody_facts["plan_sha256" if replay == "plan" else "window_fingerprint"] = replacement
    custody = SqlClientInputCustody.bind(**custody_facts)
    rebound = replace(
        evidence,
        plan_sha256=replacement if replay == "plan" else evidence.plan_sha256,
        window_fingerprint=replacement if replay == "window" else evidence.window_fingerprint,
        input_custody=custody,
    )
    changed = replace(receipt, consumed_part_evidence=rebound.to_mapping())

    assert _journal_observer(request.projection, changed, request.authority).observe(request.projection) is None


def test_changed_exact_incarnation_blocks_drop():
    request = _request()
    state = _State(_progress(request), [])
    effects = _Effects()
    effects.observe_exact_incarnation = lambda request: "f" * 64
    with pytest.raises(ValueError, match="incarnation_mismatch"):
        _service(state, effects).retire(request)
    assert effects.calls == ["contain"]


class _LoseDropAcknowledgement(_State):
    def record_drop_outcome(self, request, proof):
        self.calls.append("drop_outcome_lost")
        raise TimeoutError("acknowledgement lost")


def test_lost_drop_ack_recovery_reconciles_and_never_resends_drop():
    request = _request()
    state = _LoseDropAcknowledgement(_progress(request), [])
    first_effects = _Effects()
    with pytest.raises(TimeoutError, match="lost"):
        _service(state, first_effects).retire(request)
    assert state.progress.drop_intent is not None
    assert state.progress.drop is None
    recovered = _State(state.progress, [])
    recovery_effects = _Effects(reconciled=NativeChunkDropOutcome.SUCCEEDED)
    _service(recovered, recovery_effects).retire(request)
    assert "drop" not in recovery_effects.calls
    assert recovery_effects.calls[0] == "reconcile"


def test_reconciled_no_effect_allows_one_explicit_second_effect_attempt():
    request = _request()
    # Build the durable ambiguity at the exact post-intent/pre-result seam.
    state = _State(_progress(request), [])
    service = _service(state, _Effects())
    original = state.record_drop_outcome

    def lose(request, proof):
        raise TimeoutError("lost")

    state.record_drop_outcome = lose
    with pytest.raises(TimeoutError):
        service.retire(request)
    state.record_drop_outcome = original
    effects = _Effects(reconciled=NativeChunkDropOutcome.NO_EFFECT)
    _service(state, effects).retire(request)
    assert effects.calls[:3] == ["reconcile", "incarnation", "drop"]
    assert state.progress.drop_intent.effect_attempt == 1


def test_second_attempt_no_effect_retains_custody_without_third_drop():
    request = _request()
    state = _State(_progress(request), [])
    effects = _Effects(drop=NativeChunkDropOutcome.UNKNOWN)
    with pytest.raises(RuntimeError):
        _service(state, effects).retire(request)
    state.progress = replace(
        state.progress,
        drop_intent=NativeChunkDropIntent(state.progress.reservation.operation_sha256, 1, "a" * 64),
        drop=bind_native_chunk_drop(
            operation_sha256=state.progress.reservation.operation_sha256,
            effect_attempt=1,
            observation=NativeChunkDropObservation.INITIAL,
            outcome=NativeChunkDropOutcome.UNKNOWN,
        ),
    )
    recovery = _Effects(reconciled=NativeChunkDropOutcome.NO_EFFECT)
    with pytest.raises(RuntimeError, match="drop_not_succeeded"):
        _service(state, recovery).retire(request)
    assert recovery.calls == ["reconcile"]


@pytest.mark.parametrize("kind", ["bool", "int", "enum"])
def test_exact_types_reject_bool_int_aliases(kind):
    request = _request()
    if kind == "bool":
        with pytest.raises(ValueError):
            replace(_progress(request), work_sealed=1)
    elif kind == "int":
        with pytest.raises(ValueError):
            replace(_progress(request).lifecycle[0], lifecycle_revision=True)
    else:
        with pytest.raises(ValueError):
            NativeChunkDropIntent("a" * 64, True, "b" * 64)


def test_cross_parent_authorization_substitution_is_rejected():
    request = _request()
    observer = _journal_observer(
        request.projection, request.parent_receipt, request.authority, identity={"run_id": "other"}
    )
    assert observer.observe(request.projection) is None


def test_free_authorization_construction_cannot_become_authority():
    request = _request()
    with pytest.raises(ValueError, match="retirement_invalid"):
        NativeChunkRetirementAuthorization(
            request.authority,
            request.authorization.parent_journal_sha256,
            request.authorization.parent_identity_sha256,
            request.projection.projection_sha256,
            request.parent_receipt,
            request.authorization.authorization_sha256,
        )


def test_service_rechecks_journal_observation_before_state_or_effects():
    request = _request()
    state, effects = _State(_progress(request), []), _Effects()

    class Missing:
        def verify(self, projection, authorization):
            return False

    with pytest.raises(ValueError, match="authorization_unobserved"):
        NativeChunkRetirementService(state, effects, Missing()).retire(request)
    assert state.calls == []
    assert effects.calls == []


def test_previously_issued_authorization_fails_after_journal_changes():
    request = _request()
    observer = _AUTHORIZATION_OBSERVERS[request.projection.projection_sha256]
    observer._journal.data["publication"]["phase"] = "prepared"
    state, effects = _State(_progress(request), []), _Effects()
    with pytest.raises(ValueError, match="authorization_unobserved"):
        NativeChunkRetirementService(state, effects, observer).retire(request)
    assert state.calls == []
    assert effects.calls == []


def test_imported_issuer_output_is_not_an_observer_held_authorization():
    request = _request()
    observer = _AUTHORIZATION_OBSERVERS[request.projection.projection_sha256]
    forged = _issue_native_chunk_retirement_authorization(
        authority=request.authority,
        parent_journal_sha256=request.authorization.parent_journal_sha256,
        parent_identity_sha256=request.authorization.parent_identity_sha256,
        projection_sha256=request.authorization.projection_sha256,
        chunk_receipt=request.parent_receipt,
    )
    assert forged == request.authorization
    assert not observer.verify(request.projection, forged)
    forged_request = NativeChunkRetirementRequest(request.projection, forged)
    state, effects = _State(_progress(request), []), _Effects()
    with pytest.raises(ValueError, match="authorization_unobserved"):
        NativeChunkRetirementService(state, effects, observer).retire(forged_request)
    assert state.calls == []
    assert effects.calls == []


@pytest.mark.parametrize(
    "mutation",
    ["missing_complete", "missing_publication_field", "changed_authority", "wrong_parent_phase"],
)
def test_incomplete_or_changed_v4_snapshot_is_never_authority(mutation):
    request = _request()
    observer = _AUTHORIZATION_OBSERVERS[request.projection.projection_sha256]
    data = observer._journal.data
    if mutation == "missing_complete":
        del data["complete"]
    elif mutation == "missing_publication_field":
        del data["publication"]["prepared"]
    elif mutation == "changed_authority":
        data["publication"]["authority"]["digest"] = "f" * 64
    else:
        data["phase"] = "loading"
    assert observer.observe(request.projection) is None
    assert not observer.verify(request.projection, request.authorization)


@pytest.mark.parametrize("mutation", ["chunk_phase", "complete_rows"])
def test_consistent_rehash_cannot_hide_changed_stage_authority(mutation):
    request = _request()
    observer = _AUTHORIZATION_OBSERVERS[request.projection.projection_sha256]
    data = observer._journal.data
    if mutation == "chunk_phase":
        data["chunks"]["0"]["phase"] = "staging"
    else:
        data["complete"]["rows"] += 1
    publication = data["publication"]
    current = publication["authority"]
    rehashed = settled_parent_authority(
        data,
        current["kind"],
        publication["receipt"],
        current["fence"],
    )
    publication["authority"] = asdict(rehashed)
    state, effects = _State(_progress(request), []), _Effects()
    with pytest.raises(ValueError, match="authorization_unobserved"):
        NativeChunkRetirementService(state, effects, observer).retire(request)
    assert state.calls == []
    assert effects.calls == []


@pytest.mark.parametrize("invalid_version", [4.0, True])
def test_observer_rejects_non_exact_v4_version(invalid_version):
    request = _request()
    observer = _AUTHORIZATION_OBSERVERS[request.projection.projection_sha256]
    observer._journal.data["version"] = invalid_version
    assert observer.observe(request.projection) is None
    assert not observer.verify(request.projection, request.authorization)


def test_observer_rejects_consistently_rehashed_empty_completion():
    request = _request()
    observer = _AUTHORIZATION_OBSERVERS[request.projection.projection_sha256]
    data = observer._journal.data
    data["chunks"] = {}
    data["complete"] = {
        "rows": 0,
        "receipt_digest": hashlib.sha256(b"[]").hexdigest(),
        "metadata_digest": hashlib.sha256(b"{}").hexdigest(),
    }
    publication = data["publication"]
    current = publication["authority"]
    publication["authority"] = asdict(
        settled_parent_authority(data, current["kind"], publication["receipt"], current["fence"])
    )
    assert observer.observe(request.projection) is None
    assert not observer.verify(request.projection, request.authorization)


@pytest.mark.parametrize("allowed_callbacks", range(16))
def test_authorization_revocation_stops_at_every_operation_boundary(allowed_callbacks):
    request = _request()
    state, effects = _State(_progress(request), []), _Effects()
    delegate = _AUTHORIZATION_OBSERVERS[request.projection.projection_sha256]

    class RevokingObserver:
        def __init__(self):
            self.verifications = 0

        def verify(self, projection, authorization):
            self.verifications += 1
            return self.verifications <= allowed_callbacks and delegate.verify(projection, authorization)

    observer = RevokingObserver()
    with pytest.raises(ValueError, match="authorization_unobserved"):
        NativeChunkRetirementService(state, effects, observer).retire(request)
    assert len(state.calls) + len(effects.calls) == allowed_callbacks
    assert observer.verifications == allowed_callbacks + 1


@pytest.mark.parametrize(
    "evidence_name,field,value",
    [
        ("lifecycle", "directory_revision", 99),
        ("containment", "process_absence_sha256", "f" * 64),
        ("drop", "outcome", NativeChunkDropOutcome.UNKNOWN),
        ("absence", "settlement_sha256", "f" * 64),
        ("directory", "terminal_sha256", "f" * 64),
        ("capacity", "directory_sha256", "f" * 64),
    ],
)
def test_self_binding_evidence_rejects_field_mutation(evidence_name, field, value):
    request = _request()
    completed = _State(_progress(request), [])
    _service(completed, _Effects()).retire(request)
    evidence = (
        completed.progress.lifecycle[-1] if evidence_name == "lifecycle" else getattr(completed.progress, evidence_name)
    )
    with pytest.raises(ValueError, match="retirement_invalid"):
        replace(evidence, **{field: value})


def test_settlement_and_absence_require_exact_retirement_revision():
    request = _request()
    completed = _State(_progress(request), [])
    _service(completed, _Effects()).retire(request)
    settlement = completed.progress.settlement
    changed = bind_native_chunk_settlement(
        **{
            name: (settlement.directory_revision + 1 if name == "directory_revision" else getattr(settlement, name))
            for name in settlement.__dataclass_fields__
            if name != "proof_sha256"
        }
    )
    progress = replace(completed.progress, settlement=changed)
    with pytest.raises(ValueError, match="settlement_binding"):
        _service(_State(progress, []), _Effects()).retire(request)
    with pytest.raises(ValueError, match="retirement_invalid"):
        replace(completed.progress.absence, lifecycle_revision=11)


def test_initial_drop_cannot_return_reconciled_observation():
    request = _request()

    class ReconciledInitial(_Effects):
        def drop_exact(self, request, reservation, intent):
            return bind_native_chunk_drop(
                operation_sha256=reservation.operation_sha256,
                effect_attempt=intent.effect_attempt,
                observation=NativeChunkDropObservation.RECONCILED,
                outcome=NativeChunkDropOutcome.SUCCEEDED,
            )

    with pytest.raises(ValueError, match="drop_binding"):
        _service(_State(_progress(request), []), ReconciledInitial()).retire(request)


def test_terminal_requires_exact_retired_lifecycle_revision_and_digest():
    request = _request()
    completed = _State(_progress(request), [])
    _service(completed, _Effects()).retire(request)
    terminal = completed.progress.terminal
    changed = bind_native_chunk_retired_terminal(
        **{
            name: (terminal.lifecycle_revision + 1 if name == "lifecycle_revision" else getattr(terminal, name))
            for name in terminal.__dataclass_fields__
            if name != "proof_sha256"
        }
    )
    progress = replace(completed.progress, terminal=changed)
    with pytest.raises(ValueError, match="terminal_binding"):
        _service(_State(progress, []), _Effects()).retire(request)


@pytest.mark.parametrize(
    "mutation,error",
    [
        ("reservation", "reservation_mismatch"),
        ("drop", "drop_binding"),
        ("settlement", "settlement_binding"),
        ("absence", "absence_binding"),
        ("terminal", "terminal_binding"),
        ("directory", "directory_binding"),
        ("capacity", "capacity_binding"),
    ],
)
def test_recovered_nested_binding_mutations_fail_before_effect(mutation, error):
    request = _request()
    complete = _State(_progress(request), [])
    _service(complete, _Effects()).retire(request)
    progress = complete.progress
    if mutation == "reservation":
        progress = replace(
            progress,
            reservation=NativeChunkRetirementReservation(UUID(int=9), "f" * 64),
        )
    elif mutation == "drop":
        with pytest.raises(ValueError, match="retirement_invalid"):
            replace(progress.drop, operation_sha256="f" * 64)
        return
    elif mutation == "settlement":
        with pytest.raises(ValueError, match="retirement_invalid"):
            replace(progress.settlement, object_incarnation_sha256="f" * 64)
        return
    elif mutation == "absence":
        with pytest.raises(ValueError, match="retirement_invalid"):
            replace(progress.absence, object_incarnation_sha256="f" * 64)
        return
    elif mutation == "terminal":
        with pytest.raises(ValueError, match="retirement_invalid"):
            replace(progress.terminal, parent_authority_digest="f" * 64)
        return
    elif mutation == "directory":
        with pytest.raises(ValueError, match="retirement_invalid"):
            replace(progress.directory, terminal_sha256="f" * 64)
        return
    else:
        with pytest.raises(ValueError, match="retirement_invalid"):
            replace(progress.capacity, directory_sha256="f" * 64)
        return
    effects = _Effects()
    with pytest.raises(ValueError, match=error):
        _service(_State(progress, []), effects).retire(request)
    assert effects.calls == []
