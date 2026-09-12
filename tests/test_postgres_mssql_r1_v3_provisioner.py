from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from dpone.adapters.mssql_r1_v3_authority_import import (
    MssqlGenerationAuthorityImporterAdapter,
    MssqlR1AuthorityImportTargetV1,
)
from dpone.adapters.mssql_r1_v3_control import MssqlR1ControlError, MssqlR1ControlStateV1
from dpone.adapters.mssql_r1_v3_registration import MssqlTargetRegistrationProvisionerAdapter
from dpone.contracts.mssql_r1_v3_authority import (
    MssqlGenerationAuthorityPurposeV2,
    MssqlGenerationAuthorityRefV2,
    MssqlGenerationAuthoritySetIssuancePayloadV1,
    MssqlGenerationAuthoritySetV2,
    MssqlRevokedRegistrationCompletionOverridePayloadV1,
    MssqlSignedPayloadKindV1,
    MssqlSignedPayloadVerificationV1,
    SignedGenerationAuthoritySetCommandV1,
    SignedRevokedOverrideCommandV1,
)
from dpone.contracts.mssql_r1_v3_control import MssqlR1ControlOperationV1, MssqlR1ControlReceiptV1
from dpone.contracts.mssql_r1_v3_control_proof import (
    MssqlR1ControlFreshProofV1,
    MssqlR1WriterHeadControlObservationV1,
)
from dpone.contracts.mssql_r1_v3_identity import canonical_identifier_digest
from dpone.contracts.mssql_r1_v3_registration import (
    MssqlTargetRegistrationPayloadV1,
    MssqlTargetRegistrationVerificationV1,
    RegistrationActionV1,
    SignedTargetRegistrationCommandV1,
)

NOW = datetime(2026, 9, 4, 12, tzinfo=UTC)
D1 = hashlib.sha256(b"one").digest()
D2 = hashlib.sha256(b"two").digest()
D3 = hashlib.sha256(b"three").digest()
D4 = hashlib.sha256(b"four").digest()
D5 = hashlib.sha256(b"five").digest()
D6 = hashlib.sha256(b"six").digest()
SCHEMA_DIGEST = hashlib.sha256(b"schema-v3").digest()
PERMISSION_DIGEST = hashlib.sha256(b"permission-v3").digest()
HEAD_DIGEST = hashlib.sha256(b"head-v3").digest()
REGISTRATION_ID = UUID("11111111-1111-4111-8111-111111111111")
BINDING_ID = UUID("22222222-2222-4222-8222-222222222222")
OBJECT_ID = UUID("33333333-3333-4333-8333-333333333333")
RECOVERY_ID = UUID("44444444-4444-4444-8444-444444444444")


def _registration(
    *,
    action: RegistrationActionV1 = RegistrationActionV1.INITIAL,
    registration_id: UUID = REGISTRATION_ID,
    predecessor: UUID | None = None,
    expected_revision: int | None = None,
) -> MssqlTargetRegistrationPayloadV1:
    return MssqlTargetRegistrationPayloadV1(
        registration_id=registration_id,
        registration_action=action,
        predecessor_registration_id=predecessor,
        expected_active_registration_revision=expected_revision,
        issued_at=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
        nonce=b"n" * 16,
        profile_id="postgres-mssql-r1",
        capability_tuple_digest=D1,
        resolved_profile_digest=D2,
        route_source_authority_sha256=D3,
        target_object_profile="ordinary_disk_rowstore_v1",
        catalog_projection_version="dpone-mssql-target-catalog-v1",
        revocation_revision=0,
        target_binding_uuid=BINDING_ID,
        target_object_uuid=OBJECT_ID,
        recovery_domain_uuid=RECOVERY_ID,
        recovery_domain_epoch=1,
        server_instance_identity_sha256=D4,
        database_guid=UUID("55555555-5555-4555-8555-555555555555"),
        database_family_guid=UUID("66666666-6666-4666-8666-666666666666"),
        recovery_fork_guid=UUID("77777777-7777-4777-8777-777777777777"),
        database_name_digest=canonical_identifier_digest("warehouse"),
        schema_name_digest=canonical_identifier_digest("dbo"),
        object_name_digest=canonical_identifier_digest("orders"),
        database_name="warehouse",
        schema_name="dbo",
        object_name="orders",
        object_id=42,
        physical_generation_uuid=UUID("88888888-8888-4888-8888-888888888888"),
        catalog_contract_digest=D5,
        target_contract_revision=1,
    )


def _registration_verification(
    command: SignedTargetRegistrationCommandV1,
) -> MssqlTargetRegistrationVerificationV1:
    return MssqlTargetRegistrationVerificationV1(
        registration_payload_digest=hashlib.sha256(command.payload_bytes).digest(),
        signature_bundle_digest=hashlib.sha256(command.sigstore_bundle).digest(),
        signer_identity_digest=D1,
        trusted_root_digest=D2,
        cosign_policy_digest=D3,
        verifier_version="test-verifier-v1",
        verified_at=NOW,
        certificate_identity_digest=D4,
        certificate_issuer_digest=D5,
        payload_bytes=command.payload_bytes,
    )


def _command(payload: MssqlTargetRegistrationPayloadV1) -> SignedTargetRegistrationCommandV1:
    return SignedTargetRegistrationCommandV1(payload.canonical_bytes, b"detached-registration-bundle")


@dataclass
class _Session:
    events: list[str]
    commit_error: bool = False
    close_error: bool = False

    def begin(self) -> object:
        self.events.append("begin")
        return self

    def assert_active(self, handle: object) -> None:
        assert handle is self
        self.events.append("assert_active")

    def commit(self, handle: object) -> None:
        assert handle is self
        self.events.append("commit")
        if self.commit_error:
            raise ConnectionError("commit response lost")

    def rollback(self, handle: object) -> None:
        assert handle is self
        self.events.append("rollback")

    def close(self) -> None:
        self.events.append("close")
        if self.close_error:
            raise ConnectionError("broken session close")


class _Sessions:
    def __init__(
        self,
        events: list[str],
        *,
        first_commit_error: bool = False,
        first_close_error: bool = False,
    ) -> None:
        self.events = events
        self.first_commit_error = first_commit_error
        self.first_close_error = first_close_error
        self.open_count = 0

    def open(self) -> _Session:
        self.open_count += 1
        self.events.append(f"open:{self.open_count}")
        return _Session(
            self.events,
            self.first_commit_error and self.open_count == 1,
            self.first_close_error and self.open_count == 1,
        )


class _Verifier:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def verify_registration(self, command: SignedTargetRegistrationCommandV1) -> MssqlTargetRegistrationVerificationV1:
        self.events.append("verify_registration")
        return _registration_verification(command)

    def verify_authority_set(self, command: SignedGenerationAuthoritySetCommandV1) -> MssqlSignedPayloadVerificationV1:
        self.events.append("verify_authority")
        return _signed_verification(command, MssqlSignedPayloadKindV1.GENERATION_AUTHORITY_SET)

    def verify_revoked_override(self, command: SignedRevokedOverrideCommandV1) -> MssqlSignedPayloadVerificationV1:
        self.events.append("verify_override")
        return _signed_verification(command, MssqlSignedPayloadKindV1.REVOKED_REGISTRATION_OVERRIDE)


class _RegistrationBackend:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.proof: MssqlR1ControlFreshProofV1 | None = None
        self.before: MssqlR1ControlStateV1 | None = None
        self.physical_assertion_calls = 0
        self.physical_assertion_error_on_call: int | None = None

    def lock_physical(self, handle: object, digest: bytes) -> None:
        self.events.append("lock_physical")

    def lock_binding(self, handle: object, binding: UUID) -> None:
        self.events.append("lock_binding")

    def install_and_attest_schema(self, handle: object) -> bytes:
        self.events.append("install_schema")
        return SCHEMA_DIGEST

    def find_control_proof(self, handle: object, key: bytes) -> MssqlR1ControlFreshProofV1 | None:
        self.events.append("find_receipt")
        return self.proof

    def assert_physical_target(self, handle: object, payload: MssqlTargetRegistrationPayloadV1) -> None:
        self.events.append("assert_physical")
        self.physical_assertion_calls += 1
        if self.physical_assertion_calls == self.physical_assertion_error_on_call:
            raise MssqlR1ControlError("postgres_mssql_r1.physical_target_conflict")

    def read_registration_state(self, handle: object, binding: UUID) -> MssqlR1ControlStateV1 | None:
        self.events.append("read_state")
        return self.before

    def create_initial_registration(self, handle: object, payload: object, verification: object) -> None:
        self.events.append("create_registration")

    def rotate_registration(self, handle: object, payload: object, verification: object) -> None:
        self.events.append("rotate_registration")

    def attach_protected_properties(self, handle: object, payload: object) -> None:
        self.events.append("attach_properties")

    def initialize_heads(self, handle: object, payload: object) -> None:
        self.events.append("initialize_heads")

    def apply_and_attest_permissions(self, handle: object) -> bytes:
        self.events.append("apply_permissions")
        return PERMISSION_DIGEST

    def read_committed_candidate(self, handle: object, binding: UUID) -> MssqlR1ControlStateV1:
        self.events.append("read_candidate")
        revision = 1 if self.before is None else self.before.active_registration_revision + 1
        registration_id = REGISTRATION_ID if self.before is None else UUID("99999999-9999-4999-8999-999999999999")
        return _state(registration_id, revision)

    def read_server_time(self, handle: object) -> datetime:
        self.events.append("read_server_time")
        return NOW

    def append_control_receipt(self, handle: object, receipt: MssqlR1ControlReceiptV1) -> None:
        self.events.append("append_receipt")
        self.proof = _proof(receipt)

    def prove_control_state(self, handle: object, key: bytes) -> MssqlR1ControlFreshProofV1 | None:
        self.events.append("prove_state")
        return self.proof

    def probe_registration_fresh(self, handle: object, registration_id: UUID) -> MssqlR1ControlFreshProofV1 | None:
        self.events.append("probe_registration")
        return self.proof


def _state(registration_id: UUID, revision: int) -> MssqlR1ControlStateV1:
    payload = _registration(registration_id=registration_id)
    return MssqlR1ControlStateV1(
        registration_id=registration_id,
        active_registration_revision=revision,
        physical_authority_digest=payload.physical_identity.registered_physical_authority_digest,
        schema_contract_digest=SCHEMA_DIGEST,
        permission_contract_digest=PERMISSION_DIGEST,
        writer_head=MssqlR1WriterHeadControlObservationV1(1, HEAD_DIGEST),
    )


def _proof(receipt: MssqlR1ControlReceiptV1) -> MssqlR1ControlFreshProofV1:
    typed = receipt
    return MssqlR1ControlFreshProofV1(
        receipt=typed,
        observed_registration_id=typed.registration_id,
        observed_active_registration_revision=typed.committed_active_registration_revision,
        observed_physical_authority_digest=typed.registered_physical_authority_digest,
        observed_schema_contract_digest=typed.schema_contract_digest,
        observed_permission_contract_digest=typed.permission_contract_digest,
        observed_writer_head=MssqlR1WriterHeadControlObservationV1(
            typed.observed_writer_head_revision, typed.observed_writer_head_digest
        ),
        observed_authority_set_digest=typed.affected_authority_set_digest,
        observed_override_digest=typed.affected_override_payload_digest,
    )


def _provisioner(
    events: list[str],
    backend: _RegistrationBackend,
    sessions: _Sessions,
) -> MssqlTargetRegistrationProvisionerAdapter:
    return MssqlTargetRegistrationProvisionerAdapter(
        verifier=_Verifier(events),
        session_factory=sessions,
        backend=backend,
        schema_contract_digest=SCHEMA_DIGEST,
        permission_contract_digest=PERMISSION_DIGEST,
        initial_writer_head=MssqlR1WriterHeadControlObservationV1(1, HEAD_DIGEST),
        clock=lambda: NOW,
        receipt_id_factory=lambda: UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
    )


def test_initial_provision_verifies_before_io_and_uses_one_ordered_transaction() -> None:
    events: list[str] = []
    backend = _RegistrationBackend(events)
    sessions = _Sessions(events)

    receipt = _provisioner(events, backend, sessions).provision(_command(_registration()))

    assert receipt.operation is MssqlR1ControlOperationV1.PROVISION
    assert events == [
        "verify_registration",
        "open:1",
        "begin",
        "assert_active",
        "lock_physical",
        "lock_binding",
        "assert_physical",
        "install_schema",
        "find_receipt",
        "read_state",
        "create_registration",
        "attach_properties",
        "initialize_heads",
        "apply_permissions",
        "read_candidate",
        "read_server_time",
        "append_receipt",
        "prove_state",
        "commit",
        "close",
    ]


def test_rotation_preserves_writer_head_and_requires_exact_predecessor() -> None:
    events: list[str] = []
    backend = _RegistrationBackend(events)
    backend.before = _state(REGISTRATION_ID, 1)
    successor = UUID("99999999-9999-4999-8999-999999999999")
    payload = _registration(
        action=RegistrationActionV1.ROTATE,
        registration_id=successor,
        predecessor=REGISTRATION_ID,
        expected_revision=1,
    )

    receipt = _provisioner(events, backend, _Sessions(events)).provision(_command(payload))

    assert receipt.operation is MssqlR1ControlOperationV1.REGISTRATION_ROTATE
    assert receipt.expected_writer_head_digest == HEAD_DIGEST
    assert "initialize_heads" not in events
    assert events.index("lock_physical") < events.index("lock_binding") < events.index("rotate_registration")


def test_rotation_replay_survives_active_head_already_at_successor() -> None:
    events: list[str] = []
    backend = _RegistrationBackend(events)
    backend.before = _state(REGISTRATION_ID, 1)
    successor = UUID("99999999-9999-4999-8999-999999999999")
    command = _command(
        _registration(
            action=RegistrationActionV1.ROTATE,
            registration_id=successor,
            predecessor=REGISTRATION_ID,
            expected_revision=1,
        )
    )
    provisioner = _provisioner(events, backend, _Sessions(events))
    first = provisioner.provision(command)
    backend.before = _state(successor, 2)
    events.clear()

    second = provisioner.provision(command)

    assert second == first
    assert "read_state" not in events
    assert "rotate_registration" not in events


def test_exact_existing_receipt_replays_without_mutation() -> None:
    events: list[str] = []
    backend = _RegistrationBackend(events)
    provisioner = _provisioner(events, backend, _Sessions(events))
    command = _command(_registration())
    first = provisioner.provision(command)
    backend.before = _state(REGISTRATION_ID, 1)
    events.clear()

    second = provisioner.provision(command)

    assert second == first
    assert "create_registration" not in events
    assert events[-2:] == ["commit", "close"]


def test_conflicting_existing_receipt_blocks_and_rolls_back() -> None:
    events: list[str] = []
    backend = _RegistrationBackend(events)
    provisioner = _provisioner(events, backend, _Sessions(events))
    provisioner.provision(_command(_registration()))
    events.clear()
    conflicting = _command(_registration(registration_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")))

    with pytest.raises(MssqlR1ControlError, match="control_receipt_conflict"):
        provisioner.provision(conflicting)

    assert "rollback" in events
    assert "create_registration" not in events


def test_lost_commit_response_uses_fresh_target_only_proof() -> None:
    events: list[str] = []
    backend = _RegistrationBackend(events)
    sessions = _Sessions(events, first_commit_error=True)

    receipt = _provisioner(events, backend, sessions).provision(_command(_registration()))

    assert receipt.registration_id == REGISTRATION_ID
    assert sessions.open_count == 2
    assert events[-9:] == [
        "open:2",
        "begin",
        "assert_active",
        "lock_physical",
        "lock_binding",
        "assert_physical",
        "prove_state",
        "commit",
        "close",
    ]


def test_broken_old_session_close_cannot_skip_fresh_commit_probe() -> None:
    events: list[str] = []
    backend = _RegistrationBackend(events)
    sessions = _Sessions(events, first_commit_error=True, first_close_error=True)

    receipt = _provisioner(events, backend, sessions).provision(_command(_registration()))

    assert receipt.registration_id == REGISTRATION_ID
    assert sessions.open_count == 2
    assert events.count("prove_state") == 2


def test_fresh_commit_probe_rejects_changed_physical_target_as_unknown() -> None:
    events: list[str] = []
    backend = _RegistrationBackend(events)
    backend.physical_assertion_error_on_call = 2
    sessions = _Sessions(events, first_commit_error=True)

    with pytest.raises(MssqlR1ControlError, match="control_commit_outcome_unknown"):
        _provisioner(events, backend, sessions).provision(_command(_registration()))

    assert sessions.open_count == 2
    assert backend.physical_assertion_calls == 2


def test_missing_fresh_proof_after_lost_commit_is_unknown_not_success() -> None:
    events: list[str] = []
    backend = _RegistrationBackend(events)
    sessions = _Sessions(events, first_commit_error=True)
    original = backend.prove_control_state
    calls = 0

    def disappear(handle: object, key: bytes) -> MssqlR1ControlFreshProofV1 | None:
        nonlocal calls
        calls += 1
        return original(handle, key) if calls == 1 else None

    backend.prove_control_state = disappear  # type: ignore[method-assign]

    with pytest.raises(MssqlR1ControlError, match="outcome_unknown"):
        _provisioner(events, backend, sessions).provision(_command(_registration()))


def test_verification_binding_failure_happens_before_target_session() -> None:
    events: list[str] = []
    backend = _RegistrationBackend(events)
    sessions = _Sessions(events)
    command = _command(_registration())

    class _WrongVerifier(_Verifier):
        def verify_registration(self, ignored: object) -> MssqlTargetRegistrationVerificationV1:
            other = _command(_registration(registration_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")))
            return _registration_verification(other)

    provisioner = MssqlTargetRegistrationProvisionerAdapter(
        verifier=_WrongVerifier(events),
        session_factory=sessions,
        backend=backend,
        schema_contract_digest=SCHEMA_DIGEST,
        permission_contract_digest=PERMISSION_DIGEST,
        initial_writer_head=MssqlR1WriterHeadControlObservationV1(1, HEAD_DIGEST),
        clock=lambda: NOW,
    )

    with pytest.raises(Exception, match="signed command"):
        provisioner.provision(command)
    assert sessions.open_count == 0


def test_expired_registration_blocks_before_target_session() -> None:
    events: list[str] = []
    backend = _RegistrationBackend(events)
    sessions = _Sessions(events)
    provisioner = MssqlTargetRegistrationProvisionerAdapter(
        verifier=_Verifier(events),
        session_factory=sessions,
        backend=backend,
        schema_contract_digest=SCHEMA_DIGEST,
        permission_contract_digest=PERMISSION_DIGEST,
        initial_writer_head=MssqlR1WriterHeadControlObservationV1(1, HEAD_DIGEST),
        clock=lambda: NOW + timedelta(days=1),
    )

    with pytest.raises(MssqlR1ControlError, match="registration_not_current"):
        provisioner.provision(_command(_registration()))
    assert sessions.open_count == 0


def test_registration_expiring_inside_transaction_rolls_back() -> None:
    events: list[str] = []
    backend = _RegistrationBackend(events)
    backend.read_server_time = lambda handle: NOW + timedelta(days=1)  # type: ignore[method-assign]

    with pytest.raises(MssqlR1ControlError, match="expired_during_provision"):
        _provisioner(events, backend, _Sessions(events)).provision(_command(_registration()))

    assert "rollback" in events
    assert "append_receipt" not in events


def _authority_set() -> MssqlGenerationAuthoritySetV2:
    ref = MssqlGenerationAuthorityRefV2(
        authority_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        purpose=MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER,
        effect_key=D1,
        source_snapshot_digest=D2,
        artifact_set_digest=D3,
        mutation_plan_digest=D4,
        expected_writer_generation=None,
        candidate_writer_generation=1,
        expected_head_revision=None,
        candidate_head_revision=1,
        recovery_identity_digest=D5,
    )
    return MssqlGenerationAuthoritySetV2((ref,))


def _authority_command() -> SignedGenerationAuthoritySetCommandV1:
    authority_set = _authority_set()
    payload = MssqlGenerationAuthoritySetIssuancePayloadV1(
        issuance_id=UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
        target_binding_uuid=BINDING_ID,
        registration_id=REGISTRATION_ID,
        registration_payload_digest=_registration().payload_digest,
        registration_revocation_revision=0,
        issued_at=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
        nonce=b"a" * 16,
        authority_set_bytes=authority_set.canonical_bytes,
        authority_set_digest=authority_set.digest,
    )
    return SignedGenerationAuthoritySetCommandV1(payload.canonical_bytes, b"authority-bundle")


def _override_command() -> SignedRevokedOverrideCommandV1:
    payload = MssqlRevokedRegistrationCompletionOverridePayloadV1(
        override_id=UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"),
        target_binding_uuid=BINDING_ID,
        effect_key=D1,
        registration_id=REGISTRATION_ID,
        registration_payload_digest=_registration().payload_digest,
        revocation_receipt_digest=D2,
        sealed_intent_digest=D3,
        artifact_set_digest=D4,
        mutation_plan_digest=D5,
        expected_operation_epoch=2,
        expected_operation_projection_revision=3,
        expected_verification_policy_digest=D6,
        expires_at=NOW + timedelta(hours=1),
    )
    return SignedRevokedOverrideCommandV1(payload.canonical_bytes, b"override-bundle")


def _signed_verification(
    command: SignedGenerationAuthoritySetCommandV1 | SignedRevokedOverrideCommandV1,
    kind: MssqlSignedPayloadKindV1,
) -> MssqlSignedPayloadVerificationV1:
    return MssqlSignedPayloadVerificationV1(
        payload_kind=kind,
        payload_digest=hashlib.sha256(command.payload_bytes).digest(),
        signature_bundle_digest=hashlib.sha256(command.sigstore_bundle).digest(),
        signer_identity_digest=D1,
        trusted_root_digest=D2,
        verification_policy_digest=D3,
        verifier_version="test-verifier-v1",
        verified_at=NOW,
        payload_bytes=command.payload_bytes,
    )


class _AuthorityBackend:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.proof: MssqlR1ControlFreshProofV1 | None = None
        self.authority_reproof: tuple[object, ...] | None = None
        self.override_reproof: tuple[object, ...] | None = None
        self.expected_authority_set = _authority_set()
        self.authority_reproof_calls = 0

    def lock_physical(self, handle: object, digest: bytes) -> None:
        self.events.append("lock_physical")

    def lock_binding(self, handle: object, binding: UUID) -> None:
        self.events.append("lock_binding")

    def lock_and_reprove_authority_set_effect(
        self,
        handle: object,
        target: MssqlR1AuthorityImportTargetV1,
        payload: MssqlGenerationAuthoritySetIssuancePayloadV1,
        authority_set: MssqlGenerationAuthoritySetV2,
    ) -> None:
        self.events.append("lock_operation_artifacts")
        self.authority_reproof_calls += 1
        self.authority_reproof = (target, payload, authority_set)
        if authority_set != self.expected_authority_set:
            raise MssqlR1ControlError("postgres_mssql_r1.authority_effect_coordinates_conflict")

    def lock_and_reprove_revoked_override_effect(
        self,
        handle: object,
        target: MssqlR1AuthorityImportTargetV1,
        payload: MssqlRevokedRegistrationCompletionOverridePayloadV1,
    ) -> None:
        self.events.append("lock_operation_artifacts")
        self.override_reproof = (target, payload)
        expected = (D2, D3, D4, D5, 2, 3, D6)
        observed = (
            payload.revocation_receipt_digest,
            payload.sealed_intent_digest,
            payload.artifact_set_digest,
            payload.mutation_plan_digest,
            payload.expected_operation_epoch,
            payload.expected_operation_projection_revision,
            payload.expected_verification_policy_digest,
        )
        if observed != expected:
            raise MssqlR1ControlError("postgres_mssql_r1.override_sealed_effect_conflict")

    def assert_import_target(
        self, handle: object, target: MssqlR1AuthorityImportTargetV1, registration_revocation_revision: int
    ) -> None:
        self.events.append("assert_import_target")

    def read_target_state(self, handle: object, binding: UUID) -> MssqlR1ControlStateV1:
        self.events.append("read_target_state")
        return _state(REGISTRATION_ID, 1)

    def find_control_proof(self, handle: object, key: bytes) -> MssqlR1ControlFreshProofV1 | None:
        self.events.append("find_receipt")
        return self.proof

    def create_authority_set(self, handle: object, payload: object, verification: object) -> None:
        self.events.append("create_authority_set")

    def create_revoked_override(self, handle: object, payload: object, verification: object) -> None:
        self.events.append("create_override")

    def read_imported_authority_set_digest(self, handle: object, issuance_id: UUID) -> bytes:
        self.events.append("read_authority_set")
        return self.expected_authority_set.digest

    def read_imported_override_digest(self, handle: object, override_id: UUID) -> bytes:
        self.events.append("read_override")
        return _override_command().payload.payload_digest

    def read_server_time(self, handle: object) -> datetime:
        self.events.append("read_server_time")
        return NOW

    def append_control_receipt(self, handle: object, receipt: MssqlR1ControlReceiptV1) -> None:
        self.events.append("append_receipt")
        self.proof = _proof(receipt)

    def prove_control_state(self, handle: object, key: bytes) -> MssqlR1ControlFreshProofV1 | None:
        self.events.append("prove_state")
        return self.proof


def _importer(
    events: list[str],
    backend: _AuthorityBackend,
    *,
    revocation_revision: int = 0,
    sessions: _Sessions | None = None,
) -> MssqlGenerationAuthorityImporterAdapter:
    target = MssqlR1AuthorityImportTargetV1(
        physical_identity=_registration().physical_identity,
        target_binding_uuid=BINDING_ID,
        registration_id=REGISTRATION_ID,
        registration_payload_digest=_registration().payload_digest,
        schema_contract_digest=SCHEMA_DIGEST,
        permission_contract_digest=PERMISSION_DIGEST,
        registration_revocation_revision=revocation_revision,
        registration_verification_policy_digest=D6,
    )
    return MssqlGenerationAuthorityImporterAdapter(
        verifier=_Verifier(events),
        session_factory=sessions or _Sessions(events),
        backend=backend,
        target=target,
        clock=lambda: NOW,
        receipt_id_factory=lambda: UUID("ffffffff-ffff-4fff-8fff-ffffffffffff"),
    )


@pytest.mark.parametrize(
    ("factory", "verify_event", "create_event", "operation"),
    [
        (_authority_command, "verify_authority", "create_authority_set", MssqlR1ControlOperationV1.AUTHORITY_IMPORT),
        (
            _override_command,
            "verify_override",
            "create_override",
            MssqlR1ControlOperationV1.REVOKED_REGISTRATION_OVERRIDE_IMPORT,
        ),
    ],
)
def test_authority_import_is_verified_create_only_and_atomic(
    factory: Callable[[], object],
    verify_event: str,
    create_event: str,
    operation: MssqlR1ControlOperationV1,
) -> None:
    events: list[str] = []
    backend = _AuthorityBackend(events)
    command = factory()
    importer = _importer(
        events,
        backend,
        revocation_revision=1 if isinstance(command, SignedRevokedOverrideCommandV1) else 0,
    )

    if isinstance(command, SignedGenerationAuthoritySetCommandV1):
        receipt = importer.import_authority_set(command)
    else:
        receipt = importer.import_revoked_override(command)  # type: ignore[arg-type]

    assert receipt.operation is operation
    assert events[0] == verify_event
    assert events.index("lock_physical") < events.index("lock_binding") < events.index("lock_operation_artifacts")
    assert create_event in events
    assert events[-2:] == ["commit", "close"]
    if isinstance(command, SignedGenerationAuthoritySetCommandV1):
        assert backend.authority_reproof is not None
        observed_target, observed_payload, observed_set = backend.authority_reproof
        assert isinstance(observed_target, MssqlR1AuthorityImportTargetV1)
        assert observed_target.target_binding_uuid == BINDING_ID
        assert observed_payload == command.payload
        assert observed_set == _authority_set()
    else:
        assert isinstance(command, SignedRevokedOverrideCommandV1)
        assert backend.override_reproof is not None
        observed_target, observed_payload = backend.override_reproof
        assert isinstance(observed_target, MssqlR1AuthorityImportTargetV1)
        assert observed_target.target_binding_uuid == BINDING_ID
        assert observed_payload == command.payload


@pytest.mark.parametrize(
    "stale",
    [
        replace(_authority_set().refs[0], recovery_identity_digest=D6),
        replace(
            _authority_set().refs[0],
            purpose=MssqlGenerationAuthorityPurposeV2.REBASELINE,
            expected_writer_generation=1,
            candidate_writer_generation=2,
            expected_head_revision=1,
            candidate_head_revision=1,
        ),
        replace(_authority_set().refs[0], purpose=MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH),
        replace(_authority_set().refs[0], artifact_set_digest=D6),
    ],
)
def test_authority_import_reproves_every_authority_ref_coordinate(
    stale: MssqlGenerationAuthorityRefV2,
) -> None:
    events: list[str] = []
    backend = _AuthorityBackend(events)
    authority_set = MssqlGenerationAuthoritySetV2((stale,))
    original_command = _authority_command()
    original_payload = original_command.payload
    payload = replace(
        original_payload,
        authority_set_bytes=authority_set.canonical_bytes,
        authority_set_digest=authority_set.digest,
    )
    command = SignedGenerationAuthoritySetCommandV1(payload.canonical_bytes, original_command.sigstore_bundle)

    with pytest.raises(MssqlR1ControlError, match="authority_effect_coordinates_conflict"):
        _importer(events, backend).import_authority_set(command)

    assert "create_authority_set" not in events
    assert "rollback" in events


@pytest.mark.parametrize(
    "payload",
    [
        replace(_override_command().payload, revocation_receipt_digest=D1),
        replace(_override_command().payload, expected_operation_epoch=3),
        replace(_override_command().payload, expected_operation_projection_revision=4),
    ],
)
def test_override_import_reproves_revocation_and_exact_sealed_operation(
    payload: MssqlRevokedRegistrationCompletionOverridePayloadV1,
) -> None:
    events: list[str] = []
    backend = _AuthorityBackend(events)
    original = _override_command()
    command = SignedRevokedOverrideCommandV1(payload.canonical_bytes, original.sigstore_bundle)

    with pytest.raises(MssqlR1ControlError, match="override_sealed_effect_conflict"):
        _importer(events, backend, revocation_revision=1).import_revoked_override(command)

    assert "create_override" not in events
    assert "rollback" in events


def test_override_policy_conflict_blocks_before_target_io() -> None:
    events: list[str] = []
    backend = _AuthorityBackend(events)
    sessions = _Sessions(events)
    original = _override_command()
    payload = replace(original.payload, expected_verification_policy_digest=D1)
    command = SignedRevokedOverrideCommandV1(payload.canonical_bytes, original.sigstore_bundle)

    with pytest.raises(MssqlR1ControlError, match="override_registration_policy_conflict"):
        _importer(events, backend, revocation_revision=1, sessions=sessions).import_revoked_override(command)

    assert sessions.open_count == 0


def test_import_lost_commit_reproves_target_sealed_effect_and_imported_row() -> None:
    events: list[str] = []
    backend = _AuthorityBackend(events)
    sessions = _Sessions(events, first_commit_error=True)
    importer = _importer(events, backend, sessions=sessions)

    receipt = importer.import_authority_set(_authority_command())

    assert receipt.affected_authority_set_digest == _authority_set().digest
    assert sessions.open_count == 2
    assert events.count("lock_operation_artifacts") == 2
    assert events.count("assert_import_target") == 2
    assert events.count("read_target_state") == 2
    assert events.count("read_authority_set") == 2


def test_import_lost_commit_rejects_copied_receipt_without_exact_imported_row() -> None:
    events: list[str] = []
    backend = _AuthorityBackend(events)
    sessions = _Sessions(events, first_commit_error=True)
    original = backend.read_imported_authority_set_digest
    calls = 0

    def drifted_readback(handle: object, issuance_id: UUID) -> bytes:
        nonlocal calls
        calls += 1
        return original(handle, issuance_id) if calls == 1 else D6

    backend.read_imported_authority_set_digest = drifted_readback  # type: ignore[method-assign]

    with pytest.raises(MssqlR1ControlError, match="control_commit_outcome_unknown"):
        _importer(events, backend, sessions=sessions).import_authority_set(_authority_command())

    assert sessions.open_count == 2
    assert backend.authority_reproof_calls == 2


def test_conflicting_import_state_rolls_back_without_receipt() -> None:
    events: list[str] = []
    backend = _AuthorityBackend(events)
    target = _importer(events, backend)

    def conflict(handle: object, payload: object, verification: object) -> None:
        raise MssqlR1ControlError("postgres_mssql_r1.authority_import_conflict")

    backend.create_authority_set = conflict  # type: ignore[method-assign]

    with pytest.raises(MssqlR1ControlError, match="authority_import_conflict"):
        target.import_authority_set(_authority_command())
    assert "rollback" in events
    assert "append_receipt" not in events


def test_authority_import_exact_replay_does_not_create_rows_twice() -> None:
    events: list[str] = []
    backend = _AuthorityBackend(events)
    importer = _importer(events, backend)
    command = _authority_command()
    first = importer.import_authority_set(command)
    events.clear()

    second = importer.import_authority_set(command)

    assert second == first
    assert "create_authority_set" not in events
    assert events[-2:] == ["commit", "close"]
