from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import cast
from uuid import UUID

import pytest

from dpone.adapters.mssql_r1_v3_transaction_execution import (
    MssqlR1V3OdbcMutationCommandAdapter,
    MssqlR1V3OdbcQualityQueryAdapter,
    MssqlR1V3ProviderAdmissionError,
    MssqlR1V3RenderedAdmissionAdapter,
    MssqlR1V3TransactionHandleBinder,
)
from dpone.contracts.mssql_r1_v3_authority import (
    MssqlGenerationAuthorityPurposeV2,
    MssqlGenerationAuthorityRefV2,
    MssqlGenerationAuthoritySetV2,
)
from dpone.contracts.mssql_r1_v3_effect import MssqlR1EffectAttemptEnvelopeV3, MssqlR1EffectRequestV3
from dpone.contracts.mssql_r1_v3_identity import MssqlR1EffectIdentityV3, canonical_identifier_digest
from dpone.contracts.mssql_r1_v3_mutation import (
    MssqlBatchGenerationTransitionV3,
    MssqlXminGenerationTransitionV3,
    R1MutationResourceLimitsV1,
)
from dpone.contracts.mssql_r1_v3_plan import (
    R1BatchMutationPlanV1,
    R1MutationStepV1,
    R1MutationTemplateSetV1,
    R1XminMutationPlanV1,
)
from dpone.contracts.mssql_r1_v3_registration import MssqlTargetRegistrationPayloadV1, RegistrationActionV1
from dpone.contracts.mssql_r1_v3_rendered import (
    QUOTING_POLICY_VERSION,
    RENDERER_ID,
    RENDERER_VERSION,
    MssqlR1RenderedMutationBundleV1,
    MssqlR1RenderedStatementV1,
    MssqlR1RendererAdmissionEvidenceV1,
    MssqlR1RendererAuthorityV1,
    MssqlR1VerifiedRenderedMutationV1,
)
from dpone.contracts.mssql_r1_v3_staging import (
    R1OpenStagePlanV1,
    R1SealedStageManifestV1,
    R1StageArtifactKindV1,
    R1StageBusinessColumnV1,
    R1StageCellStateV1,
    R1TypedStageCellV1,
    R1TypedStageRowV1,
    R1TypedStageScanEvidenceV1,
    artifact_digest_for_rows,
    sealed_stage_set_digest,
)
from dpone.contracts.postgres_mssql_correctness_profile import SourceMode
from dpone.runtime.sinks.strategies.mssql.mssql_r1_v3_quality import (
    MssqlR1BatchQualityProviderV3,
    MssqlR1V3QualityError,
    MssqlR1XminQualityProviderV3,
)
from dpone.runtime.sinks.strategies.mssql.mssql_r1_v3_xmin import (
    MssqlR1V3XminMutationError,
    MssqlR1XminMutationProviderV3,
)

D1 = bytes.fromhex("11" * 32)
D2 = bytes.fromhex("22" * 32)
D3 = bytes.fromhex("33" * 32)
BINDING = UUID("10000000-0000-4000-8000-000000000001")
OBJECT = UUID("10000000-0000-4000-8000-000000000002")
REGISTRATION = UUID("10000000-0000-4000-8000-000000000003")
NOW = datetime(2026, 9, 4, 12, 0, tzinfo=UTC)


def _registration() -> MssqlTargetRegistrationPayloadV1:
    return MssqlTargetRegistrationPayloadV1(
        REGISTRATION,
        RegistrationActionV1.INITIAL,
        None,
        None,
        NOW,
        NOW + timedelta(days=30),
        b"n" * 16,
        "postgres16-mssql2022-r1",
        D1,
        D2,
        D3,
        "ordinary_disk_rowstore_v1",
        "dpone-mssql-target-catalog-v1",
        0,
        BINDING,
        OBJECT,
        UUID("10000000-0000-4000-8000-000000000005"),
        1,
        D1,
        UUID("10000000-0000-4000-8000-000000000006"),
        UUID("10000000-0000-4000-8000-000000000007"),
        UUID("10000000-0000-4000-8000-000000000008"),
        canonical_identifier_digest("warehouse"),
        canonical_identifier_digest("dbo"),
        canonical_identifier_digest("orders"),
        "warehouse",
        "dbo",
        "orders",
        37,
        UUID("10000000-0000-4000-8000-000000000009"),
        D2,
        1,
    )


def _column() -> R1StageBusinessColumnV1:
    return R1StageBusinessColumnV1(1, "order_id", 1, "order_id", "pg.int8-mssql.bigint.v1", False, True)


def _stage_row(kind: R1StageArtifactKindV1, key: bytes) -> R1TypedStageRowV1:
    complete = kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS
    payload = b"r1"
    return R1TypedStageRowV1(
        kind,
        (R1TypedStageCellV1(1, "pg.int8-mssql.bigint.v1", R1StageCellStateV1.VALUE, b"int8:" + key),),
        key,
        None if complete else payload,
        None if complete else hashlib.sha256(payload).digest(),
    )


def _sealed_manifest(
    kind: R1StageArtifactKindV1,
    *,
    effect_key: bytes,
    artifact_id: UUID = UUID("20000000-0000-4000-8000-000000000001"),
    row_keys: tuple[bytes, ...],
) -> R1SealedStageManifestV1:
    complete = kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS
    plan = R1OpenStagePlanV1(
        artifact_id,
        kind,
        BINDING,
        effect_key,
        1,
        30,
        D2,
        D2,
        D3,
        D1,
        D2,
        (_column(),),
        "__dpone_key",
        None if complete else "__dpone_row",
        None if complete else "__dpone_hash",
        64,
        0 if complete else 4096,
    )
    rows = tuple(_stage_row(kind, key) for key in row_keys)
    artifact_digest, payload_bytes = artifact_digest_for_rows(kind, D2, rows)
    evidence = R1TypedStageScanEvidenceV1(
        kind,
        artifact_digest,
        len(rows),
        payload_bytes,
        len(rows),
        len(rows),
        0 if complete else len(rows),
        len(rows),
        D3,
    )
    return R1SealedStageManifestV1(
        plan.artifact_id,
        kind,
        BINDING,
        effect_key,
        D2,
        D3,
        D1,
        D2,
        UUID("20000000-0000-4000-8000-000000000002"),
        71,
        UUID("20000000-0000-4000-8000-000000000003"),
        "dpone_stage",
        "orders_01",
        plan.ordered_business_columns,
        plan.canonical_key_payload_column,
        plan.canonical_row_payload_column,
        plan.canonical_row_hash_column,
        plan.maximum_key_bytes,
        plan.maximum_row_bytes,
        len(rows),
        payload_bytes,
        artifact_digest,
        evidence,
        None if complete else "sha256_canonical_row_v1",
        plan.digest,
        plan.exact_stage_ddl_digest,
    )


@dataclass(frozen=True)
class _Transaction:
    transaction_id: UUID = UUID(int=1)


@dataclass(frozen=True)
class _Fixture:
    attempt: MssqlR1EffectAttemptEnvelopeV3
    authority: MssqlR1RendererAuthorityV1
    evidence: MssqlR1RendererAdmissionEvidenceV1
    statements: tuple[MssqlR1RenderedStatementV1, ...]


class _Gateway:
    def __init__(self, rows: tuple[tuple[object, ...], ...] = ()) -> None:
        self.rows = rows
        self.steps: list[R1MutationStepV1] = []

    def execute(self, transaction: object, statement: object) -> None:
        assert transaction == _Transaction()
        self.steps.append(cast(MssqlR1RenderedStatementV1, statement).step_kind)

    def query(self, transaction: object, statement: object) -> tuple[tuple[object, ...], ...]:
        assert transaction == _Transaction()
        self.steps.append(cast(MssqlR1RenderedStatementV1, statement).step_kind)
        return self.rows


class _AuthorityResolver:
    def __init__(self, authority: MssqlR1RendererAuthorityV1) -> None:
        self.authority = authority
        self.calls: list[bytes] = []

    def resolve(self, resolved_profile_digest: bytes) -> MssqlR1RendererAuthorityV1:
        self.calls.append(resolved_profile_digest)
        if resolved_profile_digest != self.authority.resolved_profile_digest:
            raise ValueError("unknown signed environment profile")
        return self.authority


class _ExactTemplateVerifier:
    """Test double for the closed provider renderer, never a self-digest check."""

    def __init__(self, fixture: _Fixture) -> None:
        self.fixture = fixture
        self.calls = 0

    def verify(self, plan: object, bundle: MssqlR1RenderedMutationBundleV1, authority: object):
        self.calls += 1
        if plan != self.fixture.attempt.request.mutation_plan or authority != self.fixture.authority:
            raise ValueError("authority differs from certified closed templates")
        if bundle.statements != self.fixture.statements:
            raise ValueError("retained SQL differs from certified closed templates")
        return MssqlR1VerifiedRenderedMutationV1(
            self.fixture.attempt.request.mutation_plan,
            bundle,
            self.fixture.authority,
            self.fixture.evidence,
            self.fixture.authority.resolved_profile_digest,
        )


def _sql(step: R1MutationStepV1) -> bytes:
    templates = {
        R1MutationStepV1.BATCH_PUBLICATION: b"INSERT INTO [dbo].[orders] SELECT [order_id] FROM [dpone_stage].[batch]",
        R1MutationStepV1.DELETE: (
            b"DELETE t FROM [dbo].[orders] t WHERE NOT EXISTS "
            b"(SELECT 1 FROM [dpone_stage].[complete_keys] k WHERE k.[order_id]=t.[order_id])"
        ),
        R1MutationStepV1.UPDATE: (
            b"UPDATE t SET t.[order_id]=d.[order_id] FROM [dbo].[orders] t "
            b"JOIN [dpone_stage].[delta] d ON d.[order_id]=t.[order_id] "
            b"JOIN [dpone].[target_row_hash] h ON h.[order_id]=t.[order_id]"
        ),
        R1MutationStepV1.INSERT: (
            b"INSERT INTO [dbo].[orders] SELECT d.[order_id] FROM [dpone_stage].[delta] d "
            b"WHERE NOT EXISTS (SELECT 1 FROM [dbo].[orders] t WHERE t.[order_id]=d.[order_id])"
        ),
        R1MutationStepV1.ROW_HASH: (
            b"MERGE [dpone].[target_row_hash] h USING [dpone_stage].[delta] d ON h.[order_id]=d.[order_id] "
            b"WHEN MATCHED THEN UPDATE SET h.[row_hash]=HASHBYTES('SHA2_256',d.[__dpone_row])"
        ),
        R1MutationStepV1.CHECKPOINT: (
            b"INSERT INTO [dpone].[xmin_checkpoint]([revision],[payload]) VALUES (1,0x786D696E3D3432)"
        ),
        R1MutationStepV1.QUALITY: (
            b"SELECT COUNT_BIG(*),COUNT_BIG(*),COUNT_BIG(*),COUNT_BIG(*),COUNT_BIG(*),COUNT_BIG(*),"
            b"COUNT_BIG(*),COUNT_BIG(*) FROM [dpone_stage].[delta] d FULL JOIN [dbo].[orders] t "
            b"ON t.[order_id]=d.[order_id] WHERE NOT EXISTS "
            b"(SELECT 1 FROM [dpone_stage].[complete_keys] k WHERE k.[order_id]=d.[order_id])"
        ),
    }
    return templates[step]


def _rendered(plan: R1BatchMutationPlanV1 | R1XminMutationPlanV1):
    authority = MssqlR1RendererAuthorityV1(
        D2, RENDERER_ID, RENDERER_VERSION, D3, plan.template_set.renderer_contract_digest, QUOTING_POLICY_VERSION, D1
    )
    statements = tuple(
        MssqlR1RenderedStatementV1(
            step,
            sql := _sql(step),
            hashlib.sha256(sql).digest(),
            D1,
            plan.probe_contract_digest if step is R1MutationStepV1.QUALITY else D2,
        )
        for step in plan.template_set.ordered_steps
    )
    candidate = MssqlR1RenderedMutationBundleV1(
        plan.digest,
        RENDERER_ID,
        RENDERER_VERSION,
        D3,
        plan.template_set.renderer_contract_digest,
        QUOTING_POLICY_VERSION,
        statements,
        authority.digest,
        D1,
    )
    evidence = MssqlR1RendererAdmissionEvidenceV1(
        D2, authority.digest, plan.digest, candidate.execution_payload_digest, D1, "closed-template-verifier", "1", D3
    )
    return replace(candidate, admission_evidence_digest=evidence.digest), authority, evidence, statements


def _authority_ref(
    purpose: MssqlGenerationAuthorityPurposeV2,
    effect_key: bytes,
    artifact_set: bytes,
    plan_digest: bytes,
) -> MssqlGenerationAuthorityRefV2:
    return MssqlGenerationAuthorityRefV2(
        UUID(
            int={
                MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER: 1,
                MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH: 2,
            }[purpose]
        ),
        purpose,
        effect_key,
        D2,
        artifact_set,
        plan_digest,
        None,
        1,
        None,
        1,
        D3,
    )


def _fixture(*, xmin: bool = True, empty: bool = False) -> _Fixture:
    mode = SourceMode.XMIN_CURRENT_STATE if xmin else SourceMode.BATCH_FULL_REFRESH
    identity = MssqlR1EffectIdentityV3(
        D1, f"quality-{'xmin' if xmin else 'batch'}-{'empty' if empty else 'full'}", mode, BINDING
    )
    row_keys = () if empty else tuple(f"k{i:02}".encode() for i in range(8 if xmin else 7))
    registration = _registration()
    limits = R1MutationResourceLimitsV1(100, 1_000_000, 2_000_000, 30)
    if xmin:
        delta = _sealed_manifest(R1StageArtifactKindV1.XMIN_DELTA, effect_key=identity.effect_key, row_keys=row_keys)
        keys = _sealed_manifest(
            R1StageArtifactKindV1.XMIN_COMPLETE_KEYS,
            effect_key=identity.effect_key,
            artifact_id=UUID(int=42),
            row_keys=() if empty else tuple(f"k{i:02}".encode() for i in range(13)),
        )
        plan = R1XminMutationPlanV1(
            identity.effect_key,
            delta,
            keys,
            registration.physical_identity,
            ("order_id",),
            D2,
            D1,
            D3,
            D2,
            1,
            None,
            None,
            None,
            1,
            b"xmin=42",
            42,
            R1MutationTemplateSetV1.xmin_v1(),
            limits,
            None,
            1,
            None,
            1,
        )
        artifacts = (delta, keys)
    else:
        stage = _sealed_manifest(R1StageArtifactKindV1.BATCH_PAYLOAD, effect_key=identity.effect_key, row_keys=row_keys)
        plan = R1BatchMutationPlanV1(
            identity.effect_key,
            stage,
            registration.physical_identity,
            ("order_id",),
            D2,
            D1,
            D3,
            R1MutationTemplateSetV1.batch_v1(),
            limits,
            None,
            1,
            None,
            1,
        )
        artifacts = (stage,)
    artifact_set = sealed_stage_set_digest(artifacts)
    purposes = (MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER,) + (
        (MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH,) if empty else ()
    )
    authority_set = MssqlGenerationAuthoritySetV2(
        tuple(_authority_ref(purpose, identity.effect_key, artifact_set, plan.digest) for purpose in purposes)
    )
    if xmin:
        transition = MssqlXminGenerationTransitionV3(
            identity.effect_key,
            None,
            None,
            1,
            None,
            1,
            artifacts[1].observed_row_count,
            D2,
            artifact_set,
            plan.digest,
            D3,
            authority_set,
        )
    else:
        transition = MssqlBatchGenerationTransitionV3(
            identity.effect_key,
            None,
            None,
            1,
            None,
            1,
            artifacts[0].observed_row_count,
            D2,
            artifact_set,
            plan.digest,
            D3,
            authority_set,
        )
    bundle, authority, evidence, statements = _rendered(plan)
    request = MssqlR1EffectRequestV3(
        identity,
        REGISTRATION,
        registration.payload_digest,
        registration.canonical_bytes,
        D1,
        NOW,
        D2,
        D2,
        artifacts,
        plan,
        bundle,
        transition,
    )
    return _Fixture(
        MssqlR1EffectAttemptEnvelopeV3(request, 1, 1, D3, NOW + timedelta(minutes=1)), authority, evidence, statements
    )


def _admission(fixture: _Fixture):
    resolver = _AuthorityResolver(fixture.authority)
    verifier = _ExactTemplateVerifier(fixture)
    return MssqlR1V3RenderedAdmissionAdapter(resolver, verifier), resolver, verifier


def _tamper(fixture: _Fixture, step: R1MutationStepV1, sql: bytes) -> MssqlR1EffectAttemptEnvelopeV3:
    replacement = replace(
        next(item for item in fixture.statements if item.step_kind is step),
        statement_utf8_bytes=sql,
        statement_digest=hashlib.sha256(sql).digest(),
    )
    statements = tuple(replacement if item.step_kind is step else item for item in fixture.statements)
    bundle = replace(fixture.attempt.request.rendered_bundle, statements=statements)
    return replace(fixture.attempt, request=replace(fixture.attempt.request, rendered_bundle=bundle))


def test_xmin_provider_executes_only_independently_readmitted_closed_steps() -> None:
    fixture = _fixture()
    admission, resolver, verifier = _admission(fixture)
    gateway = _Gateway()
    provider = MssqlR1XminMutationProviderV3(gateway, admission)

    provider.apply_delta(_Transaction(), fixture.attempt)
    provider.update_row_hashes(_Transaction(), fixture.attempt)
    provider.write_checkpoint(_Transaction(), fixture.attempt)

    assert gateway.steps == [
        R1MutationStepV1.DELETE,
        R1MutationStepV1.UPDATE,
        R1MutationStepV1.INSERT,
        R1MutationStepV1.ROW_HASH,
        R1MutationStepV1.CHECKPOINT,
    ]
    assert resolver.calls == [D2, D2, D2]
    assert verifier.calls == 3


@pytest.mark.parametrize(
    ("step", "sql"),
    ((R1MutationStepV1.DELETE, b"TRUNCATE TABLE [dbo].[orders]"), (R1MutationStepV1.QUALITY, b"SELECT 1")),
)
def test_self_digested_tampered_sql_is_rejected_before_sql_io(step: R1MutationStepV1, sql: bytes) -> None:
    fixture = _fixture()
    admission, _, _ = _admission(fixture)
    tampered = _tamper(fixture, step, sql)
    gateway = _Gateway(((0,) * 18 + (True,),))

    with pytest.raises(ValueError, match="certified closed templates"):
        if step is R1MutationStepV1.DELETE:
            MssqlR1XminMutationProviderV3(gateway, admission).apply_delta(_Transaction(), tampered)
        else:
            MssqlR1XminQualityProviderV3(gateway, admission).evaluate(_Transaction(), tampered)

    assert gateway.steps == []


def test_provider_rejects_non_v3_attempt_before_authority_or_sql_io() -> None:
    fixture = _fixture()
    admission, resolver, verifier = _admission(fixture)
    gateway = _Gateway()

    with pytest.raises(MssqlR1V3XminMutationError, match="exact_v3_attempt_required"):
        MssqlR1XminMutationProviderV3(gateway, admission).apply_delta(
            _Transaction(), cast(MssqlR1EffectAttemptEnvelopeV3, SimpleNamespace(request=fixture.attempt.request))
        )

    assert resolver.calls == []
    assert verifier.calls == 0
    assert gateway.steps == []


def test_admission_rejects_untyped_verifier_result() -> None:
    fixture = _fixture()
    resolver = _AuthorityResolver(fixture.authority)

    class _UntypedVerifier:
        def verify(self, plan: object, bundle: object, authority: object) -> object:
            return SimpleNamespace(plan=plan, bundle=bundle, authority=authority, resolved_profile_digest=D2)

    with pytest.raises(MssqlR1V3ProviderAdmissionError, match="rendered_mutation_reproof_mismatch"):
        MssqlR1V3RenderedAdmissionAdapter(resolver, cast(object, _UntypedVerifier())).admit(fixture.attempt)


def test_xmin_provider_accepts_exact_empty_authority() -> None:
    fixture = _fixture(empty=True)
    admission, _, _ = _admission(fixture)
    gateway = _Gateway()

    MssqlR1XminMutationProviderV3(gateway, admission).apply_delta(_Transaction(), fixture.attempt)

    assert gateway.steps == [R1MutationStepV1.DELETE, R1MutationStepV1.UPDATE, R1MutationStepV1.INSERT]


def test_xmin_quality_derives_exact_d_i_u_n_a_from_sealed_evidence() -> None:
    fixture = _fixture()
    admission, _, _ = _admission(fixture)
    gateway = _Gateway(((10, 8, 8, 8, 2, 2, 2, 8, 13, 13, 10, 5, 2, 1, 6, 0, 0, 0, True),))

    evidence = MssqlR1XminQualityProviderV3(gateway, admission).evaluate(_Transaction(), fixture.attempt)

    assert (evidence.inserted_count, evidence.updated_count, evidence.deleted_count) == (5, 2, 2)
    assert evidence.delta_no_effect_count == 1
    assert evidence.unchanged_outside_delta_count == 5
    assert gateway.steps == [R1MutationStepV1.QUALITY]


def test_xmin_quality_accepts_exact_empty_to_empty_probe() -> None:
    fixture = _fixture(empty=True)
    admission, _, _ = _admission(fixture)
    evidence = MssqlR1XminQualityProviderV3(_Gateway(((0,) * 18 + (True,),)), admission).evaluate(
        _Transaction(), fixture.attempt
    )
    assert evidence.complete_key_count == evidence.target_row_count_after == 0


@pytest.mark.parametrize(
    ("rows", "code"),
    [
        ((), "probe_incomplete"),
        (((0,) * 18 + (True,), (0,) * 18 + (True,)), "probe_duplicate"),
        (((0,) * 17 + (True,),), "probe_incomplete"),
        ((((2**63),) + (0,) * 17 + (True,),), "probe_invalid"),
    ],
)
def test_xmin_quality_fails_closed_for_bad_probe(rows: tuple[tuple[object, ...], ...], code: str) -> None:
    fixture = _fixture()
    admission, _, _ = _admission(fixture)
    with pytest.raises(MssqlR1V3QualityError, match=code):
        MssqlR1XminQualityProviderV3(_Gateway(rows), admission).evaluate(_Transaction(), fixture.attempt)


@pytest.mark.parametrize(
    "row",
    [
        (10, 8, 8, 8, 2, 2, 2, 8, 13, 13, 10, 5, 2, 1, 6, 1, 0, 0, True),
        (10, 8, 8, 8, 2, 2, 2, 7, 13, 13, 10, 5, 2, 1, 6, 0, 0, 0, True),
        (9, 7, 7, 7, 2, 2, 2, 7, 13, 13, 10, 5, 2, 0, 6, 0, 0, 0, True),
    ],
)
def test_xmin_quality_rejects_anti_join_hash_or_manifest_mismatch(row: tuple[object, ...]) -> None:
    fixture = _fixture()
    admission, _, _ = _admission(fixture)
    with pytest.raises(MssqlR1V3QualityError, match="quality_equation_failed"):
        MssqlR1XminQualityProviderV3(_Gateway((row,)), admission).evaluate(_Transaction(), fixture.attempt)


def test_batch_quality_uses_same_independent_readmission() -> None:
    fixture = _fixture(xmin=False)
    admission, resolver, verifier = _admission(fixture)
    evidence = MssqlR1BatchQualityProviderV3(_Gateway(((7,) * 8,)), admission).evaluate(_Transaction(), fixture.attempt)
    assert evidence.staged_rows == evidence.sidecar_hash_matches == 7
    assert resolver.calls == [D2]
    assert verifier.calls == 1


class _Cursor:
    def __init__(self, session_id: str, rows: tuple[tuple[object, ...], ...] = ()) -> None:
        self.session_id = session_id
        self._rows = list(rows)
        self.sql: list[str] = []

    def execute(self, sql: str) -> _Cursor:
        self.sql.append(sql)
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self._rows.pop(0) if self._rows else None


def test_adapters_share_one_exact_handle_and_physical_session() -> None:
    fixture = _fixture()
    cursor = _Cursor("spid-37", ((1, 2),))
    binder = MssqlR1V3TransactionHandleBinder(lambda _: cursor, lambda handle: cast(_Cursor, handle).session_id)
    quality = next(item for item in fixture.statements if item.step_kind is R1MutationStepV1.QUALITY)
    delete = next(item for item in fixture.statements if item.step_kind is R1MutationStepV1.DELETE)

    MssqlR1V3OdbcMutationCommandAdapter(binder).execute(_Transaction(), delete)
    rows = MssqlR1V3OdbcQualityQueryAdapter(binder).query(_Transaction(), quality)

    assert rows == ((1, 2),)
    assert cursor.sql == [delete.statement_utf8_bytes.decode(), quality.statement_utf8_bytes.decode()]


def test_changed_handle_is_rejected_before_next_sql_even_with_same_session_label() -> None:
    fixture = _fixture()
    first = _Cursor("spid-37")
    second = _Cursor("spid-37")
    handles = iter((first, second))
    binder = MssqlR1V3TransactionHandleBinder(lambda _: next(handles), lambda handle: cast(_Cursor, handle).session_id)
    command = MssqlR1V3OdbcMutationCommandAdapter(binder)
    delete, update = fixture.statements[:2]

    command.execute(_Transaction(), delete)
    with pytest.raises(MssqlR1V3ProviderAdmissionError, match="transaction_handle_changed"):
        command.execute(_Transaction(), update)

    assert len(first.sql) == 1
    assert second.sql == []


def test_changed_physical_session_is_rejected_before_next_sql() -> None:
    fixture = _fixture()
    cursor = _Cursor("spid-37")
    binder = MssqlR1V3TransactionHandleBinder(lambda _: cursor, lambda handle: cast(_Cursor, handle).session_id)
    command = MssqlR1V3OdbcMutationCommandAdapter(binder)
    delete, update = fixture.statements[:2]

    command.execute(_Transaction(), delete)
    cursor.session_id = "spid-99"
    with pytest.raises(MssqlR1V3ProviderAdmissionError, match="transaction_handle_changed"):
        command.execute(_Transaction(), update)

    assert len(cursor.sql) == 1


def test_quality_timeout_never_returns_evidence() -> None:
    class _TimeoutGateway(_Gateway):
        def query(self, transaction: object, statement: object) -> tuple[tuple[object, ...], ...]:
            raise TimeoutError("bounded statement timeout")

    fixture = _fixture()
    admission, _, _ = _admission(fixture)
    with pytest.raises(TimeoutError, match="bounded statement timeout"):
        MssqlR1XminQualityProviderV3(_TimeoutGateway(), admission).evaluate(_Transaction(), fixture.attempt)
