from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from dpone.adapters.mssql_r1_v3_open_recovery import (
    MssqlR1V3OpenStageRecoveryAdapter,
    MssqlR1V3OpenStageRecoveryCandidate,
    MssqlR1V3OpenStageRecoveryCommand,
    MssqlR1V3OpenStageRecoveryError,
)
from dpone.adapters.mssql_r1_v3_stage_attestor import (
    MssqlR1V3ScalarCodec,
    MssqlR1V3SealedStageAttestor,
    MssqlR1V3StageAttestationError,
)
from dpone.adapters.mssql_r1_v3_stage_provider import MssqlR1V3StageProvider, MssqlR1V3StageProviderError
from dpone.adapters.mssql_r1_v3_stage_schema import (
    open_stage_plan_set_digest,
    render_mssql_r1_v3_stage_batches,
    render_stage_object_plan,
)
from dpone.contracts.mssql_r1_v3_control import build_open_stage_recovery_effect_key
from dpone.contracts.mssql_r1_v3_control_proof import MssqlOpenStageRecoveryFreshProofV1
from dpone.contracts.mssql_r1_v3_identity import MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_stage_evidence import (
    R1BoundedStageChunkV1,
    R1StageArtifactKindV1,
    R1StageBusinessColumnV1,
    R1StageCellStateV1,
    R1TypedStageCellV1,
    R1TypedStageRowV1,
    artifact_digest_for_rows,
)
from dpone.contracts.mssql_r1_v3_staging import R1OpenStagePlanV1
from dpone.contracts.postgres_mssql_hash_policy import (
    R1Column,
    R1ScalarType,
    canonical_key_payload,
    canonical_row_bytes,
)

D1 = bytes.fromhex("11" * 32)
D2 = bytes.fromhex("22" * 32)
D3 = bytes.fromhex("33" * 32)
BINDING = UUID("10000000-0000-4000-8000-000000000001")
ARTIFACT = UUID("20000000-0000-4000-8000-000000000001")
OBJECT = UUID("20000000-0000-4000-8000-000000000002")
TOKEN = UUID("20000000-0000-4000-8000-000000000003")
NOW = datetime(2026, 9, 4, 12, tzinfo=timezone(timedelta(0)))


def _column() -> R1StageBusinessColumnV1:
    return R1StageBusinessColumnV1(
        1,
        "order_id",
        1,
        "order_id",
        "pg.int8-mssql.bigint.v1",
        False,
        True,
    )


def _plan(
    kind: R1StageArtifactKindV1 = R1StageArtifactKindV1.BATCH_PAYLOAD,
    *,
    stage_schema: str = "dpone_stage",
) -> R1OpenStagePlanV1:
    complete = kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS
    candidate = R1OpenStagePlanV1(
        ARTIFACT,
        kind,
        BINDING,
        D1,
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
    return replace(candidate, exact_stage_ddl_digest=render_stage_object_plan(candidate, stage_schema).ddl_digest)


def _native_row(kind: R1StageArtifactKindV1, value: int = 7) -> tuple[object, ...]:
    spec = R1ScalarType.from_postgres("int8")
    columns = (R1Column(1, "order_id", spec, False),)
    key = canonical_key_payload(spec, value)
    if kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS:
        return (value, key)
    row = canonical_row_bytes(columns, (value,))
    return (value, key, row, hashlib.sha256(row).digest())


def _typed_row(plan: R1OpenStagePlanV1, value: int = 7) -> R1TypedStageRowV1:
    native = _native_row(plan.artifact_kind, value)
    return R1TypedStageRowV1(
        plan.artifact_kind,
        (R1TypedStageCellV1(1, _column().logical_type_id, R1StageCellStateV1.VALUE, bytes([value])),),
        native[1],  # type: ignore[arg-type]
        None if plan.artifact_kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS else native[2],  # type: ignore[arg-type]
        None if plan.artifact_kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS else native[3],  # type: ignore[arg-type]
    )


def _nullable_text_plan() -> R1OpenStagePlanV1:
    payload = R1StageBusinessColumnV1(
        2,
        "payload",
        2,
        "payload",
        "pg.text-mssql.nvarchar.v1",
        True,
        False,
    )
    candidate = replace(_plan(), ordered_business_columns=(_column(), payload))
    return replace(candidate, exact_stage_ddl_digest=render_stage_object_plan(candidate).ddl_digest)


def _nullable_text_row(
    plan: R1OpenStagePlanV1,
    key_value: int,
    payload_value: str | None,
) -> tuple[tuple[object, ...], R1TypedStageRowV1]:
    key_spec = R1ScalarType.from_postgres("int8")
    text_spec = R1ScalarType.from_postgres("text")
    columns = (
        R1Column(1, "order_id", key_spec, False),
        R1Column(2, "payload", text_spec, True),
    )
    values = (key_value, payload_value)
    key = canonical_key_payload(key_spec, key_value)
    row = canonical_row_bytes(columns, values)
    codec = MssqlR1V3ScalarCodec()
    cells = (
        R1TypedStageCellV1(
            1, _column().logical_type_id, R1StageCellStateV1.VALUE, codec.encode(_column().logical_type_id, key_value)
        ),
        R1TypedStageCellV1(
            2,
            "pg.text-mssql.nvarchar.v1",
            R1StageCellStateV1.NULL if payload_value is None else R1StageCellStateV1.VALUE,
            codec.encode("pg.text-mssql.nvarchar.v1", payload_value),
        ),
    )
    digest = hashlib.sha256(row).digest()
    return (*values, key, row, digest), R1TypedStageRowV1(plan.artifact_kind, cells, key, row, digest)


class _RowsHandle:
    def __init__(
        self,
        rows: tuple[tuple[object, ...], ...],
        *,
        plan: R1OpenStagePlanV1 | None = None,
        ddl_digest: bytes | None = None,
        object_name: str = "orders_01",
        stage_schema: str = "dpone_stage",
    ) -> None:
        self.rows = rows
        self.plan = plan or _plan()
        self.ddl_digest = ddl_digest or self.plan.exact_stage_ddl_digest
        self.object_name = object_name
        self.stage_schema = stage_schema
        self.sql: list[tuple[str, tuple[object, ...]]] = []
        self.current_sql = ""

    def execute(self, sql: str, parameters: tuple[object, ...] = ()) -> _RowsHandle:
        self.sql.append((sql, parameters))
        self.current_sql = sql
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        if "dpone_observe_stage_v3" not in self.current_sql:
            return None
        return (
            self.plan.canonical_bytes,
            "OPEN",
            self.plan.owner_epoch,
            OBJECT,
            71,
            TOKEN,
            self.stage_schema,
            self.object_name,
            None,
            None,
            NOW + timedelta(seconds=30),
            71,
            OBJECT,
            TOKEN,
            self.ddl_digest,
            D2,
            D3,
            D1,
            D2,
        )

    def fetchall(self) -> tuple[tuple[object, ...], ...]:
        return self.rows


class _RegisterHandle(_RowsHandle):
    def __init__(self, plan: R1OpenStagePlanV1, *, stage_schema: str = "dpone_stage") -> None:
        rendered = render_stage_object_plan(plan, stage_schema)
        super().__init__((), plan=plan, object_name=rendered.object_name, stage_schema=stage_schema)
        self.rendered = rendered

    def fetchone(self) -> tuple[object, ...] | None:
        if "dpone_open_stage_v3" in self.current_sql:
            return (OBJECT, 71, TOKEN, self.rendered.schema_name, self.rendered.object_name)
        return super().fetchone()


class _ScriptedSession:
    def __init__(self, handle: object, *, lose_commit_response: bool = False) -> None:
        self.handle = handle
        self.lose_commit_response = lose_commit_response
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def begin(self) -> object:
        return self.handle

    def commit(self, handle: object) -> None:
        assert handle is self.handle
        self.committed = True
        if self.lose_commit_response:
            raise TimeoutError("commit response lost")

    def rollback(self, handle: object) -> None:
        assert handle is self.handle
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


class _SessionFactory:
    def __init__(self, *handles: object, lose_first_commit_response: bool = False) -> None:
        self.sessions = [_ScriptedSession(handle) for handle in handles]
        if self.sessions and lose_first_commit_response:
            self.sessions[0].lose_commit_response = True

    def open(self) -> _ScriptedSession:
        return self.sessions.pop(0)


class _OneRowHandle:
    def __init__(self, row: tuple[object, ...]) -> None:
        self.row = row
        self.sql: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, parameters: tuple[object, ...] = ()) -> _OneRowHandle:
        self.sql.append((sql, parameters))
        return self

    def fetchone(self) -> tuple[object, ...]:
        return self.row


class _SealHandle:
    def __init__(
        self,
        plan: R1OpenStagePlanV1,
        rows: tuple[tuple[object, ...], ...],
        *,
        physical_object_id_after_seal: int = 71,
        physical_ddl_digest_after_seal: bytes | None = None,
    ) -> None:
        self.plan = plan
        self.rows = rows
        self.sql: list[tuple[str, tuple[object, ...]]] = []
        self.current_sql = ""
        self.manifest_payload: bytes | None = None
        self.physical_object_id_after_seal = physical_object_id_after_seal
        self.physical_ddl_digest_after_seal = physical_ddl_digest_after_seal or plan.exact_stage_ddl_digest

    def execute(self, sql: str, parameters: tuple[object, ...] = ()) -> _SealHandle:
        self.sql.append((sql, parameters))
        self.current_sql = sql
        if "dpone_seal_stage_v3" in sql:
            self.manifest_payload = bytes(parameters[2])  # type: ignore[arg-type]
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        if "dpone_observe_stage_v3" in self.current_sql:
            return (
                self.plan.canonical_bytes,
                "OPEN" if self.manifest_payload is None else "SEALED",
                self.plan.owner_epoch,
                OBJECT,
                71,
                TOKEN,
                "dpone_stage",
                render_stage_object_plan(self.plan).object_name,
                self.manifest_payload,
                None if self.manifest_payload is None else hashlib.sha256(self.manifest_payload).digest(),
                None if self.manifest_payload is not None else NOW + timedelta(seconds=30),
                71 if self.manifest_payload is None else self.physical_object_id_after_seal,
                OBJECT,
                TOKEN,
                (
                    self.plan.exact_stage_ddl_digest
                    if self.manifest_payload is None
                    else self.physical_ddl_digest_after_seal
                ),
                D2,
                D3,
                D1,
                D2,
            )
        return None

    def fetchall(self) -> tuple[tuple[object, ...], ...]:
        return self.rows


def test_stage_schema_is_owner_rendered_and_never_accepts_arbitrary_ddl() -> None:
    batches = render_mssql_r1_v3_stage_batches()
    rendered = "\n".join(batches)
    assert "WITH EXECUTE AS OWNER" in rendered
    assert "SYSUTCDATETIME()" in rendered
    assert "sp_getapplock" in rendered
    assert "TABLOCKX,HOLDLOCK" in rendered
    assert "dpone_open_stage_v3" in rendered
    assert "dpone_target_generation_head_v3" not in rendered
    assert "dpone_writer_operation_v3" not in rendered
    assert "HASHBYTES('SHA2_256',CONVERT(varbinary(max),@sql))" in rendered
    assert "@stage_ddl" not in rendered
    assert "@ddl_sql" not in rendered
    assert "DENY INSERT,UPDATE,DELETE ON OBJECT::" in rendered
    assert "TO [dpone_r1_runtime]" in rendered
    assert "TO [dpone_r1_loader]" in rendered
    assert "chunk_digest<>@chunk_digest OR row_count<>@row_count" in rendered


def test_chunk_admission_rejects_conflicting_digest_or_count_before_insert() -> None:
    rendered = "\n".join(render_mssql_r1_v3_stage_batches())
    conflict_guard = "chunk_digest<>@chunk_digest OR row_count<>@row_count"
    assert rendered.index(conflict_guard) < rendered.index(
        "VALUES(@artifact_id,@chunk_sequence,@chunk_digest,@row_count"
    )


def test_closed_stage_renderer_binds_names_columns_types_and_kind() -> None:
    plan = _plan()
    rendered = render_stage_object_plan(plan)
    assert rendered.schema_name == "dpone_stage"
    assert rendered.object_name == "a_20000000000040008000000000000001"
    assert "[order_id] bigint NOT NULL" in rendered.create_table_sql
    assert "[__dpone_key] varbinary(max) NOT NULL" in rendered.create_table_sql
    assert "UNIQUE ([order_id])" in rendered.create_table_sql
    assert rendered.ddl_digest == plan.exact_stage_ddl_digest
    invalid_column = R1StageBusinessColumnV1(2, "payload", 2, "payload", "sql:drop", True, False)
    with pytest.raises(ValueError, match="unsupported logical type"):
        render_stage_object_plan(replace(plan, ordered_business_columns=(_column(), invalid_column)))


def test_closed_stage_renderer_maps_parameterized_numeric_without_sql_text_input() -> None:
    plan = _plan()
    numeric = R1StageBusinessColumnV1(
        2,
        "amount",
        2,
        "amount",
        "pg.numeric(12,2)-mssql.decimal(12,2).v1",
        False,
        False,
    )
    candidate = replace(plan, ordered_business_columns=(_column(), numeric))
    rendered = render_stage_object_plan(candidate)
    assert "[amount] decimal(12,2) NOT NULL" in rendered.create_table_sql


def test_scalar_codec_round_trips_exact_hash_policy_payload() -> None:
    codec = MssqlR1V3ScalarCodec()
    for logical_type_id, value in (
        ("pg.bool-mssql.bit.v1", True),
        ("pg.int2-mssql.smallint.v1", -7),
        ("pg.int4-mssql.int.v1", 1234),
        ("pg.int8-mssql.bigint.v1", -(2**40)),
        ("pg.uuid-mssql.uniqueidentifier.v1", UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")),
        ("pg.text-mssql.nvarchar.v1", "AU0001f642"),
        ("pg.bytea-mssql.varbinary.v1", b"\x00\xff"),
    ):
        encoded = codec.encode(logical_type_id, value)
        assert codec.decode(logical_type_id, encoded) == value


def test_null_and_empty_text_remain_distinct_through_write_and_attestation() -> None:
    plan = _nullable_text_plan()
    null_native, null_typed = _nullable_text_row(plan, 1, None)
    empty_native, empty_typed = _nullable_text_row(plan, 2, "")
    load = _OneRowHandle((1,))
    MssqlR1V3StageProvider(_SessionFactory(_OneRowHandle((1,)), load)).write(
        plan,
        R1BoundedStageChunkV1(plan.artifact_kind, plan.ordered_business_columns, 0, (null_typed, empty_typed)),
    )
    inserted = tuple(parameters for sql, parameters in load.sql if sql.startswith("INSERT INTO [dpone_stage]"))
    assert inserted[0][:2] == (1, None)
    assert inserted[1][:2] == (2, "")
    scan = MssqlR1V3SealedStageAttestor().scan_open(
        _RowsHandle((null_native, empty_native), plan=plan),
        plan,
        OBJECT,
        71,
        TOKEN,
        "dpone_stage",
        "orders_01",
    )
    assert scan.rows[0].cells[1].value_state is R1StageCellStateV1.NULL
    assert scan.rows[1].cells[1].value_state is R1StageCellStateV1.VALUE
    assert scan.rows[0].cells[1].canonical_scalar_bytes == scan.rows[1].cells[1].canonical_scalar_bytes == b""


def test_attestor_computes_counts_digest_and_typed_equivalence_from_rows() -> None:
    plan = _plan()
    handle = _RowsHandle((_native_row(plan.artifact_kind, 7), _native_row(plan.artifact_kind, 9)), plan=plan)
    attestor = MssqlR1V3SealedStageAttestor()
    scan = attestor.scan_open(handle, plan, OBJECT, 71, TOKEN, "dpone_stage", "orders_01")
    assert scan.evidence.observed_row_count == 2
    assert scan.evidence.observed_payload_bytes > 0
    assert scan.evidence.canonical_reencode_matches == 2
    assert scan.evidence.row_hash_matches == 2
    assert scan.rows[0].cells[0].canonical_scalar_bytes == b"\x07"
    assert any("dpone_scan_stage_v3" in sql for sql, _parameters in handle.sql)


def test_attestor_rejects_typed_or_stored_hash_tamper() -> None:
    plan = _plan()
    native = _native_row(plan.artifact_kind, 7)
    wrong_typed = _RowsHandle(((8, *native[1:]),), plan=plan)
    with pytest.raises(MssqlR1V3StageAttestationError, match="canonical (key|row)"):
        MssqlR1V3SealedStageAttestor().scan_open(wrong_typed, plan, OBJECT, 71, TOKEN, "dpone_stage", "orders_01")
    wrong_hash = _RowsHandle(((native[0], native[1], native[2], D1),), plan=plan)
    with pytest.raises(MssqlR1V3StageAttestationError, match="stored row hash"):
        MssqlR1V3SealedStageAttestor().scan_open(wrong_hash, plan, OBJECT, 71, TOKEN, "dpone_stage", "orders_01")


class _MemoryStageProvider(MssqlR1V3StageProvider):
    """Exercise orchestration without claiming a vendor-live SQL result."""

    def __init__(self) -> None:
        super().__init__(object())
        self.events: list[str] = []

    def _register_open_once(self, plan: R1OpenStagePlanV1) -> None:
        self.events.extend(("create", "properties", "open", "lease", "grant"))

    def _write_once(self, plan: R1OpenStagePlanV1, chunk: R1BoundedStageChunkV1) -> None:
        self.events.append(f"write:{chunk.chunk_sequence}")

    def _renew_once(self, artifact_id: UUID, owner_epoch: int) -> bool:
        self.events.append("renew")
        return False


def test_stage_provider_checks_exact_ddl_and_stops_after_lease_loss() -> None:
    provider = _MemoryStageProvider()
    plan = _plan()
    provider.register_open(plan)
    assert provider.events[:5] == ["create", "properties", "open", "lease", "grant"]
    invalid = replace(plan, exact_stage_ddl_digest=D1)
    with pytest.raises(MssqlR1V3StageProviderError, match="DDL"):
        provider.register_open(invalid)
    assert provider.renew(plan.artifact_id, plan.owner_epoch) is False
    row = R1TypedStageRowV1(
        plan.artifact_kind,
        (R1TypedStageCellV1(1, _column().logical_type_id, R1StageCellStateV1.VALUE, b"\x07"),),
        _native_row(plan.artifact_kind, 7)[1],  # type: ignore[arg-type]
        _native_row(plan.artifact_kind, 7)[2],  # type: ignore[arg-type]
        _native_row(plan.artifact_kind, 7)[3],  # type: ignore[arg-type]
    )
    with pytest.raises(MssqlR1V3StageProviderError, match="LEASE"):
        provider.write(plan, R1BoundedStageChunkV1(plan.artifact_kind, plan.ordered_business_columns, 0, (row,)))


def test_stage_provider_registers_through_owner_procedure_without_runtime_ddl() -> None:
    plan = _plan()
    handle = _RegisterHandle(plan)
    sessions = _SessionFactory(handle)
    MssqlR1V3StageProvider(sessions).register_open(plan)
    statements = tuple(sql for sql, _parameters in handle.sql)
    assert any("#dpone_stage_columns_v3" in sql for sql in statements)
    assert any("dpone_open_stage_v3" in sql for sql in statements)
    assert not any("CREATE TABLE [dpone_stage]" in sql for sql in statements)


def test_open_lost_commit_response_requires_exact_fresh_stage_observation() -> None:
    plan = _plan()
    rendered = render_stage_object_plan(plan)
    opened = _RegisterHandle(plan)
    observed = _RowsHandle((), plan=plan, object_name=rendered.object_name)
    sessions = _SessionFactory(opened, observed, lose_first_commit_response=True)
    MssqlR1V3StageProvider(sessions).register_open(plan)
    assert sessions.sessions == []


def test_stage_provider_renews_then_writes_native_values_with_chunk_receipt() -> None:
    plan = _plan()
    renew = _OneRowHandle((1,))
    load = _OneRowHandle((1,))
    sessions = _SessionFactory(renew, load)
    provider = MssqlR1V3StageProvider(sessions)
    native = _native_row(plan.artifact_kind, 7)
    typed = R1TypedStageRowV1(
        plan.artifact_kind,
        (R1TypedStageCellV1(1, _column().logical_type_id, R1StageCellStateV1.VALUE, b"\x07"),),
        native[1],  # type: ignore[arg-type]
        native[2],  # type: ignore[arg-type]
        native[3],  # type: ignore[arg-type]
    )
    provider.write(plan, R1BoundedStageChunkV1(plan.artifact_kind, plan.ordered_business_columns, 5, (typed,)))
    insert = next(item for item in load.sql if item[0].startswith("INSERT INTO [dpone_stage]"))
    assert insert[1][0] == 7
    assert insert[1][1:] == native[1:]
    assert any("dpone_complete_stage_chunk_v3" in sql for sql, _parameters in load.sql)


def test_stage_provider_suppresses_exact_completed_chunk_replay() -> None:
    plan = _plan()
    renew = _OneRowHandle((1,))
    replay = _OneRowHandle((0,))
    sessions = _SessionFactory(renew, replay)
    provider = MssqlR1V3StageProvider(sessions)
    native = _native_row(plan.artifact_kind, 7)
    typed = R1TypedStageRowV1(
        plan.artifact_kind,
        (R1TypedStageCellV1(1, _column().logical_type_id, R1StageCellStateV1.VALUE, b"\x07"),),
        native[1],  # type: ignore[arg-type]
        native[2],  # type: ignore[arg-type]
        native[3],  # type: ignore[arg-type]
    )
    provider.write(plan, R1BoundedStageChunkV1(plan.artifact_kind, plan.ordered_business_columns, 5, (typed,)))
    assert not any(sql.startswith("INSERT INTO [dpone_stage]") for sql, _parameters in replay.sql)


def test_stage_provider_recovers_exact_chunk_after_lost_commit_response() -> None:
    plan = _plan()
    typed = _typed_row(plan)
    chunk = R1BoundedStageChunkV1(plan.artifact_kind, plan.ordered_business_columns, 5, (typed,))
    digest, _payload_bytes = artifact_digest_for_rows(plan.artifact_kind, plan.schema_digest, chunk.rows)
    sessions = _SessionFactory(_OneRowHandle((1,)), _OneRowHandle((1,)), _OneRowHandle((digest, "COMPLETE")))
    sessions.sessions[1].lose_commit_response = True
    MssqlR1V3StageProvider(sessions).write(plan, chunk)
    assert sessions.sessions == []


def test_stage_provider_reports_typed_unknown_for_malformed_fresh_chunk_observation() -> None:
    plan = _plan()
    typed = _typed_row(plan)
    chunk = R1BoundedStageChunkV1(plan.artifact_kind, plan.ordered_business_columns, 5, (typed,))
    sessions = _SessionFactory(_OneRowHandle((1,)), _OneRowHandle((1,)), _OneRowHandle((object(),)))
    sessions.sessions[1].lose_commit_response = True
    with pytest.raises(MssqlR1V3StageProviderError, match="STAGE_CHUNK_OUTCOME_UNKNOWN"):
        MssqlR1V3StageProvider(sessions).write(plan, chunk)


@pytest.mark.parametrize(
    "kind",
    (R1StageArtifactKindV1.BATCH_PAYLOAD, R1StageArtifactKindV1.XMIN_DELTA),
)
def test_stage_provider_seal_computes_manifest_from_typed_physical_rows(kind: R1StageArtifactKindV1) -> None:
    plan = _plan(kind)
    handle = _SealHandle(plan, (_native_row(plan.artifact_kind, 7), _native_row(plan.artifact_kind, 9)))
    manifest = MssqlR1V3StageProvider(_SessionFactory(handle)).seal(plan)
    assert manifest.observed_row_count == 2
    assert manifest.observed_payload_bytes > 0
    assert manifest.typed_scan_evidence.canonical_reencode_matches == 2
    seal_call = next(parameters for sql, parameters in handle.sql if "dpone_seal_stage_v3" in sql)
    assert bytes(seal_call[2]) == manifest.canonical_bytes  # type: ignore[arg-type]
    assert bytes(seal_call[3]) == manifest.manifest_digest  # type: ignore[arg-type]


def test_stage_provider_recovers_seal_only_after_fresh_typed_physical_attestation() -> None:
    plan = _plan()
    handle = _SealHandle(plan, (_native_row(plan.artifact_kind),))
    provider = MssqlR1V3StageProvider(_SessionFactory(handle, handle, lose_first_commit_response=True))
    assert provider.seal(plan).observed_row_count == 1


@pytest.mark.parametrize(
    "handle",
    (
        _SealHandle(_plan(), (_native_row(R1StageArtifactKindV1.BATCH_PAYLOAD),), physical_object_id_after_seal=72),
        _SealHandle(
            _plan(),
            (_native_row(R1StageArtifactKindV1.BATCH_PAYLOAD),),
            physical_ddl_digest_after_seal=D1,
        ),
    ),
)
def test_stage_provider_rejects_replaced_or_tampered_stage_after_lost_seal_response(handle: _SealHandle) -> None:
    provider = MssqlR1V3StageProvider(_SessionFactory(handle, handle, lose_first_commit_response=True))
    with pytest.raises(MssqlR1V3StageProviderError, match="STAGE_SEAL_OUTCOME_UNKNOWN"):
        provider.seal(handle.plan)


def test_complete_keys_attestation_uses_zero_row_payload_and_no_hash() -> None:
    plan = _plan(R1StageArtifactKindV1.XMIN_COMPLETE_KEYS)
    handle = _RowsHandle(
        (_native_row(plan.artifact_kind, 7),),
        plan=plan,
        ddl_digest=plan.exact_stage_ddl_digest,
    )
    scan = MssqlR1V3SealedStageAttestor().scan_open(handle, plan, OBJECT, 71, TOKEN, "dpone_stage", "orders_01")
    assert scan.rows[0].canonical_row_payload is None
    assert scan.evidence.row_hash_matches == 0
    assert scan.evidence.observed_row_count == 1


def test_scalar_codec_rejects_noncanonical_integer_payload() -> None:
    with pytest.raises(MssqlR1V3StageAttestationError, match="canonical scalar"):
        MssqlR1V3ScalarCodec().decode("pg.int8-mssql.bigint.v1", b"\x00\x07")


def test_open_plan_set_uses_fixed_artifact_kind_order() -> None:
    delta = _plan(R1StageArtifactKindV1.XMIN_DELTA)
    keys = replace(
        _plan(R1StageArtifactKindV1.XMIN_COMPLETE_KEYS),
        artifact_id=UUID("20000000-0000-4000-8000-000000000009"),
    )
    keys = replace(keys, exact_stage_ddl_digest=render_stage_object_plan(keys).ddl_digest)
    assert open_stage_plan_set_digest((delta, keys))
    with pytest.raises(ValueError, match="kind/order"):
        open_stage_plan_set_digest((keys, delta))


class _RecoveryBackend:
    def __init__(self, old_plans: tuple[R1OpenStagePlanV1, ...], *, lose_commit_response: bool = False) -> None:
        self.candidate = MssqlR1V3OpenStageRecoveryCandidate(
            D2,
            D1,
            open_stage_plan_set_digest(old_plans),
            1,
            3,
            D3,
            old_plans,
            NOW,
        )
        self.command: MssqlR1V3OpenStageRecoveryCommand | None = None
        self.proof: MssqlOpenStageRecoveryFreshProofV1 | None = None
        self.lose_commit_response = lose_commit_response

    def inspect_expired_open(
        self, operation_key: bytes, effect_key: bytes, old_artifact_set_digest: bytes
    ) -> MssqlR1V3OpenStageRecoveryCandidate:
        assert (operation_key, effect_key, old_artifact_set_digest) == (D2, D1, self.candidate.old_artifact_set_digest)
        return self.candidate

    def commit_recovery(self, command: MssqlR1V3OpenStageRecoveryCommand) -> MssqlOpenStageRecoveryFreshProofV1:
        self.command = command
        receipt = command.receipt
        self.proof = MssqlOpenStageRecoveryFreshProofV1(
            receipt,
            receipt.committed_operation_epoch,
            receipt.committed_operation_projection_revision,
            receipt.committed_operation_state,
            receipt.abandoned_artifact_ids,
            receipt.new_artifact_ids,
            receipt.new_open_plan_set_digest,
            receipt.observed_writer_head_digest,
        )
        if self.lose_commit_response:
            raise TimeoutError("commit response lost")
        return self.proof

    def probe_recovery_fresh(self, recovery_effect_key: bytes) -> MssqlOpenStageRecoveryFreshProofV1 | None:
        return (
            self.proof
            if self.proof is not None and self.proof.receipt.recovery_effect_key == recovery_effect_key
            else None
        )


def test_open_recovery_allocates_fresh_epoch_plans_and_exact_receipt() -> None:
    old = (_plan(),)
    backend = _RecoveryBackend(old)
    adapter = MssqlR1V3OpenStageRecoveryAdapter(
        backend,
        uuid_factory=iter((UUID(int=101), UUID(int=102))).__next__,
    )
    receipt = adapter.recover_expired_open(D2, D1, open_stage_plan_set_digest(old))
    assert backend.command is not None
    assert receipt.expected_operation_epoch == 1
    assert receipt.committed_operation_epoch == 2
    assert receipt.expected_operation_projection_revision == 3
    assert receipt.committed_operation_projection_revision == 4
    assert receipt.abandoned_artifact_ids == (ARTIFACT,)
    assert receipt.new_artifact_ids == (UUID(int=101),)
    assert receipt.recovery_effect_key == build_open_stage_recovery_effect_key(
        D2, D1, receipt.old_artifact_set_digest, 1, 3
    )
    assert receipt.new_open_plan_set_digest != receipt.old_artifact_set_digest


def test_open_recovery_backend_receives_exact_cas_and_writer_head_proof() -> None:
    backend = _RecoveryBackend((_plan(),))
    adapter = MssqlR1V3OpenStageRecoveryAdapter(
        backend,
        uuid_factory=iter((UUID(int=201), UUID(int=202))).__next__,
    )
    receipt = adapter.recover_expired_open(D2, D1, backend.candidate.old_artifact_set_digest)
    command = backend.command
    assert command is not None
    assert command.candidate.expected_operation_projection_revision == 3
    assert receipt.committed_operation_projection_revision == 4
    assert receipt.expected_writer_head_digest == receipt.observed_writer_head_digest == D3
    assert all(plan.owner_epoch == 2 for plan in command.new_plans)


def test_open_recovery_lost_response_requires_exact_fresh_proof() -> None:
    backend = _RecoveryBackend((_plan(),), lose_commit_response=True)
    adapter = MssqlR1V3OpenStageRecoveryAdapter(
        backend,
        uuid_factory=iter((UUID(int=301), UUID(int=302))).__next__,
    )
    receipt = adapter.recover_expired_open(D2, D1, backend.candidate.old_artifact_set_digest)
    assert backend.proof is not None
    assert receipt == backend.proof.receipt


def test_open_recovery_rejects_artifact_uuid_collision_before_backend_commit() -> None:
    backend = _RecoveryBackend((_plan(),))
    adapter = MssqlR1V3OpenStageRecoveryAdapter(backend, uuid_factory=lambda: ARTIFACT)
    with pytest.raises(MssqlR1V3OpenStageRecoveryError, match="ARTIFACT_ID_CONFLICT"):
        adapter.recover_expired_open(D2, D1, backend.candidate.old_artifact_set_digest)
    assert backend.command is None


def test_open_recovery_command_rejects_overlapping_old_and_new_artifacts() -> None:
    backend = _RecoveryBackend((_plan(),))
    MssqlR1V3OpenStageRecoveryAdapter(
        backend,
        uuid_factory=iter((UUID(int=401), UUID(int=402))).__next__,
    ).recover_expired_open(D2, D1, backend.candidate.old_artifact_set_digest)
    assert backend.command is not None
    successor = replace(backend.command.new_plans[0], artifact_id=ARTIFACT)
    successor = replace(successor, exact_stage_ddl_digest=render_stage_object_plan(successor).ddl_digest)
    with pytest.raises(MssqlR1V3ContractError, match="disjoint"):
        replace(
            backend.command.receipt,
            new_artifact_ids=(ARTIFACT,),
            new_open_plan_set_digest=open_stage_plan_set_digest((successor,)),
        )


def test_non_default_stage_schema_is_shared_by_recovery_and_successor_open() -> None:
    stage_schema = "private_stage"
    old = (_plan(stage_schema=stage_schema),)
    backend = _RecoveryBackend(old)
    recovery = MssqlR1V3OpenStageRecoveryAdapter(
        backend,
        stage_schema=stage_schema,
        uuid_factory=iter((UUID(int=501), UUID(int=502))).__next__,
    )
    sessions = _SessionFactory()
    provider = MssqlR1V3StageProvider(sessions, stage_schema=stage_schema, recovery=recovery)
    provider.recover_expired_open(D2, D1, backend.candidate.old_artifact_set_digest)
    assert backend.command is not None
    successor = backend.command.new_plans[0]
    assert successor.exact_stage_ddl_digest == render_stage_object_plan(successor, stage_schema).ddl_digest
    sessions.sessions.append(_ScriptedSession(_RegisterHandle(successor, stage_schema=stage_schema)))
    provider.register_open(successor)


def test_provider_rejects_recovery_bound_to_a_different_stage_schema() -> None:
    recovery = MssqlR1V3OpenStageRecoveryAdapter(_RecoveryBackend((_plan(),)))
    with pytest.raises(ValueError, match="share one stage schema"):
        MssqlR1V3StageProvider(object(), stage_schema="private_stage", recovery=recovery)
