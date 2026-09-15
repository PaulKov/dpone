"""Fixed freeze CAS and exact current readback; no publication or dispatch.

The procedure composer supplies authority validation and physical-owner locks.
Runtime authentication must precede this ledger boundary; JSON shape is not proof
that a build or its upstream qualification actually succeeded.
"""

from __future__ import annotations

from hashlib import sha256

from dpone.adapters.native_generation_mssql_json import canonical_reference, decode_bytes, scalar, shape, utf8
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.contracts.native_delivery import FrozenGeneration, GenerationReservation
from dpone.contracts.native_delivery_codec import decode_frozen_generation, encode_frozen_generation
from dpone.contracts.native_source_custody import SourceAdmissionClosure, SourceTrustedBuildCompletion
from dpone.contracts.native_source_custody_codec import (
    encode_source_admission_closure,
    encode_source_trusted_build_completion,
)


def freeze_parameters(
    reservation: GenerationReservation,
    admission: SourceAdmissionClosure,
    completion: SourceTrustedBuildCompletion,
    frozen: FrozenGeneration,
    *,
    expected_revision: int,
) -> tuple[object, ...]:
    """Serialize a whole validated SQL request; this does not authenticate originals."""
    if type(reservation) is not GenerationReservation:
        raise ValueError("freeze requires the exact generation reservation")
    reservation.__post_init__()
    if type(expected_revision) is not int or not 4 <= expected_revision < 9223372036854775807:
        raise ValueError("freeze revision must permit a positive SQL bigint successor")
    admission_payload = encode_source_admission_closure(admission)
    completion_payload = encode_source_trusted_build_completion(completion)
    frozen_payload = encode_frozen_generation(frozen)
    frozen = decode_frozen_generation(frozen_payload, frozen=frozen.frozen)
    if (
        (frozen.generation_id, frozen.guard_epoch, frozen.reservation, frozen.closure.revision)
        != (reservation.generation_id, reservation.guard_epoch, reservation.reservation, expected_revision)
        or (completion.executor.generation_id, completion.executor.guard_epoch, completion.executor.reservation)
        != (reservation.generation_id, reservation.guard_epoch, reservation.reservation)
        or admission.executor != completion.executor
        or admission.revision > expected_revision
        or frozen.closure.completion.sha256 != "sha256:" + sha256(completion_payload).hexdigest()
    ):
        raise ValueError("freeze SQL request differs from exact generation and completion")
    return (
        str(reservation.generation_id),
        expected_revision,
        admission_payload,
        completion_payload,
        frozen.closure.completion.locator.encode("utf-8"),
        frozen.closure.completion.sha256.encode("ascii"),
        frozen_payload,
        frozen.frozen.locator.encode("utf-8"),
        frozen.frozen.sha256.encode("ascii"),
    )


def freeze_body(
    schema: str, *, read_only: bool, current_owner_sql: str, current_snapshot_sql: str, allow_building: bool = False
) -> str:
    """Bind exact originals and revision while preserving physical-first locks.

    Both fragments are fixed internal composer outputs, never caller SQL. A lost
    acknowledgement is reconciled with the read-only variant and same request.
    """
    native_control_schema(schema)
    if type(read_only) is not bool:
        raise ValueError("freeze operation must select an exact read-only mode")
    if type(allow_building) is not bool or (allow_building and not read_only):
        raise ValueError("pending freeze inspection must be read-only")
    table = f"[{schema}].[native_generations_v1]"
    exact_completion = """
 AND admission_closure=@admission AND DATALENGTH(admission_closure)=DATALENGTH(@admission)
 AND completion_payload=@completion AND DATALENGTH(completion_payload)=DATALENGTH(@completion)
 AND completion_locator=@completion_locator AND DATALENGTH(completion_locator)=DATALENGTH(@completion_locator)
 AND completion_digest=@completion_digest AND DATALENGTH(completion_digest)=DATALENGTH(@completion_digest)"""
    accepted_phase = (
        "revision=@expected_revision+1 AND phase='FROZEN' AND writer_admission='CLOSED' AND outcome='ACTIVE'"
    )
    frozen_original = """frozen_payload=@frozen AND DATALENGTH(frozen_payload)=DATALENGTH(@frozen)
 AND frozen_locator=@locator AND DATALENGTH(frozen_locator)=DATALENGTH(@locator)
 AND frozen_digest=@digest AND DATALENGTH(frozen_digest)=DATALENGTH(@digest)"""
    phase_predicate, original_predicate = accepted_phase, frozen_original
    if allow_building:
        phase_predicate = "writer_admission='CLOSED'"
        original_predicate = f"""((revision=@expected_revision AND phase='BUILDING' AND outcome IN ('ACTIVE','UNKNOWN')
 AND frozen_payload IS NULL AND frozen_locator IS NULL AND frozen_digest IS NULL)
 OR ({accepted_phase} AND {frozen_original}))"""
    mutation = (
        ""
        if read_only
        else f"""
IF NOT EXISTS (SELECT 1 FROM {table} WHERE generation_id=@generation AND frozen_payload IS NOT NULL)
BEGIN
 UPDATE {table} SET frozen_payload=@frozen,frozen_locator=@locator,frozen_digest=@digest,
  phase='FROZEN',outcome='ACTIVE',revision=revision+1
 WHERE generation_id=@generation AND guard_epoch=@epoch AND revision=@expected_revision
  AND phase='BUILDING' AND writer_admission='CLOSED' AND outcome IN ('ACTIVE','UNKNOWN')
  AND executor=@executor AND DATALENGTH(executor)=DATALENGTH(@executor)
  AND frozen_payload IS NULL AND frozen_locator IS NULL AND frozen_digest IS NULL
  {exact_completion};
 IF @@ROWCOUNT<>1 THROW 51306, 'DPONE_NATIVE_SOURCE_FREEZE_CAS_REJECTED', 1;
END;"""
    )
    return f"""
IF @generation IS NULL OR @expected_revision IS NULL OR @expected_revision NOT BETWEEN 4 AND 9223372036854775806
 OR @admission IS NULL OR DATALENGTH(@admission) NOT BETWEEN 1 AND 1048576
 OR @completion IS NULL OR DATALENGTH(@completion) NOT BETWEEN 1 AND 1048576
 OR @completion_locator IS NULL OR DATALENGTH(@completion_locator) NOT BETWEEN 1 AND 4096
 OR @completion_digest IS NULL OR DATALENGTH(@completion_digest)<>71
 OR @frozen IS NULL OR DATALENGTH(@frozen) NOT BETWEEN 1 AND 1048576
 OR @locator IS NULL OR DATALENGTH(@locator) NOT BETWEEN 1 AND 4096
 OR @digest IS NULL OR DATALENGTH(@digest)<>71
 THROW 51306, 'DPONE_NATIVE_SOURCE_FREEZE_INPUT_INVALID', 1;
DECLARE @request varbinary(max);
SELECT @request=request FROM {table} WITH (READCOMMITTEDLOCK)
 WHERE generation_id=@generation AND authority_locator=@authority_locator
 AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator)
 AND authority_digest=@authority_digest AND DATALENGTH(authority_digest)=DATALENGTH(@authority_digest);
IF @request IS NULL THROW 51306, 'DPONE_NATIVE_SOURCE_FREEZE_OWNER_MISSING', 1;
{decode_bytes("@request", "@json")}
{current_owner_sql}
DECLARE @executor varbinary(max);
SELECT @executor=executor FROM {table} WITH (UPDLOCK,HOLDLOCK)
 WHERE generation_id=@generation AND guard_epoch=@epoch AND phase IN ('BUILDING','FROZEN')
 AND writer_admission='CLOSED' AND outcome IN ('ACTIVE','UNKNOWN')
 {exact_completion};
IF @executor IS NULL THROW 51306, 'DPONE_NATIVE_SOURCE_FREEZE_CUSTODY_MISMATCH', 1;
{decode_bytes("@executor", "@binding")}
{decode_bytes("@frozen", "@freeze_json")}
{shape("@freeze_json", dict(schema=1, generation_id=1, guard_epoch=2, revision=2, reservation=5, closure=5))}
{shape("JSON_QUERY(@freeze_json,'$.closure')", dict(payload=5, receipt=5))}
{shape("JSON_QUERY(@freeze_json,'$.reservation')", dict(locator=1, sha256=1))}
{shape("JSON_QUERY(@freeze_json,'$.closure.receipt')", dict(locator=1, sha256=1))}
DECLARE @closure_json nvarchar(max)=JSON_QUERY(@freeze_json,'$.closure.payload');
{shape("@closure_json", dict(schema=1, generation_id=1, guard_epoch=2, revision=2, reservation=5, completion=5))}
{shape("JSON_QUERY(@closure_json,'$.reservation')", dict(locator=1, sha256=1))}
{shape("JSON_QUERY(@closure_json,'$.completion')", dict(locator=1, sha256=1))}
IF {utf8(scalar("@closure_json", "$.completion.locator"))} IS NULL
 OR {utf8(scalar("@closure_json", "$.completion.sha256"))} IS NULL
 OR {utf8(scalar("@closure_json", "$.completion.locator"))}<>@completion_locator
 OR DATALENGTH({utf8(scalar("@closure_json", "$.completion.locator"))})<>DATALENGTH(@completion_locator)
 OR {utf8(scalar("@closure_json", "$.completion.sha256"))}<>@completion_digest
 OR DATALENGTH({utf8(scalar("@closure_json", "$.completion.sha256"))})<>DATALENGTH(@completion_digest)
 THROW 51306, 'DPONE_NATIVE_SOURCE_FREEZE_COMPLETION_MISMATCH', 1;
DECLARE @canonical_closure nvarchar(max)=CONVERT(nvarchar(max),N'{{"completion":')
 +{canonical_reference("@closure_json", "$.completion")}
 +N',"generation_id":"'+LOWER(CONVERT(nvarchar(36),@generation))+N'","guard_epoch":'
 +CONVERT(nvarchar(20),@epoch)+N',"reservation":'+{canonical_reference("@binding", "$.reservation")}
 +N',"revision":'+CONVERT(nvarchar(20),@expected_revision)
 +N',"schema":"dpone.native-source-closure-receipt.v1"}}';
DECLARE @closure_bytes varbinary(max)={utf8("@canonical_closure")};
DECLARE @receipt_locator varbinary(max)={utf8(scalar("@freeze_json", "$.closure.receipt.locator"))};
DECLARE @receipt_digest varbinary(max)={utf8(scalar("@freeze_json", "$.closure.receipt.sha256"))};
IF @receipt_locator IS NULL OR DATALENGTH(@receipt_locator) NOT BETWEEN 1 AND 4096
 OR @receipt_digest IS NULL OR DATALENGTH(@receipt_digest)<>71
 OR @closure_bytes IS NULL OR @closure_bytes<>{utf8("@closure_json")}
 OR DATALENGTH(@closure_bytes)<>DATALENGTH({utf8("@closure_json")})
 OR @receipt_digest<>
 CONVERT(varbinary(max),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@closure_bytes),2)))
 THROW 51306, 'DPONE_NATIVE_SOURCE_FREEZE_CLOSURE_INVALID', 1;
DECLARE @canonical_frozen nvarchar(max)=CONVERT(nvarchar(max),N'{{"closure":{{"payload":')
 +@canonical_closure+N',"receipt":'+{canonical_reference("@freeze_json", "$.closure.receipt")}
 +N'}},"generation_id":"'+LOWER(CONVERT(nvarchar(36),@generation))+N'","guard_epoch":'
 +CONVERT(nvarchar(20),@epoch)+N',"reservation":'+{canonical_reference("@binding", "$.reservation")}
 +N',"revision":'+CONVERT(nvarchar(20),@expected_revision+1)
 +N',"schema":"dpone.native-frozen-generation.v1"}}';
IF @canonical_frozen IS NULL OR {utf8("@canonical_frozen")}<>@frozen
 OR DATALENGTH({utf8("@canonical_frozen")})<>DATALENGTH(@frozen)
 OR @digest<>CONVERT(varbinary(max),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@frozen),2)))
 THROW 51306, 'DPONE_NATIVE_SOURCE_FREEZE_ORIGINAL_INVALID', 1;
{mutation}
IF NOT EXISTS (SELECT 1 FROM {table} WHERE generation_id=@generation AND guard_epoch=@epoch
 AND {phase_predicate}
 AND executor=@executor AND DATALENGTH(executor)=DATALENGTH(@executor)
 {exact_completion}
 AND {original_predicate})
 THROW 51306, 'DPONE_NATIVE_SOURCE_FREEZE_READBACK_MISMATCH', 1;
{current_snapshot_sql}"""
