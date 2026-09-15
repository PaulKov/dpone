"""Fixed protected SQL operations for initial generation and writer admission."""

from __future__ import annotations

from dpone.adapters.native_generation_mssql_completion import completion_body
from dpone.adapters.native_generation_mssql_freeze import freeze_body
from dpone.adapters.native_generation_mssql_json import canonical_reference as _canonical_reference
from dpone.adapters.native_generation_mssql_json import decode_bytes as _decode_bytes
from dpone.adapters.native_generation_mssql_json import scalar as _scalar
from dpone.adapters.native_generation_mssql_json import shape as _shape
from dpone.adapters.native_generation_mssql_json import utf8 as _utf8


def _canonical_executor() -> str:
    return f"""DECLARE @canonical nvarchar(max)=CONVERT(nvarchar(max),N'{{"command":')
 +{_canonical_reference("@binding", "$.command")}
 +N',"generation_id":"'+LOWER(CONVERT(nvarchar(36),@generation))+N'","guard_epoch":'+CONVERT(nvarchar(20),@epoch)
 +N',"invocation_id":"'+JSON_VALUE(@binding,'$.invocation_id')+N'","profile":'
 +{_canonical_reference("@binding", "$.profile")}+N',"reservation":'+{_canonical_reference("@binding", "$.reservation")}
 +N',"schema":"dpone.native-source-executor-binding.v1"}}';
IF {_utf8("@canonical")}<>@executor OR DATALENGTH({_utf8("@canonical")})<>DATALENGTH(@executor)
 THROW 51303, 'DPONE_NATIVE_GENERATION_EXECUTOR_NONCANONICAL', 1;"""


def _attempt_identity() -> str:
    return f"""
{_shape("JSON_QUERY(@json,'$.workspace_attempt')", dict(activation_id=1, attempt_id=1, workflow_id=1, write_subjects=4, request_sha256=1))}
{_shape("JSON_QUERY(@json,'$.guard')", dict(guard_id=1, fencing_epoch=2))}
DECLARE @activation_text nvarchar(max)=JSON_VALUE(@json,'$.workspace_attempt.activation_id');
DECLARE @attempt_text nvarchar(max)=JSON_VALUE(@json,'$.workspace_attempt.attempt_id');
DECLARE @workflow nvarchar(max)=JSON_VALUE(@json,'$.workspace_attempt.workflow_id');
DECLARE @epoch_text nvarchar(max)=JSON_VALUE(@json,'$.guard.fencing_epoch');
IF @activation_text IS NULL OR DATALENGTH(@activation_text)<>72
 OR TRY_CONVERT(uniqueidentifier,@activation_text) IS NULL
 OR CONVERT(varbinary(max),@activation_text)<>CONVERT(varbinary(max),LOWER(CONVERT(nvarchar(36),TRY_CONVERT(uniqueidentifier,@activation_text))))
 OR @attempt_text IS NULL OR DATALENGTH(@attempt_text)<>142 OR LEFT(@attempt_text,7)<>N'sha256:'
 OR SUBSTRING(@attempt_text,8,64) COLLATE Latin1_General_100_BIN2 LIKE N'%[^0-9a-f]%'
 OR @workflow IS NULL OR DATALENGTH(@workflow) NOT BETWEEN 2 AND 128
 OR LEFT(@workflow,1) COLLATE Latin1_General_100_BIN2 NOT LIKE N'[a-z]'
 OR @workflow COLLATE Latin1_General_100_BIN2 LIKE N'%[^a-z0-9_]%'
 OR @epoch_text IS NULL OR @epoch_text COLLATE Latin1_General_100_BIN2 LIKE N'%[^0-9]%'
 OR DATALENGTH(JSON_VALUE(@json,'$.guard.guard_id')) NOT BETWEEN 2 AND 1024
 THROW 51301, 'DPONE_NATIVE_GENERATION_ATTEMPT_GRAMMAR_INVALID', 1;
DECLARE @subjects TABLE (ordinal int PRIMARY KEY, subject nvarchar(max) COLLATE Latin1_General_100_BIN2 NOT NULL);
IF EXISTS (SELECT 1 FROM OPENJSON(@json,'$.workspace_attempt.write_subjects') WHERE type<>1)
 THROW 51301, 'DPONE_NATIVE_GENERATION_SUBJECT_TYPE_INVALID', 1;
INSERT INTO @subjects SELECT CONVERT(int,[key]),value FROM OPENJSON(@json,'$.workspace_attempt.write_subjects');
IF NOT EXISTS (SELECT 1 FROM @subjects)
 OR EXISTS (SELECT 1 FROM @subjects WHERE DATALENGTH(subject)<>142 OR LEFT(subject,7)<>N'sha256:'
 OR SUBSTRING(subject,8,64) LIKE N'%[^0-9a-f]%')
 OR EXISTS (SELECT 1 FROM @subjects a JOIN @subjects b ON b.ordinal=a.ordinal+1 WHERE a.subject>=b.subject)
 THROW 51301, 'DPONE_NATIVE_GENERATION_SUBJECT_ORDER_INVALID', 1;
DECLARE @subjects_json nvarchar(max);
SELECT @subjects_json=STRING_AGG(CONVERT(nvarchar(max),N'"')+subject+N'"',N',') WITHIN GROUP (ORDER BY ordinal) FROM @subjects;
DECLARE @unsigned nvarchar(max)=CONVERT(nvarchar(max),N'{{"activation_id":"')+@activation_text
 +N'","attempt_id":"'+@attempt_text+N'","schema":"dpone.dbt-workspace-attempt-request.v1","workflow_id":"'
 +@workflow+N'","write_subjects":['+@subjects_json+N']}}';
DECLARE @derived varchar(71)='sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',{_utf8("@unsigned")}),2));
IF {_utf8("JSON_VALUE(@json,'$.workspace_attempt.request_sha256')")} IS NULL
 OR {_utf8("JSON_VALUE(@json,'$.workspace_attempt.request_sha256')")}<>CONVERT(varbinary(max),@derived)
 THROW 51301, 'DPONE_NATIVE_GENERATION_ATTEMPT_HASH_INVALID', 1;
"""


def generation_procedure_name(operation: str) -> str:
    """Resolve only the fixed approved source-custody operations."""
    return {
        "reserve": "native_generation_reserve_v1",
        "bind": "native_source_writer_bind_v1",
        "read": "native_source_custody_read_v1",
        "close": "native_source_writer_close_v1",
        "closure_read": "native_source_writer_close_read_v1",
        "complete": "native_source_writer_completion_record_v1",
        "completion_read": "native_source_writer_completion_read_v1",
        "freeze": "native_source_freeze_v1",
        "freeze_read": "native_source_freeze_read_v1",
        "freeze_inspect": "native_source_freeze_inspect_v1",
    }[operation]


def generation_procedure(schema: str, operation: str, *, extended: bool = True, completed: bool = False) -> str:
    """Render explicit layouts, preserving exact historical procedure definitions."""
    if completed and not extended:
        raise ValueError("completion layout requires admission columns")
    if operation in {"complete", "completion_read", "freeze", "freeze_read", "freeze_inspect"} and not completed:
        raise ValueError("positive completion procedures require the completion layout")
    signatures = {
        "reserve": "@request varbinary(max), @locator varbinary(max), @digest varbinary(max)",
        "bind": "@generation uniqueidentifier, @expected_revision bigint, @executor varbinary(max)",
        "read": "@generation uniqueidentifier",
        "close": "@generation uniqueidentifier, @expected_revision bigint, @locator varbinary(max), @digest varbinary(max)",
        "closure_read": "@generation uniqueidentifier",
        "complete": "@generation uniqueidentifier, @expected_revision bigint, @admission varbinary(max), @completion varbinary(max), @locator varbinary(max), @digest varbinary(max)",
        "completion_read": "@generation uniqueidentifier, @expected_revision bigint, @admission varbinary(max), @completion varbinary(max), @locator varbinary(max), @digest varbinary(max)",
    }
    freeze_signature = (
        "@generation uniqueidentifier, @expected_revision bigint, @admission varbinary(max), "
        "@completion varbinary(max), @completion_locator varbinary(max), @completion_digest varbinary(max), "
        "@frozen varbinary(max), @locator varbinary(max), @digest varbinary(max)"
    )
    signatures.update(freeze=freeze_signature, freeze_read=freeze_signature, freeze_inspect=freeze_signature)
    if operation in {"reserve", "bind", "read"}:
        body = {"reserve": _reserve, "bind": _bind, "read": _read}[operation](
            schema, extended=extended, completed=completed
        )
    elif operation in {"complete", "completion_read"}:
        body = completion_body(
            schema,
            read_only=operation == "completion_read",
            current_owner_sql=_physical_owner(schema),
            current_snapshot_sql=_read(schema, completed=True),
        )
    elif operation in {"freeze", "freeze_read", "freeze_inspect"}:
        body = freeze_body(
            schema,
            read_only=operation != "freeze",
            allow_building=operation == "freeze_inspect",
            current_owner_sql=_physical_owner(schema),
            current_snapshot_sql=_read(schema, completed=True),
        )
    else:
        body = {"close": _close, "closure_read": _closure_read}[operation](schema)
    return f"""CREATE PROCEDURE [{schema}].[{generation_procedure_name(operation)}]
@authority_locator varbinary(max), @authority_digest varbinary(max), {signatures[operation]}
AS
BEGIN
SET NOCOUNT ON; SET XACT_ABORT ON;
IF @@TRANCOUNT=0 BEGIN TRANSACTION;
IF @@TRANCOUNT<>1 THROW 51300, 'DPONE_NATIVE_GENERATION_TRANSACTION_INVALID', 1;
IF @authority_locator IS NULL OR DATALENGTH(@authority_locator) NOT BETWEEN 1 AND 1048576
 OR @authority_digest IS NULL OR DATALENGTH(@authority_digest)<>71
 THROW 51300, 'DPONE_NATIVE_GENERATION_AUTHORITY_INVALID', 1;
IF IS_SRVROLEMEMBER('sysadmin')=1 OR IS_MEMBER('db_owner')=1
 OR HAS_PERMS_BY_NAME(N'{schema}',N'SCHEMA',N'ALTER')=1
 THROW 51300, 'DPONE_NATIVE_GENERATION_RUNTIME_PRINCIPAL_INVALID', 1;
IF NOT EXISTS (SELECT 1 FROM [{schema}].[native_original_authorities_v1] WITH (HOLDLOCK)
 WHERE authority_hash=HASHBYTES('SHA2_256',@authority_locator)
 AND authority_locator=@authority_locator AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator)
 AND authority_digest=@authority_digest AND DATALENGTH(authority_digest)=DATALENGTH(@authority_digest)
 AND runtime_principal_id=USER_ID() AND runtime_principal_sid=(SELECT sid FROM sys.database_principals WHERE principal_id=USER_ID()) AND schema_version=1)
 THROW 51300, 'DPONE_NATIVE_GENERATION_AUTHORITY_INVALID', 1;
{body}
END"""


def _read(schema: str, *, extended: bool = True, completed: bool = False) -> str:
    additional = ", writer_admission, outcome, admission_sequence, admission_closure" if extended else ""
    if completed:
        additional += ", phase, completion_payload, completion_locator, completion_digest, frozen_payload, frozen_locator, frozen_digest"
    return f"""SELECT LOWER(CONVERT(char(36),generation_id)), guard_epoch, revision,
 reservation_locator, reservation_digest, executor{additional}
FROM [{schema}].[native_generations_v1] WITH (HOLDLOCK)
WHERE generation_id=@generation AND authority_locator=@authority_locator
 AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator)
 AND authority_digest=@authority_digest AND DATALENGTH(authority_digest)=DATALENGTH(@authority_digest);"""


def _physical_owner(schema: str) -> str:
    """Authenticate current P using fields extracted from retained request bytes."""
    return f"""
{_attempt_identity()}
DECLARE @activation uniqueidentifier=TRY_CONVERT(uniqueidentifier,JSON_VALUE(@json,'$.workspace_attempt.activation_id'));
DECLARE @attempt varchar(71)=JSON_VALUE(@json,'$.workspace_attempt.attempt_id');
DECLARE @guard nvarchar(512)=JSON_VALUE(@json,'$.guard.guard_id');
DECLARE @epoch bigint=TRY_CONVERT(bigint,JSON_VALUE(@json,'$.guard.fencing_epoch'));
IF @activation IS NULL OR @attempt IS NULL OR @guard IS NULL OR @epoch IS NULL OR @epoch<=0
 THROW 51301, 'DPONE_NATIVE_GENERATION_PHYSICAL_IDENTITY_INVALID', 1;
IF NOT EXISTS (SELECT 1
 FROM [{schema}].[dbt_workspace_activations] a WITH (UPDLOCK,HOLDLOCK)
 JOIN [{schema}].[dbt_workspace_attempts] t WITH (UPDLOCK,HOLDLOCK) ON t.activation_id=a.activation_id
 JOIN [{schema}].[dbt_workspace_attempt_guards] ag WITH (UPDLOCK,HOLDLOCK) ON ag.attempt_id=t.attempt_id
 JOIN [{schema}].[dbt_workspace_activation_guards] g WITH (UPDLOCK,HOLDLOCK)
 ON g.activation_id=a.activation_id AND g.guard_id=ag.guard_id AND g.fencing_epoch=ag.fencing_epoch
 JOIN [{schema}].[semantic_refresh_guards] p WITH (UPDLOCK,HOLDLOCK)
 ON p.resource_id=g.guard_id AND p.fencing_epoch=g.fencing_epoch
 WHERE a.activation_id=@activation AND a.state=N'ACTIVE' AND t.attempt_id=@attempt AND t.state=N'RUNNING'
 AND CONVERT(varbinary(max),t.request_sha256)={_utf8("JSON_VALUE(@json,'$.workspace_attempt.request_sha256')")}
 AND CONVERT(varbinary(max),t.workflow_id)=CONVERT(varbinary(max),JSON_VALUE(@json,'$.workspace_attempt.workflow_id'))
 AND CONVERT(varbinary(max),g.guard_id)=CONVERT(varbinary(max),@guard) AND g.fencing_epoch=@epoch
 AND p.owner_id=N'dbt-workspace:'+LOWER(CONVERT(nvarchar(36),@activation))
 AND p.workflow_id=LOWER(CONVERT(nvarchar(36),@activation)) AND p.operation_id IS NULL AND p.status=N'HELD'
 AND a.environment=JSON_VALUE(@json,'$.subject.authority.environment')
 AND a.release_id=JSON_VALUE(@json,'$.subject.authority.release_id')
 AND a.deployment_id=JSON_VALUE(@json,'$.subject.authority.deployment_id'))
 THROW 51301, 'DPONE_NATIVE_GENERATION_PHYSICAL_OWNER_CHANGED', 1;
IF NOT EXISTS (SELECT 1 FROM OPENJSON(@json,'$.workspace_attempt.write_subjects'))
 OR EXISTS (SELECT 1 FROM OPENJSON(@json,'$.workspace_attempt.write_subjects') j
 LEFT JOIN [{schema}].[dbt_workspace_activation_write_subjects] w WITH (UPDLOCK,HOLDLOCK)
 ON w.activation_id=@activation AND CONVERT(varbinary(max),w.write_subject_sha256)={_utf8("j.value")}
 AND CONVERT(varbinary(max),w.guard_id)=CONVERT(varbinary(max),@guard)
 WHERE j.type<>1 OR w.guard_id IS NULL)
 OR EXISTS (SELECT 1 FROM [{schema}].[dbt_workspace_attempt_guards] WITH (UPDLOCK,HOLDLOCK)
 WHERE attempt_id=@attempt AND CONVERT(varbinary(max),guard_id)<>CONVERT(varbinary(max),@guard))
 THROW 51301, 'DPONE_NATIVE_GENERATION_WRITE_FOOTPRINT_INVALID', 1;
"""


def _reserve(schema: str, *, extended: bool = True, completed: bool = False) -> str:
    columns = ",writer_admission,outcome,admission_sequence,admission_closure" if extended else ""
    values = ",'OPEN','ACTIVE',0,NULL" if extended else ""
    if completed:
        columns += (
            ",phase,completion_payload,completion_locator,completion_digest,frozen_payload,frozen_locator,frozen_digest"
        )
        values += ",'RESERVED',NULL,NULL,NULL,NULL,NULL,NULL"
    return f"""
IF @request IS NULL OR @locator IS NULL OR @digest IS NULL OR DATALENGTH(@request) NOT BETWEEN 1 AND 1048576 OR DATALENGTH(@locator) NOT BETWEEN 1 AND 1048576
 OR DATALENGTH(@digest)<>71
 OR @digest<>CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@request),2)))
 THROW 51302, 'DPONE_NATIVE_GENERATION_REQUEST_DIGEST_INVALID', 1;
{_decode_bytes("@request", "@json")}
IF ISJSON(@json,OBJECT)<>1 OR {_utf8("@json")}<>@request OR DATALENGTH({_utf8("@json")})<>DATALENGTH(@request)
 THROW 51302, 'DPONE_NATIVE_GENERATION_REQUEST_INVALID', 1;
{_shape("@json", dict(schema=1, subject=5, workspace_attempt=5, guard=5, profile=5, command=5, requested_bytes=2))}
{_shape("JSON_QUERY(@json,'$.profile')", dict(locator=1, sha256=1))}
{_shape("JSON_QUERY(@json,'$.command')", dict(locator=1, sha256=1))}
IF {_utf8(_scalar("@json", "$.schema"))}<>CONVERT(varbinary(max),'dpone.native-generation-admission.v1') OR DATALENGTH({_utf8(_scalar("@json", "$.schema"))})<>36
 THROW 51302, 'DPONE_NATIVE_GENERATION_REQUEST_SCHEMA_INVALID', 1;
DECLARE @generation uniqueidentifier=TRY_CONVERT(uniqueidentifier,JSON_VALUE(@json,'$.subject.generation_id'));
{_shape("JSON_QUERY(@json,'$.subject')", dict(schema=1, scope=1, authority=5, generation_id=1))}
DECLARE @subject varbinary(max)={_utf8("JSON_QUERY(@json,'$.subject')")};
IF @generation IS NULL OR JSON_VALUE(@json,'$.subject.scope')<>'GENERATION'
 THROW 51302, 'DPONE_NATIVE_GENERATION_SUBJECT_INVALID', 1;
IF NOT EXISTS (SELECT 1 FROM [{schema}].[native_original_bindings_v1] WITH (HOLDLOCK)
 WHERE locator_hash=HASHBYTES('SHA2_256',@locator) AND locator=@locator AND DATALENGTH(locator)=DATALENGTH(@locator)
 AND payload_digest=@digest AND authority_locator=@authority_locator
 AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator) AND authority_digest=@authority_digest
 AND subject=@subject AND DATALENGTH(subject)=DATALENGTH(@subject)
 AND kind=CONVERT(varbinary(128),'generation_stored_file_v1'))
 THROW 51302, 'DPONE_NATIVE_GENERATION_REQUEST_ORIGINAL_UNBOUND', 1;
{_physical_owner(schema)}
DECLARE @profile_locator varbinary(max)={_utf8(_scalar("@json", "$.profile.locator"))};
DECLARE @profile_digest varbinary(max)={_utf8(_scalar("@json", "$.profile.sha256"))};
DECLARE @requested bigint=TRY_CONVERT(bigint,JSON_VALUE(@json,'$.requested_bytes'));
DECLARE @capacity bigint, @charged bigint;
SELECT @capacity=capacity_bytes,@charged=charged_bytes
FROM [{schema}].[native_generation_capacity_v1] WITH (UPDLOCK,HOLDLOCK)
WHERE guard_hash=HASHBYTES('SHA2_256',CONVERT(varbinary(max),@guard))
 AND guard_id=CONVERT(varbinary(max),@guard) AND DATALENGTH(guard_id)=DATALENGTH(@guard);
IF @capacity IS NULL OR @requested IS NULL OR @requested<=0
 OR NOT EXISTS (SELECT 1 FROM [{schema}].[native_generation_profiles_v1] WITH (HOLDLOCK)
 WHERE guard_hash=HASHBYTES('SHA2_256',CONVERT(varbinary(max),@guard))
 AND profile_hash=HASHBYTES('SHA2_256',@profile_locator)
 AND profile_locator=@profile_locator AND DATALENGTH(profile_locator)=DATALENGTH(@profile_locator)
 AND profile_digest=@profile_digest AND DATALENGTH(profile_digest)=DATALENGTH(@profile_digest)
 AND authority_locator=@authority_locator AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator)
 AND authority_digest=@authority_digest AND @requested<=max_generation_bytes)
 THROW 51302, 'DPONE_NATIVE_GENERATION_PROFILE_OR_CAPACITY_INVALID', 1;
IF EXISTS (SELECT 1 FROM [{schema}].[native_generations_v1] WITH (UPDLOCK,HOLDLOCK) WHERE generation_id=@generation)
BEGIN
 IF NOT EXISTS (SELECT 1 FROM [{schema}].[native_generations_v1]
 WHERE generation_id=@generation AND request=@request AND DATALENGTH(request)=DATALENGTH(@request)
 AND reservation_locator=@locator AND DATALENGTH(reservation_locator)=DATALENGTH(@locator)
 AND reservation_digest=@digest AND authority_locator=@authority_locator
 AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator) AND authority_digest=@authority_digest)
 THROW 51302, 'DPONE_NATIVE_GENERATION_RESERVATION_CONFLICT', 1;
END
ELSE
BEGIN
 IF @requested>@capacity-@charged THROW 51302, 'DPONE_NATIVE_GENERATION_CAPACITY_EXHAUSTED', 1;
 UPDATE [{schema}].[native_generation_capacity_v1] SET charged_bytes=charged_bytes+@requested
 WHERE guard_hash=HASHBYTES('SHA2_256',CONVERT(varbinary(max),@guard));
 INSERT INTO [{schema}].[native_generations_v1]
 (generation_id,guard_hash,guard_epoch,revision,authority_locator,authority_digest,reservation_locator,reservation_digest,request,executor{columns})
 VALUES (@generation,HASHBYTES('SHA2_256',CONVERT(varbinary(max),@guard)),@epoch,1,@authority_locator,@authority_digest,@locator,@digest,@request,NULL{values});
END
{_read(schema, extended=extended, completed=completed)}"""


def _bind(schema: str, *, extended: bool = True, completed: bool = False) -> str:
    admission = " AND writer_admission='OPEN' AND outcome='ACTIVE'" if extended else ""
    sequence = ",admission_sequence=1" if extended else ""
    if completed:
        admission += " AND phase='RESERVED'"
        sequence += ",phase='BUILDING'"
    return f"""
DECLARE @request varbinary(max);
SELECT @request=request FROM [{schema}].[native_generations_v1] WITH (READCOMMITTEDLOCK)
 WHERE generation_id=@generation AND authority_locator=@authority_locator
 AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator) AND authority_digest=@authority_digest
 AND revision=@expected_revision AND executor IS NULL{admission};
IF @request IS NULL OR @expected_revision NOT BETWEEN 1 AND 9223372036854775806
 THROW 51303, 'DPONE_NATIVE_GENERATION_WRITER_ALREADY_BOUND_OR_STALE', 1;
{_decode_bytes("@request", "@json")}
{_physical_owner(schema)}
IF DATALENGTH(@executor) NOT BETWEEN 1 AND 1048576 THROW 51303, 'DPONE_NATIVE_GENERATION_EXECUTOR_INVALID', 1;
{_decode_bytes("@executor", "@binding")}
IF ISJSON(@binding,OBJECT)<>1 OR {_utf8("@binding")}<>@executor OR DATALENGTH({_utf8("@binding")})<>DATALENGTH(@executor)
 THROW 51303, 'DPONE_NATIVE_GENERATION_EXECUTOR_INVALID', 1;
{_shape("@binding", dict(schema=1, generation_id=1, guard_epoch=2, invocation_id=1, reservation=5, profile=5, command=5))}
{_shape("JSON_QUERY(@binding,'$.reservation')", dict(locator=1, sha256=1))}
{_shape("JSON_QUERY(@binding,'$.profile')", dict(locator=1, sha256=1))}
{_shape("JSON_QUERY(@binding,'$.command')", dict(locator=1, sha256=1))}
IF {_utf8(_scalar("@binding", "$.schema"))}<>CONVERT(varbinary(max),'dpone.native-source-executor-binding.v1')
 OR DATALENGTH({_utf8(_scalar("@binding", "$.schema"))})<>39
 OR TRY_CONVERT(uniqueidentifier,JSON_VALUE(@binding,'$.generation_id')) IS NULL
 OR TRY_CONVERT(uniqueidentifier,JSON_VALUE(@binding,'$.generation_id'))<>@generation
 OR TRY_CONVERT(bigint,JSON_VALUE(@binding,'$.guard_epoch')) IS NULL
 OR TRY_CONVERT(bigint,JSON_VALUE(@binding,'$.guard_epoch'))<>@epoch
 OR TRY_CONVERT(uniqueidentifier,JSON_VALUE(@binding,'$.invocation_id')) IS NULL
 OR DATALENGTH(JSON_VALUE(@binding,'$.generation_id'))<>72
 OR DATALENGTH(JSON_VALUE(@binding,'$.invocation_id'))<>72
 OR CONVERT(varbinary(max),JSON_VALUE(@binding,'$.generation_id'))<>CONVERT(varbinary(max),LOWER(CONVERT(nvarchar(36),TRY_CONVERT(uniqueidentifier,JSON_VALUE(@binding,'$.generation_id')))))
 OR CONVERT(varbinary(max),JSON_VALUE(@binding,'$.invocation_id'))<>CONVERT(varbinary(max),LOWER(CONVERT(nvarchar(36),TRY_CONVERT(uniqueidentifier,JSON_VALUE(@binding,'$.invocation_id')))))
 OR JSON_VALUE(@binding,'$.guard_epoch') COLLATE Latin1_General_100_BIN2 LIKE N'%[^0-9]%'
 OR CONVERT(varbinary(max),JSON_VALUE(@binding,'$.guard_epoch'))<>CONVERT(varbinary(max),CONVERT(nvarchar(20),@epoch))
 OR NOT EXISTS (SELECT 1 FROM [{schema}].[native_generations_v1]
 WHERE generation_id=@generation
 AND reservation_locator={_utf8(_scalar("@binding", "$.reservation.locator"))}
 AND DATALENGTH(reservation_locator)=DATALENGTH({_utf8(_scalar("@binding", "$.reservation.locator"))})
 AND reservation_digest={_utf8(_scalar("@binding", "$.reservation.sha256"))})
 OR CONVERT(varbinary(max),JSON_QUERY(@binding,'$.profile'))<>CONVERT(varbinary(max),JSON_QUERY(@json,'$.profile'))
 OR CONVERT(varbinary(max),JSON_QUERY(@binding,'$.command'))<>CONVERT(varbinary(max),JSON_QUERY(@json,'$.command'))
 THROW 51303, 'DPONE_NATIVE_GENERATION_EXECUTOR_MISMATCH', 1;
{_canonical_executor()}
UPDATE [{schema}].[native_generations_v1] SET executor=@executor,revision=revision+1{sequence}
 WHERE generation_id=@generation AND revision=@expected_revision AND executor IS NULL{admission};
IF @@ROWCOUNT<>1 THROW 51303, 'DPONE_NATIVE_GENERATION_WRITER_ALREADY_BOUND_OR_STALE', 1;
{_read(schema, extended=extended, completed=completed)}"""


def _close(schema: str) -> str:
    return f"""
IF @generation IS NULL OR @expected_revision IS NULL OR @expected_revision NOT BETWEEN 2 AND 9223372036854775806
 OR @locator IS NULL OR DATALENGTH(@locator) NOT BETWEEN 1 AND 1048576 OR @digest IS NULL OR DATALENGTH(@digest)<>71
 THROW 51304, 'DPONE_NATIVE_SOURCE_CLOSE_IDENTITY_INVALID', 1;
DECLARE @request varbinary(max), @executor varbinary(max), @sequence bigint;
SELECT @request=request,@executor=executor,@sequence=admission_sequence
 FROM [{schema}].[native_generations_v1] WITH (READCOMMITTEDLOCK)
 WHERE generation_id=@generation AND authority_locator=@authority_locator
 AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator)
 AND authority_digest=@authority_digest AND DATALENGTH(authority_digest)=DATALENGTH(@authority_digest)
 AND reservation_locator=@locator AND DATALENGTH(reservation_locator)=DATALENGTH(@locator)
 AND reservation_digest=@digest AND DATALENGTH(reservation_digest)=DATALENGTH(@digest)
 AND revision=@expected_revision AND executor IS NOT NULL AND writer_admission='OPEN'
 AND outcome IN ('ACTIVE','UNKNOWN') AND admission_sequence>0 AND admission_closure IS NULL;
IF @request IS NULL THROW 51304, 'DPONE_NATIVE_SOURCE_CLOSE_STALE_OR_CLOSED', 1;
{_decode_bytes("@request", "@json")}
{_physical_owner(schema)}
{_decode_bytes("@executor", "@binding")}
DECLARE @closure nvarchar(max)=CONVERT(nvarchar(max),N'{{"admission_sequence":')+CONVERT(nvarchar(20),@sequence)
 +N',"executor":'+@binding+N',"revision":'+CONVERT(nvarchar(20),@expected_revision+1)
 +N',"schema":"dpone.native-source-admission-closure.v1"}}';
UPDATE [{schema}].[native_generations_v1]
 SET writer_admission='CLOSED',revision=revision+1,admission_closure={_utf8("@closure")}
 WHERE generation_id=@generation AND revision=@expected_revision AND guard_epoch=@epoch
 AND executor=@executor AND DATALENGTH(executor)=DATALENGTH(@executor)
 AND admission_sequence=@sequence AND writer_admission='OPEN' AND admission_closure IS NULL
 AND outcome IN ('ACTIVE','UNKNOWN');
IF @@ROWCOUNT<>1 THROW 51304, 'DPONE_NATIVE_SOURCE_CLOSE_STALE_OR_CLOSED', 1;
{_closure_read(schema)}"""


def _closure_read(schema: str) -> str:
    locator = "CONVERT(nvarchar(max),N'generations/')+LOWER(CONVERT(nvarchar(36),generation_id))+N'/writer-admission-closure.json'"
    return f"""SELECT admission_closure, {_utf8(locator)},
 CONVERT(varbinary(max),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',admission_closure),2)))
FROM [{schema}].[native_generations_v1] WITH (HOLDLOCK)
WHERE generation_id=@generation AND authority_locator=@authority_locator
 AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator)
 AND authority_digest=@authority_digest AND DATALENGTH(authority_digest)=DATALENGTH(@authority_digest)
 AND writer_admission='CLOSED' AND admission_closure IS NOT NULL;"""
