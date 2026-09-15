"""Fixed positive completion SQL; publication and authentication occur upstream.

The enclosing procedure supplies caller-authority validation and the existing
physical-owner predicate unchanged. This renderer grants no dispatch or cleanup.
"""

from __future__ import annotations

from dpone.adapters.native_generation_mssql_json import canonical_reference, decode_bytes, shape, utf8
from dpone.contracts.mssql_object_name import native_control_schema


def completion_body(
    schema: str,
    *,
    read_only: bool,
    current_owner_sql: str,
    current_snapshot_sql: str,
) -> str:
    """Validate the entire request; acquire physical locks before generation locks.

    The read variant contains no mutation. It requires the exact accepted next
    revision and independently revalidates physical ownership after a lost ACK.
    Both supplied SQL fragments come from the fixed internal query composer.
    """
    native_control_schema(schema)
    if type(read_only) is not bool:
        raise ValueError("completion operation must select an exact read-only mode")
    table = f"[{schema}].[native_generations_v1]"
    references = ("artifact_inventory", "build_evidence", "command", "termination", "toolchain")
    reference_shapes = "\n".join(
        shape(f"JSON_QUERY(@positive,'$.{name}')", dict(locator=1, sha256=1)) for name in references
    )
    mutation = (
        ""
        if read_only
        else f"""
IF NOT EXISTS (SELECT 1 FROM {table} WHERE generation_id=@generation AND completion_payload IS NOT NULL)
BEGIN
 UPDATE {table} SET completion_payload=@completion,completion_locator=@locator,
  completion_digest=@digest,revision=revision+1,outcome='ACTIVE'
 WHERE generation_id=@generation AND guard_epoch=@epoch AND revision=@expected_revision
  AND phase='BUILDING' AND writer_admission='CLOSED' AND outcome IN ('ACTIVE','UNKNOWN')
  AND executor=@executor AND DATALENGTH(executor)=DATALENGTH(@executor)
  AND admission_closure=@admission AND DATALENGTH(admission_closure)=DATALENGTH(@admission)
  AND completion_payload IS NULL AND completion_locator IS NULL AND completion_digest IS NULL;
 IF @@ROWCOUNT<>1 THROW 51305, 'DPONE_NATIVE_SOURCE_COMPLETION_CAS_REJECTED', 1;
END;"""
    )
    return f"""
IF @generation IS NULL OR @expected_revision IS NULL OR @expected_revision NOT BETWEEN 3 AND 9223372036854775806
 OR @admission IS NULL OR DATALENGTH(@admission) NOT BETWEEN 1 AND 1048576
 OR @completion IS NULL OR DATALENGTH(@completion) NOT BETWEEN 1 AND 1048576
 OR @locator IS NULL OR DATALENGTH(@locator) NOT BETWEEN 1 AND 4096
 OR @digest IS NULL OR DATALENGTH(@digest)<>71
 THROW 51305, 'DPONE_NATIVE_SOURCE_COMPLETION_INPUT_INVALID', 1;
DECLARE @request varbinary(max);
SELECT @request=request FROM {table} WITH (READCOMMITTEDLOCK)
 WHERE generation_id=@generation AND authority_locator=@authority_locator
 AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator)
 AND authority_digest=@authority_digest AND DATALENGTH(authority_digest)=DATALENGTH(@authority_digest);
IF @request IS NULL THROW 51305, 'DPONE_NATIVE_SOURCE_COMPLETION_OWNER_MISSING', 1;
{decode_bytes("@request", "@json")}
{current_owner_sql}
DECLARE @executor varbinary(max);
SELECT @executor=executor FROM {table} WITH (UPDLOCK,HOLDLOCK)
 WHERE generation_id=@generation AND guard_epoch=@epoch AND phase='BUILDING'
 AND writer_admission='CLOSED' AND outcome IN ('ACTIVE','UNKNOWN')
 AND admission_closure=@admission AND DATALENGTH(admission_closure)=DATALENGTH(@admission);
IF @executor IS NULL THROW 51305, 'DPONE_NATIVE_SOURCE_COMPLETION_CUSTODY_MISMATCH', 1;
{decode_bytes("@executor", "@binding")}
{decode_bytes("@completion", "@positive")}
{shape("@positive", dict(schema=1, executor=5, command=5, toolchain=5, build_evidence=5, artifact_inventory=5, termination=5))}
{reference_shapes}
IF {utf8("JSON_QUERY(@positive,'$.executor')")}<>@executor
 OR DATALENGTH({utf8("JSON_QUERY(@positive,'$.executor')")})<>DATALENGTH(@executor)
 OR {utf8("JSON_QUERY(@positive,'$.command')")}<>{utf8("JSON_QUERY(@binding,'$.command')")}
 OR DATALENGTH({utf8("JSON_QUERY(@positive,'$.command')")})<>DATALENGTH({utf8("JSON_QUERY(@binding,'$.command')")})
 THROW 51305, 'DPONE_NATIVE_SOURCE_COMPLETION_EXECUTOR_MISMATCH', 1;
DECLARE @canonical_positive nvarchar(max)=CONVERT(nvarchar(max),N'{{"artifact_inventory":')
 +{canonical_reference("@positive", "$.artifact_inventory")}
 +N',"build_evidence":'+{canonical_reference("@positive", "$.build_evidence")}
 +N',"command":'+{canonical_reference("@positive", "$.command")}
 +N',"executor":'+@binding+N',"schema":"dpone.native-source-trusted-build-completion.v1","termination":'
 +{canonical_reference("@positive", "$.termination")}
 +N',"toolchain":'+{canonical_reference("@positive", "$.toolchain")}+N'}}';
IF @canonical_positive IS NULL OR {utf8("@canonical_positive")}<>@completion
 OR DATALENGTH({utf8("@canonical_positive")})<>DATALENGTH(@completion)
 OR @digest<>CONVERT(varbinary(max),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@completion),2)))
 THROW 51305, 'DPONE_NATIVE_SOURCE_COMPLETION_ORIGINAL_INVALID', 1;
{mutation}
IF NOT EXISTS (SELECT 1 FROM {table} WHERE generation_id=@generation AND guard_epoch=@epoch
 AND revision=@expected_revision+1 AND phase='BUILDING' AND writer_admission='CLOSED' AND outcome='ACTIVE'
 AND executor=@executor AND DATALENGTH(executor)=DATALENGTH(@executor)
 AND admission_closure=@admission AND DATALENGTH(admission_closure)=DATALENGTH(@admission)
 AND completion_payload=@completion AND DATALENGTH(completion_payload)=DATALENGTH(@completion)
 AND completion_locator=@locator AND DATALENGTH(completion_locator)=DATALENGTH(@locator)
 AND completion_digest=@digest AND DATALENGTH(completion_digest)=DATALENGTH(@digest))
 THROW 51305, 'DPONE_NATIVE_SOURCE_COMPLETION_READBACK_MISMATCH', 1;
{current_snapshot_sql}"""
