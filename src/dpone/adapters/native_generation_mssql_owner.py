"""Render the retained-request identity and current physical-owner SQL predicate.

The caller supplies ``@json`` from retained request bytes inside its protected
transaction. This producer preserves the existing generation predicate, including
request identity, lease joins and exact write-subject membership; it does not
introduce a separate ownership policy.
"""

from __future__ import annotations

from dpone.adapters.native_generation_mssql_json import shape as _shape
from dpone.adapters.native_generation_mssql_json import utf8 as _utf8


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


def physical_owner(schema: str) -> str:
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
