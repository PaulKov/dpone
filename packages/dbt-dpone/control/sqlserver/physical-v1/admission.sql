CREATE PROCEDURE [{{LOCAL_SCHEMA}}].[physical_require_source_v1]
 @registration_id uniqueidentifier, @generation uniqueidentifier, @expected_invocation uniqueidentifier
AS
BEGIN
SET NOCOUNT ON;
IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 OR @registration_id IS NULL
 OR @generation IS NULL OR @expected_invocation IS NULL
 THROW 51420, 'DPONE_PHYSICAL_SOURCE_TRANSACTION_OR_ID_INVALID', 1;
{{MODEL_CALLER}}
DECLARE @payload varbinary(max), @digest varbinary(71);
SELECT @payload=payload,@digest=registration_digest
 FROM [{{LOCAL_SCHEMA}}].[physical_runtime_registrations_v1] WITH (READCOMMITTEDLOCK)
 WHERE registration_id=@registration_id;
IF @payload IS NULL OR DATALENGTH(@payload) NOT BETWEEN 1 AND 1048576
 OR @digest IS NULL OR DATALENGTH(@digest)<>71
 OR @digest<>CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@payload),2)))
 THROW 51422, 'DPONE_PHYSICAL_SOURCE_REGISTRATION_INVALID', 1;
{{REGISTRATION_DECODE}}
IF ISJSON(@registration,OBJECT)<>1 OR (SELECT COUNT(*) FROM OPENJSON(@registration))<>18
 OR JSON_VALUE(@registration,'$.schema')<>'dpone.mssql-physical-runtime-registration.v1'
 THROW 51422, 'DPONE_PHYSICAL_SOURCE_REGISTRATION_INVALID', 1;
IF NOT EXISTS (SELECT 1 FROM [{{LOCAL_SCHEMA}}].[physical_runtime_registrations_v1] r
 WHERE r.registration_id=@registration_id AND {{PROJECTION_CHECK}})
 THROW 51422, 'DPONE_PHYSICAL_SOURCE_REGISTRATION_PROJECTION_INVALID', 1;
{{MODEL_PIN}}
IF NOT EXISTS (SELECT 1 FROM [{{LOCAL_SCHEMA}}].[physical_runtime_registrations_v1]
 WHERE registration_id=@registration_id AND (
 (metadata_model_principal_id=@caller_id AND metadata_model_sid=@caller_sid
 AND DATALENGTH(metadata_model_sid)=DATALENGTH(@caller_sid)) OR
 (build_model_principal_id=@caller_id AND build_model_sid=@caller_sid
 AND DATALENGTH(build_model_sid)=DATALENGTH(@caller_sid))))
 THROW 51420, 'DPONE_PHYSICAL_SOURCE_CALLER_UNREGISTERED', 1;
DECLARE @guard_epoch bigint,@source_revision bigint,@reservation_locator varbinary(max),
 @reservation_digest varbinary(max),@executor varbinary(max),@control_id int,@control_sid varbinary(85);
EXEC {{CONTROL_DATABASE}}.[{{CONTROL_SCHEMA}}].[physical_control_require_source_v1]
 @payload=@payload,@digest=@digest,@model_id=@caller_id,@model_sid=@caller_sid,
 @generation=@generation,@expected_invocation=@expected_invocation,
 @guard_epoch=@guard_epoch OUTPUT,@source_revision=@source_revision OUTPUT,
 @reservation_locator=@reservation_locator OUTPUT,@reservation_digest=@reservation_digest OUTPUT,
 @executor=@executor OUTPUT,@control_id=@control_id OUTPUT,@control_sid=@control_sid OUTPUT;
IF @guard_epoch IS NULL OR @guard_epoch<=0 OR @source_revision IS NULL OR @source_revision<2
 OR @reservation_locator IS NULL OR DATALENGTH(@reservation_locator) NOT BETWEEN 1 AND 4096
 OR @reservation_digest IS NULL OR DATALENGTH(@reservation_digest)<>71
 OR @executor IS NULL OR DATALENGTH(@executor) NOT BETWEEN 1 AND 1048576
 OR NOT EXISTS (SELECT 1 FROM [{{LOCAL_SCHEMA}}].[physical_runtime_registrations_v1]
 WHERE registration_id=@registration_id AND (
 (metadata_model_principal_id=@caller_id AND metadata_model_sid=@caller_sid
 AND metadata_control_principal_id=@control_id AND metadata_control_sid=@control_sid
 AND DATALENGTH(metadata_control_sid)=DATALENGTH(@control_sid)) OR
 (build_model_principal_id=@caller_id AND build_model_sid=@caller_sid
 AND build_control_principal_id=@control_id AND build_control_sid=@control_sid
 AND DATALENGTH(build_control_sid)=DATALENGTH(@control_sid))))
 THROW 51423, 'DPONE_PHYSICAL_SOURCE_FACTS_INVALID', 1;
SELECT CONVERT(smallint,1) AS wire_version,@registration_id AS registration_id,
 @digest AS registration_digest,@generation AS generation_id,@expected_invocation AS executor_invocation_id,
 @guard_epoch AS guard_epoch,@source_revision AS source_revision,
 CONVERT(varbinary(4096),@reservation_locator) AS reservation_locator,
 CONVERT(varbinary(71),@reservation_digest) AS reservation_digest,@executor AS executor_payload,
 @caller_id AS observed_model_principal_id,@caller_sid AS observed_model_principal_sid,
 @control_id AS observed_control_principal_id,@control_sid AS observed_control_principal_sid;
END
-- DPONE MODULE BOUNDARY
CREATE PROCEDURE [{{CONTROL_SCHEMA}}].[physical_control_require_source_v1]
 @payload varbinary(max),@digest varbinary(max),@model_id int,@model_sid varbinary(max),
 @generation uniqueidentifier,@expected_invocation uniqueidentifier,
 @guard_epoch bigint OUTPUT,@source_revision bigint OUTPUT,@reservation_locator varbinary(max) OUTPUT,
 @reservation_digest varbinary(max) OUTPUT,@executor varbinary(max) OUTPUT,
 @control_id int OUTPUT,@control_sid varbinary(85) OUTPUT
AS
BEGIN
SET NOCOUNT ON;
IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 OR @generation IS NULL OR @expected_invocation IS NULL
 OR @payload IS NULL OR DATALENGTH(@payload) NOT BETWEEN 1 AND 1048576
 OR @digest IS NULL OR DATALENGTH(@digest)<>71
 OR @digest<>CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@payload),2)))
 OR @model_id IS NULL OR @model_id<=4 OR @model_sid IS NULL OR DATALENGTH(@model_sid) NOT BETWEEN 1 AND 85
 THROW 51420, 'DPONE_PHYSICAL_SOURCE_INTERNAL_INPUT_INVALID', 1;
{{CONTROL_CALLER}}
{{REGISTRATION_DECODE}}
{{CONTROL_PIN}}
DECLARE @role nvarchar(8);
SELECT @role=role_name FROM (VALUES (N'metadata'),(N'build')) roles(role_name)
 WHERE @model_id=TRY_CONVERT(int,JSON_VALUE(@registration,'$.principals.'+role_name+'.model.principal_id'))
 AND @model_sid=TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.'+role_name+'.model.sid_hex'),2)
 AND DATALENGTH(@model_sid)=DATALENGTH(TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.'+role_name+'.model.sid_hex'),2));
IF @role IS NULL
 OR TRY_CONVERT(int,JSON_VALUE(@registration,'$.principals.'+@role+'.control.principal_id')) IS NULL
 OR TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.'+@role+'.control.sid_hex'),2) IS NULL
 OR @caller_id<>TRY_CONVERT(int,JSON_VALUE(@registration,'$.principals.'+@role+'.control.principal_id'))
 OR @caller_sid<>TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.'+@role+'.control.sid_hex'),2)
 OR DATALENGTH(@caller_sid)<>DATALENGTH(TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.'+@role+'.control.sid_hex'),2))
 THROW 51420, 'DPONE_PHYSICAL_SOURCE_CALLER_MAPPING_INVALID', 1;
DECLARE @authority_locator varbinary(max)={{AUTHORITY_LOCATOR}},@authority_digest varbinary(max)={{AUTHORITY_DIGEST}};
IF NOT EXISTS (SELECT 1 FROM [{{CONTROL_SCHEMA}}].[native_original_authorities_v1]
 WHERE authority_hash=HASHBYTES('SHA2_256',@authority_locator)
 AND authority_locator=@authority_locator AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator)
 AND authority_digest=@authority_digest AND DATALENGTH(authority_digest)=DATALENGTH(@authority_digest)
 AND runtime_principal_id=TRY_CONVERT(int,JSON_VALUE(@registration,'$.principals.metadata.control.principal_id'))
 AND runtime_principal_sid=TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.metadata.control.sid_hex'),2)
 AND DATALENGTH(runtime_principal_sid)=DATALENGTH(TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.metadata.control.sid_hex'),2))
 AND schema_version=1)
 THROW 51424, 'DPONE_PHYSICAL_SOURCE_NATIVE_AUTHORITY_INVALID', 1;
DECLARE @request varbinary(max);
SELECT @request=request FROM [{{CONTROL_SCHEMA}}].[native_generations_v1] WITH (READCOMMITTEDLOCK)
 WHERE generation_id=@generation AND authority_locator=@authority_locator
 AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator) AND authority_digest=@authority_digest;
IF @request IS NULL OR DATALENGTH(@request) NOT BETWEEN 1 AND 1048576
 THROW 51425, 'DPONE_PHYSICAL_SOURCE_GENERATION_ABSENT', 1;
{{REQUEST_DECODE}}
{{REQUEST_SHAPE}}
{{PHYSICAL_OWNER}}
SELECT @guard_epoch=guard_epoch,@source_revision=revision,@reservation_locator=reservation_locator,
 @reservation_digest=reservation_digest,@executor=executor
 FROM [{{CONTROL_SCHEMA}}].[native_generations_v1] WITH (UPDLOCK,HOLDLOCK)
 WHERE generation_id=@generation AND guard_epoch=@epoch
 AND guard_hash=HASHBYTES('SHA2_256',CONVERT(varbinary(max),@guard))
 AND authority_locator=@authority_locator AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator)
 AND authority_digest=@authority_digest AND request=@request AND DATALENGTH(request)=DATALENGTH(@request)
 AND phase='BUILDING' AND outcome='ACTIVE' AND writer_admission IN ('OPEN','CLOSED')
 AND admission_sequence>0 AND executor IS NOT NULL
 AND completion_payload IS NULL AND completion_locator IS NULL AND completion_digest IS NULL
 AND frozen_payload IS NULL AND frozen_locator IS NULL AND frozen_digest IS NULL;
IF @executor IS NULL OR DATALENGTH(@executor) NOT BETWEEN 1 AND 1048576
 OR @guard_epoch IS NULL OR @source_revision IS NULL OR @source_revision<2
 OR @reservation_locator IS NULL OR DATALENGTH(@reservation_locator) NOT BETWEEN 1 AND 4096
 OR @reservation_digest IS NULL OR DATALENGTH(@reservation_digest)<>71
 OR @reservation_digest<>CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@request),2)))
 THROW 51425, 'DPONE_PHYSICAL_SOURCE_GENERATION_NOT_ACTIVE', 1;
{{EXECUTOR_DECODE}}
{{EXECUTOR_SHAPE}}
{{CANONICAL_EXECUTOR}}
IF TRY_CONVERT(uniqueidentifier,JSON_VALUE(@binding,'$.invocation_id')) IS NULL
 OR TRY_CONVERT(uniqueidentifier,JSON_VALUE(@binding,'$.invocation_id'))<>@expected_invocation
 OR TRY_CONVERT(uniqueidentifier,JSON_VALUE(@binding,'$.generation_id'))<>@generation
 OR TRY_CONVERT(uniqueidentifier,JSON_VALUE(@json,'$.subject.generation_id')) IS NULL
 OR TRY_CONVERT(uniqueidentifier,JSON_VALUE(@json,'$.subject.generation_id'))<>@generation
 OR TRY_CONVERT(bigint,JSON_VALUE(@binding,'$.guard_epoch'))<>@epoch
 OR {{BINDING_RESERVATION_LOCATOR}}<>@reservation_locator
 OR DATALENGTH({{BINDING_RESERVATION_LOCATOR}})<>DATALENGTH(@reservation_locator)
 OR {{BINDING_RESERVATION_DIGEST}}<>@reservation_digest
 OR CONVERT(varbinary(max),JSON_QUERY(@binding,'$.profile'))<>CONVERT(varbinary(max),JSON_QUERY(@json,'$.profile'))
 OR CONVERT(varbinary(max),JSON_QUERY(@binding,'$.command'))<>CONVERT(varbinary(max),JSON_QUERY(@json,'$.command'))
 OR CONVERT(varbinary(max),JSON_QUERY(@json,'$.subject.authority'))<>CONVERT(varbinary(max),JSON_QUERY(@registration,'$.platform_subject.authority'))
 OR {{REQUEST_PROFILE_LOCATOR}}<>{{PROFILE_LOCATOR}}
 OR DATALENGTH({{REQUEST_PROFILE_LOCATOR}})<>DATALENGTH({{PROFILE_LOCATOR}})
 OR {{REQUEST_PROFILE_DIGEST}}<>{{PROFILE_DIGEST}}
 THROW 51426, 'DPONE_PHYSICAL_SOURCE_EXECUTOR_MISMATCH', 1;
IF NOT EXISTS (SELECT 1 FROM [{{CONTROL_SCHEMA}}].[native_original_bindings_v1]
 WHERE locator_hash=HASHBYTES('SHA2_256',@reservation_locator)
 AND locator=@reservation_locator AND DATALENGTH(locator)=DATALENGTH(@reservation_locator)
 AND payload_digest=@reservation_digest AND authority_locator=@authority_locator
 AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator) AND authority_digest=@authority_digest
 AND subject={{REQUEST_SUBJECT}} AND DATALENGTH(subject)=DATALENGTH({{REQUEST_SUBJECT}})
 AND kind=CONVERT(varbinary(128),'generation_stored_file_v1'))
 THROW 51426, 'DPONE_PHYSICAL_SOURCE_RESERVATION_UNBOUND', 1;
IF NOT EXISTS (SELECT 1 FROM [{{CONTROL_SCHEMA}}].[native_generation_profiles_v1]
 WHERE guard_hash=HASHBYTES('SHA2_256',CONVERT(varbinary(max),@guard))
 AND profile_hash=HASHBYTES('SHA2_256',{{PROFILE_LOCATOR}})
 AND profile_locator={{PROFILE_LOCATOR}} AND DATALENGTH(profile_locator)=DATALENGTH({{PROFILE_LOCATOR}})
 AND profile_digest={{PROFILE_DIGEST}} AND authority_locator=@authority_locator
 AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator) AND authority_digest=@authority_digest
 AND max_generation_bytes=TRY_CONVERT(bigint,JSON_VALUE(@registration,'$.limits.max_generation_bytes')))
 OR NOT EXISTS (SELECT 1 FROM [{{CONTROL_SCHEMA}}].[native_generation_capacity_v1]
 WHERE guard_hash=HASHBYTES('SHA2_256',CONVERT(varbinary(max),@guard))
 AND resource_locator={{CAPACITY_LOCATOR}} AND DATALENGTH(resource_locator)=DATALENGTH({{CAPACITY_LOCATOR}})
 AND resource_digest={{CAPACITY_DIGEST}} AND charged_bytes>=TRY_CONVERT(bigint,JSON_VALUE(@json,'$.requested_bytes')))
 THROW 51427, 'DPONE_PHYSICAL_SOURCE_PROFILE_INVALID', 1;
SET @control_id=@caller_id;
SET @control_sid=@caller_sid;
END
