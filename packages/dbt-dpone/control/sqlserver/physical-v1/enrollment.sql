CREATE TABLE [dpone_physical].[physical_plan_enrollments_v1](
 generation_id uniqueidentifier NOT NULL,
 executor_invocation_id uniqueidentifier NOT NULL,
 registration_id uniqueidentifier NOT NULL,
 registration_digest varbinary(71) NOT NULL CONSTRAINT [CK_physical_plan_enrollments_v1_registration_digest] CHECK(DATALENGTH(registration_digest)=71),
 payload_digest varbinary(71) NOT NULL CONSTRAINT [CK_physical_plan_enrollments_v1_payload_digest] CHECK(DATALENGTH(payload_digest)=71),
 payload varbinary(max) NOT NULL CONSTRAINT [CK_physical_plan_enrollments_v1_payload] CHECK(DATALENGTH(payload) BETWEEN 1 AND 1048576),
 CONSTRAINT [PK_physical_plan_enrollments_v1] PRIMARY KEY(generation_id)
);
CREATE TABLE [dpone_physical].[physical_model_sessions_v1](
 session_registration_id uniqueidentifier NOT NULL CONSTRAINT [PK_physical_model_sessions_v1] PRIMARY KEY,
 generation_id uniqueidentifier NOT NULL,
 model_unique_id_hash binary(32) NOT NULL,
 model_unique_id varbinary(max) NOT NULL CONSTRAINT [CK_physical_model_sessions_v1_model_unique_id] CHECK(DATALENGTH(model_unique_id) BETWEEN 1 AND 4096),
 model_plan_digest varbinary(71) NOT NULL CONSTRAINT [CK_physical_model_sessions_v1_model_plan_digest] CHECK(DATALENGTH(model_plan_digest)=71),
 enrollment_digest varbinary(71) NOT NULL CONSTRAINT [CK_physical_model_sessions_v1_enrollment_digest] CHECK(DATALENGTH(enrollment_digest)=71),
 connection_id uniqueidentifier NOT NULL,
 connect_time datetime2(7) NOT NULL,
 session_id int NOT NULL CONSTRAINT [CK_physical_model_sessions_v1_session_id] CHECK(session_id>0),
 login_time datetime2(7) NOT NULL,
 build_model_principal_id int NOT NULL,
 build_model_principal_sid varbinary(85) NOT NULL CONSTRAINT [CK_physical_model_sessions_v1_build_model_principal_sid] CHECK(DATALENGTH(build_model_principal_sid) BETWEEN 1 AND 85),
 build_control_principal_id int NOT NULL,
 build_control_principal_sid varbinary(85) NOT NULL CONSTRAINT [CK_physical_model_sessions_v1_build_control_principal_sid] CHECK(DATALENGTH(build_control_principal_sid) BETWEEN 1 AND 85),
 attached_at_utc datetime2(7) NOT NULL,
 CONSTRAINT [UQ_physical_model_sessions_v1_generation_model] UNIQUE(generation_id,model_unique_id_hash)
);
-- DPONE ENROLLMENT TABLE BOUNDARY
CREATE PROCEDURE [dpone_physical].[physical_enroll_plan_set_v1]
 @registration_id uniqueidentifier,@generation uniqueidentifier,@expected_invocation uniqueidentifier,
 @payload varbinary(max),@payload_digest varbinary(max)
AS
BEGIN
SET NOCOUNT ON;
SET XACT_ABORT ON;
IF @@TRANCOUNT<>0 OR @registration_id IS NULL OR @generation IS NULL OR @expected_invocation IS NULL
 OR @payload IS NULL OR DATALENGTH(@payload) NOT BETWEEN 1 AND 1048576
 OR @payload_digest IS NULL OR DATALENGTH(@payload_digest)<>71
 THROW 51600, 'DPONE_ENROLLMENT_INPUT_INVALID', 1;
BEGIN TRY
 BEGIN TRANSACTION;
 {{SOURCE_CONTEXT}}
 {{ENROLLMENT_VALIDATE}}
 {{NAMESPACE_ALL}}
 DECLARE @retained varbinary(max);
 SELECT @retained=payload FROM [dpone_physical].[physical_plan_enrollments_v1] WITH(UPDLOCK,HOLDLOCK)
  WHERE generation_id=@generation;
 IF @retained IS NULL
  INSERT [dpone_physical].[physical_plan_enrollments_v1]
   (generation_id,executor_invocation_id,registration_id,registration_digest,payload_digest,payload)
   VALUES(@generation,@expected_invocation,@registration_id,@registration_digest,@payload_digest,@payload);
 ELSE IF NOT EXISTS(SELECT 1 FROM [dpone_physical].[physical_plan_enrollments_v1]
  WHERE generation_id=@generation AND executor_invocation_id=@expected_invocation
  AND registration_id=@registration_id AND registration_digest=@registration_digest
  AND DATALENGTH(registration_digest)=DATALENGTH(@registration_digest)
  AND payload_digest=@payload_digest AND DATALENGTH(payload_digest)=DATALENGTH(@payload_digest)
  AND payload=@payload AND DATALENGTH(payload)=DATALENGTH(@payload))
  THROW 51606, 'DPONE_ENROLLMENT_CONFLICT', 1;
 COMMIT TRANSACTION;
 SELECT CONVERT(smallint,1),@registration_id,@registration_digest,@generation,
  @expected_invocation,@guard_epoch,@payload_digest,@payload;
END TRY
BEGIN CATCH
 IF XACT_STATE()<>0 ROLLBACK TRANSACTION;
 THROW;
END CATCH;
END;
-- DPONE MODULE BOUNDARY
CREATE PROCEDURE [dpone_physical].[physical_read_plan_enrollment_v1]
 @registration_id uniqueidentifier,@generation uniqueidentifier,@expected_invocation uniqueidentifier,
 @expected_payload_digest varbinary(max)
AS
BEGIN
SET NOCOUNT ON;
SET XACT_ABORT ON;
IF @@TRANCOUNT<>0 OR @registration_id IS NULL OR @generation IS NULL OR @expected_invocation IS NULL
 OR @expected_payload_digest IS NULL OR DATALENGTH(@expected_payload_digest)<>71
 THROW 51600, 'DPONE_ENROLLMENT_INPUT_INVALID', 1;
DECLARE @payload varbinary(max),@payload_digest varbinary(max)=@expected_payload_digest;
BEGIN TRY
 BEGIN TRANSACTION;
 {{SOURCE_CONTEXT}}
 SELECT @payload=payload FROM [dpone_physical].[physical_plan_enrollments_v1] WITH(HOLDLOCK)
  WHERE generation_id=@generation AND executor_invocation_id=@expected_invocation
  AND registration_id=@registration_id AND registration_digest=@registration_digest
  AND DATALENGTH(registration_digest)=DATALENGTH(@registration_digest)
  AND payload_digest=@payload_digest AND DATALENGTH(payload_digest)=DATALENGTH(@payload_digest);
 IF @payload IS NULL THROW 51607, 'DPONE_ENROLLMENT_UNAVAILABLE', 1;
 {{ENROLLMENT_VALIDATE}}
 COMMIT TRANSACTION;
 SELECT CONVERT(smallint,1),@registration_id,@registration_digest,@generation,
  @expected_invocation,@guard_epoch,@payload_digest,@payload;
END TRY
BEGIN CATCH
 IF XACT_STATE()<>0 ROLLBACK TRANSACTION;
 THROW;
END CATCH;
END;
-- DPONE MODULE BOUNDARY
CREATE PROCEDURE [{{CONTROL_SCHEMA}}].[physical_control_require_enrollment_v1]
 @registration_payload varbinary(max),@registration_digest varbinary(max),@enrollment_payload varbinary(max),
 @model_principal_id int,@model_principal_sid varbinary(max)
AS
BEGIN
SET NOCOUNT ON;
SET XACT_ABORT ON;
IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 OR @registration_payload IS NULL
 OR DATALENGTH(@registration_payload) NOT BETWEEN 1 AND 1048576
 OR @registration_digest IS NULL OR DATALENGTH(@registration_digest)<>71
 OR @registration_digest<>CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@registration_payload),2)))
 OR @enrollment_payload IS NULL OR DATALENGTH(@enrollment_payload) NOT BETWEEN 1 AND 1048576
 OR @model_principal_id IS NULL OR @model_principal_id<=4
 OR @model_principal_sid IS NULL OR DATALENGTH(@model_principal_sid) NOT BETWEEN 1 AND 85
 THROW 51600, 'DPONE_ENROLLMENT_INPUT_INVALID', 1;
{{CONTROL_CALLER}}
{{CONTROL_SIGNATURE}}
{{REGISTRATION_DECODE}}
{{CONTROL_PIN}}
{{CONTROL_ENROLLMENT_DECODE}}
DECLARE @generation uniqueidentifier=TRY_CONVERT(uniqueidentifier,JSON_VALUE(@enrollment,'$.subject.generation_id'));
IF @generation IS NULL OR ISNULL(ISJSON(@enrollment,OBJECT),0)<>1
 THROW 51600, 'DPONE_ENROLLMENT_INPUT_INVALID', 1;
DECLARE @matching_roles int;
SELECT @matching_roles=COUNT(*) FROM (VALUES(N'metadata'),(N'build')) roles(role_name)
 WHERE @model_principal_id=TRY_CONVERT(int,JSON_VALUE(@registration,'$.principals.'+role_name+'.model.principal_id'))
 AND @model_principal_sid=TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.'+role_name+'.model.sid_hex'),2)
 AND DATALENGTH(@model_principal_sid)=DATALENGTH(TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.'+role_name+'.model.sid_hex'),2))
 AND @caller_id=TRY_CONVERT(int,JSON_VALUE(@registration,'$.principals.'+role_name+'.control.principal_id'))
 AND @caller_sid=TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.'+role_name+'.control.sid_hex'),2)
 AND DATALENGTH(@caller_sid)=DATALENGTH(TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.'+role_name+'.control.sid_hex'),2));
IF @matching_roles<>1 THROW 51602, 'DPONE_ENROLLMENT_CALLER_INVALID', 1;
DECLARE @authority_locator varbinary(max)={{AUTHORITY_LOCATOR}},@authority_digest varbinary(max)={{AUTHORITY_DIGEST}},
 @request varbinary(max),@executor varbinary(max),@reservation_locator varbinary(max),@reservation_digest varbinary(max);
SELECT @request=request,@executor=executor,@reservation_locator=reservation_locator,@reservation_digest=reservation_digest
 FROM [{{CONTROL_SCHEMA}}].[native_generations_v1] WITH(HOLDLOCK)
 WHERE generation_id=@generation AND authority_locator=@authority_locator
 AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator)
 AND authority_digest=@authority_digest AND DATALENGTH(authority_digest)=DATALENGTH(@authority_digest)
 AND phase='BUILDING' AND outcome='ACTIVE' AND writer_admission='OPEN';
IF @request IS NULL OR @executor IS NULL OR @reservation_locator IS NULL OR @reservation_digest IS NULL
 OR DATALENGTH(@request) NOT BETWEEN 1 AND 1048576 OR DATALENGTH(@executor) NOT BETWEEN 1 AND 1048576
 OR @reservation_digest<>CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@request),2)))
 THROW 51608, 'DPONE_ENROLLMENT_SOURCE_CLOSED', 1;
{{REQUEST_DECODE}}
{{CONTROL_LINKS}}
DECLARE @originals TABLE(locator varbinary(max),digest varbinary(max),kind varbinary(128));
INSERT @originals(locator,digest,kind) VALUES
 (@reservation_locator,@reservation_digest,CONVERT(varbinary(128),'generation_stored_file_v1')),
 ({{PLAN_LOCATOR}},{{PLAN_DIGEST}},CONVERT(varbinary(128),'mssql_physical_plan_set_v1')),
 ({{COMMAND_LOCATOR}},{{COMMAND_DIGEST}},CONVERT(varbinary(128),'trusted_dbt_command_plan_v1'));
IF EXISTS(SELECT 1 FROM @originals e WHERE e.locator IS NULL OR DATALENGTH(e.locator) NOT BETWEEN 1 AND 4096
 OR e.digest IS NULL OR DATALENGTH(e.digest)<>71 OR NOT EXISTS
 (SELECT 1 FROM [{{CONTROL_SCHEMA}}].[native_original_bindings_v1] b WITH(HOLDLOCK)
 WHERE b.locator_hash=HASHBYTES('SHA2_256',e.locator) AND b.locator=e.locator AND DATALENGTH(b.locator)=DATALENGTH(e.locator)
 AND b.payload_digest=e.digest AND DATALENGTH(b.payload_digest)=DATALENGTH(e.digest)
 AND b.kind=e.kind AND DATALENGTH(b.kind)=DATALENGTH(e.kind)
 AND b.subject={{ENROLLMENT_SUBJECT}} AND DATALENGTH(b.subject)=DATALENGTH({{ENROLLMENT_SUBJECT}})
 AND b.authority_locator=@authority_locator AND DATALENGTH(b.authority_locator)=DATALENGTH(@authority_locator)
 AND b.authority_digest=@authority_digest AND DATALENGTH(b.authority_digest)=DATALENGTH(@authority_digest)))
 THROW 51603, 'DPONE_ENROLLMENT_ORIGINAL_UNBOUND', 1;
END;
-- DPONE CONTEXT BOUNDARY
{{MODEL_CALLER}}
{{MODEL_SIGNATURE}}
DECLARE @source TABLE(wire_version smallint,registration_id uniqueidentifier,registration_digest varbinary(71),
 generation_id uniqueidentifier,executor_invocation_id uniqueidentifier,guard_epoch bigint,source_revision bigint,
 reservation_locator varbinary(4096),reservation_digest varbinary(71),executor_payload varbinary(max),
 model_id int,model_sid varbinary(85),control_id int,control_sid varbinary(85));
INSERT @source EXEC [dpone_physical].[physical_require_source_v1]
 @registration_id=@registration_id,@generation=@generation,@expected_invocation=@expected_invocation;
IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 OR (SELECT COUNT_BIG(*) FROM @source)<>1
 THROW 51608, 'DPONE_ENROLLMENT_SOURCE_CLOSED', 1;
DECLARE @registration_payload varbinary(max),@registration_digest varbinary(71),
 @metadata_limit int,@row_limit int,@column_limit int,@platform_subject varbinary(max),@profile_subject varbinary(max),
 @profile_locator varbinary(max),@profile_digest varbinary(71);
SELECT @registration_payload=payload,@registration_digest=registration_digest,
 @metadata_limit=max_metadata_bytes,@row_limit=max_catalog_rows,@column_limit=max_columns,
 @platform_subject=platform_subject,@profile_subject=trusted_profile_subject,
 @profile_locator=trusted_profile_locator,@profile_digest=trusted_profile_digest
 FROM [dpone_physical].[physical_runtime_registrations_v1] WITH(HOLDLOCK) WHERE registration_id=@registration_id;
IF @registration_payload IS NULL OR DATALENGTH(@registration_payload) NOT BETWEEN 1 AND 1048576
 OR @metadata_limit IS NULL OR @metadata_limit<=0 OR @row_limit IS NULL OR @row_limit<=0
 OR @column_limit IS NULL OR @column_limit<=0
 THROW 51600, 'DPONE_ENROLLMENT_INPUT_INVALID', 1;
{{REGISTRATION_DECODE}}
IF NOT EXISTS(SELECT 1 FROM [dpone_physical].[physical_runtime_registrations_v1] r
 WHERE r.registration_id=@registration_id AND {{PROJECTION_CHECK}}
 AND r.{{REQUIRED_ROLE}}_model_principal_id=@caller_id AND r.{{REQUIRED_ROLE}}_model_sid=@caller_sid
 AND DATALENGTH(r.{{REQUIRED_ROLE}}_model_sid)=DATALENGTH(@caller_sid)
 AND r.observer_mode=CONVERT(varbinary(32),'DEDICATED') AND DATALENGTH(r.observer_mode)=9)
 THROW 51602, 'DPONE_ENROLLMENT_CALLER_INVALID', 1;
{{MODEL_PIN}}
IF NOT EXISTS(SELECT 1 FROM @source s JOIN [dpone_physical].[physical_runtime_registrations_v1] r
 ON r.registration_id=s.registration_id WHERE s.wire_version=1 AND s.registration_id=@registration_id
 AND s.generation_id=@generation AND s.executor_invocation_id=@expected_invocation
 AND s.registration_digest=@registration_digest AND DATALENGTH(s.registration_digest)=71
 AND s.guard_epoch>0 AND s.source_revision>=2 AND s.executor_payload IS NOT NULL
 AND DATALENGTH(s.executor_payload) BETWEEN 1 AND @metadata_limit
 AND s.model_id=@caller_id AND s.model_sid=@caller_sid AND DATALENGTH(s.model_sid)=DATALENGTH(@caller_sid)
 AND s.control_id=r.{{REQUIRED_ROLE}}_control_principal_id AND s.control_sid=r.{{REQUIRED_ROLE}}_control_sid
 AND DATALENGTH(s.control_sid)=DATALENGTH(r.{{REQUIRED_ROLE}}_control_sid))
 THROW 51608, 'DPONE_ENROLLMENT_SOURCE_CLOSED', 1;
DECLARE @guard_epoch bigint,@source_revision bigint,@source_executor varbinary(max),
 @control_id int,@control_sid varbinary(85);
SELECT @guard_epoch=guard_epoch,@source_revision=source_revision,@source_executor=executor_payload,
 @control_id=control_id,@control_sid=control_sid FROM @source;
DECLARE @binding_payload varbinary(max),@binding_digest varbinary(71);
SELECT @binding_payload=payload,@binding_digest=binding_digest FROM [dpone_physical].[physical_catalog_bindings_v1]
 WITH(HOLDLOCK) WHERE registration_id=@registration_id AND registration_digest=@registration_digest;
IF @binding_payload IS NULL OR DATALENGTH(@binding_payload) NOT BETWEEN 1 AND @metadata_limit
 OR @binding_digest IS NULL OR DATALENGTH(@binding_digest)<>71
 OR @binding_digest<>CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@binding_payload),2)))
 THROW 51605, 'DPONE_ENROLLMENT_BINDING_MISMATCH', 1;
{{BINDING_DECODE}}
{{BINDING_SHAPES}}
{{BINDING_CANONICAL}}
{{BINDING_LINKS}}
-- DPONE VALIDATION BOUNDARY
IF @payload IS NULL OR DATALENGTH(@payload) NOT BETWEEN 1 AND @metadata_limit
 OR @payload_digest IS NULL OR DATALENGTH(@payload_digest)<>71
 OR @payload_digest<>CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@payload),2)))
 THROW 51601, 'DPONE_ENROLLMENT_BYTES_MISMATCH', 1;
{{ENROLLMENT_DECODE}}
{{DOCUMENT_CHECKS}}
{{ENROLLMENT_LINKS}}
EXEC {{CONTROL_DATABASE}}.[{{CONTROL_SCHEMA}}].[physical_control_require_enrollment_v1]
 @registration_payload=@registration_payload,@registration_digest=@registration_digest,
 @enrollment_payload=@payload,@model_principal_id=@caller_id,@model_principal_sid=@caller_sid;
IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 THROW 51608, 'DPONE_ENROLLMENT_SOURCE_CLOSED', 1;
-- DPONE MODEL LINK BOUNDARY
DECLARE @previous_model_id varbinary(max),@current_model_id nvarchar(max),@expected_name nvarchar(max),
 @name_payload nvarchar(max),@unsigned_model nvarchar(max),@spec_document nvarchar(max),@unsigned_spec nvarchar(max),
 @enrolled_filegroup nvarchar(max),@enrolled_filegroup_id int;
SET @model_position=0;
WHILE EXISTS(SELECT 1 FROM @model_documents WHERE position=@model_position)
BEGIN
 SELECT @model_document=document FROM @model_documents WHERE position=@model_position;
 SET @current_model_id={{MODEL_ID}};
 SET @spec_document=JSON_QUERY(@model_document,'$.spec');
 IF EXISTS(SELECT 1 FROM (VALUES (JSON_VALUE(@model_document,'$.generation_id')),(JSON_VALUE(@model_document,'$.candidate_name')),(JSON_VALUE(@model_document,'$.helper_name')),(JSON_VALUE(@model_document,'$.model_plan_sha256')),(JSON_VALUE(@model_document,'$.schema')),(JSON_VALUE(@model_document,'$.predecessor.kind')),(JSON_VALUE(@model_document,'$.spec.schema')),(JSON_VALUE(@model_document,'$.spec.layout')),(JSON_VALUE(@model_document,'$.spec.source_graph_sha256')),(JSON_VALUE(@model_document,'$.spec.model_spec_sha256')),(JSON_VALUE(@model_document,'$.spec.filegroup.name')),(JSON_VALUE(@model_document,'$.spec.relation.database')),(JSON_VALUE(@model_document,'$.spec.relation.schema')),(JSON_VALUE(@model_document,'$.spec.relation.table'))) required(value) WHERE value IS NULL OR DATALENGTH(value)=0)
  THROW 51601, 'DPONE_ENROLLMENT_BYTES_MISMATCH', 1;
 IF @current_model_id IS NULL OR DATALENGTH({{MODEL_ID_BYTES}}) NOT BETWEEN 1 AND 4096
  OR (@previous_model_id IS NOT NULL AND @previous_model_id>={{MODEL_ID_BYTES}})
  OR (SELECT COUNT_BIG(*) FROM OPENJSON(@spec_document,'$.columns'))>@column_limit
  OR CONVERT(varbinary(max),JSON_VALUE(@model_document,'$.generation_id'))<>CONVERT(varbinary(max),LOWER(CONVERT(nvarchar(36),@generation)))
  OR DATALENGTH(JSON_VALUE(@model_document,'$.generation_id'))<>72
  OR CONVERT(varbinary(max),JSON_QUERY(@spec_document,'$.resource_bounds'))<>CONVERT(varbinary(max),JSON_QUERY(@enrollment,'$.executor.profile'))
  OR DATALENGTH(JSON_QUERY(@spec_document,'$.resource_bounds'))<>DATALENGTH(JSON_QUERY(@enrollment,'$.executor.profile'))
  OR CONVERT(varbinary(max),JSON_VALUE(@spec_document,'$.relation.schema'))<>CONVERT(varbinary(max),{{MODEL_SCHEMA}})
  OR DATALENGTH(JSON_VALUE(@spec_document,'$.relation.schema'))<>DATALENGTH({{MODEL_SCHEMA}})
  OR CONVERT(varbinary(max),JSON_VALUE(@spec_document,'$.relation.database'))<>CONVERT(varbinary(max),{{MODEL_DATABASE_NAME}})
  OR DATALENGTH(JSON_VALUE(@spec_document,'$.relation.database'))<>DATALENGTH({{MODEL_DATABASE_NAME}})
  THROW 51601, 'DPONE_ENROLLMENT_BYTES_MISMATCH', 1;
 SET @previous_model_id={{MODEL_ID_BYTES}};
 SET @unsigned_spec=REPLACE(@spec_document,N',"model_spec_sha256":"'+JSON_VALUE(@spec_document,'$.model_spec_sha256')+N'"',N'');
 SET @unsigned_model=REPLACE(@model_document,N',"model_plan_sha256":"'+JSON_VALUE(@model_document,'$.model_plan_sha256')+N'"',N'');
 IF DATALENGTH(JSON_VALUE(@spec_document,'$.model_spec_sha256'))<>142
  OR CONVERT(varbinary(max),JSON_VALUE(@spec_document,'$.model_spec_sha256'))<>CONVERT(varbinary(max),N'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',{{UNSIGNED_SPEC_BYTES}}),2)))
  OR DATALENGTH(JSON_VALUE(@model_document,'$.model_plan_sha256'))<>142
  OR CONVERT(varbinary(max),JSON_VALUE(@model_document,'$.model_plan_sha256'))<>CONVERT(varbinary(max),N'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',{{UNSIGNED_MODEL_BYTES}}),2)))
  THROW 51601, 'DPONE_ENROLLMENT_BYTES_MISMATCH', 1;
 SET @name_payload=CONVERT(nvarchar(max),N'{"generation_id":"')+LOWER(CONVERT(nvarchar(36),@generation))
  +N'","model_unique_id":"'+REPLACE(STRING_ESCAPE(@current_model_id,'json'),NCHAR(92)+N'/',N'/')+N'","role":"';
 SET @expected_name=N'dpone_c_'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',{{CANDIDATE_NAME_BYTES}}),2));
 IF CONVERT(varbinary(max),JSON_VALUE(@model_document,'$.candidate_name'))<>CONVERT(varbinary(max),@expected_name)
  OR DATALENGTH(JSON_VALUE(@model_document,'$.candidate_name'))<>DATALENGTH(@expected_name)
  THROW 51601, 'DPONE_ENROLLMENT_BYTES_MISMATCH', 1;
 SET @expected_name=N'dpone_h_'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',{{HELPER_NAME_BYTES}}),2));
 IF CONVERT(varbinary(max),JSON_VALUE(@model_document,'$.helper_name'))<>CONVERT(varbinary(max),@expected_name)
  OR DATALENGTH(JSON_VALUE(@model_document,'$.helper_name'))<>DATALENGTH(@expected_name)
  THROW 51601, 'DPONE_ENROLLMENT_BYTES_MISMATCH', 1;
 IF JSON_VALUE(@spec_document,'$.layout')=N'columnstore'
 BEGIN
  SET @expected_name=N'dpone_i_'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',{{CCI_NAME_BYTES}}),2));
  IF CONVERT(varbinary(max),JSON_VALUE(@model_document,'$.columnstore_index_name'))<>CONVERT(varbinary(max),@expected_name)
   OR DATALENGTH(JSON_VALUE(@model_document,'$.columnstore_index_name'))<>DATALENGTH(@expected_name)
   THROW 51601, 'DPONE_ENROLLMENT_BYTES_MISMATCH', 1;
 END;
 IF @model_position=0
 BEGIN
  SET @enrolled_filegroup=JSON_VALUE(@spec_document,'$.filegroup.name');
  SET @enrolled_filegroup_id=TRY_CONVERT(int,JSON_VALUE(@spec_document,'$.filegroup.data_space_id'));
 END;
 IF @enrolled_filegroup_id IS NULL OR @enrolled_filegroup_id<=0
  OR TRY_CONVERT(int,JSON_VALUE(@spec_document,'$.filegroup.data_space_id')) IS NULL
  OR TRY_CONVERT(int,JSON_VALUE(@spec_document,'$.filegroup.data_space_id'))<>@enrolled_filegroup_id
  OR CONVERT(varbinary(max),JSON_VALUE(@spec_document,'$.filegroup.name'))<>CONVERT(varbinary(max),@enrolled_filegroup)
  OR DATALENGTH(JSON_VALUE(@spec_document,'$.filegroup.name'))<>DATALENGTH(@enrolled_filegroup)
  THROW 51601, 'DPONE_ENROLLMENT_BYTES_MISMATCH', 1;
 SET @model_position=@model_position+1;
END;
-- DPONE NAMESPACE BOUNDARY
DECLARE @json nvarchar(max);
SELECT @json=CONVERT(nvarchar(max),N'{"filegroup_name":"')
 +REPLACE(STRING_ESCAPE(@enrolled_filegroup,'json'),NCHAR(92)+N'/',N'/')+N'","objects":['
 +STRING_AGG(CONVERT(nvarchar(max),N'{"model_unique_id":"')
  +REPLACE(STRING_ESCAPE({{NAMESPACE_MODEL_ID}},'json'),NCHAR(92)+N'/',N'/')
  +N'","name":"'+REPLACE(STRING_ESCAPE(names.name,'json'),NCHAR(92)+N'/',N'/')
  +N'","role":"'+names.role+N'"}',N',') WITHIN GROUP(ORDER BY models.position,names.position)+N']}'
 FROM @model_documents models CROSS APPLY(VALUES
 (0,N'TARGET',JSON_VALUE(models.document,'$.spec.relation.table')),
 (1,N'CANDIDATE',JSON_VALUE(models.document,'$.candidate_name')),
 (2,N'HELPER',JSON_VALUE(models.document,'$.helper_name'))) names(position,role,name)
 {{NAMESPACE_FILTER}};
IF @json IS NULL OR DATALENGTH({{NAMESPACE_BYTES}})>@metadata_limit
 THROW 51600, 'DPONE_ENROLLMENT_INPUT_INVALID', 1;
{{NAMESPACE_INPUT}}
{{NAMESPACE_ASSERTION}}
IF @filegroup_id<>@enrolled_filegroup_id
 OR CONVERT(varbinary(max),@filegroup_name)<>CONVERT(varbinary(max),@enrolled_filegroup)
 OR DATALENGTH(@filegroup_name)<>DATALENGTH(@enrolled_filegroup)
 THROW 51601, 'DPONE_ENROLLMENT_BYTES_MISMATCH', 1;
-- DPONE NATIVE JSON BOUNDARY
IF @enrollment IS NULL OR ISNULL(ISJSON(@enrollment,OBJECT),0)<>1
 THROW 51600, 'DPONE_ENROLLMENT_INPUT_INVALID', 1;
DECLARE @documents TABLE(id int IDENTITY(1,1) PRIMARY KEY,document nvarchar(max),depth int,kind int);
INSERT @documents(document,depth,kind) VALUES(@enrollment,1,5);
DECLARE @document_id int=1,@document nvarchar(max),@depth int,@kind int,@tokens bigint=0,
 @members bigint,@scalars bigint,@canonical_document nvarchar(max);
DECLARE @members_table TABLE(ordinal int,[key] nvarchar(4000),value nvarchar(max),kind int);
WHILE EXISTS(SELECT 1 FROM @documents WHERE id=@document_id)
BEGIN
 SELECT @document=document,@depth=depth,@kind=kind FROM @documents WHERE id=@document_id;
 IF @depth>{{NATIVE_MAX_DEPTH}}
  THROW 51600, 'DPONE_ENROLLMENT_INPUT_INVALID', 1;
 DELETE FROM @members_table;
 INSERT @members_table(ordinal,[key],value,kind)
  SELECT TRY_CONVERT(int,[key]),[key],value,type FROM OPENJSON(@document);
 SELECT @members=COUNT_BIG(*),@scalars=COALESCE(SUM(CONVERT(bigint,CASE WHEN kind<4 THEN 1 ELSE 0 END)),0)
  FROM @members_table;
 SET @tokens=@tokens+2+CASE WHEN @members=0 THEN 0 ELSE @members-1 END+@scalars
  +CASE WHEN @kind=5 THEN 2*@members ELSE 0 END;
 IF @tokens>{{NATIVE_MAX_TOKENS}} OR (@kind=5 AND EXISTS
  (SELECT 1 FROM @members_table GROUP BY CONVERT(varbinary(8000),[key]),DATALENGTH([key]) HAVING COUNT_BIG(*)<>1))
  OR EXISTS(SELECT 1 FROM @members_table WHERE DATALENGTH(CONVERT(varbinary(max), CONVERT(varchar(max), [key] COLLATE Latin1_General_100_BIN2_UTF8)))>{{NATIVE_MAX_STRING_BYTES}}
   OR (kind=1 AND (value IS NULL OR DATALENGTH(CONVERT(varbinary(max), CONVERT(varchar(max), value COLLATE Latin1_General_100_BIN2_UTF8)))>{{NATIVE_MAX_STRING_BYTES}}
    OR (value IS NULL OR CONVERT(nvarchar(max),CONVERT(varchar(max),value COLLATE Latin1_General_100_BIN2_UTF8) COLLATE Latin1_General_100_BIN2_UTF8) IS NULL OR CONVERT(varbinary(max),value)<>CONVERT(varbinary(max),CONVERT(nvarchar(max),CONVERT(varchar(max),value COLLATE Latin1_General_100_BIN2_UTF8) COLLATE Latin1_General_100_BIN2_UTF8)) OR DATALENGTH(value)<>DATALENGTH(CONVERT(nvarchar(max),CONVERT(varchar(max),value COLLATE Latin1_General_100_BIN2_UTF8) COLLATE Latin1_General_100_BIN2_UTF8)))))
   OR (kind=2 AND (value IS NULL OR value COLLATE Latin1_General_100_BIN2 LIKE N'%[^0-9-]%'
    OR DATALENGTH(REPLACE(value,N'-',N''))>256)))
  THROW 51600, 'DPONE_ENROLLMENT_INPUT_INVALID', 1;
 SELECT @canonical_document=CASE WHEN @kind=5 THEN N'{' ELSE N'[' END
  +COALESCE(STRING_AGG(CONVERT(nvarchar(max),CASE WHEN @kind=5 THEN N'"'
   +REPLACE(STRING_ESCAPE([key],'json'),NCHAR(92)+N'/',N'/')+N'":' ELSE N'' END)
   +CASE WHEN kind=0 THEN N'null' WHEN kind=1 THEN N'"'
    +REPLACE(STRING_ESCAPE(value,'json'),NCHAR(92)+N'/',N'/')+N'"' WHEN kind=2 AND value=N'-0' THEN N'0' ELSE value END,N',')
   WITHIN GROUP(ORDER BY CASE WHEN @kind=4 THEN ordinal END,
    CASE WHEN @kind=5 THEN CONVERT(varchar(max),[key] COLLATE Latin1_General_100_BIN2_UTF8) END COLLATE Latin1_General_100_BIN2_UTF8),N'')
  +CASE WHEN @kind=5 THEN N'}' ELSE N']' END FROM @members_table;
 {{NATIVE_CANONICAL_CHECK}}
 INSERT @documents(document,depth,kind) SELECT value,@depth+1,kind FROM @members_table WHERE kind IN(4,5);
 IF (SELECT COUNT_BIG(*) FROM @documents)>{{NATIVE_MAX_TOKENS}}
  THROW 51600, 'DPONE_ENROLLMENT_INPUT_INVALID', 1;
 DELETE FROM @documents WHERE id=@document_id;
 SET @document_id=@document_id+1;
END;
-- DPONE COMPONENT BOUNDARY
DECLARE @model_documents TABLE(position int PRIMARY KEY,document nvarchar(max));
IF EXISTS(SELECT 1 FROM OPENJSON(@enrollment,'$.plan_set.payload.models') WHERE type<>5)
 OR NOT EXISTS(SELECT 1 FROM OPENJSON(@enrollment,'$.plan_set.payload.models'))
 THROW 51600, 'DPONE_ENROLLMENT_INPUT_INVALID', 1;
INSERT @model_documents(position,document)
 SELECT TRY_CONVERT(int,[key]),value FROM OPENJSON(@enrollment,'$.plan_set.payload.models');
DECLARE @model_position int=0,@model_document nvarchar(max),@column_document nvarchar(max),@column_position int;
WHILE EXISTS(SELECT 1 FROM @model_documents WHERE position=@model_position)
BEGIN
 SELECT @model_document=document FROM @model_documents WHERE position=@model_position;
 {{MODEL_COMPONENT_SHAPES}}
 IF ISNULL(CONVERT(varbinary(max),JSON_VALUE(@model_document,'$.predecessor.kind')),0x)<>CONVERT(varbinary(max),N'ABSENT') OR ISNULL(DATALENGTH(JSON_VALUE(@model_document,'$.predecessor.kind')),0)<>12
  OR ISNULL(CONVERT(varbinary(max),JSON_VALUE(@model_document,'$.schema')),0x)<>CONVERT(varbinary(max),N'dpone.mssql-physical-model-plan.v1') OR ISNULL(DATALENGTH(JSON_VALUE(@model_document,'$.schema')),0)<>68
  OR ISNULL(CONVERT(varbinary(max),JSON_VALUE(@model_document,'$.spec.schema')),0x)<>CONVERT(varbinary(max),N'dpone.mssql-physical-model-spec.v1') OR ISNULL(DATALENGTH(JSON_VALUE(@model_document,'$.spec.schema')),0)<>68
  OR ISNULL(CONVERT(varbinary(max),JSON_VALUE(@model_document,'$.spec.physical_policy')),0x)<>CONVERT(varbinary(max),N'sqlserver-table-physical-v1') OR ISNULL(DATALENGTH(JSON_VALUE(@model_document,'$.spec.physical_policy')),0)<>54
  OR NOT EXISTS(SELECT 1 FROM (VALUES(N'rowstore_none'),(N'rowstore_row'),(N'rowstore_page'),(N'columnstore')) allowed(value) WHERE CONVERT(varbinary(max),JSON_VALUE(@model_document,'$.spec.layout'))=CONVERT(varbinary(max),allowed.value) AND DATALENGTH(JSON_VALUE(@model_document,'$.spec.layout'))=DATALENGTH(allowed.value))
  OR NOT EXISTS(SELECT 1 FROM OPENJSON(@model_document,'$.spec.columns'))
  OR EXISTS(SELECT 1 FROM OPENJSON(@model_document,'$.spec.columns') WHERE type<>5)
  THROW 51600, 'DPONE_ENROLLMENT_INPUT_INVALID', 1;
 SET @column_position=0;
 WHILE EXISTS(SELECT 1 FROM OPENJSON(@model_document,'$.spec.columns') WHERE TRY_CONVERT(int,[key])=@column_position)
 BEGIN
  SELECT @column_document=value FROM OPENJSON(@model_document,'$.spec.columns') WHERE TRY_CONVERT(int,[key])=@column_position;
  IF JSON_VALUE(@column_document,'$.collation') IS NULL BEGIN {{COLUMN_SHAPE}} END ELSE BEGIN {{COLLATED_COLUMN_SHAPE}} END;
  SET @column_position=@column_position+1;
 END;
 SET @model_position=@model_position+1;
END;
IF JSON_VALUE(@enrollment,'$.command.payload.total_termination_budget_seconds') IS NULL
 OR LEFT(JSON_VALUE(@enrollment,'$.command.payload.total_termination_budget_seconds'),1) IN(N'0',N'-')
 OR (SELECT COUNT_BIG(*) FROM OPENJSON(@enrollment,'$.command.payload.commands'))<>3
 OR EXISTS(SELECT 1 FROM OPENJSON(@enrollment,'$.command.payload.commands') WHERE type<>5)
 THROW 51604, 'DPONE_ENROLLMENT_COMMAND_MISMATCH', 1;
DECLARE @argv_index int,@argument nvarchar(max),@argument_char int;
DECLARE @command_position int=0,@command_document nvarchar(max),@managed_vars nvarchar(max),@current_vars nvarchar(max),@vars_position int;
WHILE @command_position<3
BEGIN
 SELECT @command_document=value FROM OPENJSON(@enrollment,'$.command.payload.commands') WHERE TRY_CONVERT(int,[key])=@command_position;
 {{COMMAND_SHAPE}}
 SET @argv_index=0;
 WHILE EXISTS(SELECT 1 FROM OPENJSON(@command_document,'$.argv_template') WHERE TRY_CONVERT(int,[key])=@argv_index)
 BEGIN
  SELECT @argument=value FROM OPENJSON(@command_document,'$.argv_template') WHERE TRY_CONVERT(int,[key])=@argv_index;
  SET @argument_char=1;
  WHILE @argument_char<=DATALENGTH(@argument)/2
  BEGIN
   IF UNICODE(SUBSTRING(@argument,@argument_char,1))=0 THROW 51604, 'DPONE_ENROLLMENT_COMMAND_MISMATCH', 1;
   SET @argument_char=@argument_char+1;
  END;
  IF @argv_index=0 AND (CONVERT(varbinary(max),@argument)<>CONVERT(varbinary(max),N'dbt') OR DATALENGTH(@argument)<>6)
   THROW 51604, 'DPONE_ENROLLMENT_COMMAND_MISMATCH', 1;
  SET @argv_index=@argv_index+1;
 END;
 SET @argv_index=1;
 WHILE EXISTS(SELECT 1 FROM OPENJSON(@command_document,'$.argv_template') j
  JOIN (VALUES(N'--quiet'),(N'--no-use-colors'),(N'--warn-error')) flags(value)
   ON CONVERT(varbinary(max),j.value)=CONVERT(varbinary(max),flags.value) AND DATALENGTH(j.value)=DATALENGTH(flags.value)
  WHERE TRY_CONVERT(int,j.[key])=@argv_index)
  SET @argv_index=@argv_index+1;
 SET @argument=NULL;
 SELECT @argument=value FROM OPENJSON(@command_document,'$.argv_template') WHERE TRY_CONVERT(int,[key])=@argv_index;
 IF @argument IS NULL OR CONVERT(varbinary(max),@argument)<>CONVERT(varbinary(max),JSON_VALUE(@command_document,'$.verb'))
  OR DATALENGTH(@argument)<>DATALENGTH(JSON_VALUE(@command_document,'$.verb'))
  THROW 51604, 'DPONE_ENROLLMENT_COMMAND_MISMATCH', 1;
 IF TRY_CONVERT(int,JSON_VALUE(@command_document,'$.position')) IS NULL
  OR TRY_CONVERT(int,JSON_VALUE(@command_document,'$.position'))<>@command_position
  OR JSON_VALUE(@command_document,'$.verb') IS NULL OR CONVERT(varbinary(max),JSON_VALUE(@command_document,'$.verb'))<>CONVERT(varbinary(max),CASE @command_position WHEN 0 THEN N'parse' WHEN 1 THEN N'ls' ELSE N'build' END) OR DATALENGTH(JSON_VALUE(@command_document,'$.verb'))<>DATALENGTH(CASE @command_position WHEN 0 THEN N'parse' WHEN 1 THEN N'ls' ELSE N'build' END)
  OR JSON_VALUE(@command_document,'$.command_timeout_seconds') IS NULL
  OR LEFT(JSON_VALUE(@command_document,'$.command_timeout_seconds'),1) IN(N'0',N'-')
  OR JSON_VALUE(@command_document,'$.termination_allowance_seconds') IS NULL
  OR LEFT(JSON_VALUE(@command_document,'$.termination_allowance_seconds'),1) IN(N'0',N'-')
  OR EXISTS(SELECT 1 FROM OPENJSON(@command_document,'$.argv_template') WHERE type<>1)
  THROW 51604, 'DPONE_ENROLLMENT_COMMAND_MISMATCH', 1;
 IF (SELECT COUNT_BIG(*) FROM OPENJSON(@command_document,'$.argv_template')
  WHERE CONVERT(varbinary(max),value)=CONVERT(varbinary(max),N'--vars') AND DATALENGTH(value)=12)<>1
  OR EXISTS(SELECT 1 FROM OPENJSON(@command_document,'$.argv_template') WHERE value COLLATE Latin1_General_100_BIN2 LIKE N'--vars=%')
  THROW 51604, 'DPONE_ENROLLMENT_COMMAND_MISMATCH', 1;
 SELECT @vars_position=TRY_CONVERT(int,[key])+1 FROM OPENJSON(@command_document,'$.argv_template')
  WHERE CONVERT(varbinary(max),value)=CONVERT(varbinary(max),N'--vars') AND DATALENGTH(value)=12;
 SET @current_vars=NULL;
 SELECT @current_vars=value FROM OPENJSON(@command_document,'$.argv_template') WHERE TRY_CONVERT(int,[key])=@vars_position;
 IF @current_vars IS NULL OR (@command_position>0 AND
  (CONVERT(varbinary(max),@managed_vars)<>CONVERT(varbinary(max),@current_vars) OR DATALENGTH(@managed_vars)<>DATALENGTH(@current_vars)))
  THROW 51604, 'DPONE_ENROLLMENT_COMMAND_MISMATCH', 1;
 SET @managed_vars=@current_vars;
 SET @command_position=@command_position+1;
END;
{{MANAGED_VARS_NATIVE}}
{{MANAGED_INVOCATION_SHAPE}}
{{MANAGED_REFERENCE_SHAPE}}
