CREATE PROCEDURE [dpone_physical].[physical_observe_current_connection_v1]
 @connection_id uniqueidentifier OUTPUT,@connect_time datetime2(7) OUTPUT,
 @session_id int OUTPUT,@login_time datetime2(7) OUTPUT
AS
BEGIN
SET NOCOUNT ON;
SET XACT_ABORT ON;
SET @connection_id=NULL;
SET @connect_time=NULL;
SET @session_id=NULL;
SET @login_time=NULL;
IF @@TRANCOUNT<>1 OR XACT_STATE()<>1
 OR ISNULL(CONVERT(int,SERVERPROPERTY('ProductMajorVersion')),0)<>16
 OR SUSER_SID(ORIGINAL_LOGIN()) IS NULL
 THROW 51620, 'DPONE_SESSION_CONNECTION_UNOBSERVABLE', 1;
IF (SELECT COUNT_BIG(*) FROM sys.crypt_properties WHERE class=1 AND major_id=@@PROCID)<>1
 OR NOT EXISTS (SELECT 1 FROM sys.crypt_properties WHERE class=1 AND major_id=@@PROCID
 AND crypt_type='SPVC' AND thumbprint={{CONNECTION_CERTIFICATE}})
 THROW 51620, 'DPONE_SESSION_CONNECTION_UNOBSERVABLE', 1;
DECLARE @observed TABLE(connection_id uniqueidentifier,connect_time datetime2(7),
 session_id int,login_time datetime2(7),parent_connection_id uniqueidentifier,authenticated bit);
BEGIN TRY
 INSERT @observed(connection_id,connect_time,session_id,login_time,parent_connection_id,authenticated)
  SELECT c.connection_id,CONVERT(datetime2(7),c.connect_time),c.session_id,
  CONVERT(datetime2(7),s.login_time),c.parent_connection_id,
  CONVERT(bit,CASE WHEN s.is_user_process=1
  AND s.original_security_id=SUSER_SID(ORIGINAL_LOGIN())
  AND DATALENGTH(s.original_security_id)=DATALENGTH(SUSER_SID(ORIGINAL_LOGIN()))
  AND c.parent_connection_id IS NULL THEN 1 ELSE 0 END)
  FROM sys.dm_exec_connections c
  LEFT JOIN sys.dm_exec_sessions s ON s.session_id=c.session_id
  WHERE c.session_id=@@SPID;
END TRY
BEGIN CATCH
 THROW 51620, 'DPONE_SESSION_CONNECTION_UNOBSERVABLE', 1;
END CATCH;
IF (SELECT COUNT_BIG(*) FROM @observed)<>1 OR EXISTS
 (SELECT 1 FROM @observed WHERE connection_id IS NULL OR connect_time IS NULL
 OR session_id IS NULL OR session_id<=0 OR session_id<>@@SPID OR login_time IS NULL
 OR parent_connection_id IS NOT NULL OR authenticated IS NULL OR authenticated<>1)
 THROW 51620, 'DPONE_SESSION_CONNECTION_UNOBSERVABLE', 1;
SELECT @connection_id=connection_id,@connect_time=connect_time,
 @session_id=session_id,@login_time=login_time FROM @observed;
END;
-- DPONE MODULE BOUNDARY
CREATE PROCEDURE [dpone_physical].[physical_attach_session_v1]
 @registration_id uniqueidentifier,@generation uniqueidentifier,@expected_invocation uniqueidentifier,
 @plan_set_locator nvarchar(max),@plan_set_sha256 varchar(max),@model_unique_id nvarchar(max)
AS
BEGIN
SET NOCOUNT ON;
SET XACT_ABORT ON;
IF @@TRANCOUNT<>0 OR @registration_id IS NULL OR @generation IS NULL OR @expected_invocation IS NULL
 OR @plan_set_locator IS NULL OR @plan_set_sha256 IS NULL OR DATALENGTH(@plan_set_sha256)<>71
 OR @model_unique_id IS NULL OR DATALENGTH({{MODEL_INPUT_BYTES}}) NOT BETWEEN 1 AND 4096
 OR DATALENGTH({{PLAN_INPUT_BYTES}}) NOT BETWEEN 1 AND 4096
 THROW 51600, 'DPONE_ENROLLMENT_INPUT_INVALID', 1;
DECLARE @payload varbinary(max),@payload_digest varbinary(max);
BEGIN TRY
 BEGIN TRANSACTION;
 {{SOURCE_CONTEXT}}
 SELECT @payload=payload,@payload_digest=payload_digest
  FROM [dpone_physical].[physical_plan_enrollments_v1] WITH(HOLDLOCK)
  WHERE generation_id=@generation AND executor_invocation_id=@expected_invocation
  AND registration_id=@registration_id AND registration_digest=@registration_digest
  AND DATALENGTH(registration_digest)=DATALENGTH(@registration_digest);
 IF @payload IS NULL THROW 51607, 'DPONE_ENROLLMENT_UNAVAILABLE', 1;
 {{ENROLLMENT_VALIDATE}}
 {{PLAN_REFERENCE_MATCH}}
 DECLARE @selected_position int,@selected_model nvarchar(max),@selected_digest varbinary(71);
 SELECT @selected_position=models.position,@selected_model=models.document
  FROM @model_documents models WHERE {{MODEL_MATCH}};
 IF @selected_position IS NULL THROW 51600, 'DPONE_ENROLLMENT_INPUT_INVALID', 1;
 SET @selected_digest=CONVERT(varbinary(71),CONVERT(varchar(71),JSON_VALUE(@selected_model,'$.model_plan_sha256')));
 {{NAMESPACE_ALL}}
 DECLARE @connection_id uniqueidentifier,@connect_time datetime2(7),@session_id int,@login_time datetime2(7);
 EXEC [dpone_physical].[physical_observe_current_connection_v1]
  @connection_id=@connection_id OUTPUT,@connect_time=@connect_time OUTPUT,
  @session_id=@session_id OUTPUT,@login_time=@login_time OUTPUT;
 IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 OR @connection_id IS NULL OR @connect_time IS NULL
  OR @session_id IS NULL OR @session_id<>@@SPID OR @login_time IS NULL
  THROW 51620, 'DPONE_SESSION_CONNECTION_UNOBSERVABLE', 1;
 DECLARE @model_bytes varbinary(max)={{MODEL_INPUT_BYTES}},@model_hash binary(32),@retained_model varbinary(max);
 SET @model_hash=HASHBYTES('SHA2_256',@model_bytes);
 SELECT @retained_model=model_unique_id FROM [dpone_physical].[physical_model_sessions_v1] WITH(UPDLOCK,HOLDLOCK)
  WHERE generation_id=@generation AND model_unique_id_hash=@model_hash;
 IF @retained_model IS NOT NULL
 BEGIN
  IF @retained_model<>@model_bytes OR DATALENGTH(@retained_model)<>DATALENGTH(@model_bytes)
   THROW 51606, 'DPONE_ENROLLMENT_CONFLICT', 1;
  THROW 51610, 'DPONE_SESSION_ALREADY_REGISTERED', 1;
 END;
 DECLARE @session_registration_id uniqueidentifier=NEWID();
 INSERT [dpone_physical].[physical_model_sessions_v1](session_registration_id,generation_id,
  model_unique_id_hash,model_unique_id,model_plan_digest,enrollment_digest,
  connection_id,connect_time,session_id,login_time,build_model_principal_id,build_model_principal_sid,
  build_control_principal_id,build_control_principal_sid,attached_at_utc)
  VALUES(@session_registration_id,@generation,@model_hash,@model_bytes,@selected_digest,@payload_digest,
   @connection_id,@connect_time,@session_id,@login_time,@caller_id,@caller_sid,
   @control_id,@control_sid,SYSUTCDATETIME());
 COMMIT TRANSACTION;
 SELECT CONVERT(smallint,1) AS [wire_version],
  LOWER(CONVERT(varchar(36),@registration_id)) AS [registration_id],
  CONVERT(varchar(71),@registration_digest) AS [registration_digest],
  LOWER(CONVERT(varchar(36),@generation)) AS [generation_id],
  LOWER(CONVERT(varchar(36),@expected_invocation)) AS [executor_invocation_id],
  CONVERT(varchar(71),@plan_set_sha256) AS [plan_set_sha256],
  @model_unique_id AS [model_unique_id],CONVERT(varchar(71),@selected_digest) AS [model_plan_sha256],
  LOWER(CONVERT(varchar(36),@session_registration_id)) AS [session_registration_id],
  @session_id AS [session_id],@guard_epoch AS [guard_epoch],@source_revision AS [source_revision],
  CONVERT(varchar(71),JSON_VALUE(@registration,'$.program.control_program_sha256')) AS [control_program_sha256],
  JSON_QUERY(@enrollment,'$.executor') AS [executor_json],@selected_model AS [plan_json];
END TRY
BEGIN CATCH
 IF XACT_STATE()<>0 ROLLBACK TRANSACTION;
 THROW;
END CATCH;
END;
