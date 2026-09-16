CREATE PROCEDURE [{{LOCAL_SCHEMA}}].[{{ENTRY}}]
 @registration_id uniqueidentifier, @generation uniqueidentifier,
 @expected_invocation uniqueidentifier, @object_id int, @kind varchar(20)
AS
BEGIN
SET NOCOUNT ON;
IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 OR @registration_id IS NULL
 OR @generation IS NULL OR @expected_invocation IS NULL
 OR @object_id IS NULL OR @object_id<=0 OR @kind IS NULL
 OR @kind COLLATE Latin1_General_100_BIN2 NOT IN
 ('HEADER','TABLE','COLUMN','INDEX','INDEX_COLUMN','PARTITION','DEPENDENCY','FORBIDDEN_PROPERTY','COUNT')
 OR LEN(@kind)<>DATALENGTH(@kind)
 THROW 51460, 'DPONE_CATALOG_REQUEST_INVALID', 1;
IF ISNULL(CONVERT(int,SERVERPROPERTY('ProductMajorVersion')),0)<>16
 THROW 51460, 'DPONE_CATALOG_PLATFORM_UNSUPPORTED', 1;
IF (SELECT COUNT(*) FROM sys.crypt_properties WHERE class=1 AND major_id=@@PROCID)<>1
 OR NOT EXISTS (SELECT 1 FROM sys.crypt_properties WHERE class=1 AND major_id=@@PROCID
 AND crypt_type='SPVC' AND thumbprint=0x{{CERTIFICATE_THUMBPRINT}})
 THROW 51460, 'DPONE_CATALOG_SIGNATURE_INVALID', 1;
DECLARE @source TABLE (
 wire_version smallint, registration_id uniqueidentifier, registration_digest varbinary(71),
 generation_id uniqueidentifier, executor_invocation_id uniqueidentifier,
 guard_epoch bigint, source_revision bigint, reservation_locator varbinary(4096),
 reservation_digest varbinary(71), executor_payload varbinary(max),
 observed_model_principal_id int, observed_model_principal_sid varbinary(85),
 observed_control_principal_id int, observed_control_principal_sid varbinary(85));
INSERT @source EXEC [{{LOCAL_SCHEMA}}].[physical_require_source_v1]
 @registration_id=@registration_id,@generation=@generation,@expected_invocation=@expected_invocation;
IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 OR (SELECT COUNT(*) FROM @source)<>1 OR NOT EXISTS
 (SELECT 1 FROM @source WHERE wire_version=1 AND registration_id=@registration_id
 AND registration_digest IS NOT NULL
 AND DATALENGTH(registration_digest)=71 AND generation_id=@generation
 AND executor_invocation_id=@expected_invocation AND guard_epoch>0 AND source_revision>=2)
 THROW 51460, 'DPONE_CATALOG_SOURCE_MISMATCH', 1;
DECLARE @model_schema sysname={{MODEL_SCHEMA}},@object_name sysname,
 @object_create_time datetime2(7),@object_modify_time datetime2(7),
 @row_limit int,@dependency_limit int,@column_limit int,@definition_limit int,
 @metadata_limit int,@generation_limit bigint,@registered_columns int,
 @registration_payload varbinary(max),@registration_digest varbinary(71),
 @platform_subject varbinary(max),@profile_subject varbinary(max),
 @profile_locator varbinary(4096),@profile_digest varbinary(71);
SELECT @registration_payload=r.payload,@registration_digest=r.registration_digest,
 @platform_subject=r.platform_subject,@profile_subject=r.trusted_profile_subject,
 @profile_locator=r.trusted_profile_locator,@profile_digest=r.trusted_profile_digest,
 @row_limit=r.max_catalog_rows,@dependency_limit=r.max_dependency_rows,
 @definition_limit=r.max_definition_utf16_bytes,@registered_columns=r.max_columns,
 @metadata_limit=r.max_metadata_bytes,@generation_limit=r.max_generation_bytes
FROM [{{LOCAL_SCHEMA}}].[physical_runtime_registrations_v1] r WITH (HOLDLOCK)
JOIN @source source ON source.registration_id=r.registration_id
 AND source.registration_digest=r.registration_digest AND DATALENGTH(r.registration_digest)=71
WHERE r.registration_id=@registration_id AND r.model_database_id={{MODEL_DATABASE_ID}}
 AND r.model_database_guid='{{MODEL_DATABASE_GUID}}'
 AND r.model_database_create_token=CONVERT(datetime2(7),'{{MODEL_DATABASE_CREATE_TOKEN}}',126)
 AND CONVERT(varbinary(max),r.model_database_name)=CONVERT(varbinary(max),{{MODEL_DATABASE_NAME}})
 AND DATALENGTH(r.model_database_name)=DATALENGTH({{MODEL_DATABASE_NAME}})
 AND CONVERT(varbinary(max),r.local_schema)=CONVERT(varbinary(max),N'{{LOCAL_SCHEMA}}')
 AND DATALENGTH(r.local_schema)=DATALENGTH(N'{{LOCAL_SCHEMA}}')
 AND r.control_schema<>@model_schema AND ISNULL(SCHEMA_ID(r.control_schema),-1)<>{{MODEL_SCHEMA_ID}}
 AND r.observer_mode=CONVERT(varbinary(32),'DEDICATED') AND DATALENGTH(r.observer_mode)=9;
IF @registration_payload IS NULL OR DATALENGTH(@registration_payload) NOT BETWEEN 1 AND 1048576
 OR @registration_digest IS NULL OR DATALENGTH(@registration_digest)<>71
 OR @registration_digest<>CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@registration_payload),2)))
 OR @metadata_limit IS NULL OR @metadata_limit NOT BETWEEN 1 AND 1048576
 OR @generation_limit IS NULL OR @generation_limit<1
 OR @row_limit IS NULL OR @row_limit<1 OR @definition_limit IS NULL OR @definition_limit<1
 OR @dependency_limit IS NULL OR @dependency_limit NOT BETWEEN 1 AND @row_limit
 OR @registered_columns IS NULL OR @registered_columns<>256
 THROW 51470, 'DPONE_CATALOG_REGISTRATION_UNAVAILABLE', 1;
SET @column_limit=CASE WHEN @row_limit<@registered_columns THEN @row_limit ELSE @registered_columns END;
DECLARE @binding_payload varbinary(max),@binding_digest varbinary(71);
SELECT @binding_payload=b.payload,@binding_digest=b.binding_digest
FROM [{{LOCAL_SCHEMA}}].[physical_catalog_bindings_v1] b WITH (HOLDLOCK)
WHERE b.registration_id=@registration_id AND b.registration_digest=@registration_digest
 AND DATALENGTH(b.registration_digest)=DATALENGTH(@registration_digest)
 AND DATALENGTH(b.payload) BETWEEN 1 AND @metadata_limit AND DATALENGTH(b.payload)<=1048576;
IF @binding_payload IS NULL OR @binding_digest IS NULL OR DATALENGTH(@binding_digest)<>71
 OR @binding_digest<>CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@binding_payload),2)))
 THROW 51470, 'DPONE_CATALOG_BINDING_UNAVAILABLE', 1;
{{BINDING_DECODE}}
{{BINDING_SHAPES}}
{{BINDING_CANONICAL}}
{{BINDING_LINKS}}
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE schema_id={{MODEL_SCHEMA_ID}} AND principal_id=1
 AND CONVERT(varbinary(max),name)=CONVERT(varbinary(max),@model_schema)
 AND DATALENGTH(name)=DATALENGTH(@model_schema))
 THROW 51461, 'DPONE_CATALOG_SCHEMA_PIN_MISMATCH', 1;
SELECT @object_name=t.name,@object_create_time=CONVERT(datetime2(7),t.create_date),
 @object_modify_time=CONVERT(datetime2(7),t.modify_date) FROM sys.tables t WHERE t.object_id=@object_id
 AND t.schema_id={{MODEL_SCHEMA_ID}} AND t.is_ms_shipped=0 AND t.is_external=0
 AND t.is_memory_optimized=0 AND t.type='U';
IF @object_name IS NULL
 THROW 51461, 'DPONE_CATALOG_OBJECT_NOT_OBSERVABLE', 1;
IF ISNULL(HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION'),0)<>1
 OR ISNULL(HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW SECURITY DEFINITION'),0)<>1
 OR ISNULL(HAS_PERMS_BY_NAME(N'sys.sql_expression_dependencies','OBJECT','SELECT'),0)<>1
 OR ISNULL(HAS_PERMS_BY_NAME(QUOTENAME(@model_schema)+N'.'+QUOTENAME(@object_name),'OBJECT','SELECT'),0)<>1
 THROW 51462, 'DPONE_CATALOG_VISIBILITY_INSUFFICIENT', 1;
IF EXISTS (SELECT 1 FROM sys.database_permissions p
 WHERE p.grantee_principal_id IN (SELECT principal_id FROM sys.user_token)
 AND p.state='D' AND p.class IN (0,1,3)
 AND p.permission_name IN ('VIEW DEFINITION','VIEW SECURITY DEFINITION','CONTROL'))
 OR EXISTS (SELECT 1 FROM sys.server_permissions p
 WHERE p.grantee_principal_id IN (SELECT principal_id FROM sys.login_token)
 AND p.state='D' AND p.class=100
 AND p.permission_name IN ('VIEW ANY DEFINITION','VIEW ANY SECURITY DEFINITION','CONTROL SERVER'))
 THROW 51462, 'DPONE_CATALOG_METADATA_DENY_UNSUPPORTED', 1;
IF EXISTS (SELECT 1 FROM sys.security_predicates WHERE target_object_id=@object_id)
 THROW 51462, 'DPONE_CATALOG_ROW_SECURITY_UNSUPPORTED', 1;
DECLARE @row_count_exact bigint,@lock_probe int,@count_sql nvarchar(max);
IF @kind='COUNT'
BEGIN
 SET @count_sql=N'SELECT @count=COUNT_BIG(*) FROM '
  +QUOTENAME(@model_schema)+N'.'+QUOTENAME(@object_name)+N' WITH (TABLOCK,HOLDLOCK);';
 EXEC sys.sp_executesql @count_sql,N'@count bigint OUTPUT',@count=@row_count_exact OUTPUT;
 IF @row_count_exact IS NULL OR @row_count_exact<0
  THROW 51461, 'DPONE_CATALOG_COUNT_UNAVAILABLE', 1;
END
ELSE
BEGIN
 SET @count_sql=N'SELECT TOP (1) @probe=1 FROM '
  +QUOTENAME(@model_schema)+N'.'+QUOTENAME(@object_name)+N' WITH (TABLOCK,HOLDLOCK);';
 EXEC sys.sp_executesql @count_sql,N'@probe int OUTPUT',@probe=@lock_probe OUTPUT;
END
IF NOT EXISTS
 (SELECT 1 FROM sys.tables t WHERE t.object_id=@object_id AND t.schema_id={{MODEL_SCHEMA_ID}}
 AND CONVERT(varbinary(max),t.name)=CONVERT(varbinary(max),@object_name)
 AND DATALENGTH(t.name)=DATALENGTH(@object_name)
 AND CONVERT(datetime2(7),t.create_date)=@object_create_time
 AND CONVERT(datetime2(7),t.modify_date)=@object_modify_time)
 OR EXISTS (SELECT 1 FROM sys.security_predicates WHERE target_object_id=@object_id)
 THROW 51461, 'DPONE_CATALOG_OBJECT_CHANGED', 1;
IF @kind='TABLE'
BEGIN
 SELECT 1,'TABLE',@object_id,1,1,{{TABLE_PROJECTION}} FROM sys.tables t WHERE t.object_id=@object_id;
 RETURN;
END
IF @kind='COUNT'
BEGIN
 SELECT 1,'COUNT',@object_id,1,1,@row_count_exact;
 RETURN;
END
IF @kind IN ('HEADER','INDEX') AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=@object_id
 AND DATALENGTH(filter_definition)>@definition_limit)
 THROW 51463, 'DPONE_CATALOG_DEFINITION_BOUND_EXCEEDED', 1;
{{MATERIALIZE}}
IF @kind='HEADER'
BEGIN
 SELECT 1,'HEADER',@object_id,1,1,DB_ID(),r.database_guid,
  CONVERT(char(27),CONVERT(datetime2(7),t.create_date),126),
  CONVERT(char(27),CONVERT(datetime2(7),t.modify_date),126),@model_schema,t.name,t.type,
  (SELECT COUNT(*) FROM #catalog_COLUMN),(SELECT COUNT(*) FROM #catalog_INDEX),
  (SELECT COUNT(*) FROM #catalog_INDEX_COLUMN),(SELECT COUNT(*) FROM #catalog_PARTITION),
  (SELECT COUNT(*) FROM #catalog_DEPENDENCY),(SELECT COUNT(*) FROM #catalog_FORBIDDEN_PROPERTY)
 FROM sys.tables t CROSS JOIN sys.database_recovery_status r
 WHERE t.object_id=@object_id AND r.database_id=DB_ID();
 RETURN;
END
{{COLLECTION_OUTPUTS}}
END
