CREATE PROCEDURE [{{LOCAL_SCHEMA}}].[physical_discover_absent_v1]
 @registration_id uniqueidentifier,@request varbinary(max)
AS
BEGIN
SET NOCOUNT ON;
IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 OR @registration_id IS NULL
 OR ISNULL(CONVERT(int,SERVERPROPERTY('ProductMajorVersion')),0)<>16
 THROW 51480, 'DPONE_DISCOVERY_TRANSACTION_INVALID', 1;
{{MODEL_CALLER}}
{{MODEL_SIGNATURE}}
DECLARE @payload varbinary(max),@registration_digest varbinary(71),
 @metadata_limit int,@row_limit int,@platform_subject varbinary(max),@profile_subject varbinary(max),
 @profile_locator varbinary(max),@profile_digest varbinary(71);
SELECT @payload=payload,@registration_digest=registration_digest,
 @metadata_limit=max_metadata_bytes,@row_limit=max_catalog_rows,
 @platform_subject=platform_subject,@profile_subject=trusted_profile_subject,
 @profile_locator=trusted_profile_locator,@profile_digest=trusted_profile_digest
 FROM [{{LOCAL_SCHEMA}}].[physical_runtime_registrations_v1] WITH (HOLDLOCK)
 WHERE registration_id=@registration_id;
IF @payload IS NULL OR DATALENGTH(@payload) NOT BETWEEN 1 AND 1048576
 OR @registration_digest IS NULL OR DATALENGTH(@registration_digest)<>71
 OR @registration_digest<>CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@payload),2)))
 OR @metadata_limit IS NULL OR @metadata_limit NOT BETWEEN 1 AND 1048576
 OR @row_limit IS NULL OR @row_limit<1
 THROW 51480, 'DPONE_DISCOVERY_REGISTRATION_INVALID', 1;
{{REGISTRATION_DECODE}}
IF ISNULL(ISJSON(@registration,OBJECT),0)<>1 OR (SELECT COUNT(*) FROM OPENJSON(@registration))<>18
 OR ISNULL(JSON_VALUE(@registration,'$.schema'),'')<>'dpone.mssql-physical-runtime-registration.v1'
 THROW 51480, 'DPONE_DISCOVERY_REGISTRATION_INVALID', 1;
IF NOT EXISTS (SELECT 1 FROM [{{LOCAL_SCHEMA}}].[physical_runtime_registrations_v1] r
 WHERE r.registration_id=@registration_id AND {{PROJECTION_CHECK}}
 AND r.metadata_model_principal_id=@caller_id AND r.metadata_model_sid=@caller_sid
 AND DATALENGTH(r.metadata_model_sid)=DATALENGTH(@caller_sid)
 AND r.observer_mode=CONVERT(varbinary(32),'DEDICATED') AND DATALENGTH(r.observer_mode)=9
 AND r.model_database_id={{MODEL_DATABASE_ID}} AND r.model_database_guid='{{MODEL_DATABASE_GUID}}'
 AND r.model_database_create_token=CONVERT(datetime2(7),'{{MODEL_DATABASE_CREATE_TOKEN}}',126)
 AND CONVERT(varbinary(max),r.model_database_name)=CONVERT(varbinary(max),{{MODEL_DATABASE_NAME}})
 AND DATALENGTH(r.model_database_name)=DATALENGTH({{MODEL_DATABASE_NAME}})
 AND CONVERT(varbinary(max),r.control_schema)=CONVERT(varbinary(max),N'{{CONTROL_SCHEMA}}')
 AND DATALENGTH(r.control_schema)=DATALENGTH(N'{{CONTROL_SCHEMA}}'))
 THROW 51480, 'DPONE_DISCOVERY_PROJECTION_OR_CALLER_INVALID', 1;
{{MODEL_PIN}}
DECLARE @binding_payload varbinary(max),@binding_digest varbinary(71);
SELECT @binding_payload=payload,@binding_digest=binding_digest
 FROM [{{LOCAL_SCHEMA}}].[physical_catalog_bindings_v1] WITH (HOLDLOCK)
 WHERE registration_id=@registration_id AND registration_digest=@registration_digest
 AND DATALENGTH(registration_digest)=71 AND DATALENGTH(payload) BETWEEN 1 AND @metadata_limit;
IF @binding_payload IS NULL OR @binding_digest IS NULL OR DATALENGTH(@binding_digest)<>71
 OR @binding_digest<>CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@binding_payload),2)))
 THROW 51480, 'DPONE_DISCOVERY_BINDING_INVALID', 1;
{{BINDING_DECODE}}
{{BINDING_SHAPES}}
{{BINDING_CANONICAL}}
{{BINDING_LINKS}}
IF @request IS NULL OR DATALENGTH(@request) NOT BETWEEN 1 AND @metadata_limit
 THROW 51480, 'DPONE_DISCOVERY_REQUEST_BOUND', 1;
{{REQUEST_DECODE}}
{{REQUEST_CHECKS}}
DECLARE @objects TABLE(ordinal int PRIMARY KEY,model_unique_id nvarchar(max),role nvarchar(max),name nvarchar(max) COLLATE CATALOG_DEFAULT);
IF (SELECT COUNT(*) FROM OPENJSON(@json,'$.objects')) NOT BETWEEN 3 AND @row_limit
 OR (SELECT COUNT(*) FROM OPENJSON(@json,'$.objects'))%3<>0
 OR EXISTS (SELECT 1 FROM OPENJSON(@json,'$.objects') WHERE type<>5)
 THROW 51480, 'DPONE_DISCOVERY_OBJECTS_INVALID', 1;
DECLARE @item nvarchar(max),@i int=0,@count int=(SELECT COUNT(*) FROM OPENJSON(@json,'$.objects')),
 @canonical_objects nvarchar(max)=N'[',@id nvarchar(max),@role nvarchar(max),@name nvarchar(max);
WHILE @i<@count
BEGIN
 SELECT @item=value FROM OPENJSON(@json,'$.objects') WHERE [key]=CONVERT(nvarchar(12),@i);
 {{OBJECT_SHAPE}}
 {{OBJECT_CANONICAL}}
 SELECT @id=model_unique_id,@role=role,@name=name FROM OPENJSON(@item)
 WITH(model_unique_id nvarchar(max),role nvarchar(max),name nvarchar(max));
 IF @id IS NULL OR DATALENGTH(CONVERT(varchar(max),@id COLLATE Latin1_General_100_BIN2_UTF8)) NOT BETWEEN 1 AND 4096
 OR @name IS NULL OR DATALENGTH(@name) NOT BETWEEN 2 AND 256
 OR @role IS NULL OR CONVERT(varbinary(max),@role)<>CONVERT(varbinary(max),CASE @i%3 WHEN 0 THEN N'TARGET' WHEN 1 THEN N'CANDIDATE' ELSE N'HELPER' END)
 OR DATALENGTH(@role)<>DATALENGTH(CASE @i%3 WHEN 0 THEN N'TARGET' WHEN 1 THEN N'CANDIDATE' ELSE N'HELPER' END)
 THROW 51480, 'DPONE_DISCOVERY_OBJECT_INVALID', 1;
 INSERT @objects VALUES(@i,@id,@role,@name);
 SET @canonical_objects+=CASE WHEN @i=0 THEN N'' ELSE N',' END+@item;
 SET @i+=1;
END
SET @canonical_objects+=N']';
IF CONVERT(varbinary(max),@canonical_objects)<>CONVERT(varbinary(max),JSON_QUERY(@json,'$.objects'))
 OR DATALENGTH(@canonical_objects)<>DATALENGTH(JSON_QUERY(@json,'$.objects'))
 OR EXISTS(SELECT 1 FROM @objects a JOIN @objects b ON b.ordinal=a.ordinal-a.ordinal%3
 WHERE CONVERT(varbinary(max),a.model_unique_id)<>CONVERT(varbinary(max),b.model_unique_id)
 OR DATALENGTH(a.model_unique_id)<>DATALENGTH(b.model_unique_id))
 OR EXISTS(SELECT 1 FROM @objects a JOIN @objects b ON b.ordinal=a.ordinal+3 WHERE a.ordinal%3=0
 AND CONVERT(varbinary(max),CONVERT(varchar(max),a.model_unique_id COLLATE Latin1_General_100_BIN2_UTF8))>=CONVERT(varbinary(max),CONVERT(varchar(max),b.model_unique_id COLLATE Latin1_General_100_BIN2_UTF8)))
 THROW 51480, 'DPONE_DISCOVERY_OBJECT_ORDER_INVALID', 1;
DECLARE @filegroup nvarchar(max)=(SELECT value FROM OPENJSON(@json) WHERE [key]=N'filegroup_name');
IF @filegroup IS NULL OR DATALENGTH(@filegroup) NOT BETWEEN 2 AND 256
 THROW 51480, 'DPONE_DISCOVERY_FILEGROUP_INVALID', 1;
SET @i=1;
WHILE @i<=128
BEGIN
 IF EXISTS(SELECT 1 FROM @objects WHERE UNICODE(SUBSTRING(name,@i,1)) BETWEEN 0 AND 31 OR UNICODE(SUBSTRING(name,@i,1)) BETWEEN 127 AND 159)
 OR UNICODE(SUBSTRING(@filegroup,@i,1)) BETWEEN 0 AND 31 OR UNICODE(SUBSTRING(@filegroup,@i,1)) BETWEEN 127 AND 159
 THROW 51480, 'DPONE_DISCOVERY_IDENTIFIER_INVALID', 1;
 SET @i+=1;
END
DECLARE @guard_epoch bigint,@control_id int,@control_sid varbinary(85);
EXEC {{CONTROL_DATABASE}}.[{{CONTROL_SCHEMA}}].[physical_control_require_owner_v1]
 @payload=@payload,@digest=@registration_digest,@request=@request,@model_id=@caller_id,@model_sid=@caller_sid,
 @guard_epoch=@guard_epoch OUTPUT,@control_id=@control_id OUTPUT,@control_sid=@control_sid OUTPUT;
IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 OR @guard_epoch IS NULL OR @guard_epoch<=0
 OR NOT EXISTS(SELECT 1 FROM [{{LOCAL_SCHEMA}}].[physical_runtime_registrations_v1]
 WHERE registration_id=@registration_id AND metadata_control_principal_id=@control_id
 AND metadata_control_sid=@control_sid AND DATALENGTH(metadata_control_sid)=DATALENGTH(@control_sid))
 THROW 51480, 'DPONE_DISCOVERY_OWNER_FACTS_INVALID', 1;
IF ISNULL(HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION'),0)<>1
 OR ISNULL(HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW SECURITY DEFINITION'),0)<>1
 THROW 51480, 'DPONE_DISCOVERY_VISIBILITY_INSUFFICIENT', 1;
IF EXISTS(SELECT 1 FROM sys.database_permissions p WHERE p.grantee_principal_id IN(SELECT principal_id FROM sys.user_token)
 AND p.state='D' AND p.class IN(0,1,3) AND p.permission_name IN('VIEW DEFINITION','VIEW SECURITY DEFINITION','CONTROL'))
 OR EXISTS(SELECT 1 FROM sys.server_permissions p WHERE p.grantee_principal_id IN(SELECT principal_id FROM sys.login_token)
 AND p.state='D' AND p.class=100 AND p.permission_name IN('VIEW ANY DEFINITION','VIEW ANY SECURITY DEFINITION','CONTROL SERVER'))
 THROW 51480, 'DPONE_DISCOVERY_METADATA_DENY_UNSUPPORTED', 1;
IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE schema_id={{MODEL_SCHEMA_ID}} AND principal_id=1
 AND CONVERT(varbinary(max),name)=CONVERT(varbinary(max),{{MODEL_SCHEMA}}) AND DATALENGTH(name)=DATALENGTH({{MODEL_SCHEMA}}))
 OR ISNULL(SCHEMA_ID(N'{{CONTROL_SCHEMA}}'),-1)={{MODEL_SCHEMA_ID}}
 THROW 51480, 'DPONE_DISCOVERY_SCHEMA_CHANGED', 1;
IF EXISTS(SELECT 1 FROM @objects a JOIN @objects b ON a.ordinal<b.ordinal AND a.name=b.name COLLATE CATALOG_DEFAULT)
 OR EXISTS(SELECT 1 FROM sys.objects o JOIN @objects r ON o.name=r.name COLLATE CATALOG_DEFAULT WHERE o.schema_id={{MODEL_SCHEMA_ID}})
 THROW 51480, 'DPONE_DISCOVERY_NAMESPACE_COLLISION', 1;
DECLARE @filegroup_id int,@filegroup_name sysname,@filegroup_type char(2);
SELECT @filegroup_id=f.data_space_id,@filegroup_name=f.name,@filegroup_type=f.type
 FROM sys.data_spaces d JOIN sys.filegroups f ON f.data_space_id=d.data_space_id
 WHERE f.name=@filegroup COLLATE CATALOG_DEFAULT AND f.type='FG' AND d.type='FG' AND f.is_read_only=0
 AND CONVERT(varbinary(max),f.name)=CONVERT(varbinary(max),@filegroup) AND DATALENGTH(f.name)=DATALENGTH(@filegroup);
IF @filegroup_id IS NULL OR NOT EXISTS(SELECT 1 FROM sys.databases WHERE database_id=DB_ID() AND is_read_only=0 AND state=0)
 THROW 51480, 'DPONE_DISCOVERY_FILEGROUP_UNAVAILABLE', 1;
SELECT CONVERT(smallint,1),@registration_id,@registration_digest,
 CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@request),2))),
 DB_ID(),CONVERT(uniqueidentifier,'{{MODEL_DATABASE_GUID}}'),CONVERT(int,{{MODEL_SCHEMA_ID}}),{{MODEL_SCHEMA}},
 CONVERT(int,1),@guard_epoch,@count,@filegroup_id,@filegroup_name,@filegroup_type,
 @caller_id,@caller_sid,@control_id,@control_sid;
END
-- DPONE MODULE BOUNDARY
CREATE PROCEDURE [{{CONTROL_SCHEMA}}].[physical_control_require_owner_v1]
 @payload varbinary(max),@digest varbinary(max),@request varbinary(max),@model_id int,@model_sid varbinary(max),
 @guard_epoch bigint OUTPUT,@control_id int OUTPUT,@control_sid varbinary(85) OUTPUT
AS
BEGIN
SET NOCOUNT ON;
IF @@TRANCOUNT<>1 OR XACT_STATE()<>1 OR @payload IS NULL OR DATALENGTH(@payload) NOT BETWEEN 1 AND 1048576
 OR @digest IS NULL OR DATALENGTH(@digest)<>71
 OR @digest<>CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@payload),2)))
 OR @model_id IS NULL OR @model_id<=4 OR @model_sid IS NULL OR DATALENGTH(@model_sid) NOT BETWEEN 1 AND 85
 THROW 51480, 'DPONE_DISCOVERY_HELPER_INPUT_INVALID', 1;
{{CONTROL_CALLER}}
{{CONTROL_SIGNATURE}}
{{REGISTRATION_DECODE}}
{{CONTROL_PIN}}
IF ISNULL(TRY_CONVERT(int,JSON_VALUE(@registration,'$.principals.metadata.model.principal_id')),-1)<>@model_id
 OR TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.metadata.model.sid_hex'),2) IS NULL
 OR TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.metadata.model.sid_hex'),2)<>@model_sid
 OR DATALENGTH(TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.metadata.model.sid_hex'),2))<>DATALENGTH(@model_sid)
 OR ISNULL(TRY_CONVERT(int,JSON_VALUE(@registration,'$.principals.metadata.control.principal_id')),-1)<>@caller_id
 OR TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.metadata.control.sid_hex'),2) IS NULL
 OR TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.metadata.control.sid_hex'),2)<>@caller_sid
 OR DATALENGTH(TRY_CONVERT(varbinary(max),JSON_VALUE(@registration,'$.principals.metadata.control.sid_hex'),2))<>DATALENGTH(@caller_sid)
 THROW 51480, 'DPONE_DISCOVERY_CALLER_MAPPING_INVALID', 1;
DECLARE @authority_locator varbinary(max)={{AUTHORITY_LOCATOR}},@authority_digest varbinary(max)={{AUTHORITY_DIGEST}};
IF NOT EXISTS(SELECT 1 FROM [{{CONTROL_SCHEMA}}].[native_original_authorities_v1] WITH(HOLDLOCK)
 WHERE authority_hash=HASHBYTES('SHA2_256',@authority_locator) AND authority_locator=@authority_locator
 AND DATALENGTH(authority_locator)=DATALENGTH(@authority_locator) AND authority_digest=@authority_digest
 AND DATALENGTH(authority_digest)=DATALENGTH(@authority_digest) AND schema_version=1
 AND runtime_principal_id=@caller_id AND runtime_principal_sid=@caller_sid AND DATALENGTH(runtime_principal_sid)=DATALENGTH(@caller_sid))
 THROW 51480, 'DPONE_DISCOVERY_AUTHORITY_INVALID', 1;
DECLARE @metadata_limit int=TRY_CONVERT(int,JSON_VALUE(@registration,'$.limits.max_metadata_bytes'));
IF @metadata_limit IS NULL OR @metadata_limit NOT BETWEEN 1 AND 1048576
 OR @request IS NULL OR DATALENGTH(@request) NOT BETWEEN 1 AND @metadata_limit
 THROW 51480, 'DPONE_DISCOVERY_REQUEST_BOUND', 1;
{{REQUEST_DECODE}}
{{REQUEST_CHECKS}}
{{PHYSICAL_OWNER}}
IF CONVERT(varbinary(max),JSON_QUERY(@json,'$.workspace_attempt.write_subjects'))<>CONVERT(varbinary(max),N'['+@subjects_json+N']')
 OR DATALENGTH(JSON_QUERY(@json,'$.workspace_attempt.write_subjects'))<>DATALENGTH(N'['+@subjects_json+N']')
 THROW 51480, 'DPONE_DISCOVERY_SUBJECT_ARRAY_NONCANONICAL', 1;
SET @guard_epoch=@epoch;
SET @control_id=@caller_id;
SET @control_sid=@caller_sid;
END
