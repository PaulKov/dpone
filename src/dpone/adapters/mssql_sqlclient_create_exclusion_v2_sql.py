"""Fixed approved v2 queries; exact driver/SQL qualification remains required.

Own capture uses a fresh DECLARE followed by one 68-column SELECT result,
with zero parameters. XACT_STATE is sampled at acquisition entry, before
metadata reading; other transaction facts are sampled during the SELECT.
This sequential sampling is not a transactional snapshot. Partitions retain
every broad raw-predicate row. Thirteen captures plus the partitions and
creator checks total 27 execute calls and 40 SQL statements.
"""

OWN_INCARNATION_SQL = """
DECLARE @dpone_observer_entry_xact smallint = XACT_STATE();
SELECT
  -- Current scalar context and actual visibility (19 fields).
  ctx.spid, ctx.dbid, ctx.current_login, ctx.current_sid,
  ctx.original_login, ctx.original_sid, ctx.current_user_id,
  ctx.current_user_name, ctx.trancount, ctx.xact_state, ctx.implicit_tx,
  CONVERT(int, SERVERPROPERTY('ProductMajorVersion')) AS major_version,
  CONVERT(int, SERVERPROPERTY('EngineEdition')) AS engine_edition,
  HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE') AS state_permission,
  HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER PERFORMANCE STATE') AS performance_permission,
  CONVERT(nvarchar(128), SERVERPROPERTY('ServerName')) AS server_name,
  CONVERT(nvarchar(128), SERVERPROPERTY('MachineName')) AS machine_name,
  COALESCE(CONVERT(nvarchar(128), SERVERPROPERTY('InstanceName')), N'MSSQLSERVER') AS instance_name,
  CONVERT(nvarchar(128), SERVERPROPERTY('ComputerNamePhysicalNetBIOS')) AS physical_machine_name,
  -- Raw physical connections: count plus all actual connection fields (9).
  c.row_count, CONVERT(uniqueidentifier, c.uuid_text), c.session_id, c.connect_time,
  CONVERT(uniqueidentifier, c.parent_text), c.net_transport, c.protocol_type,
  c.auth_scheme, c.encrypt_option,
  -- Raw sessions: count plus all actual session fields (10).
  s.row_count, s.session_id, s.login_time, s.is_user_process,
  s.open_transaction_count, s.database_id, s.login_name, s.sid_hex,
  s.original_login_name, s.original_sid_hex,
  -- Authentication database is separately listed (1).
  s.authenticating_database_id,
  -- Database and recovery catalogs independently counted (6).
  d.row_count, d.database_id, d.database_name, d.owner_sid_hex,
  recovery.row_count, CONVERT(uniqueidentifier, recovery.guid_text),
  -- Actual server principal: count plus complete catalog row (6).
  login.row_count, login.principal_id, login.name, login.sid_hex, login.type_desc,
  IS_SRVROLEMEMBER('sysadmin', ctx.current_login) AS actual_sysadmin,
  -- Candidate database principals: count plus two complete raw row slots (11).
  candidates.row_count,
  candidates.p1_id, candidates.p1_name, candidates.p1_sid,
  candidates.p1_type, candidates.p1_auth,
  candidates.p2_id, candidates.p2_name, candidates.p2_sid,
  candidates.p2_type, candidates.p2_auth,
  -- Independently actual current database principal (4).
  current_principal.row_count, current_principal.principal_id,
  current_principal.name, current_principal.sid_hex,
  -- Independent raw exclusions (2).
  children.row_count, transactions.row_count
FROM (SELECT
  CONVERT(int, @@SPID) AS spid, DB_ID() AS dbid,
  SUSER_SNAME() AS current_login, SUSER_SID() AS current_sid,
  ORIGINAL_LOGIN() AS original_login, SUSER_SID(ORIGINAL_LOGIN()) AS original_sid,
  USER_ID() AS current_user_id, USER_NAME() AS current_user_name,
  @@TRANCOUNT AS trancount, @dpone_observer_entry_xact AS xact_state, (@@OPTIONS & 2) AS implicit_tx
) AS ctx
CROSS APPLY (
  SELECT COUNT_BIG(*) AS row_count,
    MAX(CONVERT(char(36), connection_id)) AS uuid_text,
    MAX(session_id) AS session_id, MAX(connect_time) AS connect_time,
    MAX(CONVERT(char(36), parent_connection_id)) AS parent_text,
    MAX(net_transport) AS net_transport, MAX(protocol_type) AS protocol_type,
    MAX(auth_scheme) AS auth_scheme, MAX(encrypt_option) AS encrypt_option
  FROM sys.dm_exec_connections WHERE session_id = ctx.spid
) AS c
CROSS APPLY (
  SELECT COUNT_BIG(*) AS row_count, MAX(CONVERT(int, session_id)) AS session_id,
    MAX(login_time) AS login_time, MAX(CONVERT(int, is_user_process)) AS is_user_process,
    MAX(open_transaction_count) AS open_transaction_count,
    MAX(CONVERT(int, database_id)) AS database_id,
    MAX(login_name) AS login_name, MAX(CONVERT(varchar(170), security_id, 2)) AS sid_hex,
    MAX(original_login_name) AS original_login_name,
    MAX(CONVERT(varchar(170), original_security_id, 2)) AS original_sid_hex,
    MAX(authenticating_database_id) AS authenticating_database_id
  FROM sys.dm_exec_sessions WHERE session_id = ctx.spid
) AS s
CROSS APPLY (
  SELECT COUNT_BIG(*) AS row_count, MAX(database_id) AS database_id,
    MAX(name) AS database_name, MAX(CONVERT(varchar(170), owner_sid, 2)) AS owner_sid_hex
  FROM sys.databases WHERE database_id = ctx.dbid
) AS d
CROSS APPLY (
  SELECT COUNT_BIG(*) AS row_count,
    MAX(CONVERT(char(36), database_guid)) AS guid_text
  FROM sys.database_recovery_status WHERE database_id = ctx.dbid
) AS recovery
CROSS APPLY (
  SELECT COUNT_BIG(*) AS row_count, MAX(principal_id) AS principal_id,
    MAX(name) AS name, MAX(CONVERT(varchar(170), sid, 2)) AS sid_hex,
    MAX(type_desc) AS type_desc
  FROM sys.server_principals WHERE sid = ctx.current_sid
) AS login
CROSS APPLY (
  SELECT COUNT_BIG(*) AS row_count,
    MAX(CASE WHEN slot = 1 THEN principal_id END) AS p1_id,
    MAX(CASE WHEN slot = 1 THEN name END) AS p1_name,
    MAX(CASE WHEN slot = 1 THEN sid_hex END) AS p1_sid,
    MAX(CASE WHEN slot = 1 THEN type_desc END) AS p1_type,
    MAX(CASE WHEN slot = 1 THEN authentication_type_desc END) AS p1_auth,
    MAX(CASE WHEN slot = 2 THEN principal_id END) AS p2_id,
    MAX(CASE WHEN slot = 2 THEN name END) AS p2_name,
    MAX(CASE WHEN slot = 2 THEN sid_hex END) AS p2_sid,
    MAX(CASE WHEN slot = 2 THEN type_desc END) AS p2_type,
    MAX(CASE WHEN slot = 2 THEN authentication_type_desc END) AS p2_auth
  FROM (
    SELECT principal_id, name, CONVERT(varchar(170), sid, 2) AS sid_hex,
      type_desc, authentication_type_desc,
      ROW_NUMBER() OVER (ORDER BY principal_id) AS slot
    FROM sys.database_principals
    WHERE principal_id = 1 OR sid = ctx.current_sid
  ) AS raw_candidates
) AS candidates
CROSS APPLY (
  SELECT COUNT_BIG(*) AS row_count, MAX(principal_id) AS principal_id,
    MAX(name) AS name, MAX(CONVERT(varchar(170), sid, 2)) AS sid_hex
  FROM sys.database_principals WHERE principal_id = ctx.current_user_id
) AS current_principal
CROSS APPLY (
  SELECT COUNT_BIG(*) AS row_count FROM sys.dm_exec_connections
  WHERE parent_connection_id = CONVERT(uniqueidentifier, c.uuid_text)
) AS children
CROSS APPLY (
  SELECT COUNT_BIG(*) AS row_count FROM sys.dm_tran_session_transactions
  WHERE session_id = ctx.spid
) AS transactions;
"""

CONNECTIONS_SQL = """
WITH raw_rows AS (
  SELECT connection_id, parent_connection_id, session_id, connect_time
  FROM sys.dm_exec_connections
  WHERE connection_id = CONVERT(uniqueidentifier, ?)
     OR parent_connection_id = CONVERT(uniqueidentifier, ?)
     OR session_id = ?
), classified AS (
  SELECT CASE WHEN connection_id = CONVERT(uniqueidentifier, ?)
                    AND session_id = ?
                    AND connect_time = CONVERT(datetime, ?)
                    AND parent_connection_id IS NULL
              THEN CONVERT(bigint, 1) ELSE CONVERT(bigint, 0) END AS own_hit,
         CASE WHEN connection_id = CONVERT(uniqueidentifier, ?)
                    OR parent_connection_id = CONVERT(uniqueidentifier, ?)
              THEN CONVERT(bigint, 1) ELSE CONVERT(bigint, 0) END AS original_hit
  FROM raw_rows
)
SELECT COUNT_BIG(*), COALESCE(SUM(own_hit), CONVERT(bigint, 0)),
       COALESCE(SUM(original_hit), CONVERT(bigint, 0)) FROM classified;
"""

SESSIONS_SQL = """
WITH raw_rows AS (
  SELECT session_id, login_time FROM sys.dm_exec_sessions WHERE session_id = ?
)
SELECT COUNT_BIG(*),
       COALESCE(SUM(CASE WHEN session_id = ?
                              AND login_time = CONVERT(datetime, ?)
                        THEN CONVERT(bigint, 1) ELSE CONVERT(bigint, 0) END),
                CONVERT(bigint, 0))
FROM raw_rows;
"""

REQUESTS_SQL = """
WITH raw_rows AS (
  SELECT connection_id, session_id, request_id, start_time
  FROM sys.dm_exec_requests
  WHERE session_id = ? OR connection_id = CONVERT(uniqueidentifier, ?)
), classified AS (
  SELECT connection_id, session_id, request_id, start_time,
         CASE WHEN connection_id = CONVERT(uniqueidentifier, ?)
                    AND session_id = ? AND session_id = @@SPID
                    AND request_id = CURRENT_REQUEST_ID()
              THEN CONVERT(bigint, 1) ELSE CONVERT(bigint, 0) END AS own_hit,
         CASE WHEN connection_id = CONVERT(uniqueidentifier, ?)
              THEN CONVERT(bigint, 1) ELSE CONVERT(bigint, 0) END AS original_hit
  FROM raw_rows
)
SELECT COUNT_BIG(*), COALESCE(SUM(own_hit), CONVERT(bigint, 0)),
       COALESCE(SUM(original_hit), CONVERT(bigint, 0)),
       CONVERT(uniqueidentifier,
         MAX(CASE WHEN own_hit = 1 THEN CONVERT(char(36), connection_id) END)),
       MAX(CASE WHEN own_hit = 1 THEN CONVERT(int, session_id) END),
       MAX(CASE WHEN own_hit = 1 THEN request_id END),
       MAX(CASE WHEN own_hit = 1 THEN start_time END)
FROM classified;
"""

TRANSACTIONS_SQL = """
SELECT COUNT_BIG(*) FROM sys.dm_tran_session_transactions WHERE session_id = ?;
"""


class FixedSqlClientCreateExclusionQueries:
    """Build the reviewed six-part sweep for one original and observer pair."""

    def __init__(self, sample_kind):
        self._kind = sample_kind

    def queries(self, *, original, observer):
        kind = self._kind
        old, own = str(original.connection_id), str(observer.connection_id)
        session, own_session = original.session_id, observer.session_id
        connection = (
            kind.CONNECTIONS,
            CONNECTIONS_SQL,
            (old, old, session, own, own_session, observer.connect_time, old, old),
        )
        sessions = (kind.SESSIONS, SESSIONS_SQL, (session, own_session, observer.login_time))
        return (
            connection,
            sessions,
            (kind.REQUESTS, REQUESTS_SQL, (session, old, own, own_session, old)),
            (kind.TRANSACTIONS, TRANSACTIONS_SQL, (session,)),
            connection,
            sessions,
        )
