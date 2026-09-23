"""Fixed broad SPID queries for restricted-session departure."""

CONNECTIONS_SQL = """
WITH raw_rows AS (
  SELECT connection_id, session_id, connect_time
  FROM sys.dm_exec_connections WHERE session_id = ?
)
SELECT COUNT_BIG(*),
       COALESCE(SUM(CASE WHEN connection_id = CONVERT(uniqueidentifier, ?)
                              AND session_id = ? AND connect_time = CONVERT(datetime, ?)
                         THEN CONVERT(bigint,1) ELSE CONVERT(bigint,0) END), CONVERT(bigint,0))
FROM raw_rows;
"""

SESSIONS_SQL = """
WITH raw_rows AS (
  SELECT session_id, login_time FROM sys.dm_exec_sessions WHERE session_id = ?
)
SELECT COUNT_BIG(*),
       COALESCE(SUM(CASE WHEN session_id = ? AND login_time = CONVERT(datetime, ?)
                         THEN CONVERT(bigint,1) ELSE CONVERT(bigint,0) END), CONVERT(bigint,0)),
       COALESCE(SUM(CASE WHEN session_id = ? AND login_time = CONVERT(datetime, ?)
                         THEN CONVERT(bigint,1) ELSE CONVERT(bigint,0) END), CONVERT(bigint,0))
FROM raw_rows;
"""

REQUESTS_SQL = """
WITH raw_rows AS (
  SELECT connection_id, session_id, request_id, start_time
  FROM sys.dm_exec_requests WHERE session_id = ?
), classified AS (
  SELECT connection_id, session_id, request_id, start_time,
         CASE WHEN connection_id = CONVERT(uniqueidentifier, ?)
                    AND session_id = ? AND session_id = @@SPID
                    AND request_id = CURRENT_REQUEST_ID()
              THEN CONVERT(bigint,1) ELSE CONVERT(bigint,0) END AS own_hit
  FROM raw_rows
)
SELECT COUNT_BIG(*), COALESCE(SUM(own_hit), CONVERT(bigint,0)),
       CONVERT(uniqueidentifier, MAX(CASE WHEN own_hit=1 THEN CONVERT(char(36), connection_id) END)),
       MAX(CASE WHEN own_hit=1 THEN CONVERT(int, session_id) END),
       MAX(CASE WHEN own_hit=1 THEN request_id END),
       MAX(CASE WHEN own_hit=1 THEN start_time END)
FROM classified;
"""

TRANSACTIONS_SQL = """
SELECT COUNT_BIG(*) FROM sys.dm_tran_session_transactions WHERE session_id = ?;
"""
