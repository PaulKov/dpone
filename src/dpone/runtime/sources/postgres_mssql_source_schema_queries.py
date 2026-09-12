"""Immutable SQL templates for PostgreSQL R1 relation-schema observation."""

SET_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
SET_LOCK_TIMEOUT_SQL = "SET LOCAL lock_timeout = '5000ms'"
LOCK_RELATION_TEMPLATE = "LOCK TABLE ONLY {schema_identifier}.{relation_identifier} IN ACCESS SHARE MODE"

INITIAL_SNAPSHOT_WITNESS_SQL = """WITH snapshot_witness AS MATERIALIZED (
    SELECT pg_catalog.txid_current_snapshot() AS snapshot_token
)
SELECT
    s.snapshot_token::text AS snapshot_token,
    pg_catalog.txid_snapshot_xmax(s.snapshot_token)::text::bigint
        AS visible_horizon,
    lock_witness.transaction_incarnation,
    pg_catalog.current_setting('transaction_isolation') AS isolation_level,
    pg_catalog.current_setting('transaction_read_only') AS read_only,
    pg_catalog.pg_backend_pid() AS backend_pid,
    pg_catalog.current_database() AS database_name,
    d.oid::bigint AS database_oid,
    CURRENT_USER AS effective_principal,
    effective_role.oid::bigint AS effective_principal_oid,
    SESSION_USER AS session_principal,
    session_role.oid::bigint AS session_principal_oid,
    pg_catalog.pg_is_in_recovery() AS in_recovery,
    n.oid::bigint AS namespace_oid,
    n.nspname AS schema_name,
    c.oid::bigint AS relation_oid,
    c.relname AS relation_name,
    lock_witness.lock_witness_count
FROM snapshot_witness AS s
JOIN pg_catalog.pg_database AS d
  ON d.datname = pg_catalog.current_database()
JOIN pg_catalog.pg_roles AS effective_role
  ON effective_role.rolname = CURRENT_USER
JOIN pg_catalog.pg_roles AS session_role
  ON session_role.rolname = SESSION_USER
LEFT JOIN pg_catalog.pg_namespace AS n ON n.nspname = %s
LEFT JOIN pg_catalog.pg_class AS c
  ON c.relnamespace = n.oid AND c.relname = %s
LEFT JOIN LATERAL (
    SELECT
        pg_catalog.count(*)::integer AS lock_witness_count,
        pg_catalog.min(l.virtualtransaction) AS transaction_incarnation
    FROM pg_catalog.pg_locks AS l
    WHERE l.pid = pg_catalog.pg_backend_pid()
      AND l.locktype = 'relation'
      AND l.relation = c.oid
      AND l.mode = 'AccessShareLock'
      AND l.granted
) AS lock_witness ON true"""

V1_PHYSICAL_IDENTITY_SQL = """SELECT
    (pg_catalog.pg_control_system()).system_identifier::text
        AS system_identifier,
    CASE
        WHEN pg_catalog.pg_is_in_recovery()
        THEN (pg_catalog.pg_control_checkpoint()).timeline_id::bigint
        ELSE ('x' || pg_catalog.substring(
            pg_catalog.pg_walfile_name(pg_catalog.pg_current_wal_lsn()),
            1,
            8
        ))::bit(32)::bigint
    END AS timeline_id"""

ACTIVE_SCOPE_REVALIDATION_SQL = """SELECT
    pg_catalog.txid_current_snapshot()::text AS snapshot_token,
    pg_catalog.min(l.virtualtransaction) AS transaction_incarnation,
    pg_catalog.count(*)::integer AS lock_witness_count
FROM pg_catalog.pg_locks AS l
WHERE l.pid = pg_catalog.pg_backend_pid()
  AND l.locktype = 'relation'
  AND l.relation = %s
  AND l.mode = 'AccessShareLock'
  AND l.granted"""

RELATION_PROFILE_SQL = """SELECT
    c.oid::bigint AS relation_oid,
    c.relnamespace::bigint AS namespace_oid,
    n.nspname AS schema_name,
    c.relname AS relation_name,
    c.relkind,
    c.relpersistence,
    c.relhassubclass
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE c.oid = %s AND c.relnamespace = %s"""

COLUMN_CATALOG_SQL = """SELECT
    c.oid::bigint AS relation_oid,
    c.relnamespace::bigint AS namespace_oid,
    c.relkind,
    c.relpersistence,
    c.relhassubclass,
    a.attnum::integer AS attribute_number,
    a.attname AS column_name,
    a.atttypid::bigint AS type_oid,
    t.typnamespace::bigint AS type_namespace_oid,
    tn.nspname AS type_namespace_name,
    t.typname AS type_name,
    t.typtype AS type_kind,
    a.atttypmod::integer AS type_modifier,
    NOT a.attnotnull AS nullable,
    a.attcollation::bigint AS collation_oid,
    a.attgenerated AS generated_kind,
    a.attidentity AS identity_kind
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid
JOIN pg_catalog.pg_type AS t ON t.oid = a.atttypid
JOIN pg_catalog.pg_namespace AS tn ON tn.oid = t.typnamespace
WHERE c.oid = %s
  AND c.relnamespace = %s
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum
LIMIT 1025"""

__all__ = (
    "ACTIVE_SCOPE_REVALIDATION_SQL",
    "COLUMN_CATALOG_SQL",
    "INITIAL_SNAPSHOT_WITNESS_SQL",
    "LOCK_RELATION_TEMPLATE",
    "RELATION_PROFILE_SQL",
    "SET_LOCK_TIMEOUT_SQL",
    "SET_TRANSACTION_SQL",
    "V1_PHYSICAL_IDENTITY_SQL",
)
