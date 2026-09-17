from __future__ import annotations

from dpone.runtime.state.postgres_full_refresh_journal import JOURNAL_DDL


def test_schema_defines_create_once_and_cas_constraints() -> None:
    normalized = " ".join(JOURNAL_DDL.lower().split())

    assert "create schema if not exists dpone_full_refresh_v1" in normalized
    assert "invocation_key_sha256 text primary key" in normalized
    assert "unique (authority, planned_name)" in normalized
    assert "exact_identity_json jsonb null" in normalized
    assert "credential_version text null" in normalized
    assert "update dpone_full_refresh_v1.attempt" not in normalized
    assert "password" not in normalized
    assert "secret" not in normalized


def test_sql_uses_database_clock_and_expected_epoch_version_cas() -> None:
    from dpone.runtime.state.postgres_full_refresh_journal import (
        ACQUIRE_EPOCH_SQL,
        ADVANCE_ATTEMPT_SQL,
        ADVANCE_CREATE_SQL,
        BIND_RESOURCE_SQL,
        PLAN_CREATE_SQL,
    )

    for statement in (ACQUIRE_EPOCH_SQL, ADVANCE_ATTEMPT_SQL, BIND_RESOURCE_SQL, ADVANCE_CREATE_SQL):
        normalized = " ".join(statement.lower().split())
        assert "lease_epoch = %s" in normalized
        assert "version = %s" in normalized
    assert "clock_timestamp()" in ACQUIRE_EPOCH_SQL.lower()
    assert "owner_id = %s" in ACQUIRE_EPOCH_SQL.lower()
    assert "effect.state in ('granted', 'dispatched', 'terminated')" in " ".join(ACQUIRE_EPOCH_SQL.lower().split())
    assert "update dpone_full_refresh_v1.attempt" in PLAN_CREATE_SQL.lower()
    assert "updated_effect" in ADVANCE_CREATE_SQL and "updated_resource" in ADVANCE_CREATE_SQL
