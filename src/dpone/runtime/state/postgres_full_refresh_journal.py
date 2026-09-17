# PostgreSQL authority for fenced bounded-full-refresh attempts. Every mutation
# uses one primary transaction. Invocation races use a unique digest; all later
# mutations require expected state, row version, and lease epoch. External
# effects are intentionally never claimed atomic with journal transactions.

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from dpone.runtime import full_refresh_attempt as attempts
from dpone.runtime.connectors.postgres import PostgresConnector

JOURNAL_DDL = """CREATE SCHEMA IF NOT EXISTS dpone_full_refresh_v1;
CREATE TABLE IF NOT EXISTS dpone_full_refresh_v1.invocation (invocation_key_sha256 text PRIMARY KEY, stable_fields_json jsonb NOT NULL, transfer_run_id uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(), attempt_nonce uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(), created_at timestamptz NOT NULL DEFAULT clock_timestamp(), CONSTRAINT invocation_distinct_ids CHECK (transfer_run_id <> attempt_nonce));
CREATE TABLE IF NOT EXISTS dpone_full_refresh_v1.attempt (attempt_id uuid PRIMARY KEY, invocation_key_sha256 text NOT NULL UNIQUE REFERENCES dpone_full_refresh_v1.invocation(invocation_key_sha256), state text NOT NULL, lease_epoch bigint NOT NULL CHECK (lease_epoch > 0), owner_id text NOT NULL, lease_until timestamptz NOT NULL, facts_json jsonb NOT NULL DEFAULT '{}'::jsonb, version bigint NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), terminal_at timestamptz NULL);
CREATE TABLE IF NOT EXISTS dpone_full_refresh_v1.resource (resource_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), attempt_id uuid NOT NULL REFERENCES dpone_full_refresh_v1.attempt(attempt_id), lease_epoch bigint NOT NULL, kind text NOT NULL, authority text NOT NULL, planned_name text NOT NULL, schema_hash text NOT NULL, exact_identity_json jsonb NULL, generation_hash text NULL, state text NOT NULL, expires_at timestamptz NULL, version bigint NOT NULL DEFAULT 0, UNIQUE (authority, planned_name));
CREATE TABLE IF NOT EXISTS dpone_full_refresh_v1.effect (effect_id uuid PRIMARY KEY DEFAULT gen_random_uuid(), attempt_id uuid NOT NULL REFERENCES dpone_full_refresh_v1.attempt(attempt_id), lease_epoch bigint NOT NULL, kind text NOT NULL, resource_id uuid NULL REFERENCES dpone_full_refresh_v1.resource(resource_id), external_query_id text NULL, credential_version text NULL, grant_until timestamptz NULL, state text NOT NULL, version bigint NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS dpone_full_refresh_v1.event (event_seq bigserial PRIMARY KEY, attempt_id uuid NOT NULL REFERENCES dpone_full_refresh_v1.attempt(attempt_id), db_recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(), payload jsonb NOT NULL);
"""

ADVANCE_ATTEMPT_SQL = """UPDATE dpone_full_refresh_v1.attempt
SET state = %s, facts_json = facts_json || %s::jsonb, version = version + 1, updated_at = clock_timestamp(), terminal_at = CASE WHEN %s THEN clock_timestamp() ELSE terminal_at END
WHERE attempt_id = %s AND state = %s AND lease_epoch = %s AND version = %s AND owner_id = %s AND lease_until > clock_timestamp() RETURNING *"""

ACQUIRE_EPOCH_SQL = """UPDATE dpone_full_refresh_v1.attempt AS candidate
SET lease_epoch = candidate.lease_epoch + 1, owner_id = %s, lease_until = clock_timestamp() + (%s * interval '1 second'), version = candidate.version + 1, updated_at = clock_timestamp()
WHERE candidate.attempt_id = %s AND candidate.state = %s AND candidate.owner_id = %s AND candidate.lease_epoch = %s AND candidate.version = %s AND candidate.lease_until <= clock_timestamp() AND NOT EXISTS (SELECT 1 FROM dpone_full_refresh_v1.effect AS effect WHERE effect.attempt_id = candidate.attempt_id AND effect.state IN ('granted', 'dispatched', 'terminated')) RETURNING candidate.*"""

BIND_RESOURCE_SQL = """UPDATE dpone_full_refresh_v1.resource
SET exact_identity_json = %s::jsonb, state = 'bound', version = version + 1
WHERE resource_id = %s AND attempt_id = %s AND lease_epoch = %s AND version = %s AND state = 'create_observed' AND exact_identity_json IS NULL AND EXISTS (SELECT 1 FROM dpone_full_refresh_v1.attempt AS attempt WHERE attempt.attempt_id = resource.attempt_id AND attempt.state = %s AND attempt.lease_epoch = %s AND attempt.version = %s AND attempt.owner_id = %s AND attempt.lease_until > clock_timestamp()) AND EXISTS (SELECT 1 FROM dpone_full_refresh_v1.effect AS effect WHERE effect.effect_id = %s AND effect.resource_id = resource.resource_id AND effect.kind = 'create' AND effect.attempt_id = resource.attempt_id AND effect.lease_epoch = resource.lease_epoch AND effect.version = %s AND effect.state = 'applied') RETURNING *"""

PLAN_CREATE_SQL = """WITH bumped AS (UPDATE dpone_full_refresh_v1.attempt SET version = version + 1, updated_at = clock_timestamp() WHERE attempt_id = %s AND state = %s AND lease_epoch = %s AND version = %s AND owner_id = %s AND lease_until > clock_timestamp() RETURNING attempt_id),
resource AS (INSERT INTO dpone_full_refresh_v1.resource (attempt_id, lease_epoch, kind, authority, planned_name, schema_hash, state) SELECT %s, %s, %s, %s, %s, %s, %s FROM bumped RETURNING resource_id),
effect AS (INSERT INTO dpone_full_refresh_v1.effect (attempt_id, lease_epoch, kind, resource_id, state) SELECT %s, %s, %s, resource_id, %s FROM resource RETURNING effect_id)
SELECT resource.resource_id, effect.effect_id FROM resource CROSS JOIN effect"""

ADVANCE_CREATE_SQL = """WITH updated_effect AS (UPDATE dpone_full_refresh_v1.effect SET state = %s, credential_version = COALESCE(%s, credential_version), grant_until = COALESCE(%s, grant_until), external_query_id = COALESCE(%s, external_query_id), version = version + 1 WHERE effect_id = %s AND resource_id = %s AND kind = 'create' AND attempt_id = %s AND lease_epoch = %s AND version = %s AND state = %s AND (%s::timestamptz IS NULL OR %s > clock_timestamp()) AND (%s <> 'dispatched' OR grant_until > clock_timestamp()) AND EXISTS (SELECT 1 FROM dpone_full_refresh_v1.attempt AS attempt WHERE attempt.attempt_id = effect.attempt_id AND attempt.state = %s AND attempt.lease_epoch = %s AND attempt.version = %s AND attempt.owner_id = %s AND attempt.lease_until > clock_timestamp()) RETURNING effect_id),
updated_resource AS (UPDATE dpone_full_refresh_v1.resource SET state = %s, version = version + 1 WHERE resource_id = %s AND attempt_id = %s AND lease_epoch = %s AND version = %s AND state = %s AND EXISTS (SELECT 1 FROM updated_effect) RETURNING resource_id)
SELECT updated_effect.effect_id FROM updated_effect CROSS JOIN updated_resource"""

RETRY_EFFECT_SQL = """WITH old_effect AS (SELECT 1 FROM dpone_full_refresh_v1.effect WHERE effect_id = %s AND resource_id = %s AND kind = %s AND attempt_id = %s AND lease_epoch = %s AND version = %s AND state IN ('not_applied', 'revoked') AND EXISTS (SELECT 1 FROM dpone_full_refresh_v1.attempt AS attempt WHERE attempt.attempt_id = effect.attempt_id AND attempt.state = %s AND attempt.lease_epoch = %s AND attempt.version = %s AND attempt.owner_id = %s AND attempt.lease_until > clock_timestamp()) FOR UPDATE),
updated_resource AS (UPDATE dpone_full_refresh_v1.resource SET state = %s, lease_epoch = %s, version = version + 1 WHERE resource_id = %s AND lease_epoch = %s AND version = %s AND state = %s AND EXISTS (SELECT 1 FROM old_effect) RETURNING resource_id),
new_effect AS (INSERT INTO dpone_full_refresh_v1.effect (attempt_id, lease_epoch, kind, resource_id, state) SELECT %s, %s, %s, resource_id, %s FROM updated_resource RETURNING effect_id)
SELECT effect_id FROM new_effect"""

RECOVER_CREATE_SQL = """WITH updated_effect AS (UPDATE dpone_full_refresh_v1.effect SET state = %s, version = version + 1 WHERE effect_id = %s AND resource_id = %s AND kind = 'create' AND attempt_id = %s AND lease_epoch = %s AND version = %s AND state = %s AND EXISTS (SELECT 1 FROM dpone_full_refresh_v1.attempt AS attempt WHERE attempt.attempt_id = effect.attempt_id AND attempt.state = %s AND attempt.lease_epoch = %s AND attempt.version = %s AND attempt.owner_id = %s AND attempt.lease_until <= clock_timestamp()) RETURNING effect_id),
updated_resource AS (UPDATE dpone_full_refresh_v1.resource SET state = %s, version = version + 1 WHERE resource_id = %s AND attempt_id = %s AND lease_epoch = %s AND version = %s AND state = %s AND EXISTS (SELECT 1 FROM updated_effect) RETURNING resource_id)
SELECT updated_effect.effect_id FROM updated_effect CROSS JOIN updated_resource"""


class FullRefreshJournalError(RuntimeError):
    # Base error for durable full-refresh journal invariants.
    pass


class InvocationConflictError(FullRefreshJournalError):
    # A digest exists with different canonical stable fields.
    pass


class ConcurrentJournalUpdateError(FullRefreshJournalError):
    # Expected state, epoch, version, owner, or lease no longer matches.
    pass


JournalRow = Mapping[str, Any]


class PostgresFullRefreshJournal:
    # Create-once and CAS journal backed only by PostgreSQL primary state.

    def __init__(self, connector: PostgresConnector) -> None:
        self._connector = connector
        self._initialized = False

    # Idempotently install the private operational schema.
    def initialize(self) -> None:
        if self._initialized:
            return
        self._connector.execute_query(JOURNAL_DDL)
        self._initialized = True

    # Atomically create or return the one mapping for a scheduler key.
    def create_or_get(
        self,
        invocation: attempts.InvocationKey,
        *,
        owner_id: str,
        lease_seconds: int,
    ) -> attempts.AttemptRecord:
        if not owner_id.strip():
            raise ValueError("owner_id must be non-empty")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        self.initialize()
        stable_json = _json(invocation.canonical_fields())
        with self._connector.connection.transaction(), self._connector.connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO dpone_full_refresh_v1.invocation (invocation_key_sha256, stable_fields_json) "
                "VALUES (%s, %s::jsonb) ON CONFLICT (invocation_key_sha256) DO NOTHING",
                (invocation.digest, stable_json),
            )
            cursor.execute(
                "SELECT * FROM dpone_full_refresh_v1.invocation WHERE invocation_key_sha256 = %s FOR UPDATE",
                (invocation.digest,),
            )
            invocation_row = self._required(cursor, "invocation create-once readback failed", FullRefreshJournalError)
            stored_fields = invocation_row["stable_fields_json"]
            if (
                json.loads(stored_fields) if isinstance(stored_fields, str) else stored_fields
            ) != invocation.canonical_fields():
                raise InvocationConflictError("invocation digest maps to different stable fields")
            statement = "INSERT INTO dpone_full_refresh_v1.attempt (attempt_id, invocation_key_sha256, state, lease_epoch, owner_id, lease_until) VALUES (%s, %s, %s, 1, %s, clock_timestamp() + (%s * interval '1 second')) ON CONFLICT (invocation_key_sha256) DO NOTHING"
            params = (invocation_row["transfer_run_id"], invocation.digest, attempts.AttemptState.PLANNED.value)
            cursor.execute(statement, params + (owner_id, lease_seconds))
            row = self._select_attempt(cursor, invocation.digest)
            record = attempts.AttemptRecord.from_mappings(row, invocation, invocation_row)
            if record.state.terminal:
                raise FullRefreshJournalError("terminal invocation key cannot be recreated")
            if record.owner_id != owner_id or not row["lease_active"]:
                raise ConcurrentJournalUpdateError("existing invocation requires explicit epoch acquisition")
            return record

    # Read current durable state for an exact attempt identity.
    def read(self, identity: attempts.AttemptIdentity) -> attempts.AttemptRecord:
        self.initialize()
        with self._connector.connection.cursor() as cursor:
            row, invocation_row = self._select_identity(cursor, identity)
        return attempts.AttemptRecord.from_mappings(row, identity.invocation, invocation_row)

    # Advance only for the exact prior state/version/epoch.
    def compare_and_advance(
        self,
        expected: attempts.AttemptRecord,
        *,
        expected_owner_id: str,
        next_state: attempts.AttemptState,
        facts: Mapping[str, Any],
    ) -> attempts.AttemptRecord:
        attempts.assert_attempt_transition(expected.state, next_state)
        attempts.assert_journal_safe(facts)
        self.initialize()
        with self._connector.connection.transaction(), self._connector.connection.cursor() as cursor:
            self._lock_attempt(cursor, expected, expected_owner_id)
            params: tuple[Any, ...] = (
                next_state.value,
                _json(facts),
                next_state.terminal,
                expected.identity.attempt_id,
            )
            params += (expected.state.value, expected.lease_epoch, expected.version, expected_owner_id)
            cursor.execute(ADVANCE_ATTEMPT_SQL, params)
            row = self._required(cursor, "attempt state/version/epoch CAS rejected")
            self._append_event(cursor, expected.identity.attempt_id, {"state": next_state.value, "facts": facts})
            invocation_row = self._select_invocation(cursor, expected.identity.invocation.digest)
            return attempts.AttemptRecord.from_mappings(row, expected.identity.invocation, invocation_row)

    def acquire_next_epoch(
        self,
        expected: attempts.AttemptRecord,
        *,
        expected_owner_id: str,
        new_owner_id: str,
        lease_seconds: int,
    ) -> attempts.AttemptRecord:
        self.initialize()
        if not new_owner_id.strip() or lease_seconds < 1:
            raise ValueError("new_owner_id and positive lease_seconds are required")
        with self._connector.connection.transaction(), self._connector.connection.cursor() as cursor:
            self._lock_attempt(cursor, expected, expected_owner_id, active=False)
            params: tuple[Any, ...] = (new_owner_id, lease_seconds, expected.identity.attempt_id, expected.state.value)
            params += (expected_owner_id, expected.lease_epoch, expected.version)
            cursor.execute(ACQUIRE_EPOCH_SQL, params)
            row = self._required(cursor, "lease takeover CAS rejected")
            self._append_event(cursor, expected.identity.attempt_id, {"lease_epoch": int(row["lease_epoch"])})
            invocation_row = self._select_invocation(cursor, expected.identity.invocation.digest)
            return attempts.AttemptRecord.from_mappings(row, expected.identity.invocation, invocation_row)

    def plan_create(
        self,
        expected: attempts.AttemptRecord,
        *,
        expected_owner_id: str,
        resource_kind: attempts.ResourceKind,
        authority: str,
        planned_name: str,
        schema_hash: str,
    ) -> attempts.PlannedCreate:
        self.initialize()
        with self._connector.connection.transaction(), self._connector.connection.cursor() as cursor:
            self._lock_attempt(cursor, expected, expected_owner_id)
            attempt_id = expected.identity.attempt_id
            params: tuple[Any, ...] = (attempt_id, expected.state.value, expected.lease_epoch)
            params += (expected.version, expected_owner_id)
            params += (attempt_id, expected.lease_epoch, resource_kind.value, authority, planned_name, schema_hash)
            params += (attempts.ResourceState.CREATE_PLANNED.value, attempt_id, expected.lease_epoch)
            params += (attempts.EffectKind.CREATE.value, attempts.EffectState.PLANNED.value)
            cursor.execute(PLAN_CREATE_SQL, params)
            row = self._required(cursor, "create planning transaction CAS rejected")
            self._append_event(cursor, expected.identity.attempt_id, {"resource_id": str(row["resource_id"])})
            resource = attempts.PlannedResource(
                resource_id=row["resource_id"],
                authority=authority,
                planned_name=planned_name,
                resource_kind=resource_kind,
                schema_hash=schema_hash,
            )
            return attempts.PlannedCreate(
                resource, row["effect_id"], expected.with_version(expected.version + 1), expected.lease_epoch
            )

    def retry_not_applied(
        self,
        planned: attempts.PlannedCreate,
        *,
        effect_kind: attempts.EffectKind,
        expected_owner_id: str,
    ) -> attempts.PlannedCreate:
        states = {
            attempts.EffectKind.CREATE: (
                attempts.ResourceState.CREATE_NOT_APPLIED,
                attempts.ResourceState.CREATE_PLANNED,
            ),
            attempts.EffectKind.RENAME: (
                attempts.ResourceState.RENAME_NOT_APPLIED,
                attempts.ResourceState.RENAME_PLANNED,
            ),
        }
        if effect_kind not in states:
            raise ValueError("only CREATE and RENAME not-applied effects are retryable")
        previous, retry_state = states[effect_kind]
        with self._connector.connection.transaction(), self._connector.connection.cursor() as cursor:
            self._lock_attempt(cursor, planned.attempt, expected_owner_id)
            params: tuple[Any, ...] = (planned.effect_id, planned.resource.resource_id, effect_kind.value)
            params += (planned.attempt.identity.attempt_id, planned.fence_epoch, planned.effect_version)
            params += (planned.attempt.state.value, planned.attempt.lease_epoch, planned.attempt.version)
            params += (expected_owner_id,)
            params += (retry_state.value, planned.attempt.lease_epoch, planned.resource.resource_id)
            params += (planned.fence_epoch, planned.resource_version, previous.value)
            params += (planned.attempt.identity.attempt_id, planned.attempt.lease_epoch)
            params += (effect_kind.value, attempts.EffectState.PLANNED.value)
            cursor.execute(RETRY_EFFECT_SQL, params)
            row = self._required(cursor, "not-applied effect retry CAS rejected")
        resource_version = planned.resource_version + 1
        return attempts.PlannedCreate(
            planned.resource, row["effect_id"], planned.attempt, planned.attempt.lease_epoch, resource_version
        )

    def compare_and_advance_create(
        self,
        planned: attempts.PlannedCreate,
        *,
        transition: Any,
        expected_owner_id: str,
    ) -> attempts.PlannedCreate:
        credential_ref = transition.validate(planned)
        with self._connector.connection.transaction(), self._connector.connection.cursor() as cursor:
            self._lock_attempt(cursor, planned.attempt, expected_owner_id)
            if transition.grant_until is not None:
                cursor.execute("SELECT %s > clock_timestamp() AS valid", (transition.grant_until,))
                if not cursor.fetchone()["valid"]:
                    raise ValueError("grant_until must be strictly future according to PostgreSQL")
            params: tuple[Any, ...] = (transition.next_effect_state.value, transition.credential_version_ref)
            params += (transition.grant_until, transition.external_query_id)
            attempt_id = planned.attempt.identity.attempt_id
            params += (planned.effect_id, planned.resource.resource_id, attempt_id, planned.attempt.lease_epoch)
            params += (planned.effect_version, transition.expected_effect_state.value)
            params += (transition.grant_until, transition.grant_until, transition.next_effect_state.value)
            params += (planned.attempt.state.value,)
            params += (planned.attempt.lease_epoch, planned.attempt.version, expected_owner_id)
            params += (transition.next_resource_state.value, planned.resource.resource_id)
            params += (attempt_id, planned.attempt.lease_epoch, planned.resource_version)
            params += (transition.expected_resource_state.value,)
            cursor.execute(ADVANCE_CREATE_SQL, params)
            if cursor.fetchone() is None:
                raise ConcurrentJournalUpdateError("paired CREATE resource/effect CAS rejected")
        return planned.advanced(credential_ref)

    def reconcile_expired_create(
        self, planned: attempts.PlannedCreate, *, receipt: Any, expected_owner_id: str
    ) -> attempts.PlannedCreate:
        if not receipt.proves(planned):
            raise ValueError("collector receipt does not prove the exact attempt/effect/resource/epoch")
        attempts.require_opaque_reference(receipt.authority_receipt_ref, field_name="authority_receipt_ref")
        with self._connector.connection.transaction(), self._connector.connection.cursor() as cursor:
            self._lock_attempt(cursor, planned.attempt, expected_owner_id, active=False)
            params: tuple[Any, ...] = (receipt.next_effect_state.value, planned.effect_id)
            params += (planned.resource.resource_id, planned.attempt.identity.attempt_id, planned.attempt.lease_epoch)
            params += (planned.effect_version, receipt.expected_effect_state.value, planned.attempt.state.value)
            params += (planned.attempt.lease_epoch, planned.attempt.version, expected_owner_id)
            params += (receipt.next_resource_state.value, planned.resource.resource_id)
            params += (planned.attempt.identity.attempt_id, planned.attempt.lease_epoch, planned.resource_version)
            params += (receipt.expected_resource_state.value,)
            cursor.execute(RECOVER_CREATE_SQL, params)
            self._required(cursor, "expired CREATE recovery CAS rejected")
            payload = {"effect_id": str(planned.effect_id), "recovery_receipt_ref": receipt.authority_receipt_ref}
            self._append_event(cursor, planned.attempt.identity.attempt_id, payload)
        return planned.advanced()

    def bind_resource(
        self,
        planned: attempts.PlannedCreate,
        *,
        readback: attempts.AuthenticatedResourceReadback,
        expected_owner_id: str,
    ) -> attempts.PlannedCreate:
        if not readback.proves(
            planned.resource,
            effect_id=planned.effect_id,
            credential_version_ref=planned.credential_version_ref or "",
        ):
            raise ValueError("authenticated CREATE readback does not match planned resource and grant")
        attempts.assert_journal_safe(readback.exact_identity)
        with self._connector.connection.transaction(), self._connector.connection.cursor() as cursor:
            self._lock_attempt(cursor, planned.attempt, expected_owner_id)
            params: tuple[Any, ...] = (_json(readback.exact_identity), planned.resource.resource_id)
            params += (planned.attempt.identity.attempt_id, planned.attempt.lease_epoch, planned.resource_version)
            params += (planned.attempt.state.value, planned.attempt.lease_epoch)
            params += (planned.attempt.version, expected_owner_id)
            params += (planned.effect_id, planned.effect_version)
            cursor.execute(BIND_RESOURCE_SQL, params)
            if cursor.fetchone() is None:
                raise ConcurrentJournalUpdateError("resource identity bind CAS rejected")
            payload = {
                "resource_id": str(planned.resource.resource_id),
                "server_receipt_ref": readback.server_receipt_ref,
            }
            payload["state"] = attempts.ResourceState.BOUND.value
            self._append_event(cursor, planned.attempt.identity.attempt_id, payload)
        return planned.advanced(effect=False)

    @staticmethod
    def _append_event(cursor: Any, attempt_id: UUID, payload: Mapping[str, Any]) -> None:
        cursor.execute(
            "INSERT INTO dpone_full_refresh_v1.event (attempt_id, payload) VALUES (%s, %s::jsonb)",
            (attempt_id, _json(payload)),
        )

    @staticmethod
    def _required(
        cursor: Any, message: str, error: type[FullRefreshJournalError] = ConcurrentJournalUpdateError
    ) -> JournalRow:
        row = cursor.fetchone()
        if row is None:
            raise error(message)
        return row

    @staticmethod
    def _lock_attempt(cursor: Any, expected: attempts.AttemptRecord, owner_id: str, active: bool = True) -> JournalRow:
        if expected.state.terminal:
            raise ConcurrentJournalUpdateError("terminal attempt mutation rejected")
        lease_predicate = ">" if active else "<="
        cursor.execute(
            "SELECT * FROM dpone_full_refresh_v1.attempt WHERE attempt_id = %s AND state = %s "
            f"AND lease_epoch = %s AND version = %s AND owner_id = %s AND lease_until {lease_predicate} "
            "clock_timestamp() FOR UPDATE",
            (
                expected.identity.attempt_id,
                expected.state.value,
                expected.lease_epoch,
                expected.version,
                owner_id,
            ),
        )
        return PostgresFullRefreshJournal._required(cursor, "attempt owner/state/version/epoch/lease lock rejected")

    @staticmethod
    def _select_attempt(cursor: Any, digest: str) -> JournalRow:
        cursor.execute(
            "SELECT *, lease_until > clock_timestamp() AS lease_active "
            "FROM dpone_full_refresh_v1.attempt WHERE invocation_key_sha256 = %s FOR UPDATE",
            (digest,),
        )
        return PostgresFullRefreshJournal._required(cursor, "attempt readback failed", FullRefreshJournalError)

    @staticmethod
    def _select_invocation(cursor: Any, digest: str) -> JournalRow:
        cursor.execute(
            "SELECT * FROM dpone_full_refresh_v1.invocation WHERE invocation_key_sha256 = %s",
            (digest,),
        )
        return PostgresFullRefreshJournal._required(cursor, "invocation readback failed", FullRefreshJournalError)

    def _select_identity(self, cursor: Any, identity: attempts.AttemptIdentity) -> tuple[JournalRow, JournalRow]:
        invocation_row = self._select_invocation(cursor, identity.invocation.digest)
        if (
            invocation_row["transfer_run_id"] != identity.transfer_run_id
            or invocation_row["attempt_nonce"] != identity.attempt_nonce
        ):
            raise InvocationConflictError("attempt UUIDs do not match invocation mapping")
        return self._select_attempt(cursor, identity.invocation.digest), invocation_row


def _json(value: object) -> str:
    attempts.assert_journal_safe(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
