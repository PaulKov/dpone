"""Canonical mutable-operation fence trigger body."""

from __future__ import annotations

import re


def render_operation_trigger_body() -> str:
    """Allow heartbeat/reacquire only; keep operation identity immutable."""

    return """BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM deleted AS d LEFT JOIN inserted AS i
               ON i.operation_key = d.operation_key WHERE i.operation_key IS NULL)
        THROW 51000, 'DPONE_LOAD_OPERATION_IMMUTABLE', 1;
    IF EXISTS (
        SELECT 1 FROM inserted AS i INNER JOIN deleted AS d
            ON i.operation_key = d.operation_key
        WHERE i.attempt_key <> d.attempt_key OR i.scope_hash <> d.scope_hash
    )
        THROW 51000, 'DPONE_LOAD_OPERATION_IMMUTABLE', 1;
    IF EXISTS (
        SELECT 1 FROM inserted AS i INNER JOIN deleted AS d
            ON i.operation_key = d.operation_key
        WHERE NOT (
            (i.current_epoch = d.current_epoch
             AND i.current_owner_digest = d.current_owner_digest
             AND ((d.lease_expires_at_utc IS NULL AND i.lease_expires_at_utc IS NULL)
                  OR (d.lease_expires_at_utc IS NOT NULL
                      AND i.lease_expires_at_utc IS NOT NULL
                      AND d.lease_expires_at_utc > SYSUTCDATETIME()
                      AND i.lease_expires_at_utc >= d.lease_expires_at_utc
                      AND i.lease_expires_at_utc > SYSUTCDATETIME())))
            OR
            (i.current_epoch = d.current_epoch + 1
             AND i.current_owner_digest <> d.current_owner_digest)
        )
    )
        THROW 51000, 'DPONE_LOAD_OPERATION_EPOCH_INVALID', 1;
    IF EXISTS (
        SELECT 1 FROM inserted AS i INNER JOIN deleted AS d
            ON i.operation_key = d.operation_key
        WHERE i.current_owner_digest <> d.current_owner_digest
          AND (d.lease_expires_at_utc IS NULL
               OR d.lease_expires_at_utc > SYSUTCDATETIME()
               OR i.lease_expires_at_utc IS NULL
               OR i.lease_expires_at_utc <= SYSUTCDATETIME())
    )
        THROW 51000, 'DPONE_LOAD_OPERATION_ALREADY_OWNED', 1;
END;"""


def canonical_trigger_body(definition: str) -> str:
    """Return the exact body after the final CREATE TRIGGER ``AS`` token."""

    match = re.search(r"\bAS\s+(BEGIN\b)", definition, flags=re.IGNORECASE)
    body = definition[match.start(1) :] if match else definition
    return re.sub(r"\s+", "", body).rstrip(";").upper()


__all__ = ["canonical_trigger_body", "render_operation_trigger_body"]
