"""Canonical monotonic target-fence trigger body."""

from __future__ import annotations


def render_fence_trigger_body() -> str:
    """Allow only one-step authority transfer; reject deletes and rewinds."""

    return """BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM deleted AS d LEFT JOIN inserted AS i
               ON i.target_identity = d.target_identity WHERE i.target_identity IS NULL)
        THROW 51000, 'DPONE_TARGET_FENCE_IMMUTABLE', 1;
    IF EXISTS (
        SELECT 1 FROM inserted AS i INNER JOIN deleted AS d
            ON i.target_identity = d.target_identity
        WHERE i.current_generation <> d.current_generation + 1
           OR i.current_attempt_key = d.current_attempt_key
    )
        THROW 51000, 'DPONE_TARGET_FENCE_GENERATION_INVALID', 1;
END;"""


__all__ = ["render_fence_trigger_body"]
