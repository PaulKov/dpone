"""Writer observations and receipts for target-local native import."""

from __future__ import annotations

from typing import Any


def writer_observation(
    outcome: str,
    consumed: int | None,
    *,
    quiescence: str = "unverified",
    row_count: int | None = None,
    limbs: list[str] | None = None,
    diagnostic: str = "mssql_native.writer_observed",
) -> dict[str, Any]:
    """Create the closed writer observation projected into the durable journal."""

    return dict(
        writer_outcome=outcome,
        input_rows_consumed=consumed,
        row_count=row_count,
        count_overflow=False if row_count is not None else None,
        limbs=limbs,
        quiescence=quiescence,
        diagnostic_code=diagnostic,
    )


def aggregate_limbs(digest: Any, aggregate: tuple[Any, ...]) -> list[str]:
    """Project exactly eight hash limbs from either approved aggregate layout."""

    offset = 3 if getattr(digest, "mutation_watermark", None) is not None else 2
    if len(aggregate) != offset + 8:
        raise ValueError("mssql_native.target_digest_row_shape")
    return [str(value) for value in aggregate[offset:]]


def verified_receipt(
    importer: Any,
    plan: Any,
    file: Any,
    attempt_id: str,
    object_id: int,
    digest: Any,
    artifact: Any,
) -> Any:
    """Bind an importer receipt to the optional persisted mutation watermark."""

    watermark = getattr(digest, "mutation_watermark", None)
    if watermark is None:
        return importer._receipt(plan, file, attempt_id, object_id, digest.typed_sum, artifact)
    return importer._receipt(
        plan,
        file,
        attempt_id,
        object_id,
        digest.typed_sum,
        artifact,
        mutation_watermark=watermark,
    )


__all__ = ["aggregate_limbs", "verified_receipt", "writer_observation"]
