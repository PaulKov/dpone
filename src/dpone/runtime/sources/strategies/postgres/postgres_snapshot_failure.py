"""Secondary-safe rollback and cleanup for PostgreSQL snapshot ownership."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def rollback_preserving_primary(connector: Any, primary: BaseException) -> None:
    """Attempt rollback without replacing a primary extraction failure."""

    try:
        connector.rollback()
    except BaseException as secondary:
        add_redacted_secondary_note(primary, "postgres_snapshot.rollback_failed", secondary)


def cleanup_preserving_primary(
    cleanup: Callable[[], object],
    primary: BaseException,
    *,
    code: str,
) -> None:
    """Attempt artifact cleanup and retain only a redacted failure type note."""

    try:
        cleanup()
    except BaseException as secondary:
        add_redacted_secondary_note(primary, code, secondary)


def add_redacted_secondary_note(
    primary: BaseException,
    code: str,
    secondary: BaseException,
) -> None:
    """Annotate with a stable type identity and never include exception text."""

    error_type = f"{type(secondary).__module__}.{type(secondary).__qualname__}"
    note = f"{code}:{error_type}"
    add_note = getattr(primary, "add_note", None)
    if callable(add_note):
        add_note(note)


def add_redacted_error_code_note(
    primary: BaseException,
    code: str,
    error_code: str | None,
) -> None:
    """Attach an already-redacted terminal error type from a receipt."""

    value = str(error_code or "unknown")
    safe_value = value if value.replace(".", "").replace("_", "").isalnum() else "unknown"
    add_note = getattr(primary, "add_note", None)
    if callable(add_note):
        add_note(f"{code}:{safe_value}")


__all__ = [
    "add_redacted_error_code_note",
    "add_redacted_secondary_note",
    "cleanup_preserving_primary",
    "rollback_preserving_primary",
]
