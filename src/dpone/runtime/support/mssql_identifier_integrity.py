"""Fail-closed helpers for SQL Server catalog identifier inspection."""

from __future__ import annotations

from collections.abc import Iterable


def require_case_unambiguous_identifiers(values: Iterable[object], *, error_code: str) -> tuple[str, ...]:
    """Return exact catalog spellings and reject case-variant aliases.

    SQL Server can host identifiers that differ only by case when a database
    uses a case-sensitive collation.  Safety contracts must therefore retain
    catalog spelling rather than folding names before matching constraints.
    """

    names = tuple(str(value or "") for value in values)
    spellings: dict[str, str] = {}
    for name in names:
        folded = name.casefold()
        previous = spellings.setdefault(folded, name)
        if not name or previous != name:
            raise RuntimeError(error_code)
    return names


__all__ = ["require_case_unambiguous_identifiers"]
