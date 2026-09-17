"""Exact SQL collation tokens; lexical acceptance never proves availability."""

import re

PHYSICAL_COLLATION_PATTERN = r"[A-Za-z][A-Za-z0-9_]{0,127}"


def require_physical_collation_token(value: object) -> str:
    """Return the original ASCII name, excluding the database-default alias.

    No trimming, case conversion, quoting, catalog lookup or fallback occurs.
    Consumers must independently authenticate selection and observe availability.
    """
    if type(value) is not str or re.fullmatch(PHYSICAL_COLLATION_PATTERN, value) is None:
        raise ValueError(
            "physical_collation.name requires 1–128 ASCII letters, digits or underscores, starting with a letter"
        )
    if value.casefold() == "database_default":
        raise ValueError("physical_collation.name requires an explicit name, not database_default")
    return value
