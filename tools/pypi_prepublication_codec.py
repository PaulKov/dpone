"""Strict JSON codec shared by PyPI prepublication sources."""

from __future__ import annotations

import json
from typing import Any

if __package__:
    from .pypi_prepublication_contract import PrepublicationGateError, fail
else:
    from pypi_prepublication_contract import PrepublicationGateError, fail


def _closed_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise fail("PYPI_PREPUBLICATION_JSON_DUPLICATE_KEY")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise fail("PYPI_PREPUBLICATION_JSON_NONFINITE_NUMBER")


def decode_json(raw: bytes, *, invalid_code: str) -> Any:
    """Decode bounded UTF-8 JSON while rejecting duplicates and non-finite values."""

    try:
        return json.loads(
            raw.decode("utf-8", "strict"),
            object_pairs_hook=_closed_object,
            parse_constant=_reject_constant,
        )
    except PrepublicationGateError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise fail(invalid_code) from exc
