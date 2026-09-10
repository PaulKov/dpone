"""Dependency-free Kubernetes resource validation shared by build and provider.

The authoring surface is deliberately smaller than Kubernetes' device resource
API. Existing provider v1 device names retain their bounded-string contract.
Supported built-in quantities are preserved verbatim and compared using exact
decimal arithmetic; no Kubernetes or Airflow SDK is imported at parse time.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from decimal import Decimal, localcontext
from typing import Any

RESOURCE_NAMES = frozenset({"cpu", "memory", "ephemeral-storage"})
RESOURCE_SECTIONS = frozenset({"requests", "limits"})
QUANTITY_PATTERN = r"\+?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+|[numkMGTPE]|[KMGTPE]i)?"
_QUANTITY = re.compile(rf"({QUANTITY_PATTERN})\Z")
_PARTS = re.compile(r"(\+?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))(.*)\Z")
_DECIMAL_EXPONENTS = {"n": -9, "u": -6, "m": -3, "": 0, "k": 3, "M": 6, "G": 9, "T": 12, "P": 15, "E": 18}
_MAX_QUANTITY = Decimal(2**63 - 1)


class KubernetesResourceError(ValueError):
    """A bounded authoring or wire field is invalid; values are never echoed."""

    code = "DPONE_AIRFLOW_RESOURCES_INVALID"


def validate_kubernetes_resources(
    value: object, *, field: str, allow_extended: bool = False
) -> dict[str, dict[str, str]]:
    """Validate a resource declaration without inserting requests or limits.

    Missing sections stay missing. Kubernetes admission owns limit-to-request
    defaulting and namespace policies. ``allow_extended`` preserves the prior
    provider v1 structural contract for resource names outside the author API.
    """

    if not isinstance(value, Mapping) or not value:
        raise _invalid(field, "must contain a nonempty requests or limits object")
    if set(value) - RESOURCE_SECTIONS:
        raise _invalid(field, "supports only requests and limits")
    result: dict[str, dict[str, str]] = {}
    parsed: dict[str, dict[str, Decimal]] = {}
    for section_name, section in value.items():
        section_field = f"{field}.{section_name}"
        if not isinstance(section, Mapping) or not section or len(section) > 64:
            raise _invalid(section_field, "must be a nonempty bounded resource object")
        result[section_name] = {}
        parsed[section_name] = {}
        for name, quantity in section.items():
            if not _bounded_text(name, 253):
                raise _invalid(section_field, "resource names must be bounded strings")
            if name not in RESOURCE_NAMES and not allow_extended:
                raise _invalid(f"{section_field}.{name}", "supports only cpu, memory and ephemeral-storage")
            quantity_field = f"{section_field}.{name}"
            if not _bounded_text(quantity, 64):
                raise _invalid(quantity_field, "must be a quoted Kubernetes quantity (at most 64 characters)")
            if name in RESOURCE_NAMES:
                parsed[section_name][name] = _quantity(quantity, resource=name, field=quantity_field)
            result[section_name][name] = quantity
    for name, request in parsed.get("requests", {}).items():
        limit = parsed.get("limits", {}).get(name)
        if limit is not None and request > limit:
            raise _invalid(f"{field}.requests.{name}", f"must not exceed {field}.limits.{name}")
    return result


def kubernetes_resources_schema() -> dict[str, Any]:
    """JSON shape for the authoring surface; cross-field checks run in Python."""

    quantity = {"type": "string", "maxLength": 64, "pattern": rf"^{QUANTITY_PATTERN}$"}
    section = {
        "type": "object",
        "minProperties": 1,
        "additionalProperties": False,
        "properties": {name: dict(quantity) for name in sorted(RESOURCE_NAMES)},
    }
    return {
        "type": "object",
        "minProperties": 1,
        "additionalProperties": False,
        "properties": {name: dict(section) for name in sorted(RESOURCE_SECTIONS)},
        "description": "Base-container resources for runtime and separate hooks. Quote quantities; requests must not exceed limits. CPU precision is 1m; magnitude is at most 2^63-1 and precision at most 1n. Missing sections retain Kubernetes admission defaults.",
    }


def _quantity(value: str, *, resource: str, field: str) -> Decimal:
    if _QUANTITY.fullmatch(value) is None:
        raise _invalid(field, "must be a nonnegative Kubernetes quantity, for example 250m, 1Gi or 2G")
    match = _PARTS.fullmatch(value)
    assert match is not None
    number, suffix = match.groups()
    with localcontext() as context:
        context.prec = 128
        if suffix.endswith("i"):
            factor = Decimal(1024) ** ("KMGTPE".index(suffix[0]) + 1)
        else:
            exponent = _DECIMAL_EXPONENTS.get(suffix)
            if exponent is None:
                exponent = int(suffix[1:])
            if abs(exponent) > 128:
                raise _invalid(field, "quantity exponent is outside supported bounds")
            factor = Decimal(10) ** exponent
        result = Decimal(number) * factor
        if result > _MAX_QUANTITY or result % Decimal("1e-9"):
            raise _invalid(field, "quantity must be at most 2^63-1 with precision no finer than 1n")
        if resource == "cpu" and result % Decimal("0.001"):
            raise _invalid(field, "CPU precision must be no finer than 1m (0.001 CPU)")
        return result


def _bounded_text(value: object, maximum: int) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= maximum
        and value == value.strip()
        and not any(ord(character) < 32 for character in value)
    )


def _invalid(field: str, message: str) -> KubernetesResourceError:
    return KubernetesResourceError(f"{field} {message}; configure workload airflow.resources.")
