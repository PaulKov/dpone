from __future__ import annotations

import re


def safe_identifier(name: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", name.strip().lower()).strip("_")
    if not normalized:
        raise ValueError("Connector name must contain at least one alphanumeric character")
    if normalized[0].isdigit():
        normalized = f"connector_{normalized}"
    return normalized


def class_name(connector: str) -> str:
    return "".join(part.capitalize() for part in connector.split("_"))
