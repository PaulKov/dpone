from __future__ import annotations

from dpone.kubernetes_names import KUBERNETES_DNS_LABEL_MAX_LENGTH, KUBERNETES_DNS_LABEL_PATTERN


def kubernetes_secret_name_schema(*, nullable: bool = False) -> dict[str, object]:
    schema: dict[str, object] = {
        "type": "string",
        "pattern": KUBERNETES_DNS_LABEL_PATTERN,
        "maxLength": KUBERNETES_DNS_LABEL_MAX_LENGTH,
    }
    if nullable:
        schema["type"] = ["string", "null"]
    return schema


__all__ = ["kubernetes_secret_name_schema"]
