"""Dependency-free Kubernetes naming helpers."""

from __future__ import annotations

import re

KUBERNETES_DNS_LABEL_MAX_LENGTH = 63
KUBERNETES_DNS_LABEL_PATTERN = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"

_DNS_LABEL_RE = re.compile(KUBERNETES_DNS_LABEL_PATTERN)


def is_valid_kubernetes_dns_label(value: object) -> bool:
    """Return True when value is a safe Kubernetes DNS label."""

    if not isinstance(value, str):
        return False
    return (
        bool(value)
        and value == value.strip()
        and len(value) <= KUBERNETES_DNS_LABEL_MAX_LENGTH
        and bool(_DNS_LABEL_RE.fullmatch(value))
    )


def require_kubernetes_dns_label(value: object, *, context: str) -> str:
    """Return a normalized DNS label or raise without echoing unsafe input."""

    if not is_valid_kubernetes_dns_label(value):
        raise ValueError(f"{context} must be a safe Kubernetes DNS label")
    assert isinstance(value, str)
    return value


__all__ = [
    "KUBERNETES_DNS_LABEL_MAX_LENGTH",
    "KUBERNETES_DNS_LABEL_PATTERN",
    "is_valid_kubernetes_dns_label",
    "require_kubernetes_dns_label",
]
