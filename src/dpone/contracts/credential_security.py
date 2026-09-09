"""Shared non-secret credential contract constants."""

from __future__ import annotations

from collections.abc import Mapping

FORBIDDEN_SECRET_KEYS = frozenset(
    {
        "access_key",
        "access_key_id",
        "client_secret",
        "jwt",
        "password",
        "private_key",
        "secret",
        "secret_id",
        "secret_key",
        "session_token",
        "token",
        "vault_path",
        "vault_token",
    }
)

_REFERENCE_MAPPING_KEYS = frozenset({"env_var_fields", "fields"})


def forbidden_inline_secret_paths(
    value: object,
    *,
    parent: str | None = None,
    path: tuple[str, ...] = (),
) -> tuple[str, ...]:
    """Return safe key paths whose values must never enter an artifact.

    ``fields`` and ``env_var_fields`` are declarative key mappings, so names
    such as ``password`` are references there rather than secret values.
    """

    found: list[str] = []
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key)
            normalized = key.lower()
            child_path = (*path, key)
            if parent not in _REFERENCE_MAPPING_KEYS and normalized in FORBIDDEN_SECRET_KEYS:
                found.append(".".join(child_path))
                continue
            found.extend(
                forbidden_inline_secret_paths(
                    child,
                    parent=normalized,
                    path=child_path,
                )
            )
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            found.extend(
                forbidden_inline_secret_paths(
                    child,
                    parent=parent,
                    path=(*path, str(index)),
                )
            )
    return tuple(sorted(set(found)))


__all__ = ["FORBIDDEN_SECRET_KEYS", "forbidden_inline_secret_paths"]
