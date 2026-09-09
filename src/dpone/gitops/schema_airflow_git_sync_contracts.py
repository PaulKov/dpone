from __future__ import annotations

from typing import Any


def airflow_git_sync_schema() -> dict[str, Any]:
    return _object_schema(
        required=(
            "enabled",
            "repo",
            "ref",
            "image",
            "root",
            "link",
            "worktree_path",
            "sparse_checkout_file",
            "clone",
            "auth",
            "sparse_paths",
        ),
        properties={
            "enabled": _boolean_schema(),
            "repo": _string_schema(),
            "ref": _string_schema(),
            "image": _string_schema(),
            "root": _string_schema(),
            "link": _string_schema(),
            "worktree_path": _string_schema(),
            "sparse_checkout_file": _string_schema(),
            "clone": airflow_git_sync_clone_schema(),
            "auth": airflow_git_sync_auth_schema(),
            "sparse_paths": _array_schema(airflow_git_sync_sparse_path_schema()),
        },
    )


def airflow_git_sync_clone_schema() -> dict[str, Any]:
    return _object_schema(
        required=("depth", "filter"),
        properties={
            "depth": _integer_schema(),
            "filter": {"type": ["string", "null"], "enum": ["blob:none", "tree:0", None]},
        },
    )


def airflow_git_sync_auth_schema() -> dict[str, Any]:
    return _object_schema(
        required=("mode",),
        properties={
            "mode": _string_schema(),
            "ssh_secret": airflow_git_sync_ssh_secret_schema(),
            "https_secret": airflow_git_sync_https_secret_schema(),
        },
    )


def airflow_git_sync_ssh_secret_schema() -> dict[str, Any]:
    return _object_schema(
        required=("name", "ssh_key", "known_hosts_key"),
        properties={
            "name": _string_schema(),
            "ssh_key": _string_schema(),
            "known_hosts_key": _string_schema(),
        },
    )


def airflow_git_sync_https_secret_schema() -> dict[str, Any]:
    return _object_schema(
        required=("name", "username_key", "password_key"),
        properties={
            "name": _string_schema(),
            "username_key": _string_schema(),
            "password_key": _string_schema(),
        },
    )


def airflow_git_sync_sparse_path_schema() -> dict[str, Any]:
    return _object_schema(
        required=("path", "kind", "source", "required", "exists", "is_dir", "reason"),
        properties={
            "path": _string_schema(),
            "kind": _string_schema(),
            "source": _string_schema(),
            "required": _boolean_schema(),
            "exists": _boolean_schema(),
            "is_dir": _boolean_schema(),
            "reason": _string_schema(),
        },
    )


def _object_schema(*, required: tuple[str, ...] = (), properties: dict[str, Any] | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "object", "additionalProperties": True}
    if required:
        schema["required"] = list(required)
    if properties is not None:
        schema["properties"] = properties
    return schema


def _array_schema(items: dict[str, Any]) -> dict[str, Any]:
    return {"type": "array", "items": items}


def _string_schema() -> dict[str, str]:
    return {"type": "string"}


def _integer_schema() -> dict[str, str]:
    return {"type": "integer"}


def _boolean_schema() -> dict[str, str]:
    return {"type": "boolean"}


__all__ = ["airflow_git_sync_schema"]
