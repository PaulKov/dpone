from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class GitOpsAirflowConnectionEnvRef:
    connection_id: str
    env_name: str
    secret_name: str | None = None
    secret_key: str | None = None

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "connection_id": self.connection_id,
            "env_name": self.env_name,
        }
        if self.secret_name:
            payload["secret_ref"] = {"name": self.secret_name, "key": self.secret_key or self.env_name}
        return payload


@dataclass(frozen=True, slots=True)
class GitOpsAirflowConnectionBridge:
    mode: str
    runtime_mode: str
    required_connection_ids: tuple[str, ...]
    env: tuple[GitOpsAirflowConnectionEnvRef, ...]
    secret_name: str | None = None
    warnings: tuple[dict[str, str], ...] = ()
    blockers: tuple[dict[str, str], ...] = ()
    enabled: bool = True

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "mode": self.mode,
            "runtime_mode": self.runtime_mode,
            "secret_name": self.secret_name,
            "required_connection_ids": list(self.required_connection_ids),
            "env": [item.to_jsonable() for item in self.env],
            "warnings": [dict(item) for item in self.warnings],
            "blockers": [dict(item) for item in self.blockers],
        }


def connection_bridge_from_mapping(value: object) -> GitOpsAirflowConnectionBridge | None:
    if not isinstance(value, Mapping) or not value.get("enabled", True):
        return None
    raw_env = value.get("env")
    env = tuple(_env_ref(item) for item in raw_env if isinstance(item, Mapping)) if isinstance(raw_env, list) else ()
    return GitOpsAirflowConnectionBridge(
        mode=str(value.get("mode") or "k8s_secret"),
        runtime_mode=str(value.get("runtime_mode") or "runtime_only"),
        secret_name=_optional(value.get("secret_name")),
        required_connection_ids=tuple(str(item) for item in value.get("required_connection_ids", ()) if str(item)),
        env=env,
        warnings=tuple(dict(item) for item in value.get("warnings", ()) if isinstance(item, Mapping)),
        blockers=tuple(dict(item) for item in value.get("blockers", ()) if isinstance(item, Mapping)),
    )


def _env_ref(value: Mapping[str, Any]) -> GitOpsAirflowConnectionEnvRef:
    secret = value.get("secret_ref") if isinstance(value.get("secret_ref"), Mapping) else {}
    return GitOpsAirflowConnectionEnvRef(
        connection_id=str(value.get("connection_id") or ""),
        env_name=str(value.get("env_name") or ""),
        secret_name=_optional(secret.get("name")),
        secret_key=_optional(secret.get("key")),
    )


def _optional(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


__all__ = [
    "GitOpsAirflowConnectionBridge",
    "GitOpsAirflowConnectionEnvRef",
    "connection_bridge_from_mapping",
]
