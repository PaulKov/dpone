"""Environment-chain contract for schema migration promotion."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Protocol

import yaml

ENVIRONMENT_CONTRACT_SCHEMA = "dpone.schema_migration_environments.v1"
_ENV_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_INLINE_SECRET_KEYS = frozenset({"password", "token", "secret", "client_secret", "private_key", "access_key"})


@dataclass(frozen=True, slots=True)
class MigrationEnvironmentPolicy:
    require_same_pack_id: bool = True
    require_previous_certification: bool = True
    require_impact_gate: bool = True
    prod_requires_approval: bool = True

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> MigrationEnvironmentPolicy:
        payload = raw or {}
        return cls(
            require_same_pack_id=bool(payload.get("require_same_pack_id", True)),
            require_previous_certification=bool(payload.get("require_previous_certification", True)),
            require_impact_gate=bool(payload.get("require_impact_gate", True)),
            prod_requires_approval=bool(payload.get("prod_requires_approval", True)),
        )

    def to_dict(self) -> dict[str, bool]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class MigrationEnvironmentRef:
    name: str
    ledger: Path
    actual: Path | None = None
    target_connection: Path | None = None

    @classmethod
    def from_mapping(cls, *, name: str, raw: Mapping[str, Any], base_dir: Path) -> MigrationEnvironmentRef:
        if not _ENV_NAME_RE.match(name):
            raise ValueError(f"invalid migration environment name: {name}")
        ledger = raw.get("ledger")
        if not ledger:
            raise ValueError(f"migration environment {name} must define ledger")
        return cls(
            name=name,
            ledger=_resolve_path(base_dir, ledger),
            actual=_optional_path(base_dir, raw.get("actual")),
            target_connection=_target_connection_path(base_dir, raw.get("target_connection")),
        )

    def to_dict(self) -> dict[str, str | None]:
        return {
            "name": self.name,
            "ledger": str(self.ledger),
            "actual": str(self.actual) if self.actual else None,
            "target_connection": str(self.target_connection) if self.target_connection else None,
        }


@dataclass(frozen=True, slots=True)
class MigrationEnvironmentContract:
    chain: tuple[str, ...]
    environments: dict[str, MigrationEnvironmentRef]
    policy: MigrationEnvironmentPolicy = field(default_factory=MigrationEnvironmentPolicy)
    path: Path | None = None

    @classmethod
    def from_file(cls, path: str | Path) -> MigrationEnvironmentContract:
        full_path = Path(path)
        return cls.from_config(load_mapping_file(full_path), base_dir=full_path.parent, path=full_path)

    @classmethod
    def from_config(
        cls,
        raw: Mapping[str, Any],
        *,
        base_dir: str | Path = ".",
        path: str | Path | None = None,
    ) -> MigrationEnvironmentContract:
        if raw.get("schema_version") not in {None, ENVIRONMENT_CONTRACT_SCHEMA}:
            raise ValueError(f"unsupported migration environment schema_version: {raw.get('schema_version')}")
        chain = tuple(str(item) for item in raw.get("chain", ()) if str(item))
        if not chain:
            raise ValueError("migration environment chain must not be empty")
        if len(set(chain)) != len(chain):
            raise ValueError("migration environment chain must be unique")
        env_raw = raw.get("environments", {})
        if not isinstance(env_raw, Mapping):
            raise ValueError("migration environments must be an object")
        missing = [name for name in chain if name not in env_raw]
        if missing:
            raise ValueError(f"migration environment missing definitions: {', '.join(missing)}")
        base = Path(base_dir)
        environments = {
            name: MigrationEnvironmentRef.from_mapping(name=name, raw=_as_mapping(env_raw.get(name)), base_dir=base)
            for name in sorted(chain)
        }
        return cls(
            chain=chain,
            environments=environments,
            policy=MigrationEnvironmentPolicy.from_mapping(
                raw.get("policy") if isinstance(raw.get("policy"), Mapping) else None
            ),
            path=Path(path) if path else None,
        )

    def environment(self, name: str) -> MigrationEnvironmentRef:
        if name not in self.environments:
            raise ValueError(f"unknown migration environment: {name}")
        return self.environments[name]

    def previous_environment(self, name: str) -> str | None:
        idx = self.chain.index(name) if name in self.chain else -1
        return self.chain[idx - 1] if idx > 0 else None

    def is_final_environment(self, name: str) -> bool:
        return bool(self.chain) and self.chain[-1] == name

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": ENVIRONMENT_CONTRACT_SCHEMA,
            "chain": list(self.chain),
            "policy": self.policy.to_dict(),
            "environments": {name: self.environments[name].to_dict() for name in self.chain},
        }


class EnvironmentLedgerStore(Protocol):
    def records(self, *, environment: str | None = None) -> tuple[dict[str, Any], ...]: ...


@dataclass(frozen=True, slots=True)
class ArtifactEnvironmentLedgerStore:
    """Environment-aware view over the JSON migration artifact ledger."""

    path: Path

    def records(self, *, environment: str | None = None) -> tuple[dict[str, Any], ...]:
        if not self.path.exists():
            return ()
        records = load_mapping_file(self.path).get("records", [])
        if not isinstance(records, list):
            raise ValueError("migration ledger records must be a list")
        items = tuple(item for item in records if isinstance(item, dict))
        if environment is None:
            return items
        return tuple(record for record in items if record.get("environment") in {None, environment})


def load_mapping_file(path: Path) -> dict[str, Any]:
    raw_text = path.read_text(encoding="utf-8")
    raw = json.loads(raw_text) if path.suffix.lower() == ".json" else yaml.safe_load(raw_text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _target_connection_path(base_dir: Path, raw: Any) -> Path | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        return _resolve_path(base_dir, raw)
    if not isinstance(raw, Mapping):
        raise ValueError("target_connection must be a path or object")
    if _INLINE_SECRET_KEYS & set(raw):
        raise ValueError("migration environment target_connection must not contain inline secrets")
    path = raw.get("path")
    return _resolve_path(base_dir, path) if path else None


def _resolve_path(base_dir: Path, raw: Any) -> Path:
    path = Path(str(raw))
    return path if path.is_absolute() else base_dir / path


def _optional_path(base_dir: Path, raw: Any) -> Path | None:
    return _resolve_path(base_dir, raw) if raw else None


def _as_mapping(raw: Any) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping):
        raise ValueError("migration environment entry must be an object")
    return raw


__all__ = [
    "ENVIRONMENT_CONTRACT_SCHEMA",
    "ArtifactEnvironmentLedgerStore",
    "EnvironmentLedgerStore",
    "MigrationEnvironmentContract",
    "MigrationEnvironmentPolicy",
    "MigrationEnvironmentRef",
    "load_mapping_file",
]
