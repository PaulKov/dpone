"""Public PostgreSQL XMin initial/incremental execution policy."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

_HANDOFF_ID = re.compile(r"^[a-z][a-z0-9_.-]{0,127}$")
_POLICY_KEYS = frozenset({"mode", "handoff_id"})


class PostgresXminExecutionMode(StrEnum):
    """Lifecycle phase selected by one authored pipeline."""

    AUTO = "auto"
    INITIAL = "initial"
    INCREMENTAL = "incremental"


@dataclass(frozen=True, slots=True)
class PostgresXminExecutionPolicy:
    """Closed, identity-bearing public policy."""

    mode: PostgresXminExecutionMode = PostgresXminExecutionMode.AUTO
    handoff_id: str | None = None
    version: int = 1

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueError("postgres_xmin_handoff.version_unsupported")
        if self.mode is PostgresXminExecutionMode.AUTO:
            if self.handoff_id is not None:
                raise ValueError("postgres_xmin_handoff.auto_forbids_handoff_id")
            return
        if not isinstance(self.handoff_id, str) or not _HANDOFF_ID.fullmatch(self.handoff_id):
            raise ValueError("postgres_xmin_handoff.handoff_id_invalid")

    def to_contract(self) -> dict[str, Any]:
        """Return the stable public JSON identity."""

        return {
            "version": self.version,
            "mode": self.mode.value,
            "handoff_id": self.handoff_id,
        }


def postgres_xmin_execution_policy(options: Mapping[str, Any] | None) -> PostgresXminExecutionPolicy:
    """Parse ``source.options.xmin_execution`` without connector I/O."""

    if options is None:
        return PostgresXminExecutionPolicy()
    if not isinstance(options, Mapping):
        raise ValueError("postgres_xmin_handoff.options_invalid")
    if "xmin_execution" not in options:
        return PostgresXminExecutionPolicy()
    raw = options["xmin_execution"]
    if not isinstance(raw, Mapping):
        raise ValueError("postgres_xmin_handoff.policy_invalid")
    unexpected = sorted(str(key) for key in raw if key not in _POLICY_KEYS)
    if unexpected:
        raise ValueError("postgres_xmin_handoff.policy_unknown_fields:" + ",".join(unexpected))
    raw_mode = raw.get("mode")
    if not isinstance(raw_mode, str):
        raise ValueError("postgres_xmin_handoff.mode_invalid")
    try:
        mode = PostgresXminExecutionMode(raw_mode)
    except ValueError as exc:
        raise ValueError("postgres_xmin_handoff.mode_invalid") from exc
    handoff_id = raw.get("handoff_id")
    if handoff_id is not None and not isinstance(handoff_id, str):
        raise ValueError("postgres_xmin_handoff.handoff_id_invalid")
    return PostgresXminExecutionPolicy(mode=mode, handoff_id=handoff_id)


def checkpoint_process_identity(
    policy: PostgresXminExecutionPolicy,
    *,
    authored_process: str,
) -> str:
    """Resolve the checkpoint owner shared by initial and incremental manifests."""

    process = str(authored_process or "").strip()
    if policy.mode is PostgresXminExecutionMode.AUTO:
        if not process:
            raise ValueError("postgres_xmin_handoff.authored_process_required")
        return process
    assert policy.handoff_id is not None
    return f"xmin-handoff:{policy.handoff_id}"


def xmin_handoff_seed_load_id(*, handoff_id: str, state_key_digest: bytes) -> str:
    """Derive one retry-stable receipt identity for the handoff seed."""

    if _HANDOFF_ID.fullmatch(str(handoff_id or "")) is None:
        raise ValueError("postgres_xmin_handoff.handoff_id_invalid")
    if type(state_key_digest) is not bytes or len(state_key_digest) != 32:
        raise ValueError("postgres_xmin_handoff.state_key_invalid")
    digest = hashlib.sha256(handoff_id.encode("utf-8") + b"\0" + state_key_digest).hexdigest()
    return "xmin-seed-" + digest[:40]


__all__ = [
    "PostgresXminExecutionMode",
    "PostgresXminExecutionPolicy",
    "checkpoint_process_identity",
    "postgres_xmin_execution_policy",
    "xmin_handoff_seed_load_id",
]
