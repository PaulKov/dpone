from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class CompiledProcess:
    """A single compiled process (raw dict + a stable selector)."""

    name: str
    selector: str
    raw_config: dict[str, Any]


_RESERVED_VARS = {
    # built-ins
    "env_code",
    "manifest_path",
    "manifest_dir",
    "manifest_name",
    "manifest_stem",
    # per-table reserved
    "src_schema",
    "src_table",
}
