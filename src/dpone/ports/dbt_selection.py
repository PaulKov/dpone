"""Build-plane port for exact dbt workflow selection."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.contracts.dbt_invocation import DbtInvocationTarget
    from dpone.contracts.dbt_selection import ResolvedDbtSelection


class DbtSelectionResolver(Protocol):
    """Resolve one workflow selection without changing project sources."""

    def resolve(
        self,
        *,
        project_root: Path,
        manifest_bytes: bytes,
        selected_unique_ids: tuple[str, ...],
        profiles_dir: Path | None,
        profile_name: str,
        target_name: str,
        dbt_core_version: str,
        dbt_adapter: str,
        dbt_adapter_version: str,
    ) -> ResolvedDbtSelection: ...


class DbtParseTargetResolver(Protocol):
    """Observe the rendered base target under the same captured parse context."""

    def resolve(
        self,
        *,
        project_root: Path,
        profiles_dir: Path,
        parse_args: tuple[str, ...],
        environment: Mapping[str, str],
    ) -> DbtInvocationTarget: ...


__all__ = ["DbtSelectionResolver", "DbtParseTargetResolver"]
