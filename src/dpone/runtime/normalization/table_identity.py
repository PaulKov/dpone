"""Fail-closed generated-table identity for one normalization run."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class _TableBinding:
    physical_table: str
    source_path: str
    parent_table: str | None = None
    unique_key: tuple[str, ...] = ()


class GeneratedTableRegistry:
    """Keep physical spelling stable while rejecting ambiguous destination identity."""

    def __init__(self, *, root_table: str) -> None:
        self._bindings: dict[str, _TableBinding] = {
            root_table.lower(): _TableBinding(root_table, "$"),
        }

    def register_generated(
        self,
        *,
        physical_table: str,
        source_path: str,
        parent_table: str,
        unique_key: tuple[str, ...] = (),
    ) -> None:
        """Register one generated target without changing its physical spelling."""

        if not physical_table.isascii():
            raise ValueError(
                "non-ASCII generated physical identifier "
                f"`{physical_table}` for source path `{source_path}` is unsupported; "
                "configure an explicit ASCII table mapping"
            )
        self._register(
            _TableBinding(
                physical_table=physical_table,
                source_path=source_path,
                parent_table=parent_table,
                unique_key=unique_key,
            )
        )

    def register_auxiliary(self, *, physical_table: str, source_path: str) -> None:
        """Reserve a runtime-owned raw or quarantine target."""

        if not physical_table.isascii():
            raise ValueError(
                "non-ASCII generated physical identifier "
                f"`{physical_table}` for source path `{source_path}` is unsupported; "
                "configure an explicit ASCII table mapping"
            )
        self._register(_TableBinding(physical_table, source_path))

    def table_keys(self) -> dict[str, tuple[str, ...]]:
        """Return configured child keys keyed by their resolved physical table."""

        return {binding.physical_table: binding.unique_key for binding in self._bindings.values() if binding.unique_key}

    def table_parents(self) -> dict[str, str]:
        """Return the resolved direct parent for each generated child table."""

        return {
            binding.physical_table: binding.parent_table
            for binding in self._bindings.values()
            if binding.parent_table is not None
        }

    def _register(self, binding: _TableBinding) -> None:
        collision_key = binding.physical_table.lower()
        existing = self._bindings.get(collision_key)
        if existing is None:
            self._bindings[collision_key] = binding
            return
        if existing == binding:
            return
        raise ValueError(
            "generated table collision: "
            f"source path `{existing.source_path}` resolves to `{existing.physical_table}`, "
            f"while `{binding.source_path}` resolves to `{binding.physical_table}` "
            f"under lowercase collision key `{collision_key}`; "
            "configure distinct ASCII table mappings"
        )


__all__ = ["GeneratedTableRegistry"]
