"""Shared projection policy for flat and domain-first project selection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from dpone.manifest.project_selection_contracts import ProjectCheckedSource
from dpone.manifest.selection import SelectionError, SelectionNode


class SelectionNodeFactory(Protocol):
    def __call__(
        self,
        checked: ProjectCheckedSource,
        *,
        catalog_domain: str | None,
        catalog_owner: str | None,
        tags: tuple[str, ...],
        groups: tuple[str, ...],
        semantic_fingerprint: str | None = None,
    ) -> SelectionNode: ...


def selection_node(
    checked: ProjectCheckedSource,
    *,
    catalog_domain: str | None,
    catalog_owner: str | None,
    tags: tuple[str, ...],
    groups: tuple[str, ...],
    semantic_fingerprint: str | None = None,
) -> SelectionNode:
    """Project one checked authoring source into an immutable selection node."""

    metadata = checked.payload.get("metadata")
    metadata = metadata if isinstance(metadata, Mapping) else {}
    source_domain = optional_text(metadata.get("domain"))
    source_owner = optional_text(metadata.get("owner"))
    if catalog_domain and source_domain and catalog_domain != source_domain:
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Domain catalog and authoring domain disagree.")
    if catalog_owner and source_owner and catalog_owner != source_owner:
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Domain catalog and authoring owner disagree.")
    source_tags = text_tuple(metadata.get("tags", ()), field="metadata.tags")
    sources: set[str] = set()
    sinks: set[str] = set()
    for process in checked.compilation.processes:
        source = process.get("source")
        sink = process.get("sink")
        if isinstance(source, Mapping) and optional_text(source.get("type")):
            sources.add(str(source["type"]))
        if isinstance(sink, Mapping) and optional_text(sink.get("type")):
            sinks.add(str(sink["type"]))
    return SelectionNode(
        node_id=checked.workload_id,
        source_path=checked.source_label,
        semantic_fingerprint=semantic_fingerprint or checked.compilation.semantic_fingerprint,
        domain=source_domain or catalog_domain,
        owner=source_owner or catalog_owner,
        tags=tuple(sorted(set((*source_tags, *tags)))),
        sources=tuple(sorted(sources)),
        sinks=tuple(sorted(sinks)),
        groups=groups,
    )


def text_tuple(value: Any, *, field: str) -> tuple[str, ...]:
    """Validate and normalize one bounded list-like metadata value."""

    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", f"{field} must be a list of strings.")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", f"{field} contains an invalid value.")
        result.append(item.strip())
    return tuple(sorted(set(result)))


def optional_text(value: Any) -> str | None:
    """Validate one optional non-empty catalog metadata value."""

    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Catalog metadata value is invalid.")
    return value.strip()


__all__ = ["SelectionNodeFactory", "optional_text", "selection_node", "text_tuple"]
