"""Connector clone boundary for parallel partition workers."""

from __future__ import annotations

from typing import Protocol, TypeVar

TConnector = TypeVar("TConnector")
TConnector_co = TypeVar("TConnector_co", covariant=True)


class SupportsPartitionClone(Protocol[TConnector_co]):
    """Connector contract for isolated partition worker connections."""

    def clone_for_partition(self, partition_index: int) -> TConnector_co:
        """Return a connector safe for one partition worker."""


class PartitionCloneFactory:
    """Resolve worker connectors without strategies knowing concrete classes."""

    def clone(self, connector: TConnector, partition_index: int) -> TConnector:
        clone_method = getattr(connector, "clone_for_partition", None)
        if callable(clone_method):
            return clone_method(partition_index)
        return connector


__all__ = ["PartitionCloneFactory", "SupportsPartitionClone"]
