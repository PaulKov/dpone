"""Deprecated compatibility facade for connector partition-clone ports."""

from __future__ import annotations

from dpone.ports.partition_clone import (
    PartitionCloneFactory,
    SupportsPartitionClone,
)

__all__ = ["PartitionCloneFactory", "SupportsPartitionClone"]
