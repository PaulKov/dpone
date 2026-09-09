"""Backward-compatible row artifact facade."""

from __future__ import annotations

from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.internal_query_artifact import InternalQueryArtifact
from dpone.runtime.streaming_rows import StreamingRowsArtifact

__all__ = ["InMemoryRowsArtifact", "InternalQueryArtifact", "StreamingRowsArtifact"]
