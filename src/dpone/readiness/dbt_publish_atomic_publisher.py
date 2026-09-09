"""Thin adapter for atomic, compare-and-swap protected artifact publication."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from dpone.contracts.dbt_artifact_publication import DbtArtifactOutputConflict, DbtArtifactPublicationError
from dpone.runtime.immutable_local_tree import (
    ImmutableLocalTreeDurabilityError,
    ImmutableLocalTreeError,
    materialize_immutable_local_tree,
)


class DbtArtifactTreePublisher:
    """Publish a complete tree once, or prove that its existing bytes are equal."""

    def publish(self, output_dir: Path, files: Mapping[str, bytes]) -> str:
        root = output_dir.absolute()
        destination_existed = root.exists()
        try:
            return materialize_immutable_local_tree(
                root,
                files,
                allowed_parent=root.parent,
                root=root.parent,
            )
        except ImmutableLocalTreeDurabilityError as exc:
            raise DbtArtifactPublicationError(
                "dbt compile output became visible but durable publication could not be proven"
            ) from exc
        except ImmutableLocalTreeError as exc:
            raise DbtArtifactOutputConflict("dbt compile output conflicts with existing content") from exc
        except (OSError, ValueError) as exc:
            if destination_existed or root.exists():
                raise DbtArtifactOutputConflict("dbt compile output conflicts with existing content") from exc
            raise DbtArtifactPublicationError("dbt compile output could not be published atomically") from exc


__all__ = [
    "DbtArtifactOutputConflict",
    "DbtArtifactPublicationError",
    "DbtArtifactTreePublisher",
]
