"""Render and publish the general connector certification projection."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dpone.connector_sdk.certification_artifacts import CertificationArtifactPublisher


class ConnectorCertificationRenderer:
    """Serialize authoritative JSON and its human-readable projection."""

    def write(
        self,
        artifact_dir: str | Path,
        *,
        payload: dict[str, Any],
        markdown: str,
    ) -> tuple[Path, Path]:
        return CertificationArtifactPublisher().publish(
            artifact_dir,
            base_name="connector-certification",
            json_content=json.dumps(payload, indent=2, sort_keys=True) + "\n",
            markdown_content=markdown,
        )


__all__ = ["ConnectorCertificationRenderer"]
