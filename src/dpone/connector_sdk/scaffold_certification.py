from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import yaml

from dpone.connector_sdk.certification import ConnectorCertificationTemplateService
from dpone.connector_sdk.scaffold_io import write_text_files


class ConnectorCertificationTemplateBuilder(Protocol):
    def build(
        self,
        *,
        connector: str,
        connector_type: str,
        capabilities: tuple[str, ...],
        native_capabilities: tuple[str, ...],
    ) -> dict[str, Any]: ...


class ConnectorCertificationScaffoldWriter:
    """Write certification artifacts for generated connector packages."""

    def __init__(self, templates: ConnectorCertificationTemplateBuilder | None = None) -> None:
        self._templates = templates or ConnectorCertificationTemplateService()

    def write(
        self,
        *,
        sdk_root: Path,
        connector: str,
        connector_type: str,
        capabilities: tuple[str, ...],
        native_capabilities: tuple[str, ...],
        certification_manifest: Path,
    ) -> list[Path]:
        manifest = self._templates.build(
            connector=connector,
            connector_type=connector_type,
            capabilities=capabilities,
            native_capabilities=native_capabilities,
        )
        files = {
            certification_manifest: yaml.safe_dump(manifest, sort_keys=False),
            sdk_root / "certification" / "run_certification.py": _certification_runner_template(connector),
            sdk_root / ".github" / "workflows" / "certification.yml": _certification_workflow_template(),
        }
        return write_text_files(files)


def _certification_runner_template(connector: str) -> str:
    return f"""from __future__ import annotations

from pathlib import Path


def main() -> int:
    manifest = Path(__file__).with_name("certification.yaml")
    print(f"Run dpone connectors certify with {{manifest}} and publish artifacts for {connector}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
"""


def _certification_workflow_template() -> str:
    return """name: Connector certification

on:
  workflow_dispatch:
  pull_request:

jobs:
  certification:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
      - run: uv sync
      - run: uv run pytest
      - run: uv run python certification/run_certification.py
"""
