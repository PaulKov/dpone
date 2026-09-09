from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path

from dpone.connector_sdk.models import ConnectorSdkScaffoldResult
from dpone.connector_sdk.native_transfer_capabilities import NATIVE_TRANSFER_CAPABILITY_NAMES
from dpone.connector_sdk.scaffold_certification import (
    ConnectorCertificationScaffoldWriter,
    ConnectorCertificationTemplateBuilder,
)
from dpone.connector_sdk.scaffold_io import write_text_files
from dpone.connector_sdk.scaffold_names import class_name, safe_identifier

_VALID_CAPABILITIES = {"source", "sink", "state"}
_VALID_CONNECTOR_TYPES = {"api", "database", "event", "file"}


class ConnectorSdkScaffoldService:
    """Generate a production-oriented community connector package scaffold."""

    def __init__(
        self,
        certification_templates: ConnectorCertificationTemplateBuilder
        | ConnectorCertificationScaffoldWriter
        | None = None,
    ) -> None:
        if isinstance(certification_templates, ConnectorCertificationScaffoldWriter):
            self._certification_writer = certification_templates
        else:
            self._certification_writer = ConnectorCertificationScaffoldWriter(certification_templates)

    @staticmethod
    def native_capability_choices() -> tuple[str, ...]:
        return NATIVE_TRANSFER_CAPABILITY_NAMES

    def scaffold(
        self,
        *,
        name: str,
        root: str | Path = ".",
        connector_type: str = "api",
        capabilities: Iterable[str] = ("source",),
        native_capabilities: Iterable[str] = (),
        package_prefix: str = "dpone_connector",
        include_certification: bool = True,
    ) -> ConnectorSdkScaffoldResult:
        connector = safe_identifier(name)
        normalized_type = self._normalize_connector_type(connector_type)
        normalized_capabilities = self._normalize_capabilities(tuple(capabilities))
        normalized_native_capabilities = self._normalize_native_capabilities(tuple(native_capabilities))
        import_package = f"{safe_identifier(package_prefix)}_{connector}"
        package_name = import_package.replace("_", "-")
        sdk_root = Path(root) / package_name
        files: list[Path] = []

        files.extend(
            self._write_base_files(
                sdk_root,
                connector,
                normalized_type,
                normalized_capabilities,
                normalized_native_capabilities,
                package_name,
                import_package,
            )
        )
        files.extend(self._write_runtime_files(sdk_root, connector, normalized_capabilities, import_package))
        certification_manifest = None
        if include_certification:
            certification_manifest = sdk_root / "certification" / "certification.yaml"
            files.extend(
                self._write_certification_files(
                    sdk_root,
                    connector,
                    normalized_type,
                    normalized_capabilities,
                    normalized_native_capabilities,
                    certification_manifest,
                )
            )

        return ConnectorSdkScaffoldResult(
            connector=connector,
            connector_type=normalized_type,
            capabilities=normalized_capabilities,
            package_name=package_name,
            import_package=import_package,
            sdk_root=sdk_root,
            files=tuple(files),
            certification_manifest=certification_manifest,
            native_capabilities=normalized_native_capabilities,
        )

    def _normalize_connector_type(self, connector_type: str) -> str:
        normalized = connector_type.lower().strip()
        if normalized not in _VALID_CONNECTOR_TYPES:
            valid = ", ".join(sorted(_VALID_CONNECTOR_TYPES))
            raise ValueError(f"Unsupported connector_type={connector_type!r}; expected one of: {valid}")
        return normalized

    def _normalize_capabilities(self, capabilities: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(dict.fromkeys(item.lower().strip() for item in capabilities if item.strip()))
        if not normalized:
            return ("source",)
        unknown = sorted(set(normalized) - _VALID_CAPABILITIES)
        if unknown:
            valid = ", ".join(sorted(_VALID_CAPABILITIES))
            raise ValueError(f"Unsupported capabilities={unknown!r}; expected one of: {valid}")
        return normalized

    def _normalize_native_capabilities(self, capabilities: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(dict.fromkeys(item.lower().strip() for item in capabilities if item.strip()))
        unknown = sorted(set(normalized) - set(NATIVE_TRANSFER_CAPABILITY_NAMES))
        if unknown:
            valid = ", ".join(sorted(NATIVE_TRANSFER_CAPABILITY_NAMES))
            raise ValueError(f"Unsupported native_capabilities={unknown!r}; expected one of: {valid}")
        return normalized

    def _write_base_files(
        self,
        sdk_root: Path,
        connector: str,
        connector_type: str,
        capabilities: tuple[str, ...],
        native_capabilities: tuple[str, ...],
        package_name: str,
        import_package: str,
    ) -> list[Path]:
        files = {
            sdk_root / "pyproject.toml": _pyproject_template(package_name, connector),
            sdk_root / "README.md": _readme_template(connector, package_name),
            sdk_root / "dpone_connector_manifest.json": json.dumps(
                {
                    "contract_version": "1",
                    "connector": connector,
                    "connector_type": connector_type,
                    "capabilities": list(capabilities),
                    "native_capabilities": list(native_capabilities),
                    "import_package": import_package,
                },
                indent=2,
            )
            + "\n",
            sdk_root / "docs" / f"{connector}.md": _docs_template(connector),
            sdk_root / "examples" / f"{connector}_to_postgres.yaml": _example_manifest_template(connector),
            sdk_root / "tests" / f"test_{connector}_contract.py": _contract_test_template(connector, import_package),
        }
        return write_text_files(files)

    def _write_runtime_files(
        self,
        sdk_root: Path,
        connector: str,
        capabilities: tuple[str, ...],
        import_package: str,
    ) -> list[Path]:
        package_dir = sdk_root / "src" / import_package
        files = {
            package_dir / "__init__.py": _init_template(connector, capabilities),
            package_dir / "connector.py": _connector_template(connector),
        }
        if "source" in capabilities:
            files[package_dir / "source.py"] = _source_template(connector)
        if "sink" in capabilities:
            files[package_dir / "sink.py"] = _sink_template(connector)
        if "state" in capabilities:
            files[package_dir / "state.py"] = _state_template(connector)
        return write_text_files(files)

    def _write_certification_files(
        self,
        sdk_root: Path,
        connector: str,
        connector_type: str,
        capabilities: tuple[str, ...],
        native_capabilities: tuple[str, ...],
        certification_manifest: Path,
    ) -> list[Path]:
        return self._certification_writer.write(
            sdk_root=sdk_root,
            connector=connector,
            connector_type=connector_type,
            capabilities=capabilities,
            native_capabilities=native_capabilities,
            certification_manifest=certification_manifest,
        )


def _pyproject_template(package_name: str, connector: str) -> str:
    return f'''[build-system]
requires = ["hatchling>=1.26"]
build-backend = "hatchling.build"

[project]
name = "{package_name}"
version = "0.1.0"
description = "dpone community connector for {connector}."
readme = "README.md"
requires-python = ">=3.11,<3.13"
license = {{text = "Apache-2.0"}}
dependencies = ["dpone>=0.2.0"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
'''


def _readme_template(connector: str, package_name: str) -> str:
    return f"""# {package_name}

Community connector package for `dpone`.

## Local development

```bash
uv sync
uv run pytest
```

## Certification

```bash
dpone connectors certify --artifact-dir test_artifacts/connectors/{connector}
python certification/run_certification.py
```

The generated `certification/certification.yaml` is the source of truth for required connector evidence.
"""


def _docs_template(connector: str) -> str:
    return f"""# {connector} connector

## Overview

This page documents the generated `{connector}` connector skeleton.

## Manifest example

```yaml
source:
  type: {connector}
  connection_id: {connector}_source
  connection_type: env
sink:
  type: postgres
  connection_id: postgres_target
  connection_type: env
  table: {{schema: public, name: {connector}_events}}
  strategy: {{mode: incremental_append}}
```

## Runbook

1. Keep secrets in `env`, Vault, Airflow connections, or params managed by your deployment system.
2. Run `dpone plan path/to/manifest.yaml` before writing target data.
3. Run connector certification before publishing a package.
"""


def _example_manifest_template(connector: str) -> str:
    return f"""source:
  type: {connector}
  connection_id: {connector}_source
  connection_type: env
  options:
    batch_size: 10000

sink:
  type: postgres
  connection_id: postgres_target
  connection_type: env
  table:
    schema: public
    name: {connector}_events
  strategy:
    mode: incremental_append
  options:
    schema_evolution:
      enabled: true
    lineage:
      enabled: true
      preset: standard
"""


def _contract_test_template(connector: str, import_package: str) -> str:
    return f'''from __future__ import annotations

import importlib


def test_{connector}_package_imports_without_side_effects() -> None:
    module = importlib.import_module("{import_package}")

    assert module.CONNECTOR_NAME == "{connector}"
'''


def _init_template(connector: str, capabilities: tuple[str, ...]) -> str:
    capabilities_literal = ", ".join(repr(item) for item in capabilities)
    return f'''"""dpone connector package for {connector}."""

CONNECTOR_NAME = "{connector}"
CAPABILITIES = ({capabilities_literal},)
'''


def _connector_template(connector: str) -> str:
    connector_class = class_name(connector)
    return f'''from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class {connector_class}ConnectorConfig:
    connection_id: str
    options: dict[str, Any]


class {connector_class}Connector:
    """Shared connector helpers for `{connector}`."""

    def __init__(self, config: {connector_class}ConnectorConfig) -> None:
        self._config = config

    @property
    def connection_id(self) -> str:
        return self._config.connection_id
'''


def _source_template(connector: str) -> str:
    connector_class = class_name(connector)
    return f'''from __future__ import annotations

from collections.abc import Iterable
from typing import Any


class {connector_class}Source:
    """Extract rows from `{connector}`.

    Replace `extract_rows` with connector-specific API/database access. Keep network clients injected
    through the constructor in production code so tests can use fakes.
    """

    def extract_rows(self) -> Iterable[dict[str, Any]]:
        return ()
'''


def _sink_template(connector: str) -> str:
    connector_class = class_name(connector)
    return f'''from __future__ import annotations

from collections.abc import Iterable
from typing import Any


class {connector_class}Sink:
    """Load rows into `{connector}` through a staging-first finalizer."""

    def load_rows(self, rows: Iterable[dict[str, Any]]) -> int:
        return sum(1 for _ in rows)
'''


def _state_template(connector: str) -> str:
    connector_class = class_name(connector)
    return f'''from __future__ import annotations

from typing import Any


class {connector_class}StateBackend:
    """State backend skeleton for `{connector}`."""

    def read_state(self, key: str) -> dict[str, Any] | None:
        del key
        return None

    def write_state(self, key: str, value: dict[str, Any]) -> None:
        del key, value
'''
