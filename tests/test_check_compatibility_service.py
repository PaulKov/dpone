from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.services.docs.check_compatibility_service import CheckCompatibilityService


def _ctx(repo_root: Path) -> AppContext:
    return AppContext(
        settings=Settings(
            repo_root=repo_root,
            project_dir=repo_root,
            manifest_dir=repo_root / "etl-process-manifest",
            sources_registry_paths=(),
        ),
        logger=logging.getLogger("test.compatibility_check"),
        fs=LocalFileSystem(),
        yaml=PyYamlCodec(),
    )


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_check_compatibility_service_write_and_check(tmp_path: Path) -> None:
    pkg = tmp_path / "src" / "dpone"
    _write(pkg / "__init__.py", "")
    for module in [
        "dag/__init__.py",
        "runtime/sources/__init__.py",
        "runtime/sinks/__init__.py",
        "runtime/etl/__init__.py",
        "runtime/etl_logging/__init__.py",
        "runtime/credentials/__init__.py",
        "runtime/state/__init__.py",
        "runtime/reconciliation/__init__.py",
        "runtime/sql_helpers/__init__.py",
        "runtime/xmin/__init__.py",
        "runtime/connectors/__init__.py",
        "runtime/artifacts.py",
        "contracts/errors.py",
        "contracts/process_types.py",
        "contracts/run_context.py",
        "contracts/technical_columns.py",
        "ports/db_connector.py",
        "runtime/support/data_type_mapper.py",
        "runtime/support/timezone.py",
        "yaml_config_handler/__init__.py",
        "source/__init__.py",
        "sink/__init__.py",
        "etl/__init__.py",
        "etl_logging/__init__.py",
        "credentials/__init__.py",
        "state/__init__.py",
        "reconciliation/__init__.py",
        "sql_helpers/__init__.py",
        "xmin/__init__.py",
        "lib/connectors/__init__.py",
        "core/artifacts.py",
        "core/errors.py",
        "core/etl_types.py",
        "core/runtime.py",
        "lib/technical_columns.py",
        "lib/connectors/base.py",
        "lib/utils/data_type_mapper.py",
        "lib/utils/timezone_converter.py",
    ]:
        _write(pkg / module, "")

    registry = tmp_path / "docs" / "compatibility_registry.yaml"
    _write(
        registry,
        """
version: 1
entries:
  - deprecated: dpone.yaml_config_handler
    canonical: dpone.dag
    scope: package
    status: deprecated-shim
    removal: later
  - deprecated: dpone.source
    canonical: dpone.runtime.sources
    scope: package
    status: deprecated-shim
    removal: later
  - deprecated: dpone.sink
    canonical: dpone.runtime.sinks
    scope: package
    status: deprecated-shim
    removal: later
  - deprecated: dpone.etl
    canonical: dpone.runtime.etl
    scope: package
    status: deprecated-shim
    removal: later
  - deprecated: dpone.etl_logging
    canonical: dpone.runtime.etl_logging
    scope: package
    status: deprecated-shim
    removal: later
  - deprecated: dpone.credentials
    canonical: dpone.runtime.credentials
    scope: package
    status: deprecated-shim
    removal: later
  - deprecated: dpone.state
    canonical: dpone.runtime.state
    scope: package
    status: deprecated-shim
    removal: later
  - deprecated: dpone.reconciliation
    canonical: dpone.runtime.reconciliation
    scope: package
    status: deprecated-shim
    removal: later
  - deprecated: dpone.sql_helpers
    canonical: dpone.runtime.sql_helpers
    scope: package
    status: deprecated-shim
    removal: later
  - deprecated: dpone.xmin
    canonical: dpone.runtime.xmin
    scope: package
    status: deprecated-shim
    removal: later
  - deprecated: dpone.lib.connectors
    canonical: dpone.runtime.connectors
    scope: package
    status: deprecated-shim
    removal: later
  - deprecated: dpone.core.artifacts
    canonical: dpone.runtime.artifacts
    scope: module
    status: transitional-shim
    removal: later
  - deprecated: dpone.core.errors
    canonical: dpone.contracts.errors
    scope: module
    status: transitional-shim
    removal: later
  - deprecated: dpone.core.etl_types
    canonical: dpone.contracts.process_types
    scope: module
    status: transitional-shim
    removal: later
  - deprecated: dpone.core.runtime
    canonical: dpone.contracts.run_context
    scope: module
    status: transitional-shim
    removal: later
  - deprecated: dpone.lib.technical_columns
    canonical: dpone.contracts.technical_columns
    scope: module
    status: transitional-shim
    removal: later
  - deprecated: dpone.lib.connectors.base
    canonical: dpone.ports.db_connector
    scope: module
    status: transitional-shim
    removal: later
  - deprecated: dpone.lib.utils.data_type_mapper
    canonical: dpone.runtime.support.data_type_mapper
    scope: module
    status: transitional-shim
    removal: later
  - deprecated: dpone.lib.utils.timezone_converter
    canonical: dpone.runtime.support.timezone
    scope: module
    status: transitional-shim
    removal: later
""".strip()
        + "\n",
    )
    doc = tmp_path / "docs" / "compatibility.md"
    _write(
        doc,
        "# Compatibility\n\n<!-- DPONE_COMPAT_MATRIX_START -->\nold\n<!-- DPONE_COMPAT_MATRIX_END -->\n",
    )

    svc = CheckCompatibilityService(ctx=_ctx(tmp_path))
    code, payload = svc.run(
        argparse.Namespace(
            format="json",
            registry="docs/compatibility_registry.yaml",
            doc="docs/compatibility.md",
            package="src/dpone",
            write_doc=True,
        )
    )
    assert code == 0
    assert isinstance(payload, dict)
    assert payload["ok"] is True
    assert "dpone.runtime.sources" in doc.read_text(encoding="utf-8")
    json.dumps(payload)
