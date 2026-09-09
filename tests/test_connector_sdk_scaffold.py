from __future__ import annotations

import json
from pathlib import Path

import yaml

from dpone.connector_sdk.certification import ConnectorCertificationTemplateService
from dpone.connector_sdk.scaffold import ConnectorSdkScaffoldService


def test_connector_sdk_scaffold_generates_package_docs_examples_and_certification(tmp_path: Path) -> None:
    result = ConnectorSdkScaffoldService().scaffold(
        name="demo_api",
        root=tmp_path,
        connector_type="api",
        capabilities=("source", "sink"),
    )

    connector_root = tmp_path / "dpone-connector-demo-api"
    expected_files = {
        connector_root / "pyproject.toml",
        connector_root / "README.md",
        connector_root / "dpone_connector_manifest.json",
        connector_root / "src" / "dpone_connector_demo_api" / "__init__.py",
        connector_root / "src" / "dpone_connector_demo_api" / "connector.py",
        connector_root / "src" / "dpone_connector_demo_api" / "source.py",
        connector_root / "src" / "dpone_connector_demo_api" / "sink.py",
        connector_root / "docs" / "demo_api.md",
        connector_root / "examples" / "demo_api_to_postgres.yaml",
        connector_root / "tests" / "test_demo_api_contract.py",
        connector_root / "certification" / "certification.yaml",
        connector_root / "certification" / "run_certification.py",
        connector_root / ".github" / "workflows" / "certification.yml",
    }

    assert expected_files.issubset(set(result.files))
    assert result.connector == "demo_api"
    assert result.package_name == "dpone-connector-demo-api"
    assert result.import_package == "dpone_connector_demo_api"
    assert result.certification_manifest == connector_root / "certification" / "certification.yaml"

    manifest = json.loads((connector_root / "dpone_connector_manifest.json").read_text(encoding="utf-8"))
    certification = yaml.safe_load(
        (connector_root / "certification" / "certification.yaml").read_text(encoding="utf-8")
    )
    pyproject = (connector_root / "pyproject.toml").read_text(encoding="utf-8")
    readme = (connector_root / "README.md").read_text(encoding="utf-8")

    assert manifest["connector"] == "demo_api"
    assert manifest["capabilities"] == ["source", "sink"]
    assert certification["connector"] == "demo_api"
    assert certification["contract_version"] == "1"
    assert certification["capabilities"]["source"]["strategies"] == ["full_refresh", "incremental_append"]
    assert certification["capabilities"]["sink"]["strategies"] == [
        "full_refresh",
        "incremental_append",
        "incremental_merge",
        "replace",
    ]
    assert "dpone-connector-demo-api" in pyproject
    assert 'requires-python = ">=3.11,<3.13"' in pyproject
    assert "dpone connectors certify" in readme


def test_connector_sdk_scaffold_generates_native_transfer_capability_contract(tmp_path: Path) -> None:
    result = ConnectorSdkScaffoldService().scaffold(
        name="warehouse_db",
        root=tmp_path,
        connector_type="database",
        capabilities=("source", "sink"),
        native_capabilities=("stream_export", "stream_staging_load"),
    )

    connector_root = tmp_path / "dpone-connector-warehouse-db"
    manifest = json.loads((connector_root / "dpone_connector_manifest.json").read_text(encoding="utf-8"))
    certification = yaml.safe_load(
        (connector_root / "certification" / "certification.yaml").read_text(encoding="utf-8")
    )

    assert result.native_capabilities == ("stream_export", "stream_staging_load")
    assert manifest["native_capabilities"] == ["stream_export", "stream_staging_load"]
    assert certification["native_transfer"]["capabilities"]["stream_export"] == {
        "formats": ["tabseparated", "jsonl"],
        "bounded": True,
        "supports_checksum": True,
        "supports_cleanup": True,
    }
    assert certification["native_transfer"]["capabilities"]["stream_staging_load"] == {
        "formats": ["tabseparated", "jsonl"],
        "staging_safe": True,
        "supports_abort": True,
        "supports_idempotency_key": True,
    }


def test_connector_sdk_scaffold_source_only_omits_sink_runtime_files(tmp_path: Path) -> None:
    result = ConnectorSdkScaffoldService().scaffold(
        name="warehouse_api",
        root=tmp_path,
        connector_type="api",
        capabilities=("source",),
    )

    connector_root = tmp_path / "dpone-connector-warehouse-api"

    assert (connector_root / "src" / "dpone_connector_warehouse_api" / "source.py").exists()
    assert not (connector_root / "src" / "dpone_connector_warehouse_api" / "sink.py").exists()
    assert all(path.name != "sink.py" for path in result.files)


def test_connector_certification_template_is_deterministic_and_capability_scoped() -> None:
    template = ConnectorCertificationTemplateService().build(
        connector="demo_api",
        connector_type="api",
        capabilities=("source",),
        native_capabilities=("file_export",),
    )

    assert template["connector"] == "demo_api"
    assert list(template["capabilities"]) == ["source"]
    assert template["capabilities"]["source"]["required_tests"] == [
        "import_safety",
        "manifest_validation",
        "schema_inference",
        "full_refresh_extract",
        "incremental_append_extract",
        "quality_contracts",
        "run_artifact_evidence",
    ]
    assert template["native_transfer"]["capabilities"]["file_export"]["supports_cleanup"] is True
    assert template["evidence"]["artifacts_dir"] == "test_artifacts/connectors/demo_api"
