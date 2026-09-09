from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read_doc(name: str) -> str:
    return (ROOT / "docs" / name).read_text(encoding="utf-8")


def test_connector_sdk_docs_cover_user_and_developer_workflows() -> None:
    user_doc = _read_doc("connector-sdk.md")
    developer_doc = _read_doc("developer-connector-sdk.md")
    connectors_doc = _read_doc("connectors.md")

    assert "dpone connectors scaffold" in user_doc
    assert "--native-capability stream_export" in user_doc
    assert "dpone connectors certify --profile static --capability native_transfer.stream" in user_doc
    assert "certification/certification.yaml" in user_doc
    assert "dpone.connector_sdk" in developer_doc
    assert "ConnectorSdkScaffoldService" in developer_doc
    assert "ConnectorCapabilityCertificationService" in developer_doc
    assert "connector-sdk.md" in connectors_doc
