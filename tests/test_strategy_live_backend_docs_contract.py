from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _doc(path: str) -> str:
    return (ROOT / "docs" / path).read_text(encoding="utf-8")


def test_live_backend_docs_cover_injected_clients_and_sink_specific_backends() -> None:
    user_doc = _doc("strategy-intelligence.md")
    developer_doc = _doc("developer-strategy-intelligence.md")

    assert "Live replay backends" in user_doc
    assert "MssqlReplayBackend" in developer_doc
    assert "PostgresReplayBackend" in developer_doc
    assert "ClickHouseReplayBackend" in developer_doc
    assert "KafkaLiveReplayBackend" in developer_doc
    assert "ReplaySqlClient" in developer_doc
    assert "ReplayKafkaClient" in developer_doc
    assert "injected clients" in developer_doc
