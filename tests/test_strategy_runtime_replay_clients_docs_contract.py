from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_strategy_docs_describe_runtime_replay_clients() -> None:
    docs = (ROOT / "docs" / "strategy-intelligence.md").read_text(encoding="utf-8")

    assert "Runtime replay clients" in docs
    assert "RuntimeReplayBackendFactory" in docs
    assert "ReplayBackendConnection" in docs


def test_developer_docs_describe_runtime_replay_client_ports() -> None:
    docs = (ROOT / "docs" / "developer-strategy-intelligence.md").read_text(encoding="utf-8")

    assert "Runtime replay client adapters" in docs
    assert "RuntimeSqlReplayClient" in docs
    assert "RuntimeKafkaReplayClient" in docs
    assert "No SDK imports in replay backends" in docs
