"""Selected replay must reach both credential factories without changing defaults."""

from types import SimpleNamespace

import pytest

from dpone.runtime.credentials.factory import SinkFactory
from dpone.runtime.credentials.resolved_endpoint_factory import ResolvedEndpointFactory


def connection(kind="clickhouse"):
    return SimpleNamespace(descriptor=SimpleNamespace(connection_type=kind), credentials=object())


@pytest.mark.parametrize("factory", ["legacy", "resolved", "wrapper"])
def test_selected_replay_is_forwarded_after_one_connector_resolution(factory, monkeypatch):
    captured = []
    connector = object()
    monkeypatch.setattr(SinkFactory, "_create_clickhouse_connector", lambda *a: connector)
    monkeypatch.setattr(
        "dpone.runtime.credentials.resolved_endpoint_factory.ResolvedConnectorFactory.create", lambda *a, **k: connector
    )
    monkeypatch.setattr("dpone.runtime.sinks.clickhouse.ClickHouseSink", lambda **kw: captured.append(kw) or kw)
    if factory == "legacy":
        result = SinkFactory.create(
            "target", None, credentials_source="params", connection_type="clickhouse", durable_quality_replay=True
        )
    elif factory == "resolved":
        result = ResolvedEndpointFactory.create_sink(connection(), None, durable_quality_replay=True)
    else:
        result = SinkFactory.create_resolved(connection(), None, durable_quality_replay=True)
    assert result["connector"] is connector
    assert captured[0]["durable_quality_replay"] is True


@pytest.mark.parametrize("factory", ["legacy", "resolved"])
@pytest.mark.parametrize("kind,value", [("postgres", True), ("clickhouse", "true"), ("clickhouse", None)])
def test_invalid_selection_is_rejected_before_credentials(factory, kind, value, monkeypatch):
    monkeypatch.setattr(SinkFactory, "_create_clickhouse_connector", lambda *a: pytest.fail("credentials"))
    monkeypatch.setattr(
        "dpone.runtime.credentials.resolved_endpoint_factory.ResolvedConnectorFactory.create",
        lambda *a, **k: pytest.fail("credentials"),
    )
    with pytest.raises(ValueError, match="DPONE_REPLAY_QUALITY"):
        if factory == "legacy":
            SinkFactory.create(
                "target", None, credentials_source="params", connection_type=kind, durable_quality_replay=value
            )
        else:
            ResolvedEndpointFactory.create_sink(connection(kind), None, durable_quality_replay=value)
