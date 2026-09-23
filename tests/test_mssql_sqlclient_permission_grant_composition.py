"""P7 composition persists coordinator INTENT before delegating the transcript."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from dpone.app import mssql_sqlclient_permission_grant_composition as composition


def test_composition_creates_exact_intent_before_held_service(tmp_path, monkeypatch):
    events = []
    identity = object()
    association = SimpleNamespace(assert_ready=lambda: events.append("ready"), identity=identity)
    launch = SimpleNamespace(operation=identity, credential_payload=object())
    coordinator = object()
    expected = object()

    def create(*args, **kwargs):
        events.append("create")
        assert args[2] is identity
        return coordinator

    def hold(*args, **kwargs):
        events.append("hold")
        assert args[:4] == (association, coordinator, "launcher", launch)
        assert args[5] is launch.credential_payload
        return expected

    monkeypatch.setattr(composition, "create_tds_coordinator", create)
    monkeypatch.setattr(composition, "hold_permission_grant", hold)
    result = composition.hold_mssql_sqlclient_permission(
        cast(Any, "pool"),
        cast(Any, "store"),
        association,
        cast(Any, "limits"),
        cast(Any, "lease"),
        "launcher",
        cast(Any, launch),
        b"admission",
        Path(tmp_path),
        supervisor_token="token",
        deadline=1.0,
    )
    assert result is expected and events == ["ready", "create", "hold"]


def test_relative_evidence_root_rejects_before_intent(monkeypatch):
    association = SimpleNamespace(assert_ready=lambda: None, identity=object())
    launch = SimpleNamespace(operation=association.identity)
    monkeypatch.setattr(
        composition,
        "create_tds_coordinator",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not create")),
    )
    try:
        composition.hold_mssql_sqlclient_permission(
            cast(Any, "pool"),
            cast(Any, "store"),
            association,
            cast(Any, "limits"),
            cast(Any, "lease"),
            "launcher",
            cast(Any, launch),
            b"admission",
            Path("relative"),
            supervisor_token="token",
            deadline=1.0,
        )
    except ValueError:
        pass
    else:
        raise AssertionError("relative root accepted")


def test_pool_exhaustion_retains_coordinator_failure_before_launch(tmp_path, monkeypatch):
    association = SimpleNamespace(assert_ready=lambda: None, identity=object())
    launch = SimpleNamespace(operation=association.identity)
    failure = RuntimeError("pool exhausted")
    monkeypatch.setattr(composition, "create_tds_coordinator", lambda *args, **kwargs: (_ for _ in ()).throw(failure))
    monkeypatch.setattr(
        composition,
        "hold_permission_grant",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("launch entered")),
    )
    with pytest.raises(RuntimeError, match="pool exhausted"):
        composition.hold_mssql_sqlclient_permission(
            cast(Any, "pool"),
            cast(Any, "store"),
            association,
            cast(Any, "limits"),
            cast(Any, "lease"),
            "launcher",
            cast(Any, launch),
            b"admission",
            Path(tmp_path),
            supervisor_token="token",
            deadline=1.0,
        )
