"""P8a composition allocates one pinned actor then delegates."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from dpone.app import mssql_sqlclient_permission_grant_release_composition as composition


def test_composition_delegates_exact_owner_and_actor(tmp_path, monkeypatch):
    calls = []
    owner = SimpleNamespace(binding=object())
    pool = SimpleNamespace(open=lambda build, deadline: calls.append(("open", deadline)) or "actor")
    expected = object()

    def release(*args, **kwargs):
        calls.append(("release", args, kwargs))
        actor = args[1]("subject", owner.binding)
        assert actor == "actor"
        return expected

    monkeypatch.setattr(composition, "release_permission_grant_locally", release)
    result = composition.release_mssql_sqlclient_permission_locally(
        cast(Any, pool), owner, Path(tmp_path), deadline=1.0, containment_deadline=2.0
    )
    assert result is expected and calls[0][0] == "release"


def test_relative_root_rejected_before_effect(monkeypatch):
    monkeypatch.setattr(
        composition,
        "release_permission_grant_locally",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("effect")),
    )
    with pytest.raises(ValueError):
        composition.release_mssql_sqlclient_permission_locally(
            cast(Any, object()), object(), Path("relative"), deadline=1.0, containment_deadline=2.0
        )
