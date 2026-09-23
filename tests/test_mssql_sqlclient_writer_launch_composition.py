"""P10b composition injects fixed adapters without moving launch policy."""

from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown
from dpone.app import mssql_sqlclient_writer_launch_composition as composition
from dpone.services.mssql_tds_writer_launch import SqlClientEvidenceOpenUnknown


class Pool:
    def __init__(self, *, result=None, failure=None):
        self.result, self.failure, self.calls = result, failure, []

    def open(self, build, *, deadline):
        self.calls.append((build, deadline))
        if self.failure is not None:
            raise self.failure
        return self.result


def test_composition_delegates_exact_dependencies_and_opens_typed_evidence(monkeypatch):
    admitted, gateway, result = object(), object(), object()
    pool = Pool(result=gateway)
    writer_factory = object()
    launcher_factory = object()
    monkeypatch.setattr(composition.SqlClientLauncher, "for_input_descriptor", launcher_factory)
    captured = {}

    def run(value, **dependencies):
        captured.update(dependencies)
        assert value is admitted
        return result

    monkeypatch.setattr(composition, "launch_and_register_sqlclient_writer", run)

    def clock():
        return 1.0

    observed = composition.launch_mssql_sqlclient_writer(
        admitted,
        pool=pool,
        evidence_writer_factory=writer_factory,
        clock=clock,
    )

    assert observed is result
    assert captured["clock"] is clock
    assert captured["build_launcher"] is launcher_factory
    assert captured["open_evidence"]("a" * 64, 3.0) is gateway
    build, deadline = pool.calls[0]
    actor = build(3.0, clock)
    assert type(actor) is composition.SqlClientEvidenceActor
    assert actor._factory is writer_factory
    assert actor._attempt_sha256 == "a" * 64
    assert deadline == 3.0


@pytest.mark.parametrize("orphan", (None, SimpleNamespace()))
def test_actor_open_unknown_preserves_exact_optional_orphan(monkeypatch, orphan):
    pool = Pool(failure=TdsJournalActorUnknown(orphan))
    captured = {}

    def run(value, **dependencies):
        captured.update(dependencies)
        return object()

    monkeypatch.setattr(composition, "launch_and_register_sqlclient_writer", run)
    composition.launch_mssql_sqlclient_writer(
        object(),
        pool=pool,
        evidence_writer_factory=object(),
    )

    with pytest.raises(SqlClientEvidenceOpenUnknown) as caught:
        captured["open_evidence"]("b" * 64, 4.0)

    assert caught.value.gateway is orphan
