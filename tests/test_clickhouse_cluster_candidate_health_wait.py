"""Replicated publication waits for transient health without weakening ownership."""

from dataclasses import replace

from dpone.runtime.sinks.clickhouse_cluster_full_refresh_publication import (
    ClickHouseClusterFullRefreshPublicationService,
)
from tests.test_clickhouse_cluster_full_refresh_publication import (
    _Authority,
    _Bootstrap,
    _Candidate,
    _Catalog,
    _config,
    _Ddl,
)


def test_count_equal_candidate_waits_for_replica_health_before_prepared_authority():
    authority = _Authority()

    class Catalog(_Catalog):
        observations = 0

        def generations(self, *args):
            self.observations += 1
            facts = super().generations(*args)
            if self.observations <= 2:
                assert authority.current is None, "unready candidate must not acquire authority"
                return tuple(replace(fact, candidate_healthy=False) for fact in facts)
            return facts

    catalog = Catalog()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda _: authority, ddl, _Bootstrap())

    receipt = service.publish(_config(), _Candidate(), staged_rows=2)

    assert receipt.authority.phase.value == "COMMITTED"
    assert ddl.dispatches == 1


class _Clock:
    def __init__(self):
        self.now = None
        self.sleeps = []
        self.deadlines = []

    def monotonic(self):
        assert self.now is not None
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def _use_clock(monkeypatch, clock):
    from dpone.runtime.sinks import clickhouse_cluster_full_refresh_publication as publication

    wait = publication.require_pre_dispatch_generation

    def timed_wait(*args, deadline):
        if clock.now is None:
            clock.now = deadline - 300
        clock.deadlines.append(deadline)
        return wait(*args, deadline=deadline, monotonic=clock.monotonic, sleep=clock.sleep)

    monkeypatch.setattr(publication, "require_pre_dispatch_generation", timed_wait)


def test_unhealthy_candidate_times_out_without_authority_or_ddl(monkeypatch):
    import pytest

    from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError

    class Catalog(_Catalog):
        def generations(self, *args):
            return tuple(replace(f, candidate_healthy=False) for f in super().generations(*args))

    catalog, authority, clock = Catalog(), _Authority(), _Clock()
    _use_clock(monkeypatch, clock)
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda _: authority, ddl, _Bootstrap())

    with pytest.raises(ClusterPublicationError) as raised:
        service.publish(_config(), _Candidate(), staged_rows=2)

    assert raised.value.code == "DPONE_CLICKHOUSE_CLUSTER_CANDIDATE_NOT_READY"
    assert "node-1" in raised.value.detail and "node-2" in raised.value.detail
    assert authority.current is None
    assert ddl.dispatches == 0
    assert sum(clock.sleeps) == pytest.approx(300)


def test_generation_drift_is_fatal_without_wait_or_authority(monkeypatch):
    import pytest

    from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError

    class Catalog(_Catalog):
        observations = 0

        def generations(self, *args):
            self.observations += 1
            facts = super().generations(*args)
            if self.observations > 1:
                return tuple(replace(f, candidate=replace(f.candidate, uuid="unrelated")) for f in facts)
            return facts

    catalog, authority, clock = Catalog(), _Authority(), _Clock()
    _use_clock(monkeypatch, clock)
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda _: authority, ddl, _Bootstrap())

    with pytest.raises(ClusterPublicationError) as raised:
        service.publish(_config(), _Candidate(), staged_rows=2)

    assert raised.value.code == "DPONE_CLICKHOUSE_CLUSTER_GENERATION_DIVERGED"
    assert clock.sleeps == []
    assert authority.current is None
    assert ddl.dispatches == 0


def test_health_race_after_ownership_waits_before_single_dispatch(monkeypatch):
    authority, clock = _Authority(), _Clock()
    _use_clock(monkeypatch, clock)

    class Catalog(_Catalog):
        lag_seen = False

        def generations(self, *args):
            facts = super().generations(*args)
            if authority.current is not None and not self.committed and not self.lag_seen:
                self.lag_seen = True
                return tuple(replace(f, candidate_healthy=False) for f in facts)
            return facts

    catalog = Catalog()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda _: authority, ddl, _Bootstrap())

    receipt = service.publish(_config(), _Candidate(), staged_rows=2)

    assert receipt.authority.phase.value == "COMMITTED"
    assert ddl.dispatches == 1
    assert clock.sleeps == [2]
    assert len(clock.deadlines) == 2 and len(set(clock.deadlines)) == 1


def test_two_barriers_share_deadline_and_never_dispatch_after_persistent_race(monkeypatch):
    import pytest

    from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError

    authority, clock = _Authority(), _Clock()
    _use_clock(monkeypatch, clock)

    class Catalog(_Catalog):
        def generations(self, *args):
            facts = super().generations(*args)
            ready = clock.now is not None and sum(clock.sleeps) >= 298 and authority.current is None
            return tuple(replace(f, candidate_healthy=ready) for f in facts)

    catalog = Catalog()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda _: authority, ddl, _Bootstrap())

    with pytest.raises(ClusterPublicationError) as raised:
        service.publish(_config(), _Candidate(), staged_rows=2)

    assert raised.value.code == "DPONE_CLICKHOUSE_CLUSTER_CANDIDATE_NOT_READY"
    assert len(clock.deadlines) == 2 and len(set(clock.deadlines)) == 1
    assert sum(clock.sleeps) == pytest.approx(300)
    assert authority.current.record.phase.value == "PREPARED"
    assert ddl.dispatches == 0


def test_expired_shared_budget_after_authority_never_dispatches(monkeypatch):
    import pytest

    from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError

    clock = _Clock()
    _use_clock(monkeypatch, clock)

    class Authority(_Authority):
        def create_if_absent(self, record):
            result = super().create_if_absent(record)
            clock.now += 301
            return result

    catalog, authority = _Catalog(), Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda _: authority, ddl, _Bootstrap())

    with pytest.raises(ClusterPublicationError) as raised:
        service.publish(_config(), _Candidate(), staged_rows=2)

    assert raised.value.code == "DPONE_CLICKHOUSE_CLUSTER_CANDIDATE_NOT_READY"
    assert authority.current.record.phase.value == "PREPARED"
    assert ddl.dispatches == 0
    assert clock.sleeps == []


def test_health_ready_only_at_deadline_is_not_admitted(monkeypatch):
    import pytest

    from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError

    clock = _Clock()
    _use_clock(monkeypatch, clock)

    class Catalog(_Catalog):
        def generations(self, *args):
            facts = super().generations(*args)
            ready = clock.now is not None and sum(clock.sleeps) >= 300
            return tuple(replace(f, candidate_healthy=ready) for f in facts)

    catalog, authority = Catalog(), _Authority()
    ddl = _Ddl(catalog)
    service = ClickHouseClusterFullRefreshPublicationService(catalog, lambda _: authority, ddl, _Bootstrap())

    with pytest.raises(ClusterPublicationError) as raised:
        service.publish(_config(), _Candidate(), staged_rows=2)

    assert raised.value.code == "DPONE_CLICKHOUSE_CLUSTER_CANDIDATE_NOT_READY"
    assert authority.current is None
    assert ddl.dispatches == 0
    assert sum(clock.sleeps) == pytest.approx(300)
