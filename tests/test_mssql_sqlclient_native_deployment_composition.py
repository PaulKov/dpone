"""Focused contracts for the final SqlClient production assembler."""

from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from threading import Event
from typing import Any, cast

import pytest

import dpone.app.mssql_sqlclient_native_deployment_composition as subject
from dpone.adapters.mssql_sqlclient_input_custody import FileSqlClientInputCustody
from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.app.mssql_sqlclient_fresh_chunk_execution_composition import SqlClientFreshChunkDeployment
from dpone.app.mssql_sqlclient_native_retirement_composition import SqlClientNativeRetirementDeployment
from dpone.app.mssql_sqlclient_stage_locator_composition import _AdmittedSqlClientStoreFactory
from dpone.contracts.bounded_window import WindowLease
from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy, NativeChunkLimits, NativeChunkPlan
from dpone.ports.mssql_native_route_backend import NativeActorCapacity
from dpone.runtime.native_wire_models import SourceNativeWireContract


class Store:
    def acquire(self, *_args): ...
    def assert_lease(self, *_args): ...
    def renew(self, *_args): ...
    def release(self, *_args): ...
    def load(self, *_args): ...
    def save(self, *_args): ...


def _deployment(tmp_path: Path, effects: list[str], *, width: int = 1) -> subject.SqlClientNativeDeployment:
    pool = TdsActorPool(capacity=12)
    custody = FileSqlClientInputCustody(tmp_path / "custody")
    prepared = subject.SqlClientPreparedAttemptAuthority(
        input_custody=custody,
        pool=pool,
        directory_limits=cast(Any, object()),
        supervisor_token="token",
        implementation_sha256="a" * 64,
        database="database",
        schema="dbo",
        table="stage",
        owner_binding="b" * 64,
        create_columns=(),
        wire_columns=(),
        max_row_bytes=1024,
        create_launcher=cast(Any, object()),
        departure_launcher=cast(Any, object()),
        observe_launcher=cast(Any, object()),
        creator_admission=cast(Any, object()),
        creator_principal=cast(Any, object()),
        writer_admission=cast(Any, object()),
        writer_principal=cast(Any, object()),
        create_connection_material=cast(Any, lambda: object()),
        observer_connection_material=cast(Any, lambda: object()),
        baseline=cast(Any, object()),
        baseline_authority=cast(Any, object()),
        build=cast(Any, object()),
        evidence_root=tmp_path / "evidence",
        operation_deadline=10.0,
        create_startup_timeout=1.0,
        helper_startup_timeout=1.0,
        observe_startup_timeout=1.0,
        termination_timeout=1.0,
    )
    fresh = object.__new__(SqlClientFreshChunkDeployment)
    object.__setattr__(fresh, "pool", pool)
    plan = NativeChunkPlan(
        "run",
        "target",
        "query",
        "window",
        "schema",
        "wire",
        transport=NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30),
    )
    wire = object.__new__(SourceNativeWireContract)

    @contextmanager
    def store_factory():
        effects.append("store")
        yield cast(Any, Store())

    @contextmanager
    def source(_config, _bindings):
        effects.append("source")
        yield {"rows": tuple(tuple(range(width)) for _ in range(2))}

    source_authority = subject.SqlClientNativeSourceAuthority(
        plan_factory=lambda _config: plan,
        wire_factory=lambda _config: wire,
        limits=NativeChunkLimits(1024, 1024),
        work_dir=tmp_path,
        open_payload=source,
        rows=lambda payload: payload["rows"],
    )
    connector = type(
        "Connector",
        (),
        {
            "get_records": lambda *_args: (),
            "get_records_iterator": lambda *_args: iter(()),
            "execute_query": lambda *_args: None,
            "open_session": lambda *_args, **_kwargs: object(),
        },
    )()
    sink = type("Sink", (), {"_strategy_map": {}})()
    sql = subject.SqlClientNativeSqlAuthority(
        sink=cast(Any, sink),
        target_connector=connector,
        database="database",
        schema="dbo",
        target_headroom=0,
        file_custody=custody,
        prepared=prepared,
        retirement=object.__new__(SqlClientNativeRetirementDeployment),
        admission=cast(Any, lambda *_args: object()),
        rollback_no_commit=lambda: {},
        evidence_reader=cast(Any, object()),
        input_custody=cast(Any, object()),
        checkpoint=cast(Any, object()),
        inspect_projection=lambda *_args: None,
        failed_attempt=subject.SqlClientFailedAttemptAuthority(
            resolve_attempt=lambda *_args: None,
            observe_stage=lambda *_args: None,
            observe_input_custody=lambda *_args: "a" * 64,
        ),
        allocated_bytes=lambda: 0,
    )
    worker = subject.SqlClientNativeWorkerAuthority(
        fresh=fresh,
        capacity=NativeActorCapacity(8, 2, 2, 1),
        implementation_sha256="a" * 64,
        state_domain_timeout=10.0,
    )
    return subject.SqlClientNativeDeployment(
        store_factory=store_factory,
        store=cast(Any, Store()),
        target_id="target",
        source=source_authority,
        sql=sql,
        worker=worker,
        config_validator=subject.SqlClientNativeConfigValidator(),
        quality=lambda *_args: None,
        evidence=lambda *_args: None,
    )


@pytest.mark.parametrize("width", [1, 100])
def test_compose_is_pure_for_narrow_and_wide_authorities(tmp_path, width):
    effects: list[str] = []
    deployment = _deployment(tmp_path, effects, width=width)

    route = subject.compose_sqlclient_native_deployment(deployment)

    assert effects == []
    assert route.target_id == "target"
    assert route.source is deployment.source.open_payload
    assert route.invocation_factory is not None


def test_lease_factory_admits_domain_and_binds_payload_rows_without_opening_source(monkeypatch, tmp_path):
    effects: list[str] = []
    deployment = _deployment(tmp_path, effects)
    admitted = object.__new__(_AdmittedSqlClientStoreFactory)
    invocation = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(subject, "admit_sqlclient_state_domain", lambda **values: admitted)

    def replacement(candidate, **changes):
        captured["prepared_candidate"] = candidate
        captured.update(changes)
        return candidate

    monkeypatch.setattr(subject, "replace", replacement)
    monkeypatch.setattr(
        subject, "SqlClientNativeProductionDeployment", lambda **values: captured.update(values) or values
    )

    class Producer:
        def __init__(self, production):
            captured["production"] = production

        def __call__(self, config, lease, cancelled):
            captured["call"] = (config, lease, cancelled)
            return invocation

    monkeypatch.setattr(subject, "SqlClientNativeInvocationProducer", Producer)
    route = subject.compose_sqlclient_native_deployment(deployment)
    lease = WindowLease("target", "owner", 7)
    cancelled = Event()

    assert route.invocation_factory(object(), lease, cancelled) is invocation
    assert effects == []
    assert captured["admitted_factory"] is admitted
    assert captured["row_source"] is deployment.source.rows
    assert captured["store"] is deployment.store


def test_payload_rows_are_read_once_from_the_exact_opened_payload(tmp_path):
    effects: list[str] = []
    deployment = _deployment(tmp_path, effects, width=100)
    payload = {"rows": ((1,) * 100,)}

    first = deployment.source.rows(payload)

    assert first is payload["rows"]
    assert effects == []


def test_invalid_target_and_cross_authority_custody_fail_before_effects(tmp_path):
    effects: list[str] = []
    deployment = _deployment(tmp_path, effects)
    wrong = FileSqlClientInputCustody(tmp_path / "other")
    object.__setattr__(deployment.sql, "prepared", replace(deployment.sql.prepared, input_custody=wrong))

    with pytest.raises(ValueError, match="deployment_composition_invalid"):
        subject.compose_sqlclient_native_deployment(deployment)

    assert effects == []


def test_expired_state_domain_budget_fails_before_store_or_source(tmp_path):
    effects: list[str] = []
    deployment = _deployment(tmp_path, effects)
    ticks = iter((0.0, 2.0))
    expired_pool = TdsActorPool(capacity=12, clock=lambda: next(ticks))
    object.__setattr__(deployment.worker.fresh, "pool", expired_pool)
    prepared = replace(deployment.sql.prepared, pool=expired_pool)
    worker = replace(deployment.worker, state_domain_timeout=1.0)
    route = subject.compose_sqlclient_native_deployment(
        replace(deployment, sql=replace(deployment.sql, prepared=prepared), worker=worker)
    )

    with pytest.raises(TdsJournalActorUnknown):
        route.invocation_factory(object(), WindowLease("target", "owner", 7), Event())

    assert effects == []


def test_arbitrary_preflight_callback_is_rejected_before_effects(tmp_path):
    effects: list[str] = []
    deployment = _deployment(tmp_path, effects)

    with pytest.raises(ValueError, match="deployment_composition_invalid"):
        subject.compose_sqlclient_native_deployment(
            replace(deployment, config_validator=cast(Any, lambda _config: effects.append("preflight")))
        )

    assert effects == []
