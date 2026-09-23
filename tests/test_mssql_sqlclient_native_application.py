"""Focused contract for the explicit SqlClient application bootstrap."""

from typing import Any, cast

import dpone.app.mssql_sqlclient_native_application as subject
import dpone.app.mssql_sqlclient_native_deployment_composition as composition


def test_bootstrap_composes_deployment_before_runtime(monkeypatch):
    events: list[tuple[str, object]] = []
    deployment = object()
    route = object()
    runtime = object()

    monkeypatch.setattr(
        composition,
        "compose_sqlclient_native_deployment",
        lambda value: events.append(("deployment", value)) or route,
    )
    monkeypatch.setattr(
        composition,
        "compose_sqlclient_native_runtime",
        lambda value: events.append(("runtime", value)) or runtime,
    )

    actual = subject.compose_sqlclient_native_application(cast(Any, deployment))

    assert actual is runtime
    assert events == [("deployment", deployment), ("runtime", route)]


def test_process_runner_composes_exact_deployment_only_when_native_run_starts(monkeypatch):
    deployment = object.__new__(composition.SqlClientNativeDeployment)
    runtime = object()
    process_config = object()
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr(
        subject,
        "compose_sqlclient_native_application",
        lambda value: calls.append(("compose", value)) or runtime,
    )
    runner = subject.compose_sqlclient_native_process_runner(
        cast(Any, lambda value: calls.append(("deployment", value)) or deployment)
    )

    actual = runner._native_runtime_factory(process_config)

    assert actual is runtime
    assert calls == [("deployment", process_config), ("compose", deployment)]


def test_install_registers_the_explicit_process_runner(monkeypatch):
    registered = []
    monkeypatch.setattr(subject, "register_process_runner", registered.append)

    subject.install_sqlclient_native_process_runner(cast(Any, lambda _config: object()))

    assert len(registered) == 1
    assert isinstance(registered[0], subject.DefaultProcessRunner)
