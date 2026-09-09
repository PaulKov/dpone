from __future__ import annotations

from dataclasses import dataclass

import pytest

from dpone.runtime.credentials.workload_scope import WorkloadScopedCredentialResolver


@dataclass(frozen=True)
class _Resolved:
    connection_ref: str
    version: int


class _Resolver:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def resolve(self, connection_ref: str) -> _Resolved:
        self.calls.append(connection_ref)
        return _Resolved(connection_ref=connection_ref, version=len(self.calls))


def test_workload_scoped_resolver_resolves_each_ref_once() -> None:
    delegate = _Resolver()
    resolver = WorkloadScopedCredentialResolver(delegate)

    first = resolver.resolve("clickhouse_dev")
    second = resolver.resolve("clickhouse_dev")
    source = resolver.resolve("mssql_dev")

    assert first is second
    assert source.connection_ref == "mssql_dev"
    assert delegate.calls == ["clickhouse_dev", "mssql_dev"]


def test_workload_scoped_resolver_does_not_cache_failures() -> None:
    class FailingOnceResolver:
        def __init__(self) -> None:
            self.calls = 0

        def resolve(self, connection_ref: str) -> _Resolved:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("temporary failure")
            return _Resolved(connection_ref=connection_ref, version=self.calls)

    delegate = FailingOnceResolver()
    resolver = WorkloadScopedCredentialResolver(delegate)

    with pytest.raises(RuntimeError, match="temporary failure"):
        resolver.resolve("clickhouse_dev")

    assert resolver.resolve("clickhouse_dev").version == 2
    assert delegate.calls == 2
