"""Closed manifest selection for the ClickHouse raw source snapshot."""

from types import SimpleNamespace

import pytest

from dpone.manifest.clickhouse_raw_snapshot_policy import native_source_snapshot_policy


def _config(snapshot: object = ...):  # type: ignore[no-untyped-def]
    native = {} if snapshot is ... else {"source_snapshot": snapshot}
    return SimpleNamespace(options={"native_transfer": native})


def test_omission_preserves_query_visible_without_materializing_replica_scope() -> None:
    policy = native_source_snapshot_policy(_config())
    assert (policy.mode, policy.replica_scope) == ("query_visible", None)
    explicit = native_source_snapshot_policy(_config({"mode": "query_visible"}))
    assert explicit == policy
    for native in (None, False):
        policy = native_source_snapshot_policy(SimpleNamespace(options={"native_transfer": native}))
        assert (policy.mode, policy.replica_scope) == ("query_visible", None)


@pytest.mark.parametrize("scope", ["single_server", "connected_replica"])
def test_exact_raw_rows_accepts_only_declared_replica_scopes(scope: str) -> None:
    policy = native_source_snapshot_policy(_config({"mode": "exact_raw_rows", "replica_scope": scope}))
    assert (policy.mode, policy.replica_scope) == ("exact_raw_rows", scope)


@pytest.mark.parametrize(
    "snapshot",
    [
        None,
        False,
        [],
        {},
        {"mode": "EXACT_RAW_ROWS", "replica_scope": "single_server"},
        {"mode": "exact_raw_rows"},
        {"mode": "exact_raw_rows", "replica_scope": None},
        {"mode": "exact_raw_rows", "replica_scope": "all_replicas"},
        {"mode": "query_visible", "replica_scope": "single_server"},
        {"mode": "query_visible", "final": True},
        {"mode": "exact_raw_rows", "replica_scope": "single_server", "final": False},
    ],
)
def test_invalid_or_open_selector_fails_closed(snapshot: object) -> None:
    with pytest.raises(ValueError, match=r"^mssql_native\.source_snapshot_mode_invalid$"):
        native_source_snapshot_policy(_config(snapshot))


@pytest.mark.parametrize("options", [[], False, 0, ""])
def test_falsey_wrong_type_options_cannot_be_mistaken_for_omission(options: object) -> None:
    with pytest.raises(ValueError, match=r"^mssql_native\.source_snapshot_mode_invalid$"):
        native_source_snapshot_policy(SimpleNamespace(options=options))
