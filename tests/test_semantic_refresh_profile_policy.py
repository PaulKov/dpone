"""Platform-profile activation tests for semantic refresh V2."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from dpone.contracts.semantic_refresh_profile import SemanticRefreshProfilePolicy
from dpone.manifest.dbt_publish_profiles import DbtPublishProfileRegistry


def _policy() -> dict[str, object]:
    return {
        "schema": "dpone.semantic-refresh-profile.v1",
        "enabled": True,
        "capability": "scope_stable_event_fact",
        "scope": {"grain": "day", "timezone": "UTC", "interval": "half_open"},
        "mutation": {"protocol": "update_insert_v1", "deletes": "ignore_missing"},
        "initial_load": "require_existing_complete_relation",
        "concurrency": "exclusive_workflow",
        "source_snapshot": "snapshot",
        "publication": {
            "database_engine": "Atomic",
            "table_engine": "MergeTree",
            "replica_count": 1,
            "strategy": "full_table_exchange",
        },
        "workflow_publish_atomicity": "none",
        "automatic_sql_retry": False,
    }


def test_semantic_refresh_profile_is_closed_and_digest_bound() -> None:
    policy = SemanticRefreshProfilePolicy.from_mapping(_policy())

    assert policy.to_jsonable() == _policy()
    assert policy.profile_sha256.startswith("sha256:")


def test_registry_reads_refresh_only_from_platform_profile(tmp_path: Path) -> None:
    source = Path("examples/dbt-inline-publishing/dpone/dbt-publish-profiles.yml")
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["schema"] = "dpone.dbt-publish-policy.v2"
    payload["profiles"]["mssql_to_clickhouse_mart"]["refresh"] = _policy()
    path = tmp_path / "dbt-publish-profiles.yml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    registry, issues = DbtPublishProfileRegistry.load(
        "examples/dbt-inline-publishing/fixtures/manifest.v12.json",
        path,
    )

    assert issues == ()
    assert registry is not None
    profile = registry.profile("mssql_to_clickhouse_mart")
    assert profile is not None
    assert profile.semantic_refresh is not None
    assert profile.semantic_refresh.to_jsonable() == _policy()


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("publication", "replica_count"), 2),
        (("mutation", "deletes"), "delete_missing"),
        (("automatic_sql_retry",), True),
    ],
)
def test_semantic_refresh_profile_rejects_cell_expansion(
    path: tuple[str, ...],
    value: object,
) -> None:
    payload = deepcopy(_policy())
    target = payload
    for component in path[:-1]:
        nested = target[component]
        assert isinstance(nested, dict)
        target = nested
    target[path[-1]] = value

    with pytest.raises(ValueError, match="closed V2.0 capability cell"):
        SemanticRefreshProfilePolicy.from_mapping(payload)
