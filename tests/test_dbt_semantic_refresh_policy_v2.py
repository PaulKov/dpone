from __future__ import annotations

from pathlib import Path

import yaml

from dpone.manifest.dbt_publish_profiles import DbtPublishProfileRegistry

ROOT = Path(__file__).parents[1]
DEMO_POLICY = ROOT / "examples" / "dbt-inline-publishing" / "dpone" / "dbt-publish-profiles.yml"


def _v2_policy() -> dict:
    policy = yaml.safe_load(DEMO_POLICY.read_text(encoding="utf-8"))
    policy["schema"] = "dpone.dbt-publish-policy.v2"
    next(iter(policy["profiles"].values()))["refresh"] = {
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
    return policy


def test_v2_registry_parses_semantic_refresh_only_under_v2_discriminator(tmp_path: Path) -> None:
    path = tmp_path / "policy.yml"
    path.write_text(yaml.safe_dump(_v2_policy(), sort_keys=False), encoding="utf-8")

    registry, issues = DbtPublishProfileRegistry.load(tmp_path / "target" / "manifest.json", path)

    assert issues == ()
    assert registry is not None
    assert registry.profile("mssql_to_clickhouse_mart").semantic_refresh is not None


def test_v1_parser_rejects_v2_refresh_bytes(tmp_path: Path) -> None:
    policy = _v2_policy()
    policy["schema"] = "dpone.dbt-publish-policy.v1"
    path = tmp_path / "policy.yml"
    path.write_text(yaml.safe_dump(policy, sort_keys=False), encoding="utf-8")

    registry, issues = DbtPublishProfileRegistry.load(tmp_path / "target" / "manifest.json", path)

    assert registry is None
    assert {issue.code for issue in issues} == {"DPONE_DBT_PROFILES_INVALID"}


def test_v2_policy_rejects_bootstrap_instead_of_locked_initial_load(tmp_path: Path) -> None:
    policy = _v2_policy()
    refresh = next(iter(policy["profiles"].values()))["refresh"]
    refresh["bootstrap"] = refresh.pop("initial_load")
    path = tmp_path / "policy.yml"
    path.write_text(yaml.safe_dump(policy, sort_keys=False), encoding="utf-8")

    registry, issues = DbtPublishProfileRegistry.load(
        tmp_path / "target" / "manifest.json",
        path,
    )

    assert registry is None
    assert {issue.code for issue in issues} == {"DPONE_DBT_PROFILES_INVALID"}
