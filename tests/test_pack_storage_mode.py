"""Contract tests for Airflow pack storage modes."""

from __future__ import annotations

import pytest

from dpone.gitops.pack_storage_mode import (
    normalize_pack_storage_mode,
    pack_storage_policy_from_mapping,
    pack_storage_policy_from_workload_set,
)


def test_default_mode_is_gitops() -> None:
    assert normalize_pack_storage_mode(None) == "gitops"
    assert normalize_pack_storage_mode("") == "gitops"
    policy = pack_storage_policy_from_mapping({})
    assert policy.mode == "gitops"
    assert policy.commits_git_artifacts is True
    assert policy.allows_git_fallback is False


def test_object_storage_alias_maps_to_remote() -> None:
    assert normalize_pack_storage_mode("object_storage") == "remote"
    policy = pack_storage_policy_from_mapping(
        {
            "mode": "object_storage",
            "kind": "s3",
            "uri_prefix": "s3://bucket/{git_sha}/",
            "latest_index_uri": "s3://bucket/latest/pack-index.json",
            "reader_connection_id": "s3_reader",
            "writer_connection_id": "s3_writer",
        }
    )
    assert policy.mode == "remote"
    assert policy.commits_git_artifacts is False
    assert policy.allows_git_fallback is False
    assert policy.requires_remote_publish is True


def test_hybrid_allows_git_fallback_and_still_publishes() -> None:
    policy = pack_storage_policy_from_mapping(
        {
            "mode": "hybrid",
            "kind": "s3",
            "uri_prefix": "s3://bucket/{git_sha}/",
            "latest_index_uri": "s3://bucket/latest/pack-index.json",
            "reader_connection_id": "s3_reader",
        }
    )
    assert policy.allows_git_fallback is True
    assert policy.commits_git_artifacts is True
    assert policy.requires_remote_publish is True


def test_remote_requires_kind_and_uris() -> None:
    with pytest.raises(ValueError, match="uri_prefix"):
        pack_storage_policy_from_mapping({"mode": "remote", "kind": "s3"})


def test_workload_set_extracts_nested_storage() -> None:
    policy = pack_storage_policy_from_workload_set(
        {
            "gitops": {
                "artifacts": {
                    "airflow_pack": {
                        "storage": {
                            "mode": "gitops",
                        }
                    }
                }
            }
        }
    )
    assert policy.mode == "gitops"
