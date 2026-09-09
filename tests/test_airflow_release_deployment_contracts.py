from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.contracts.airflow_deployment import (
    canonical_fingerprint,
    deployment_id,
    is_sha256_digest,
    release_id,
)


def test_release_id_ignores_provenance_and_changes_when_pack_changes() -> None:
    base = {
        "schema": "dpone.release-set.v1",
        "release_id": "sha256:placeholder",
        "created_at": "2026-07-11T12:00:00Z",
        "label": "prod",
        "provenance": {
            "source_commit": "7ac31f2",
            "build_id": "github-actions:1",
            "built_at": "2026-07-11T12:00:00Z",
        },
        "artifacts": {
            "dag_specs": [
                {"id": "orders_daily", "artifact_ref": "cache://dags/orders_daily.json", "sha256": "sha256:dag"}
            ],
            "workload_packs": [
                {"id": "load_orders", "artifact_ref": "cache://packs/load_orders.json", "sha256": "sha256:pack"}
            ],
            "canonical_schemas": [],
        },
    }
    rebuilt = {
        **base,
        "created_at": "2026-07-12T12:00:00Z",
        "provenance": {
            "source_commit": "different-commit",
            "build_id": "github-actions:2",
            "built_at": "2026-07-12T12:00:00Z",
        },
    }
    changed_pack = json.loads(json.dumps(base))
    changed_pack["artifacts"]["workload_packs"][0]["sha256"] = "sha256:changed"

    assert release_id(base) == release_id(rebuilt)
    assert release_id(base) != release_id(changed_pack)


def test_deployment_id_includes_credential_runtime_but_ignores_created_at() -> None:
    payload = {
        "schema": "dpone.deployment-set.v1",
        "deployment_id": "sha256:placeholder",
        "created_at": "2026-07-11T12:10:00Z",
        "environment": "prod",
        "release_ref": "sha256:release",
        "binding_set_ref": "sha256:bindings",
        "connection_registry_ref": "sha256:registry",
        "credential_runtime_ref": "sha256:credential-runtime-a",
        "runtime_image_digest": "sha256:image",
        "airflow_bundle_ref": "git:7ac31f2",
        "runtime_artifact_delivery": {"mode": "init_fetch"},
    }
    rebuilt = {**payload, "created_at": "2026-07-12T12:10:00Z"}
    changed_credential_runtime = {**payload, "credential_runtime_ref": "sha256:credential-runtime-b"}

    assert deployment_id(payload) == deployment_id(rebuilt)
    assert deployment_id(payload) != deployment_id(changed_credential_runtime)


def test_deployment_id_ignores_only_top_level_attestation_metadata() -> None:
    payload = {
        "schema": "dpone.deployment-set.v1",
        "deployment_id": "sha256:placeholder",
        "environment": "prod",
        "release_ref": "sha256:release",
        "attestations": ["build://one"],
        "runtime_artifact_delivery": {
            "mode": "init_fetch",
            "verify": {"checksums": "required", "attestations": "optional"},
        },
    }
    changed_top_level_metadata = {**payload, "attestations": ["build://two"]}
    changed_runtime_policy = json.loads(json.dumps(payload))
    changed_runtime_policy["runtime_artifact_delivery"]["verify"]["attestations"] = "required_for_prod"

    assert deployment_id(payload) == deployment_id(changed_top_level_metadata)
    assert deployment_id(payload) != deployment_id(changed_runtime_policy)


def test_canonical_fingerprint_sorts_artifacts_by_logical_id() -> None:
    left = {
        "items": [
            {"id": "b", "sha256": "sha256:b"},
            {"id": "a", "sha256": "sha256:a"},
        ]
    }
    right = {
        "items": [
            {"id": "a", "sha256": "sha256:a"},
            {"id": "b", "sha256": "sha256:b"},
        ]
    }

    assert canonical_fingerprint(left) == canonical_fingerprint(right)


def test_is_sha256_digest_matches_public_schema_pattern() -> None:
    assert is_sha256_digest("sha256:" + "a" * 64) is True
    assert is_sha256_digest("sha256:" + "A" * 64) is True
    assert is_sha256_digest("sha256:" + "g" * 64) is False
    assert is_sha256_digest("sha256:deployment") is False
    assert is_sha256_digest("sha256:" + "a" * 63) is False
    assert is_sha256_digest(None) is False


def test_schema_files_validate_minimal_release_and_deployment_examples() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema_root = Path("docs/schemas/gitops")

    release_schema = json.loads((schema_root / "release-set.schema.json").read_text(encoding="utf-8"))
    deployment_schema = json.loads((schema_root / "deployment-set.schema.json").read_text(encoding="utf-8"))

    jsonschema.validate(
        {
            "schema": "dpone.release-set.v1",
            "release_id": "sha256:" + "a" * 64,
            "artifacts": {"dag_specs": [], "workload_packs": [], "canonical_schemas": []},
        },
        release_schema,
    )
    jsonschema.validate(
        {
            "schema": "dpone.deployment-set.v1",
            "deployment_id": "sha256:" + "b" * 64,
            "environment": "local-preview",
            "deployment_type": "preview",
            "runnable": False,
            "release_ref": "sha256:" + "a" * 64,
            "runtime_artifact_delivery": {"mode": "local_preview"},
        },
        deployment_schema,
    )
