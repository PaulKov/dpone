from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
from dpone_airflow_pack.deployment_index import AirflowDeploymentIndexError
from dpone_airflow_pack.runtime_artifact_delivery_contract import (
    validate_runtime_artifact_delivery,
)

from dpone.contracts.runtime_artifact_delivery import (
    ARTIFACT_REGISTRY_LOGICAL_REF_PATTERN,
    ARTIFACT_REGISTRY_REF_PATTERN,
    CONFIG_MAP_KEY_PATTERN,
    OCI_RUNTIME_IMAGE_REF_PATTERN,
    STRICT_INIT_FETCH_REQUIRED_FIELDS,
    is_pinned_artifact_registry_ref,
    is_safe_artifact_registry_logical_ref,
    missing_init_fetch_delivery_fields,
    missing_init_fetch_delivery_paths,
    normalize_config_map_ref,
    validate_runtime_image_reference,
)
from dpone.gitops.schema_contracts import get_gitops_schema_contract
from dpone.gitops.schema_validation import GitOpsSchemaValidator

STRICT_SCHEMA_PATHS = (
    Path("docs/schemas/gitops/airflow-deployment-index-v2.schema.json"),
    Path("docs/schemas/gitops/deployment-set-v2.schema.json"),
)
LEGACY_SCHEMA_PATHS = (
    Path("docs/schemas/gitops/airflow-deployment-index.schema.json"),
    Path("docs/schemas/gitops/deployment-set.schema.json"),
    Path("docs/schemas/gitops/airflow-operator-diagnostics.schema.json"),
    Path("docs/schemas/gitops/safe-sample-airflow-deployment-context.schema.json"),
    Path("docs/schemas/gitops/safe-sample-execution-plan.schema.json"),
    Path("docs/schemas/gitops/safe-sample-runtime-execution.schema.json"),
)
ARTIFACT_DIGEST_SCHEMA_PATHS = (
    Path("docs/schemas/gitops/airflow-deployment-index-v2.schema.json"),
    Path("docs/schemas/gitops/safe-sample-airflow-deployment-context.schema.json"),
    Path("docs/schemas/gitops/safe-sample-execution-plan.schema.json"),
    Path("docs/schemas/gitops/safe-sample-runtime-execution.schema.json"),
)

MUTABLE_REGISTRY_REFS = (
    "current",
    "CURRENT",
    "Latest",
    "cache://current/index.json",
    "cache://releases/LATEST/index.json",
    "s3://bucket/releases/Current/index.json",
    "registry.example/dpone:latest",
    "registry.example/dpone@CURRENT",
    "https://registry.example/artifacts?tag=latest",
    "https://registry.example/artifacts#CURRENT",
    "https://registry.example/artifacts?tag=%6catest",
    "cache://releases/%63urrent/index.json",
)

PINNED_REGISTRY_REFS = (
    "dpone-prod-artifacts@sha256:registry",
    "cache://releases/sha256-release/index.json",
    "s3://bucket/releases/sha256:abc/index.json",
)

LOGICAL_REGISTRY_REFS = ("dpone-prod-artifacts", "artifact_registry.v1")
UNSAFE_LOGICAL_REGISTRY_REFS = (
    "https://registry.example/artifacts",
    "user:password@registry",
    "dpone-prod-artifacts?token=value",
    "DPONE_TOKEN=secret",
    "current",
    "sk-proj-1234567890abcdef",
)


@pytest.mark.parametrize("registry_ref", MUTABLE_REGISTRY_REFS)
def test_runtime_artifact_delivery_rejects_mutable_registry_aliases(registry_ref: str) -> None:
    assert is_pinned_artifact_registry_ref(registry_ref) is False
    assert re.fullmatch(ARTIFACT_REGISTRY_REF_PATTERN, registry_ref) is None


@pytest.mark.parametrize("registry_ref", PINNED_REGISTRY_REFS)
def test_runtime_artifact_delivery_accepts_immutable_registry_refs(registry_ref: str) -> None:
    assert is_pinned_artifact_registry_ref(registry_ref) is True
    assert re.fullmatch(ARTIFACT_REGISTRY_REF_PATTERN, registry_ref)


def test_provider_wire_policy_preserves_preview_and_requires_v2_for_init_fetch() -> None:
    preview = {"mode": "local_preview"}

    assert (
        validate_runtime_artifact_delivery(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "runtime_artifact_delivery": preview,
            },
            path=None,
        )
        == preview
    )
    with pytest.raises(AirflowDeploymentIndexError) as exc_info:
        validate_runtime_artifact_delivery(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "runtime_artifact_delivery": _init_fetch_delivery(PINNED_REGISTRY_REFS[0]),
            },
            path=None,
        )

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_DELIVERY_MIGRATION_REQUIRED"


@pytest.mark.parametrize("schema_path", STRICT_SCHEMA_PATHS, ids=lambda path: path.stem)
def test_indexed_runtime_delivery_schemas_require_logical_registry_refs(
    schema_path: Path,
) -> None:
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    patterns = _artifact_registry_ref_patterns(schema)

    assert patterns == (
        ARTIFACT_REGISTRY_LOGICAL_REF_PATTERN,
        ARTIFACT_REGISTRY_LOGICAL_REF_PATTERN,
    )
    for registry_ref in (*MUTABLE_REGISTRY_REFS, *PINNED_REGISTRY_REFS):
        assert all(re.fullmatch(pattern, registry_ref) is None for pattern in patterns)
    for registry_ref in LOGICAL_REGISTRY_REFS:
        assert all(re.fullmatch(pattern, registry_ref) for pattern in patterns)


@pytest.mark.parametrize("schema_path", LEGACY_SCHEMA_PATHS, ids=lambda path: path.stem)
def test_legacy_runtime_delivery_schemas_preserve_compatibility_predicate(
    schema_path: Path,
) -> None:
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    patterns = _artifact_registry_ref_patterns(schema)

    assert patterns == (ARTIFACT_REGISTRY_REF_PATTERN, ARTIFACT_REGISTRY_REF_PATTERN)


def test_v1_indexed_schemas_preserve_legacy_init_fetch_and_local_preview() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    release_id = "sha256:" + "a" * 64
    deployment_id = "sha256:" + "b" * 64
    delivery = _init_fetch_delivery("cache://releases/sha256-release/index.json")
    payloads = (
        (
            Path("docs/schemas/gitops/deployment-set.schema.json"),
            {
                "schema": "dpone.deployment-set.v1",
                "deployment_id": deployment_id,
                "environment": "prod",
                "release_ref": release_id,
                "runtime_artifact_delivery": delivery,
            },
        ),
        (
            Path("docs/schemas/gitops/airflow-deployment-index.schema.json"),
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": release_id,
                "deployment_id": deployment_id,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": delivery,
            },
        ),
    )

    for schema_path, payload in payloads:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        jsonschema.validate(payload, schema)
        jsonschema.validate(
            {
                **payload,
                "runtime_artifact_delivery": {"mode": "local_preview"},
            },
            schema,
        )
        if schema_path.name == "airflow-deployment-index.schema.json":
            assert "bytes" not in schema["$defs"]["artifact"]["required"]


def test_legacy_safe_sample_init_fetch_has_no_v2_only_missing_fields() -> None:
    delivery = {
        "mode": "init_fetch",
        "artifact_registry_ref": "dpone-dev-artifacts",
        "identity": {
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
        },
        "source": {"artifact_registry_ref": "dpone-dev-artifacts"},
        "verify": {"checksums": "required", "attestations": "optional"},
    }

    assert missing_init_fetch_delivery_fields(delivery) == ()
    assert missing_init_fetch_delivery_paths(delivery) == ()
    assert "namespace" not in delivery["identity"]
    assert "registry_config_ref" not in delivery
    assert "trust_policy_ref" not in delivery
    assert "trust_tier" not in delivery
    # Safe-sample must keep the soft v1 checker; strict v2 fields stay KPO-only.
    assert tuple(field for field in STRICT_INIT_FETCH_REQUIRED_FIELDS if field not in delivery) == (
        "trust_tier",
        "registry_config_ref",
    )


def test_v2_indexed_contracts_are_registered_and_python_validation_is_closed() -> None:
    expected = {
        "dpone.deployment-set.v2": "deployment-set-v2",
        "dpone.airflow-deployment-index.v2": "airflow-deployment-index-v2",
    }
    validator = GitOpsSchemaValidator()

    for kind, name in expected.items():
        contract = get_gitops_schema_contract(kind)
        assert contract is not None
        assert contract.name == name
        assert contract.schema["additionalProperties"] is False
        issues = validator.validate(
            {"schema": kind, "secret": "must-not-enter-the-contract"},
            expected_kind=kind,
        )
        assert any(issue.code == "schema_additional_property_forbidden" and issue.path == "secret" for issue in issues)


def test_migration_guide_complete_v2_index_example_matches_public_schema() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    text = Path("docs/airflow-provider-cache-migration.md").read_text(encoding="utf-8")
    example = text.split("<!-- DPONE_V2_INDEX_EXAMPLE_START -->", 1)[1].split(
        "<!-- DPONE_V2_INDEX_EXAMPLE_END -->",
        1,
    )[0]
    payload = json.loads(example.split("```json", 1)[1].split("```", 1)[0])
    schema = json.loads(Path("docs/schemas/gitops/airflow-deployment-index-v2.schema.json").read_text(encoding="utf-8"))

    jsonschema.validate(payload, schema)


@pytest.mark.parametrize("schema_path", ARTIFACT_DIGEST_SCHEMA_PATHS, ids=lambda path: path.stem)
def test_generated_artifact_delivery_schemas_require_canonical_digests(
    schema_path: Path,
) -> None:
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    digest_pattern = schema["$defs"]["sha256"]["pattern"]

    assert digest_pattern == "^sha256:[0-9a-f]{64}$"
    assert re.fullmatch(digest_pattern, "sha256:" + "a" * 64)
    assert re.fullmatch(digest_pattern, "sha256:" + "A" * 64) is None


@pytest.mark.parametrize("registry_ref", LOGICAL_REGISTRY_REFS)
def test_indexed_init_fetch_accepts_only_bounded_logical_registry_refs(registry_ref: str) -> None:
    assert is_safe_artifact_registry_logical_ref(registry_ref) is True


@pytest.mark.parametrize("registry_ref", UNSAFE_LOGICAL_REGISTRY_REFS)
def test_indexed_init_fetch_rejects_uri_assignment_mutable_and_secret_shaped_refs(
    registry_ref: str,
) -> None:
    assert is_safe_artifact_registry_logical_ref(registry_ref) is False


@pytest.mark.parametrize(
    "registry",
    ("registry.example", "registry.example:443", "registry.example:65535"),
)
def test_runtime_image_reference_requires_matching_oci_digest(registry: str) -> None:
    digest = "sha256:" + "a" * 64
    image_ref = f"{registry}/data/dpone-runtime@{digest}"

    assert re.fullmatch(OCI_RUNTIME_IMAGE_REF_PATTERN, image_ref)
    assert validate_runtime_image_reference(image_ref, digest) == image_ref

    with pytest.raises(ValueError, match="runtime image digest"):
        validate_runtime_image_reference(image_ref, "sha256:" + "b" * 64)
    with pytest.raises(ValueError, match="OCI"):
        validate_runtime_image_reference("registry.example/data/dpone-runtime:latest", digest)


@pytest.mark.parametrize("port", (0, 65536, 70000))
def test_runtime_image_reference_rejects_out_of_range_registry_port(port: int) -> None:
    digest = "sha256:" + "a" * 64
    image_ref = f"registry.example:{port}/data/dpone-runtime@{digest}"

    assert re.fullmatch(OCI_RUNTIME_IMAGE_REF_PATTERN, image_ref) is None
    with pytest.raises(ValueError, match="OCI"):
        validate_runtime_image_reference(image_ref, digest)


def test_config_map_reference_is_closed_digest_pinned_and_secret_free() -> None:
    digest = "sha256:" + "c" * 64
    reference = {
        "kind": "kubernetes_config_map",
        "name": "dpone-artifact-registry-4f3a",
        "key": "registry.json",
        "sha256": digest,
    }

    assert re.fullmatch(CONFIG_MAP_KEY_PATTERN, reference["key"])
    assert normalize_config_map_ref(reference, field="registry_config_ref") == reference

    for invalid in (
        {**reference, "token": "secret"},
        {**reference, "name": "dpone-artifact-registry\nTOKEN=secret"},
        {**reference, "key": "../registry.json"},
        {**reference, "sha256": "sha256:" + "A" * 64},
    ):
        with pytest.raises(ValueError):
            normalize_config_map_ref(invalid, field="registry_config_ref")


def _init_fetch_delivery(registry_ref: str) -> dict[str, Any]:
    return {
        "mode": "init_fetch",
        "artifact_registry_ref": registry_ref,
        "identity": {
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
        },
        "source": {"artifact_registry_ref": registry_ref},
        "verify": {
            "checksums": "required",
            "attestations": "required_for_prod",
        },
    }


def _artifact_registry_ref_patterns(value: object) -> tuple[str, ...]:
    patterns: list[str] = []

    def visit(node: object) -> None:
        if isinstance(node, dict):
            properties = node.get("properties")
            if isinstance(properties, dict):
                registry_ref = properties.get("artifact_registry_ref")
                if isinstance(registry_ref, dict) and isinstance(registry_ref.get("pattern"), str):
                    patterns.append(registry_ref["pattern"])
            for nested in node.values():
                visit(nested)
        elif isinstance(node, list):
            for nested in node:
                visit(nested)

    visit(value)
    return tuple(patterns)
