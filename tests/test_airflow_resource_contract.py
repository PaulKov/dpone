"""Exact resource quantities and authoring/selection compatibility."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest
from dpone_airflow_pack.kubernetes_resources import (
    KubernetesResourceError,
    kubernetes_resources_schema,
    validate_kubernetes_resources,
)
from jsonschema import validate

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.manifest.authoring import AuthoringCompilationError, AuthoringCompiler
from dpone.readiness.airflow_authoring_validation import authoring_check_view
from tests.test_airflow_authoring_v1 import _classic, _flow, _process
from tests.test_airflow_workload_resources import RESOURCES


def _legacy_config(_path: Path) -> dict:
    payload = _process()
    for side in ("source", "sink"):
        endpoint = payload[side]
        endpoint["connection_id"] = endpoint.pop("connection_ref")
        endpoint["connection_type"] = "airflow"
    return payload


@pytest.mark.parametrize(
    ("resource", "requested", "limit"),
    [
        ("cpu", "1000m", "1"),
        ("cpu", ".5", "500m"),
        ("cpu", "1e-3", "1m"),
        ("memory", "1024Mi", "1Gi"),
        ("memory", "1G", "1Gi"),
        ("ephemeral-storage", "2e9", "2Gi"),
        ("memory", "1.5Gi", "1536Mi"),
        ("memory", "1n", "1u"),
        ("cpu", "0", "0"),
        ("memory", "+1", "1."),
    ],
)
def test_exact_quantities_preserved(resource: str, requested: str, limit: str) -> None:
    resources = {"requests": {resource: requested}, "limits": {resource: limit}}
    assert validate_kubernetes_resources(resources, field="airflow.resources") == resources
    validate(resources, kubernetes_resources_schema())


@pytest.mark.parametrize(
    "quantity",
    [
        "1KB",
        "1K",
        "1ki",
        "1MiB",
        " 1",
        "1 ",
        "1\n",
        "-1",
        "NaN",
        "Infinity",
        "",
        "1e",
        "1e99999999999999999",
        "1e-999999999999999",
        "1e-10",
        "9223372036854775808",
        "8Ei",
        "1e20",
        True,
        1,
        0.5,
        None,
        [],
        {},
    ],
)
def test_quantity_rejects_invalid_or_unrepresentable_values(quantity: object) -> None:
    with pytest.raises(KubernetesResourceError, match="airflow.resources.requests.memory"):
        validate_kubernetes_resources({"requests": {"memory": quantity}}, field="airflow.resources")


@pytest.mark.parametrize("resource", ["cpu", "memory", "ephemeral-storage"])
def test_request_limit_comparison_is_not_lexical(resource: str) -> None:
    with pytest.raises(KubernetesResourceError, match="must not exceed"):
        validate_kubernetes_resources(
            {"requests": {resource: "2"}, "limits": {resource: "1000m"}}, field="airflow.resources"
        )


def test_provider_v1_extended_resource_names_retain_structural_compatibility() -> None:
    legacy = {"limits": {"nvidia.com/gpu": "1"}}
    assert validate_kubernetes_resources(legacy, field="provider.resources", allow_extended=True) == legacy
    with pytest.raises(KubernetesResourceError, match="supports only"):
        validate_kubernetes_resources(legacy, field="airflow.resources")


@pytest.mark.parametrize("factory", [_flow, _classic])
def test_resource_only_change_updates_semantic_selection_and_check_view(tmp_path: Path, factory) -> None:
    path = Path("pipelines/orders_daily/pipeline.yaml")
    payload = factory(path)
    compiler = AuthoringCompiler()
    original = compiler.compile(payload, source_path=tmp_path / path, project_root=tmp_path)
    # Existing fingerprints have no added absent-resource key.
    metadata = deepcopy(payload["metadata"])
    metadata["tags"] = sorted(metadata["tags"])
    assert original.semantic_fingerprint == canonical_fingerprint(
        {"metadata": metadata, "processes": list(original.processes)}
    )
    payload["gitops"] = {"airflow": {"resources": deepcopy(RESOURCES)}}
    compiled = compiler.compile(payload, source_path=tmp_path / path, project_root=tmp_path)
    assert original.semantic_fingerprint != compiled.semantic_fingerprint
    details, _ = authoring_check_view(payload, compiled)
    assert details["airflow_resources"] == RESOURCES
    payload["gitops"]["airflow"]["resources"]["requests"]["cpu"] = "300m"
    changed = compiler.compile(payload, source_path=tmp_path / path, project_root=tmp_path)
    assert changed.semantic_fingerprint != compiled.semantic_fingerprint
    payload["gitops"]["airflow"]["resources"]["requests"]["cpu"] = "2"
    with pytest.raises(AuthoringCompilationError, match="gitops.airflow.resources.requests.cpu"):
        compiler.compile(payload, source_path=tmp_path / path, project_root=tmp_path)


@pytest.mark.parametrize(
    ("filename", "factory"),
    [
        ("etl-flow-manifest.schema.json", _flow),
        ("etl-batch-manifest.schema.json", _classic),
        ("etl-config.schema.json", _legacy_config),
    ],
)
def test_public_schemas_share_resource_shape(filename: str, factory) -> None:
    schema = json.loads((Path(__file__).parents[1] / "src/dpone/schema" / filename).read_text())
    resource_schema = schema["properties"]["gitops"]["properties"]["airflow"]["properties"]["resources"]
    assert resource_schema == kubernetes_resources_schema()
    payload = factory(Path("pipelines/orders_daily/pipeline.yaml"))
    payload["gitops"] = {"airflow": {"resources": RESOURCES}}
    validate(payload, schema)
