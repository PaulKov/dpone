"""Author resource preservation, validation, and strict delivery regressions."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from dpone.contracts.airflow_resources import workload_airflow_resources
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.workload_catalog import WorkloadCatalogResolver
from dpone.readiness.airflow_compact_pack_release_helpers import rewrite_strict_init_fetch_dag_spec
from dpone.readiness.airflow_compact_pack_release_models import CompactPackReleaseError
from dpone.readiness.airflow_local_workload_pack import build_local_airflow_workload_pack

RESOURCES = {
    "requests": {"cpu": "250m", "memory": "256Mi", "ephemeral-storage": "2Gi"},
    "limits": {"cpu": "1", "memory": "1Gi", "ephemeral-storage": "8Gi"},
}


def _project(root: Path, resources: object = RESOURCES) -> Path:
    manifest = root / "pipeline.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "name": "orders",
                "source": {"type": "postgres", "connection_id": "pg", "table": {"schema": "src", "name": "orders"}},
                "sink": {
                    "type": "postgres",
                    "connection_id": "pg",
                    "table": {"schema": "dst", "name": "orders"},
                    "mode": "append",
                },
                "gitops": {"airflow": {"resources": resources}},
            }
        )
    )
    (root / "catalog.yaml").write_text(yaml.safe_dump({"workloads": {"orders": {"manifest": "pipeline.yaml"}}}))
    (root / "workloads.yaml").write_text(
        yaml.safe_dump(
            {
                "gitops": {
                    "includes": [{"path": "catalog.yaml"}],
                    "defaults": {"image": "registry.example/dpone:dev"},
                }
            }
        )
    )
    return manifest


def _pack(root: Path) -> dict:
    catalog = WorkloadCatalogResolver(repo_root=root).resolve("workloads.yaml", env="dev")
    assert not catalog.blockers
    return (
        AirflowCompactPackBuilder()
        .build(workload=catalog.workloads[0], repo_root=root, output_path="packs/orders.json")
        .to_jsonable()
    )


def test_author_resources_survive_pack_and_change_identity(tmp_path: Path) -> None:
    _project(tmp_path)
    first = _pack(tmp_path)
    assert first["provider_execution"]["pod_spec"]["spec"]["containers"][0]["resources"] == RESOURCES
    assert first["pod_spec"]["spec"]["containers"][0]["resources"] == RESOURCES
    assert _pack(tmp_path)["pack_fingerprint"] == first["pack_fingerprint"]
    changed = deepcopy(RESOURCES)
    changed["limits"]["ephemeral-storage"] = "16Gi"
    _project(tmp_path, changed)
    assert _pack(tmp_path)["pack_fingerprint"] != first["pack_fingerprint"]


def test_local_preview_preserves_manifest_resources(tmp_path: Path) -> None:
    source = _project(tmp_path)
    pack = build_local_airflow_workload_pack(
        root=tmp_path,
        source_path=source,
        pipeline_payload=yaml.safe_load(source.read_text()),
        pipeline_id="orders",
        runtime_image="registry.example/dpone:dev",
        runtime_image_digest="sha256:" + "a" * 64,
    )
    assert pack["provider_execution"]["pod_spec"]["spec"]["containers"][0]["resources"] == RESOURCES


def test_resources_use_existing_catalog_leaf_precedence(tmp_path: Path) -> None:
    _project(tmp_path, {"requests": {"cpu": "200m"}})
    config = yaml.safe_load((tmp_path / "workloads.yaml").read_text())
    config["gitops"]["defaults"]["airflow"] = {"resources": RESOURCES}
    config["gitops"]["environments"] = {"dev": {"airflow": {"resources": {"limits": {"memory": "2Gi"}}}}}
    (tmp_path / "workloads.yaml").write_text(yaml.safe_dump(config))
    (tmp_path / "catalog.yaml").write_text(
        yaml.safe_dump(
            {
                "workloads": {
                    "orders": {
                        "manifest": "pipeline.yaml",
                        "airflow": {"resources": {"requests": {"cpu": "300m"}}},
                    }
                }
            }
        )
    )
    expected = deepcopy(RESOURCES)
    expected["requests"]["cpu"] = "300m"
    expected["limits"]["memory"] = "2Gi"
    assert _pack(tmp_path)["provider_execution"]["pod_spec"]["spec"]["containers"][0]["resources"] == expected


def test_absent_resources_preserve_legacy_pod_defaults(tmp_path: Path) -> None:
    source = _project(tmp_path)
    payload = yaml.safe_load(source.read_text())
    del payload["gitops"]
    source.write_text(yaml.safe_dump(payload))
    pack = _pack(tmp_path)
    assert "resources" not in pack["provider_execution"]["pod_spec"]["spec"]["containers"][0]
    assert "resources" not in pack["pod_spec"]["spec"]["containers"][0]


def test_resource_named_placement_labels_remain_compatible() -> None:
    labels = {"resources": "high-memory", "requests": "batch"}
    for airflow in (
        {"node_selector": labels},
        {"nodeSelector": labels},
        {"runner": {"placement": {"node_selector": labels}}},
    ):
        assert workload_airflow_resources({"airflow": airflow}) is None


@pytest.mark.parametrize(
    "resources",
    [
        None,
        {},
        {"requests": {}},
        {"requests": {"cpu": "nope"}},
        {"requests": {"cpu": "0.0001"}},
        {"requests": {"gpu": "1"}},
        {"requests": {"memory": "2Gi"}, "limits": {"memory": "1024Mi"}},
        {"requests": {"cpu": 1}},
        {"limits": {"memory": "-1Gi"}},
    ],
)
def test_catalog_rejects_invalid_resources_before_build(tmp_path: Path, resources: object) -> None:
    _project(tmp_path, resources)
    report = WorkloadCatalogResolver(repo_root=tmp_path).resolve("workloads.yaml", env="dev")
    assert report.blockers
    assert "airflow.resources" in report.blockers[0].message


@pytest.mark.parametrize(
    "key", ["pod_template_dict", "pod_template_file", "full_pod_spec", "container_resources", "resources"]
)
def test_strict_rewrite_rejects_resource_override_instead_of_dropping(key: str) -> None:
    with pytest.raises(CompactPackReleaseError, match=rf"operator_overrides\.{key}.*airflow.resources"):
        rewrite_strict_init_fetch_dag_spec({"operator_overrides": {key: {}}}, workload_ids=["orders"])


@pytest.mark.parametrize(
    ("value", "path"),
    [
        (
            {"executor_config": {"pod_override": {"spec": {"containers": [{"resources": RESOURCES}]}}}},
            "executor_config.pod_override.spec.containers[0].resources",
        ),
        ({"init_containers": [{"resources": RESOURCES}]}, "init_containers[0].resources"),
    ],
)
def test_nested_resource_overrides_fail_with_full_path(value: dict, path: str) -> None:
    with pytest.raises(CompactPackReleaseError) as caught:
        rewrite_strict_init_fetch_dag_spec({"operator_overrides": value}, workload_ids=["orders"])
    assert f"operator_overrides.{path}" in str(caught.value)
    assert "airflow.resources" in str(caught.value)
