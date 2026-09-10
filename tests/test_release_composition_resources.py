"""Resource-only authoring survives the ordinary release source-closure check."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from dpone.contracts.release_composition_ordinary import OrdinaryReleaseInventoryError
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.workload_catalog import WorkloadCatalogResolver
from dpone.manifest.release_composition_ordinary_closure import OrdinaryPackClosureVerifier
from tests.test_airflow_workload_resources import RESOURCES
from tests.test_release_composition_ordinary import IMAGE, _capture, _mutate_pack, ordinary_root


def _resource_root(tmp_path: Path) -> Path:
    root = ordinary_root(tmp_path, extra_manifest=yaml.safe_dump({"gitops": {"airflow": {"resources": RESOURCES}}}))
    author = tmp_path / "author"
    (author / "catalog.yaml").write_text(
        yaml.safe_dump({"domain": "sample", "workloads": {"orders": {"manifest": "transfer.yaml"}}})
    )
    (author / "workloads.yaml").write_text(
        yaml.safe_dump({"gitops": {"includes": [{"path": "catalog.yaml"}], "defaults": {"image": IMAGE}}})
    )
    catalog = WorkloadCatalogResolver(repo_root=author).resolve("workloads.yaml", env="dev")
    assert not catalog.blockers
    pack = AirflowCompactPackBuilder().build(
        workload=catalog.workloads[0], repo_root=author, output_path="orders/airflow-pack.json"
    )
    assert not pack.blockers
    (root / "orders/airflow-pack.json").write_text(pack.to_json())
    return root


def test_manifest_resources_survive_ordinary_source_reconstruction(tmp_path: Path) -> None:
    result = _capture(_resource_root(tmp_path))
    pack = json.loads(result.pack_files["packs/orders.airflow-pack.json"])
    assert pack["provider_execution"]["pod_spec"]["spec"]["containers"][0]["resources"] == RESOURCES
    assert result.relation_writes[0].relation == "orders"


def test_rehashed_resource_projection_cannot_bypass_source_reconstruction(tmp_path: Path) -> None:
    root = _resource_root(tmp_path)

    def change_resources(pack: dict) -> None:
        pack["provider_execution"]["pod_spec"]["spec"]["containers"][0]["resources"]["limits"]["cpu"] = "2"

    _mutate_pack(root, change_resources)
    with pytest.raises(OrdinaryReleaseInventoryError, match="differs from its declarative producer"):
        _capture(root)


@pytest.mark.parametrize(
    "gitops",
    [
        None,
        {},
        {"airflow": {}},
        {"airflow": {"resources": RESOURCES, "runner": {}}},
        {"airflow": {"resources": RESOURCES}, "runner": {}},
        {"airflow": {"resources": {"requests": {"cpu": "2"}, "limits": {"cpu": "1"}}}},
    ],
)
def test_resource_exception_keeps_other_gitops_authority_closed(gitops: object) -> None:
    manifest = {"name": "orders", "source": {"type": "postgres"}, "sink": {"type": "postgres"}, "gitops": gitops}
    with pytest.raises(OrdinaryReleaseInventoryError, match="gitops.airflow.resources"):
        OrdinaryPackClosureVerifier._require_single_transfer(manifest, "orders")
