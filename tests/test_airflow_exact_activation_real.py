"""Installed-Airflow proof for exact activation tags, ACK, and serialization."""

from __future__ import annotations

import json
import shutil
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("airflow", reason="installed Airflow is required")

from airflow.providers.dpone import load_and_acknowledge_dpone_dags  # noqa: E402
from dpone_airflow_pack.dag_spec_loader import compute_dag_spec_fingerprint  # noqa: E402

from tests.test_airflow_dag_loader_contract import (  # noqa: E402
    DEPLOYMENT_ID,
    RELEASE_ID,
    _sha256,
    _write_deployment_index_fixture,
)

ACTIVATION_ID = "12345678-1234-4234-9234-123456789abc"


def _serializer() -> Any:
    module = import_module("airflow.serialization.serialized_objects")
    for class_name in ("SerializedDAG", "DagSerialization"):
        serializer = getattr(module, class_name, None)
        if serializer is None:
            continue
        for method in ("to_dict", "serialize_dag"):
            candidate = getattr(serializer, method, None)
            if candidate is not None:
                return candidate
    raise RuntimeError("Airflow serialization API is unavailable")


def _activate_fixture(tmp_path: Path, *, selected_node: bool = False) -> tuple[Path, Path]:
    original_index = _write_deployment_index_fixture(tmp_path)
    if selected_node:
        _select_single_process_fixture(original_index)
    cache_root = tmp_path / ".dpone-cache"
    activation = (
        cache_root
        / "activations"
        / "prod"
        / DEPLOYMENT_ID.replace(
            ":",
            "-",
        )
    )
    activation.parent.mkdir(parents=True)
    shutil.copytree(original_index.parent, activation)
    activated_index = activation / "airflow-index.json"
    index_payload = json.loads(activated_index.read_text(encoding="utf-8"))
    index_payload["environment"] = "prod"
    activated_index.write_text(
        json.dumps(index_payload, sort_keys=True),
        encoding="utf-8",
    )
    (cache_root / "current").symlink_to(
        Path("activations") / "prod" / activation.name,
        target_is_directory=True,
    )
    (cache_root / "current-pointer.json").write_text(
        json.dumps(
            {
                "schema": "dpone.current-pointer.v1",
                "activation_id": ACTIVATION_ID,
                "environment": "prod",
                "deployment_id": DEPLOYMENT_ID,
                "release_id": RELEASE_ID,
                "promoted_by": "test://airflow-compat",
                "promoted_at": "2026-07-27T09:00:00+00:00",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return (
        cache_root / "current" / "airflow-index.json",
        cache_root / "status" / "loader-ack.json",
    )


def _select_single_process_fixture(index_path: Path) -> None:
    cache_root = index_path.parents[3]
    index = json.loads(index_path.read_text(encoding="utf-8"))
    pack_entry = index["workload_packs"][0]
    pack_path = cache_root / str(pack_entry["artifact_ref"]).removeprefix("cache://")
    pack = json.loads(pack_path.read_text(encoding="utf-8"))
    pack.update(
        {
            "runtime_selection": {"mode": "process_plan", "required_for_selected_nodes": True},
            "process_plans": {
                "load_orders": {
                    "selector": "load_orders",
                    "dag_node": {
                        "node_id": "load_orders",
                        "process_name": "load_orders",
                        "visibility": "inline",
                        "task_group": None,
                        "estimated_visible_tasks": 1,
                        "depends_on_process_selectors": [],
                    },
                    "runtime_commands": {
                        "inline": "dpone run manifests/orders.yaml --selector load_orders --format json",
                        "expanded": "dpone run manifests/orders.yaml --selector load_orders --format json",
                    },
                    "mapping_plan": {},
                    "steps": [],
                }
            },
        }
    )
    pack_path.write_text(json.dumps(pack, sort_keys=True), encoding="utf-8")
    pack_entry.update({"sha256": _sha256(pack_path), "bytes": pack_path.stat().st_size})

    spec_entry = index["dag_specs"][0]
    spec_path = cache_root / str(spec_entry["artifact_ref"]).removeprefix("cache://")
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    spec["nodes"][0].update({"selector": "load_orders", "visibility": "inline", "estimated_visible_tasks": 1})
    spec["spec_fingerprint"] = compute_dag_spec_fingerprint(spec)
    spec_path.write_text(json.dumps(spec, sort_keys=True), encoding="utf-8")
    spec_entry.update({"sha256": _sha256(spec_path), "bytes": spec_path.stat().st_size})
    index_path.write_text(json.dumps(index, sort_keys=True), encoding="utf-8")


def test_exact_activation_survives_real_airflow_serialization(
    tmp_path: Path,
) -> None:
    index_path, ack_path = _activate_fixture(tmp_path)
    namespace: dict[str, object] = {}

    result = load_and_acknowledge_dpone_dags(
        namespace,
        index_path=index_path,
        ack_path=ack_path,
    )

    dag = namespace["orders_daily"]
    activation_tag = f"dpone_activation:{ACTIVATION_ID}"
    assert activation_tag in dag.tags
    assert result.report.activation_id == ACTIVATION_ID
    assert result.acknowledgement.activation_id == ACTIVATION_ID
    assert json.loads(ack_path.read_text(encoding="utf-8"))["activation_id"] == ACTIVATION_ID
    serialized = _serializer()(dag)
    encoded = json.dumps(serialized, default=str, sort_keys=True)
    assert activation_tag in encoded


def test_exact_activation_preserves_selected_single_process_task_identity(tmp_path: Path) -> None:
    index_path, ack_path = _activate_fixture(tmp_path, selected_node=True)
    namespace: dict[str, object] = {}

    result = load_and_acknowledge_dpone_dags(namespace, index_path=index_path, ack_path=ack_path)

    assert result.report.errors == ()
    dag = namespace["orders_daily"]
    assert set(dag.task_dict) == {"load_orders__dpone_runtime"}

    index = json.loads(index_path.read_text(encoding="utf-8"))
    pack_ref = str(index["workload_packs"][0]["artifact_ref"])
    pack_path = index_path.parents[1] / pack_ref.removeprefix("cache://")
    pack = json.loads(pack_path.read_text(encoding="utf-8"))
    assert "--selector load_orders" in pack["process_plans"]["load_orders"]["runtime_commands"]["inline"]

    serialized = json.dumps(_serializer()(dag), default=str, sort_keys=True)
    assert "load_orders__dpone_runtime" in serialized
