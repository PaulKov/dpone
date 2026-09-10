"""Producer-backed, offline verification of the standalone composition boundary."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from dpone.app.release_composition import build_ordinary_release_inventory_reader
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.airflow_dag_spec import DagSpecNode, GitOpsAirflowDagSpec, parse_dag_declaration
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
from dpone.manifest.confined_files import read_confined_file

IMAGE = "example.invalid/runtime@sha256:" + "a" * 64
SIDECAR = "example.invalid/sidecar@sha256:" + "b" * 64


def ordinary_root(tmp_path: Path, *, sql_file: bool = False, extra_manifest: str = "") -> Path:
    """Use the public deterministic producers, never hand-assert producer identity."""
    author = tmp_path / "author"
    author.mkdir()
    (author / "transfer.yaml").write_text(
        "name: orders\nsource:\n  type: postgres\n  connection_ref: source\n"
        "  query: SELECT 1 AS id\nsink:\n  type: postgres\n  connection_ref: target\n"
        "  table:\n    schema: public\n    name: orders\n  strategy:\n    mode: full_refresh\n"
    )
    if sql_file:
        path = author / "transfer.yaml"
        path.write_text(path.read_text().replace("query: SELECT 1 AS id", "sql_file: query.sql"))
        (author / "query.sql").write_text("SELECT 1 AS id")
    if extra_manifest:
        path = author / "transfer.yaml"
        path.write_text(path.read_text() + extra_manifest)
    workload = GitOpsWorkloadDefinition(
        workload_id="orders",
        manifest="transfer.yaml",
        domain="sample",
        catalog_path="domains/sample.yaml",
        effective_config={"image": IMAGE, "image_digest": "sha256:" + "a" * 64},
        provenance={},
    )
    pack = AirflowCompactPackBuilder().build(
        workload=workload,
        output_path="orders/airflow-pack.json",
        repo_root=author,
    )
    assert not pack.blockers
    declaration, issues = parse_dag_declaration(
        "ordinary",
        {"schedule": None, "start_date": "2026-01-01", "workloads": ["orders"]},
    )
    assert declaration is not None and not issues
    dag = GitOpsAirflowDagSpec(
        declaration=declaration,
        domain="sample",
        source_path="domains/sample.yaml",
        nodes=(DagSpecNode(node_id="orders", workload_id="orders"),),
        edges=(),
        topological_order=("orders",),
    )
    root = tmp_path / "ordinary"
    (root / "_dags").mkdir(parents=True)
    (root / "orders").mkdir()
    (root / "_dags/ordinary.dag-spec.json").write_text(dag.to_json())
    (root / "orders/airflow-pack.json").write_text(pack.to_json())
    return root


def test_capture_real_producer_detaches_source_and_rewrites_transport(tmp_path: Path) -> None:
    root = ordinary_root(tmp_path)
    reader = build_ordinary_release_inventory_reader(read_file=read_confined_file)
    result = reader.capture(root, xcom_sidecar_image=SIDECAR)
    assert set(result.files) == {"_dags/ordinary.dag-spec.json", "orders/airflow-pack.json"}
    assert set(result.dag_files) == {"dags/ordinary.dag-spec.json"}
    pack = json.loads(result.pack_files["packs/orders.airflow-pack.json"])
    assert pack["connection_projection"] == {}
    assert pack["xcom"]["sidecar_image"] == SIDECAR
    assert result.relation_writes[0].relation == "orders"
    before = result.inventory_sha256
    (root / "orders/airflow-pack.json").unlink()
    assert result.inventory_sha256 == before
    with pytest.raises(TypeError):
        result.inventory["dag_specs"][0]["id"] = "changed"


def _mutate_pack(root: Path, mutate: Callable[[dict[str, Any]], None], *, resign: bool = True) -> None:
    from dpone_airflow_pack.pack_identity import compute_pack_fingerprint

    path = root / "orders/airflow-pack.json"
    pack = json.loads(path.read_bytes())
    mutate(pack)
    if resign:
        pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    path.write_text(json.dumps(pack))


def _capture(root: Path, sidecar: str = SIDECAR):
    return build_ordinary_release_inventory_reader(read_file=read_confined_file).capture(
        root, xcom_sidecar_image=sidecar
    )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda pack: pack.update(runtime_payload_ids=[]),
        lambda pack: pack.update(connection_projection={"mode": "kubernetes_secret_volume"}),
        lambda pack: pack.update(connection_projection=None),
        lambda pack: pack["runtime_bootstrap"]["commands"].update(
            hidden={"argv": ["dpone", "dbt", "execute-pack", "hidden.json"]}
        ),
        lambda pack: pack["runtime_bootstrap"]["commands"]["__workload__"].update(argv=["sh", "-c", "true"]),
        lambda pack: pack["runtime_manifest"].update(sha256="sha256:" + "0" * 64),
        lambda pack: pack["workload_dependencies"].clear(),
        lambda pack: pack["workload_dependencies"][0].update(sha256="0" * 64),
        lambda pack: pack.update(runtime_command="dpone dbt execute-pack hidden.json"),
        lambda pack: pack.update(producer="custom producer"),
        lambda pack: pack["workload"]["effective_config"].update(airflow={"runner": {"embed_paths": ["hidden.json"]}}),
        lambda pack: pack.update(include_live_gates=True),
        lambda pack: pack.update(unknown_authority={"argv": ["true"]}),
    ],
)
def test_refingerprinted_unsupported_or_forged_pack_is_rejected(tmp_path: Path, mutation) -> None:
    root = ordinary_root(tmp_path)
    _mutate_pack(root, mutation)
    with pytest.raises(ValueError):
        _capture(root)


def test_original_fingerprint_is_checked_before_rewriting(tmp_path: Path) -> None:
    root = ordinary_root(tmp_path)
    _mutate_pack(root, lambda pack: pack.update(runtime_command="changed"), resign=False)
    with pytest.raises(ValueError):
        _capture(root)


def test_source_inventory_does_not_depend_on_transport(tmp_path: Path) -> None:
    root = ordinary_root(tmp_path)
    first = _capture(root)
    second = _capture(root, "example.invalid/sidecar@sha256:" + "c" * 64)
    assert first.inventory_sha256 == second.inventory_sha256
    assert first.files == second.files
    assert first.pack_files != second.pack_files


@pytest.mark.parametrize("repository", ["registry.example/team/xcom", "registry.example:65535/team/xcom"])
def test_valid_sidecar_preserves_inventory_identity_and_exact_transport(tmp_path: Path, repository: str) -> None:
    root = ordinary_root(tmp_path)
    original = _capture(root)
    image = repository + "@sha256:" + "c" * 64
    result = _capture(root, image)
    assert result.inventory_sha256 == original.inventory_sha256
    assert result.files == original.files
    assert json.loads(result.pack_files["packs/orders.airflow-pack.json"])["xcom"]["sidecar_image"] == image


def test_semantically_empty_projection_is_supported(tmp_path: Path) -> None:
    root = ordinary_root(tmp_path)
    _mutate_pack(root, lambda pack: pack.update(connection_projection={}))
    assert _capture(root).relation_writes[0].workflow_id == "ordinary"


@pytest.mark.parametrize(
    "entry", ["orphan/airflow-pack.json", "runtime/hidden.json", "release-set.json", "_dags/unexpected.txt"]
)
def test_orphan_or_unregistered_root_artifact_is_rejected(tmp_path: Path, entry: str) -> None:
    root = ordinary_root(tmp_path)
    path = root / entry
    path.parent.mkdir(exist_ok=True)
    path.write_text("{}")
    with pytest.raises(ValueError):
        _capture(root)


def test_duplicate_dag_membership_is_rejected(tmp_path: Path) -> None:
    from dpone_airflow_pack.dag_spec_loader import compute_dag_spec_fingerprint

    root = ordinary_root(tmp_path)
    spec = json.loads((root / "_dags/ordinary.dag-spec.json").read_bytes())
    spec["dag_id"] = "second"
    spec["spec_fingerprint"] = compute_dag_spec_fingerprint(spec)
    (root / "_dags/second.dag-spec.json").write_text(json.dumps(spec))
    with pytest.raises(ValueError, match="duplicated"):
        _capture(root)


def test_missing_pack_is_rejected(tmp_path: Path) -> None:
    root = ordinary_root(tmp_path)
    (root / "orders/airflow-pack.json").unlink()
    with pytest.raises(ValueError):
        _capture(root)


def test_symlink_is_rejected_without_reading_target(tmp_path: Path) -> None:
    root = ordinary_root(tmp_path)
    path = root / "orders/airflow-pack.json"
    target = tmp_path / "external.json"
    path.rename(target)
    path.symlink_to(target)
    with pytest.raises(ValueError, match="symlink"):
        _capture(root)


def test_oversized_source_rejected_before_acquisition(tmp_path: Path) -> None:
    root = ordinary_root(tmp_path)
    with (root / "orders/airflow-pack.json").open("wb") as stream:
        stream.truncate(8 * 1024 * 1024 + 1)

    def forbidden_read(*args, **kwargs):
        pytest.fail("oversized sources must fail before acquisition")

    with pytest.raises(ValueError, match="limit"):
        build_ordinary_release_inventory_reader(read_file=forbidden_read).capture(root, xcom_sidecar_image=SIDECAR)


def test_bad_sidecar_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        _capture(ordinary_root(tmp_path), "sidecar:latest")


def test_capture_constructor_detaches_nested_input_and_rejects_unknown_types() -> None:
    from dpone.contracts.release_composition_ordinary import OrdinaryReleaseCapture

    inventory = {"schema": "test", "dag_specs": [{"id": "before"}]}
    files = {"a": b"before"}
    result = OrdinaryReleaseCapture(inventory, files, {}, {}, ())
    identity = result.inventory_sha256
    inventory["dag_specs"][0]["id"] = "after"
    files["a"] = b"after"
    assert result.inventory_sha256 == identity and result.files["a"] == b"before"
    with pytest.raises(TypeError, match="unsupported"):
        OrdinaryReleaseCapture({"bad": object()}, {}, {}, {}, ())


def _replace_archive(pack: dict, files: dict[str, bytes]) -> None:
    from dpone.gitops.airflow_compact_pack_bootstrap import RuntimePayloadBuilder

    pack["runtime_payload"] = (
        RuntimePayloadBuilder(repo_root=Path("."), paths=(), generated_files=files).build().to_jsonable()
    )


def test_undeclared_archive_member_rejected_even_with_correct_hashes(tmp_path: Path) -> None:
    root = ordinary_root(tmp_path)
    source = (tmp_path / "author/transfer.yaml").read_bytes()
    _mutate_pack(
        root, lambda pack: _replace_archive(pack, {"transfer.yaml": source, "runtime/dbt-execution-pack.json": b"{}"})
    )
    with pytest.raises(ValueError, match="undeclared"):
        _capture(root)


def test_rehashed_archive_manifest_drift_rejected(tmp_path: Path) -> None:
    root = ordinary_root(tmp_path)
    source = (tmp_path / "author/transfer.yaml").read_bytes().replace(b"name: orders", b"name: changed")
    _mutate_pack(root, lambda pack: _replace_archive(pack, {"transfer.yaml": source}))
    with pytest.raises(ValueError):
        _capture(root)


def test_rehashed_sql_pin_cannot_replace_dependency_reconstruction(tmp_path: Path) -> None:
    import hashlib

    root = ordinary_root(tmp_path)
    source = (tmp_path / "author/transfer.yaml").read_bytes().replace(b"query: SELECT 1 AS id", b"sql_file: absent.sql")

    def mutation(pack):
        _replace_archive(pack, {"transfer.yaml": source})
        pack["workload_dependencies"][0]["sha256"] = hashlib.sha256(source).hexdigest()
        pack["runtime_manifest"]["sha256"] = "sha256:" + hashlib.sha256(source).hexdigest()

    _mutate_pack(root, mutation)
    with pytest.raises(ValueError):
        _capture(root)


def test_bad_dag_fingerprint_rejected(tmp_path: Path) -> None:
    root = ordinary_root(tmp_path)
    path = root / "_dags/ordinary.dag-spec.json"
    spec = json.loads(path.read_bytes())
    spec["schedule"] = "@hourly"
    path.write_text(json.dumps(spec))
    with pytest.raises(ValueError, match="fingerprint"):
        _capture(root)


def test_duplicate_json_keys_rejected(tmp_path: Path) -> None:
    root = ordinary_root(tmp_path)
    path = root / "orders/airflow-pack.json"
    path.write_text(
        path.read_text().replace('"kind": "gitops.airflow_pack",', '"kind": "dbt", "kind": "gitops.airflow_pack",', 1)
    )
    with pytest.raises(ValueError):
        _capture(root)


def test_inventory_dict_is_a_detached_json_projection(tmp_path: Path) -> None:
    capture = _capture(ordinary_root(tmp_path))
    projection = capture.inventory_dict()
    assert json.loads(json.dumps(projection)) == projection
    original = capture.inventory_sha256
    projection["dag_specs"][0]["id"] = "changed"
    assert capture.inventory_sha256 == original


def test_real_producer_sql_file_closure_is_supported(tmp_path: Path) -> None:
    result = _capture(ordinary_root(tmp_path, sql_file=True))
    pack = json.loads(result.pack_files["packs/orders.airflow-pack.json"])
    assert {item["path"] for item in pack["workload_dependencies"]} == {"transfer.yaml", "query.sql"}


def test_real_producer_unsupported_manifest_extension_is_rejected(tmp_path: Path) -> None:
    root = ordinary_root(tmp_path, extra_manifest="runtime: {}\n")
    with pytest.raises(ValueError, match="plain transfer"):
        _capture(root)


def test_archive_checksum_failure_has_sanitized_inventory_error(tmp_path: Path) -> None:
    from dpone.contracts.release_composition_ordinary import OrdinaryReleaseInventoryError

    root = ordinary_root(tmp_path)
    _mutate_pack(root, lambda pack: pack["runtime_payload"]["archive"].update(sha256="sha256:" + "0" * 64))
    with pytest.raises(OrdinaryReleaseInventoryError):
        _capture(root)
