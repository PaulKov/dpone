from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("dpone_ci_quality_shards_test", ROOT / "tools/ci_quality_shards.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_manifest_is_deterministic_and_assigns_every_node_once() -> None:
    module = _load()
    nodeids = ("tests/a.py::test_a", "tests/b.py::test_b", "tests/c.py::test_c", "tests/d.py::test_d")

    first = module.ShardManifest(4, module.normalize_nodeids(nodeids)).payload()
    second = module.ShardManifest(4, module.normalize_nodeids(reversed(nodeids))).payload()

    assert first == second
    assigned = [nodeid for shard in first["shards"] for nodeid in shard["nodeids"]]
    assert sorted(assigned) == sorted(nodeids)
    assert len(assigned) == len(set(assigned))


@pytest.mark.parametrize("nodeids", [(), ("tests/a.py::test_a", "tests/a.py::test_a")])
def test_manifest_rejects_empty_or_duplicate_population(nodeids: tuple[str, ...]) -> None:
    module = _load()

    with pytest.raises(ValueError):
        module.normalize_nodeids(nodeids)


def test_shard_mapping_is_independent_of_collection_order() -> None:
    module = _load()

    assert module.shard_for("tests/a.py::test_a", 4) == module.shard_for("tests/a.py::test_a", 4)


def test_shard_receipt_binds_membership_to_the_exact_head() -> None:
    module = _load()

    receipt = module.shard_receipt(
        nodeids=("tests/a.py::test_a", "tests/b.py::test_b", "tests/c.py::test_c"),
        shard_index=0,
        shard_count=2,
        head_sha="a" * 40,
        python_version="3.12",
        duration_seconds=12.5,
    )

    assert receipt["head_sha"] == "a" * 40
    assert receipt["python_version"] == "3.12"
    assert receipt["selected_nodeids"]
    assert receipt["selected_count"] == len(receipt["selected_nodeids"])
    assert receipt["duration_seconds"] == 12.5


def test_receipt_validation_rejects_missing_shard() -> None:
    module = _load()
    head = "a" * 40
    nodeids = ("tests/a.py::test_a", "tests/b.py::test_b", "tests/c.py::test_c")
    receipts = [
        module.shard_receipt(nodeids=nodeids, shard_index=index, shard_count=2, head_sha=head, python_version="3.12")
        for index in range(2)
    ]

    assert (
        module.validate_receipts(receipts=receipts, head_sha=head, python_version="3.12", shard_count=2)["status"]
        == "PASS"
    )
    with pytest.raises(ValueError, match="receipt count"):
        module.validate_receipts(receipts=receipts[:1], head_sha=head, python_version="3.12", shard_count=2)
    with pytest.raises(ValueError, match="head SHA"):
        module.validate_receipts(receipts=receipts, head_sha="b" * 40, python_version="3.12", shard_count=2)
    with pytest.raises(ValueError, match="interpreter"):
        module.validate_receipts(receipts=receipts, head_sha=head, python_version="3.11", shard_count=2)
