"""Generate explicit synthetic legacy-shape vectors, never source-history evidence.

The fixed recipe preserves schema constraints. Identity comes from readable new
preimages; no original commit, report, captured credentials or Git probe is used.
The resulting JSON envelopes expressly disclaim historical bytes/certification.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

ORIGIN_PREIMAGE = "dpone synthetic binding evidence fixture v1"
DIRECTORY = Path(__file__).resolve().parents[1] / "fixtures/binding_evidence_synthetic_v1"
RECIPE_SHA256 = "efa31a6f90f0b77fcf4d481125d6f3fe73f17e558171df24bebb55e2e1a54f04"


def canonical(value: object) -> bytes:
    """Use the evidence protocol's deterministic JSON representation."""
    return (json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n").encode()


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate recipe key")
        result[key] = value
    return result


def _recipe() -> dict[str, Any]:
    path = DIRECTORY / "recipe.json"
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 32768:
        raise ValueError("invalid recipe file")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != RECIPE_SHA256:
        raise ValueError("changed fixture recipe")
    document = json.loads(raw, object_pairs_hook=_unique)
    if (
        set(document)
        != {"recipe_version", "origin_preimage", "original_wire_bytes_preserved", "schema_template", "production_paths"}
        or type(document["recipe_version"]) is not int
        or document["recipe_version"] != 1
    ):
        raise ValueError("invalid fixture recipe")
    if document["origin_preimage"] != ORIGIN_PREIMAGE or document["original_wire_bytes_preserved"] is not False:
        raise ValueError("invalid fixture provenance")
    return document


def _layout(layout: object) -> int:
    if type(layout) is not int or layout not in (1, 2, 3):
        raise ValueError("invalid synthetic layout")
    return layout


def _identity(role: str) -> str:
    return hashlib.sha1(f"{ORIGIN_PREIMAGE}: {role}".encode(), usedforsecurity=False).hexdigest()


def schema(layout: object) -> dict[str, Any]:
    """Build a closed legacy-shape schema with newly authored identity bytes."""
    selected = _layout(layout)
    recipe = _recipe()
    document = deepcopy(recipe["schema_template"])
    document["$id"] = f"https://example.invalid/synthetic-binding-evidence/layout-{selected}.schema.json"
    properties = document["properties"]
    properties["integration_base_commit"]["const"] = _identity("integration base")
    properties["producer_contract_version"]["const"] = f"dpone-postgres-mssql-r1-v3-binding-v2-evidence-{selected}"
    owners = recipe["production_paths"][str(selected)]
    paths = properties["production_path_tuple"]["oneOf"][1]
    paths.update(prefixItems=[{"const": path} for path in owners], minItems=len(owners), maxItems=len(owners))
    return document


def vector(layout: object, phase: object) -> dict[str, Any]:
    """Return a clearly labeled schema vector; it does not establish a Git chain."""
    selected = _layout(layout)
    if type(phase) is not str or phase not in ("red", "candidate"):
        raise ValueError("invalid synthetic phase")
    candidate = phase == "candidate"
    digest = hashlib.sha256(f"{ORIGIN_PREIMAGE}: schema example".encode()).hexdigest()
    counts = dict.fromkeys(("passed", "failed", "skipped", "xfailed", "xpassed", "errors"), 0)
    probe = dict(
        probe_kind="collection",
        pytest_args=["test_x.py"],
        exit_code=0,
        ordered_nodeids=["test_x.py::test_x"],
        reports=[],
        semantic_observations=[],
        input_vectors=[],
        outcome_counts=counts,
        import_origins=[],
    )
    document: dict[str, Any] = {
        "record_kind": "binding_contract_evidence",
        "producer_contract_version": f"dpone-postgres-mssql-r1-v3-binding-v2-evidence-{selected}",
        "phase": phase,
        "subject_commit": _identity("subject"),
        "task_authority_path": "docs/agent-task-contracts/postgres-mssql-r1-v3-provider-binding-v2-"
        + ("green.yml" if candidate else "red.yml"),
        "integration_base_commit": _identity("integration base"),
        "final_evidence_commit": None,
        "collection_probe": probe,
        "behavior_probe": {**probe, "probe_kind": "behavior"},
        "case_results": [
            dict(case_id=f"case-{i}", nodeid=f"test_x.py::test_x[{i}]", status="PASS", observed_class="typed_rejection")
            for i in range(246)
        ],
        "behavioral_status": "PASS" if candidate else "RED",
        "certification_status": "UNVERIFIED",
        "adapter_layer": "pure",
        "production_path_tuple": _recipe()["production_paths"][str(selected)] if candidate else None,
        "red_evidence_artifact_sha256": digest if candidate else None,
    }
    for field in (
        "approved_specification",
        "red_task_contract",
        "red_test_authority",
        "evidence_protocol",
        "deterministic_red_pin",
    ):
        document[f"{field}_commit"] = _identity(field)
    for field in ("red_evidence", "green_task_contract", "implementation"):
        document[f"{field}_commit"] = _identity(field) if candidate else None
    for field in (
        "task_authority",
        "producer",
        "schema",
        "registry",
        "ordered_nodeids",
        "behavioral_harness_tree",
        "evidence_protocol_tree",
        "input_vector",
    ):
        document[f"{field}_sha256"] = digest
    return {
        "fixture_kind": "authored_synthetic_binding_evidence",
        "origin_preimage": ORIGIN_PREIMAGE,
        "original_wire_bytes_preserved": False,
        "live_certification": False,
        "document": document,
    }


def outputs() -> dict[str, bytes]:
    """Return exact filenames and producer bytes for create-only fixture generation."""
    result = {}
    for layout in (1, 2, 3):
        result[f"layout-{layout}.schema.json"] = canonical(schema(layout))
        for phase in ("red", "candidate"):
            result[f"layout-{layout}-{phase}.json"] = canonical(vector(layout, phase))
    return result


def generate() -> None:
    """Write new fixture files only; a differing existing fixture is an error."""
    for name, content in outputs().items():
        path = DIRECTORY / name
        if path.is_symlink():
            raise ValueError("symlink fixture")
        if path.exists():
            if path.read_bytes() != content:
                raise ValueError("fixture conflict")
        else:
            with path.open("xb") as stream:
                stream.write(content)


if __name__ == "__main__":
    generate()
