"""A workspace snapshot is a complete, immutable, canonical source contract."""

from __future__ import annotations

import json
from collections.abc import MutableMapping
from copy import deepcopy
from dataclasses import replace
from typing import Any, cast

import pytest

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2, dbt_runtime_payload_trio
from dpone.contracts.dbt_source_inventory import (
    DbtProjectSource,
    DbtSourceInventory,
    DbtWorkflowSource,
)


def _project(name: str, *, workflow: str | None = None) -> DbtProjectSource:
    workflow = workflow or f"{name}_daily"
    project = sha256_bytes(f"project:{name}".encode())
    manifest = sha256_bytes(f"manifest:{name}".encode())
    selection = f"selection:{workflow}".encode()
    ids = dbt_runtime_payload_trio(
        workflow_id=workflow,
        project_sha256=project,
        manifest_sha256=manifest,
        selection_lock_payload=selection,
        wire_contract=DBT_RUNTIME_WIRE_V2,
    )
    return DbtProjectSource(
        project_path=f"dbt/{name}",
        project_name=name,
        project_bundle_sha256=project,
        manifest_sha256=manifest,
        toolchain_sha256=sha256_bytes(b"toolchain"),
        workflows=(
            DbtWorkflowSource(
                workflow_id=workflow,
                dag_id=f"DAG__{name}__{workflow}__refresh",
                workload_id=f"dbt__{workflow}",
                runtime_payload_ids=ids,
                selection_lock_sha256=sha256_bytes(selection),
            ),
        ),
    )


def _inventory() -> DbtSourceInventory:
    return DbtSourceInventory.build((_project("alpha"), _project("beta")))


def _reseal(value: dict[str, object]) -> bytes:
    value["snapshot_sha256"] = canonical_fingerprint({k: v for k, v in value.items() if k != "snapshot_sha256"})
    return json.dumps(value).encode()


def test_two_projects_round_trip_and_discovery_order_does_not_change_identity() -> None:
    snapshot = _inventory()
    reversed_build = DbtSourceInventory.build((_project("beta"), _project("alpha")))
    assert snapshot == reversed_build
    payload = snapshot.to_dict()
    assert payload["schema"] == "dpone.dbt-source-snapshot.v2"
    assert payload["snapshot_sha256"] == snapshot.snapshot_sha256
    assert DbtSourceInventory.from_payload(json.dumps(payload).encode()) == snapshot
    assert [p.project_path for p in snapshot.projects] == ["dbt/alpha", "dbt/beta"]


def test_nested_input_collections_cannot_mutate_a_validated_snapshot() -> None:
    source = _project("alpha")
    workflow_list = list(source.workflows)
    source = replace(source, workflows=workflow_list)  # type: ignore[arg-type]
    project_list = [source]
    snapshot = DbtSourceInventory(projects=project_list)  # type: ignore[arg-type]
    workflow_list.clear()
    project_list.clear()
    assert len(snapshot.projects) == 1
    assert len(snapshot.projects[0].workflows) == 1


@pytest.mark.parametrize("key", ["schema", "projects", "snapshot_sha256"])
def test_missing_root_field_rejected(key: str) -> None:
    value = _inventory().to_dict()
    value.pop(key)
    with pytest.raises(ValueError):
        DbtSourceInventory.from_payload(json.dumps(value).encode())


@pytest.mark.parametrize(
    "key", ["project_path", "project_name", "project_bundle_sha256", "manifest_sha256", "toolchain_sha256", "workflows"]
)
def test_missing_project_field_rejected(key: str) -> None:
    value = _inventory().to_dict()
    value["projects"][0].pop(key)
    with pytest.raises(ValueError):
        DbtSourceInventory.from_payload(_reseal(value))


@pytest.mark.parametrize(
    "key", ["workflow_id", "dag_id", "workload_id", "runtime_payload_ids", "selection_lock_sha256"]
)
def test_missing_workflow_field_rejected(key: str) -> None:
    value = _inventory().to_dict()
    value["projects"][0]["workflows"][0].pop(key)
    with pytest.raises(ValueError):
        DbtSourceInventory.from_payload(_reseal(value))


@pytest.mark.parametrize("level", ["root", "project", "workflow"])
def test_unknown_fields_rejected_even_with_valid_fingerprint(level: str) -> None:
    value = _inventory().to_dict()
    target = value if level == "root" else value["projects"][0]
    if level == "workflow":
        target = target["workflows"][0]
    target["unexpected"] = "value"
    with pytest.raises(ValueError):
        DbtSourceInventory.from_payload(_reseal(value))


def test_parser_rejects_unsorted_projects_instead_of_rewriting_signed_input() -> None:
    value = _inventory().to_dict()
    value["projects"].reverse()
    with pytest.raises(ValueError):
        DbtSourceInventory.from_payload(_reseal(value))


@pytest.mark.parametrize("field", ["project_bundle_sha256", "manifest_sha256"])
def test_foreign_project_or_manifest_trio_rejected(field: str) -> None:
    source = _project("alpha")
    changes: dict[str, Any] = {field: sha256_bytes(b"foreign")}
    with pytest.raises(ValueError):
        replace(source, **changes)


def test_selection_byte_digest_cannot_be_substituted() -> None:
    workflow = _project("alpha").workflows[0]
    with pytest.raises(ValueError):
        replace(workflow, selection_lock_sha256=sha256_bytes(b"other"))


def test_workflow_must_own_its_execution_workload() -> None:
    with pytest.raises(ValueError):
        replace(_project("alpha").workflows[0], workload_id="dbt__foreign")


@pytest.mark.parametrize(
    "path",
    ["../alpha", "/alpha", "dbt//alpha", "dbt/./alpha", "dbt\\alpha", ".git/alpha", "dbt/alpha\n", "dbt/e\u0301"],
)
def test_nonportable_or_unconfined_project_paths_rejected(path: str) -> None:
    with pytest.raises(ValueError):
        replace(_project("alpha"), project_path=path)


@pytest.mark.parametrize("field", ["project_path", "project_name"])
def test_duplicate_project_identity_rejected(field: str) -> None:
    first, second = _project("alpha"), _project("beta")
    second = replace(second, **{field: getattr(first, field)})
    with pytest.raises(ValueError):
        DbtSourceInventory.build((first, second))


def test_parent_and_child_project_roots_cannot_overlap() -> None:
    with pytest.raises(ValueError):
        DbtSourceInventory.build((_project("alpha"), replace(_project("beta"), project_path="dbt/alpha/nested")))


def test_global_workflow_and_dag_collisions_rejected() -> None:
    with pytest.raises(ValueError):
        DbtSourceInventory.build((_project("alpha", workflow="shared"), _project("beta", workflow="shared")))
    first, second = _project("alpha"), _project("beta")
    second = replace(second, workflows=(replace(second.workflows[0], dag_id=first.workflows[0].dag_id),))
    with pytest.raises(ValueError):
        DbtSourceInventory.build((first, second))


@pytest.mark.parametrize("payload", [b'{"schema":"v2","schema":"v2"}', b'{"x":NaN}', b"[]", b"{", b"\xff"])
def test_ambiguous_or_malformed_json_rejected(payload: bytes) -> None:
    with pytest.raises(ValueError):
        DbtSourceInventory.from_payload(payload)


def test_tampered_fingerprint_and_wrong_version_rejected() -> None:
    original = _inventory().to_dict()
    tampered = deepcopy(original)
    tampered["snapshot_sha256"] = sha256_bytes(b"wrong")
    with pytest.raises(ValueError):
        DbtSourceInventory.from_payload(json.dumps(tampered).encode())
    original["schema"] = "dpone.dbt-source-snapshot.v1"
    with pytest.raises(ValueError):
        DbtSourceInventory.from_payload(_reseal(original))


def test_empty_inventory_and_oversized_payload_rejected() -> None:
    with pytest.raises(ValueError):
        DbtSourceInventory.build(())
    with pytest.raises(ValueError):
        DbtSourceInventory.from_payload(b" " * (1024 * 1024 + 1))


def test_portable_parent_overlap_is_checked_independently_of_raw_sort_order() -> None:
    with pytest.raises(ValueError):
        DbtSourceInventory.build((_project("alpha"), replace(_project("beta"), project_path="dbt/ALPHA/nested")))


def test_payload_limit_applies_to_complete_inventory_after_deduplication() -> None:
    projects = [_project(f"p{index:02d}") for index in range(21)]
    DbtSourceInventory.build(projects)  # 21 * 3 = 63 unique payloads
    first = projects[0]
    extra = _project("p00", workflow="p00_extra").workflows[0]
    projects[0] = replace(first, workflows=(*first.workflows, extra))
    DbtSourceInventory.build(projects)  # Reuse project/manifest: exactly 64.
    another = _project("p00", workflow="p00_final").workflows[0]
    projects[0] = replace(first, workflows=(*first.workflows, extra, another))
    with pytest.raises(ValueError):
        DbtSourceInventory.build(projects)


@pytest.mark.parametrize("path", ["dbt/.GIT/alpha", ".Worktrees/alpha"])
def test_reserved_paths_are_rejected_on_case_insensitive_filesystems(path: str) -> None:
    with pytest.raises(ValueError):
        replace(_project("alpha"), project_path=path)


def test_workspace_root_project_is_supported_only_as_the_sole_project() -> None:
    source = replace(_project("alpha"), project_path=".")
    inventory = DbtSourceInventory.build((source,))
    assert DbtSourceInventory.from_payload(json.dumps(inventory.to_dict()).encode()) == inventory
    with pytest.raises(ValueError):
        DbtSourceInventory.build((source, _project("beta")))


def test_bundle_binding_detaches_inputs_and_uses_canonical_project_order() -> None:
    inventory = _inventory()
    bundles = {"dbt/beta": b"project:beta", "dbt/alpha": b"project:alpha"}
    frozen = inventory.bind_project_bundles(bundles)
    bundles.clear()
    assert list(frozen) == ["dbt/alpha", "dbt/beta"]
    assert frozen["dbt/alpha"] == b"project:alpha"
    with pytest.raises(TypeError):
        cast(MutableMapping[str, bytes], frozen)["dbt/alpha"] = b"changed"
    assert inventory.project_directories == frozenset({".", "dbt", "dbt/alpha", "dbt/beta"})


@pytest.mark.parametrize("defect", ["missing", "extra", "swapped", "empty", "mutable", "text"])
def test_bundle_binding_rejects_incomplete_or_changed_bytes(defect: str) -> None:
    # Deliberately inject malformed caller values into the runtime boundary.
    bundles: dict[str, Any] = {"dbt/alpha": b"project:alpha", "dbt/beta": b"project:beta"}
    if defect == "missing":
        del bundles["dbt/beta"]
    elif defect == "extra":
        bundles["dbt/orphan"] = bundles["dbt/alpha"]
    else:
        bundles["dbt/beta"] = {
            "swapped": b"project:alpha",
            "empty": b"",
            "mutable": bytearray(b"project:beta"),
            "text": "project:beta",
        }[defect]
    with pytest.raises(ValueError, match="(inventory|archive)"):
        _inventory().bind_project_bundles(bundles)


def test_bundle_binding_enforces_per_object_and_total_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.contracts import dbt_source_inventory as contract

    inventory = _inventory()
    bundles = {"dbt/alpha": b"project:alpha", "dbt/beta": b"project:beta"}
    total = sum(map(len, bundles.values()))
    monkeypatch.setattr(contract, "MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES", total)
    assert inventory.bind_project_bundles(bundles) == bundles
    monkeypatch.setattr(contract, "MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES", total - 1)
    with pytest.raises(ValueError, match="aggregate"):
        inventory.bind_project_bundles(bundles)
    monkeypatch.setattr(contract, "MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES", total)
    monkeypatch.setattr(contract, "MAX_DBT_RUNTIME_PAYLOAD_BYTES", len(bundles["dbt/alpha"]) - 1)
    with pytest.raises(ValueError, match="byte bound"):
        inventory.bind_project_bundles(bundles)


def test_bundle_binding_charges_identical_bytes_once_without_certifying_archive_contents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.contracts import dbt_source_inventory as contract

    second = replace(_project("alpha", workflow="beta_daily"), project_path="dbt/beta", project_name="beta")
    inventory = DbtSourceInventory.build((_project("alpha"), second))
    monkeypatch.setattr(contract, "MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES", len(b"project:alpha"))
    bundles = {project.project_path: b"project:alpha" for project in inventory.projects}
    assert inventory.bind_project_bundles(bundles) == bundles


def test_root_bundle_binding_does_not_invent_a_parent():
    inventory = DbtSourceInventory((replace(_project("alpha"), project_path="."),))
    assert inventory.project_directories == frozenset({"."})
    assert inventory.bind_project_bundles({".": b"project:alpha"}) == {".": b"project:alpha"}
