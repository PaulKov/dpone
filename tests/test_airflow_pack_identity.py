from __future__ import annotations

import hashlib
import json
import math
from copy import deepcopy
from typing import Any

import pytest
from dpone_airflow_pack.pack_identity import (
    PACK_IDENTITY_SCHEMA,
    PackIdentityError,
    compute_pack_fingerprint,
    identity_payload,
    parse_pack_json,
    verify_pack_fingerprint,
)

_ADVISORY_TOP_LEVEL_FIELDS = (
    "producer",
    "meta",
    "artifact_dir",
    "output_path",
    "bundle_path",
    "next_actions",
    "warnings",
    "blockers",
)


def _pack() -> dict[str, Any]:
    return {
        "kind": "gitops.airflow_pack",
        "schema_version": "3",
        "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
        "producer": "dpone gitops airflow pack",
        "meta": {"kind": "gitops.airflow_pack", "path": "packs/orders.json"},
        "artifact_dir": "packs",
        "output_path": "packs/orders.json",
        "bundle_path": "bundles/orders.json",
        "next_actions": ["promote"],
        "warnings": [{"code": "advisory"}],
        "blockers": [],
        "workload": {
            "workload_id": "orders",
            "meta": {"owner": "sales"},
        },
        "steps": [
            {"name": "extract", "argv": ["dpone", "extract"]},
            {"name": "load", "argv": ["dpone", "load"]},
        ],
        "airflow": {"execution": {"retries": 0}},
        "runtime_selection": {
            "mode": "process_plan",
            "required_for_selected_nodes": True,
        },
        "mapping_plan": {
            "mode": "visible",
            "items": [
                {"item_index": 0, "partition": "2026-07-18"},
                {"item_index": 1, "partition": "2026-07-19"},
            ],
        },
        "xcom": {"sidecar_image": "registry.example/xcom@sha256:" + "a" * 64},
        "outcome_gate": {"required_status": "success"},
        "provider_execution": {
            "schema": "dpone.airflow-provider-execution.v1",
            "kpo_kwargs": {"task_id": "orders__dpone_runtime"},
            "pod_spec": {"spec": {"nodeSelector": {"pool": "etl"}}},
        },
        "runtime_command": "dpone run --manifest orders.yaml",
        "kpo_kwargs": {"task_id": "orders__legacy", "retries": 0},
        "pod_spec": {"spec": {"restartPolicy": "Never"}},
        "process_plans": {
            "orders": {
                "selector": "orders",
                "runtime_command": ["dpone", "run", "--selector", "orders"],
            }
        },
        "artifact_index": {"runtime_manifest": "payload/orders.yaml"},
        "future_contract": {"enabled": True, "threshold": 1.5},
    }


def _signed_pack() -> dict[str, Any]:
    pack = _pack()
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    return pack


def _replace(payload: dict[str, Any], path: tuple[str | int, ...], value: Any) -> None:
    target: Any = payload
    for segment in path[:-1]:
        target = target[segment]
    target[path[-1]] = value


def _reverse_object_keys(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _reverse_object_keys(value[key]) for key in reversed(value)}
    if isinstance(value, list):
        return [_reverse_object_keys(item) for item in value]
    return value


def test_identity_payload_excludes_only_approved_top_level_fields() -> None:
    pack = _signed_pack()
    original = deepcopy(pack)

    payload = identity_payload(pack)

    assert pack == original
    assert "pack_fingerprint" not in payload
    assert all(field not in payload for field in _ADVISORY_TOP_LEVEL_FIELDS)
    assert payload["workload"]["meta"] == {"owner": "sales"}
    assert set(payload) == set(pack) - {"pack_fingerprint", *_ADVISORY_TOP_LEVEL_FIELDS}


def test_compute_pack_fingerprint_uses_canonical_utf8_json() -> None:
    pack = {
        "kind": "gitops.airflow_pack",
        "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
        "steps": [{"name": "café", "argv": ["dpone", "run"]}],
        "pack_fingerprint": "sha256:" + "f" * 64,
        "producer": "ignored",
    }
    canonical = (
        '{"kind":"gitops.airflow_pack","pack_identity":'
        '{"schema":"dpone.airflow-pack-identity.v1"},'
        '"steps":[{"argv":["dpone","run"],"name":"café"}]}'
    ).encode()

    assert compute_pack_fingerprint(pack) == "sha256:" + hashlib.sha256(canonical).hexdigest()


@pytest.mark.parametrize(
    ("surface", "path", "replacement"),
    [
        ("steps", ("steps", 0, "argv", 1), "extract-v2"),
        ("airflow_execution", ("airflow", "execution", "retries"), 1),
        ("runtime_selection", ("runtime_selection", "mode"), "legacy"),
        ("mapping", ("mapping_plan", "items", 0, "partition"), "2026-07-20"),
        ("xcom", ("xcom", "sidecar_image"), "registry.example/xcom@sha256:" + "b" * 64),
        ("outcome_gate", ("outcome_gate", "required_status"), "warning"),
        ("provider_execution", ("provider_execution", "kpo_kwargs", "task_id"), "orders__changed"),
        ("legacy_runtime_command", ("runtime_command",), "dpone run --manifest changed.yaml"),
        ("legacy_kpo_kwargs", ("kpo_kwargs", "retries"), 2),
        ("legacy_pod_spec", ("pod_spec", "spec", "restartPolicy"), "Always"),
        ("legacy_process_plans", ("process_plans", "orders", "selector"), "changed"),
        ("legacy_artifact_index", ("artifact_index", "runtime_manifest"), "payload/changed.yaml"),
    ],
)
def test_execution_bearing_mutations_change_fingerprint(
    surface: str,
    path: tuple[str | int, ...],
    replacement: Any,
) -> None:
    del surface
    pack = _pack()
    original = compute_pack_fingerprint(pack)

    _replace(pack, path, replacement)

    assert compute_pack_fingerprint(pack) != original


def test_unknown_fields_are_bound_by_default() -> None:
    pack = _pack()
    original = compute_pack_fingerprint(pack)

    pack["new_execution_surface"] = {"argv": ["dpone", "future"]}

    assert compute_pack_fingerprint(pack) != original


@pytest.mark.parametrize("field", _ADVISORY_TOP_LEVEL_FIELDS)
def test_approved_advisory_top_level_mutations_do_not_change_fingerprint(field: str) -> None:
    pack = _pack()
    original = compute_pack_fingerprint(pack)

    pack[field] = {"changed": field}

    assert compute_pack_fingerprint(pack) == original


@pytest.mark.parametrize("field", ("pack_fingerprint", *_ADVISORY_TOP_LEVEL_FIELDS))
def test_top_level_exclusion_names_remain_bound_when_nested(field: str) -> None:
    pack = _pack()
    pack["workload"][field] = "first"
    original = compute_pack_fingerprint(pack)

    pack["workload"][field] = "second"

    assert compute_pack_fingerprint(pack) != original


def test_array_order_is_preserved_and_identity_significant() -> None:
    pack = _pack()
    payload = identity_payload(pack)
    original = compute_pack_fingerprint(pack)

    assert payload["steps"] == pack["steps"]
    pack["steps"].reverse()

    assert compute_pack_fingerprint(pack) != original


def test_object_key_order_and_source_whitespace_are_not_identity_significant() -> None:
    pack = _signed_pack()
    reordered = _reverse_object_keys(pack)
    compact = json.dumps(pack, ensure_ascii=False, separators=(",", ":"))
    indented = json.dumps(reordered, ensure_ascii=False, indent=4)

    assert compute_pack_fingerprint(compact) == compute_pack_fingerprint(indented)
    assert verify_pack_fingerprint(compact) == pack["pack_fingerprint"]
    assert verify_pack_fingerprint(indented.encode()) == pack["pack_fingerprint"]


def test_string_content_is_identity_significant() -> None:
    pack = _pack()
    original = compute_pack_fingerprint(pack)

    pack["runtime_command"] += " "

    assert compute_pack_fingerprint(pack) != original


@pytest.mark.parametrize(
    "source",
    [
        '{"kind":"first","kind":"second"}',
        '{"nested":{"selector":"first","selector":"second"}}',
    ],
)
def test_duplicate_json_keys_fail_closed_at_any_depth(source: str) -> None:
    with pytest.raises(PackIdentityError, match="duplicate JSON key"):
        parse_pack_json(source)
    with pytest.raises(PackIdentityError, match="duplicate JSON key"):
        compute_pack_fingerprint(source)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_json_constants_fail_closed(constant: str) -> None:
    source = f'{{"value":{constant}}}'

    with pytest.raises(PackIdentityError, match="non-finite"):
        parse_pack_json(source)


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_non_finite_python_numbers_fail_closed(value: float) -> None:
    pack = _pack()
    pack["future_contract"]["threshold"] = value

    with pytest.raises(PackIdentityError, match="non-finite"):
        compute_pack_fingerprint(pack)


@pytest.mark.parametrize(
    "value",
    [
        b"not JSON",
        ("arrays", "must", "be", "lists"),
        {"sets", "are", "not", "JSON"},
        object(),
    ],
)
def test_non_json_python_values_fail_closed_without_string_coercion(value: object) -> None:
    pack = _pack()
    pack["future_contract"]["value"] = value

    with pytest.raises(PackIdentityError, match="JSON"):
        compute_pack_fingerprint(pack)


def test_non_string_object_keys_fail_closed() -> None:
    pack = _pack()
    pack["future_contract"]["values"] = {1: "one"}

    with pytest.raises(PackIdentityError, match="string keys"):
        compute_pack_fingerprint(pack)


def test_recursive_python_values_fail_closed() -> None:
    recursive: list[Any] = []
    recursive.append(recursive)
    pack = _pack()
    pack["future_contract"]["recursive"] = recursive

    with pytest.raises(PackIdentityError, match="recursive"):
        compute_pack_fingerprint(pack)


@pytest.mark.parametrize(
    "source",
    [
        "[]",
        '"pack"',
        "null",
        b"\xff",
    ],
)
def test_parser_requires_one_utf8_json_object(source: str | bytes) -> None:
    with pytest.raises(PackIdentityError, match="JSON object"):
        parse_pack_json(source)


def test_parser_accepts_bytes_and_preserves_array_order() -> None:
    source = b'{"steps":[{"name":"second"},{"name":"first"}]}'

    assert parse_pack_json(source)["steps"] == [{"name": "second"}, {"name": "first"}]


@pytest.mark.parametrize(
    "identity",
    [
        None,
        {},
        {"schema": "dpone.airflow-pack-identity.v2"},
        {"schema": PACK_IDENTITY_SCHEMA, "version": 1},
        {"schema": PACK_IDENTITY_SCHEMA, "extra": None},
    ],
)
def test_verification_requires_exact_pack_identity_schema(identity: object) -> None:
    pack = _signed_pack()
    pack["pack_identity"] = identity

    with pytest.raises(PackIdentityError, match="pack_identity"):
        verify_pack_fingerprint(pack)


@pytest.mark.parametrize(
    "claim",
    [
        None,
        1,
        "sha256:" + "A" * 64,
        "SHA256:" + "a" * 64,
        "a" * 64,
        "sha256:" + "a" * 63,
        "sha256:" + "g" * 64,
        " sha256:" + "a" * 64,
        "sha256:" + "a" * 64 + " ",
    ],
)
def test_verification_requires_canonical_lowercase_sha256_claim(claim: object) -> None:
    pack = _signed_pack()
    pack["pack_fingerprint"] = claim

    with pytest.raises(PackIdentityError, match="pack_fingerprint"):
        verify_pack_fingerprint(pack)


def test_verification_rejects_a_canonical_but_incorrect_claim() -> None:
    pack = _signed_pack()
    pack["pack_fingerprint"] = "sha256:" + "0" * 64

    with pytest.raises(PackIdentityError, match="does not match"):
        verify_pack_fingerprint(pack)


def test_verification_returns_the_independently_derived_fingerprint() -> None:
    pack = _signed_pack()

    assert verify_pack_fingerprint(pack) == compute_pack_fingerprint(pack)


def test_compact_pack_helpers_do_not_export_alternate_fingerprint_authority() -> None:
    import dpone.gitops.airflow_compact_pack_helpers as helpers

    assert not hasattr(helpers, "compact_pack_fingerprint")
    assert "compact_pack_fingerprint" not in helpers.__all__
    assert helpers.__all__ == [
        "compact_pack_artifact_index",
        "compact_pack_image_pull_secrets",
        "compact_pack_outcome_gate",
        "compact_pack_pod_annotations",
        "compact_pack_runtime_image_pull_policy",
        "compact_pack_workload_dependencies",
        "compact_pack_xcom",
        "dict_mapping",
    ]
