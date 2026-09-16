"""Closed build inventory shapes do not authenticate source outcomes."""

import json
from dataclasses import replace

import pytest

from dpone.contracts.native_generation_build_cohort import (
    NativeBuildArtifact,
    NativeBuildArtifactInventory,
    decode_native_build_artifact_inventory,
    encode_native_build_artifact_inventory,
)
from dpone.contracts.native_source_custody import NativeSourceCustodyError
from tests.test_native_source_positive_completion import completion


def inventory():
    value = completion()
    return NativeBuildArtifactInventory(
        value.executor,
        value.command,
        value.toolchain,
        value.termination,
        value.command,
        value.build_evidence,
        "observed-dbt-invocation",
        (
            NativeBuildArtifact("MANIFEST", "build/target/manifest.json", 12, value.artifact_inventory),
            NativeBuildArtifact("RUN_RESULTS", "build/target/run_results.json", 14, value.artifact_inventory),
        ),
    )


def test_inventory_preserves_observed_dbt_identity_separately_from_executor():
    value = inventory()
    assert value.dbt_invocation_id != str(value.executor.invocation_id)
    assert tuple(item.role for item in value.artifacts) == ("MANIFEST", "RUN_RESULTS")


@pytest.mark.parametrize(
    "path",
    [
        "../manifest.json",
        "/manifest.json",
        "a/../manifest.json",
        "a//manifest.json",
        "a/./manifest.json",
        "target/run_results.json",
    ],
)
def test_artifact_rejects_unconfined_or_wrong_role_path(path):
    with pytest.raises(NativeSourceCustodyError):
        replace(inventory().artifacts[0], relative_path=path)


@pytest.mark.parametrize("size", [True, False, 0, -1, 9223372036854775808, "12", 12.0])
def test_artifact_size_requires_exact_positive_bigint(size):
    with pytest.raises(NativeSourceCustodyError):
        replace(inventory().artifacts[0], size_bytes=size)


@pytest.mark.parametrize("mutation", ["missing", "reverse", "duplicate", "list", "different_target"])
def test_inventory_requires_complete_ordered_same_target_membership(mutation):
    value = inventory()
    items = value.artifacts
    changed = {
        "missing": items[:1],
        "reverse": items[::-1],
        "duplicate": (items[0], items[0]),
        "list": list(items),
        "different_target": (items[0], replace(items[1], relative_path="other/run_results.json")),
    }
    with pytest.raises(NativeSourceCustodyError):
        replace(value, artifacts=changed[mutation])


@pytest.mark.parametrize("identity", ["", " padded ", "line\nbreak", True, None])
def test_inventory_requires_nonempty_exact_observed_identity(identity):
    with pytest.raises(NativeSourceCustodyError):
        replace(inventory(), dbt_invocation_id=identity)


def test_inventory_canonical_roundtrip_has_no_own_descriptor():
    payload = encode_native_build_artifact_inventory(inventory())
    raw = json.loads(payload)
    assert raw["schema"] == "dpone.native-build-artifact-inventory.v1"
    assert set(raw) == {
        "schema",
        "executor",
        "command",
        "toolchain",
        "termination",
        "execution_pack",
        "build_evidence",
        "dbt_invocation_id",
        "artifacts",
    }
    assert decode_native_build_artifact_inventory(payload) == inventory()


@pytest.mark.parametrize(
    "mutation", ["receipt", "missing", "schema", "bool", "extra_artifact", "extra_nested", "reverse"]
)
def test_inventory_decoder_rejects_closed_shape_mutations(mutation):
    raw = json.loads(encode_native_build_artifact_inventory(inventory()))
    if mutation == "receipt":
        raw["receipt"] = raw["command"]
    elif mutation == "missing":
        del raw["termination"]
    elif mutation == "schema":
        raw["schema"] = "dpone.native-build-artifact-inventory.v0"
    elif mutation == "bool":
        raw["artifacts"][0]["size_bytes"] = True
    elif mutation == "extra_artifact":
        raw["artifacts"].append(raw["artifacts"][0])
    elif mutation == "extra_nested":
        raw["artifacts"][0]["success"] = True
    else:
        raw["artifacts"].reverse()
    with pytest.raises(NativeSourceCustodyError):
        decode_native_build_artifact_inventory(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode())


def test_inventory_decoder_rejects_noncanonical_bytes():
    with pytest.raises(NativeSourceCustodyError):
        decode_native_build_artifact_inventory(b" " + encode_native_build_artifact_inventory(inventory()))
