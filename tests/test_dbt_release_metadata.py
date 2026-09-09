"""Pure metadata preserves legacy list semantics without certifying pack bytes."""

import hashlib
import json

import pytest

from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2


def _metadata(selections, *, wire=DBT_RUNTIME_WIRE_V1, receipts=None):
    from dpone.contracts.dbt_release import build_dbt_release_metadata

    return build_dbt_release_metadata(
        dag_specs=[],
        workload_packs=[],
        runtime_descriptors=[{"id": name} for name in ("project", "manifest", "selection")],
        canonical_schema_descriptors=[],
        source_snapshot_sha256="sha256:" + "c" * 64,
        selection_authority="dbt_cli",
        route_certifications=receipts or [{"variant_id": "z"}, {"variant_id": "a"}],
        selection_fingerprints=selections,
        producer_version="0.74.28",
        wire_contract=wire,
    )


@pytest.mark.parametrize("wire", [DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2])
def test_selection_multiplicity_is_legacy_only_but_order_never_matters(wire):
    a, b = "sha256:" + "a" * 64, "sha256:" + "b" * 64
    releases = [_metadata(values, wire=wire) for values in ([b, a, a], [a, b, a], [a, b])]
    assert releases[0]["selection_fingerprint"] == releases[1]["selection_fingerprint"]
    assert (releases[0]["selection_fingerprint"] == releases[2]["selection_fingerprint"]) == (
        wire == DBT_RUNTIME_WIRE_V2
    )
    for index, release in enumerate(releases):
        expected_selections = [a, b] if wire == DBT_RUNTIME_WIRE_V2 or index == 2 else [a, a, b]
        canonical = json.dumps(
            {
                "source_snapshot_sha256": "sha256:" + "c" * 64,
                "selection_fingerprints": expected_selections,
                "route_certifications": [{"variant_id": "a"}, {"variant_id": "z"}],
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        assert release["selection_fingerprint"] == "sha256:" + hashlib.sha256(canonical).hexdigest()
        assert release["provenance"]["selection_fingerprints"] == [a, b]


def test_v1_other_arrays_keep_input_order_even_when_selection_identity_sorts_receipts():
    first = _metadata(["sha256:" + "a" * 64])
    second = _metadata(["sha256:" + "a" * 64], receipts=[{"variant_id": "a"}, {"variant_id": "z"}])
    assert [row["id"] for row in first["artifacts"]["runtime_payloads"]] == ["project", "manifest", "selection"]
    assert first["provenance"]["route_certifications"] == [{"variant_id": "z"}, {"variant_id": "a"}]
    assert first["selection_fingerprint"] == second["selection_fingerprint"]
    # Provenance is deliberately excluded from release identity.
    assert first["release_id"] == second["release_id"]


def test_metadata_rejects_unknown_wire_without_falling_back():
    with pytest.raises(ValueError, match="unsupported"):
        _metadata([], wire="unknown")


def _materialized_inputs():
    from dpone.contracts.dbt_release import dbt_release_artifact_descriptor

    return dict(
        dag_files={"dag": b"dag bytes"},
        pack_files={"pack": b"pack bytes"},
        verified_packs={"pack": (b"pack bytes", "sha256:" + "d" * 64, ("runtime",))},
        runtime_files={"runtime.json": b"runtime bytes"},
        runtime_descriptors=[dbt_release_artifact_descriptor("runtime", "runtime.json", b"runtime bytes")],
        canonical_schema_files={"schema.json": b"schema bytes"},
        canonical_schema_descriptors=[dbt_release_artifact_descriptor("schema", "schema.json", b"schema bytes")],
        source_snapshot_sha256="sha256:" + "c" * 64,
        selection_authority="dbt_cli",
        route_certifications=[],
        selection_fingerprints=[],
        producer_version="0.74.29",
    )


@pytest.mark.parametrize("wire", [DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2])
def test_materialized_release_uses_supplied_verified_fingerprints(wire):
    from dpone.contracts.dbt_release import build_dbt_release_from_files, dbt_release_dag_descriptors

    inputs = _materialized_inputs()
    release = build_dbt_release_from_files(
        **inputs, dag_specs=dbt_release_dag_descriptors(inputs["dag_files"]), wire_contract=wire
    )
    pack = release["artifacts"]["workload_packs"][0]
    assert pack["pack_fingerprint"] == inputs["verified_packs"]["pack"][1]
    assert pack["runtime_payload_ids"] == ["runtime"]
    assert pack["sha256"] == "sha256:" + hashlib.sha256(b"pack bytes").hexdigest()
    assert release["producer"]["wire_contract"] == wire
    # The pure constructor binds bytes; it does not parse/verify framework packs.
    assert inputs["pack_files"]["pack"] == b"pack bytes"


@pytest.mark.parametrize("section", ["runtime_descriptors", "canonical_schema_descriptors"])
@pytest.mark.parametrize("mutation", ["bytes", "sha256", "path"])
def test_materialized_release_rejects_descriptor_body_disagreement(section, mutation):
    from dpone.contracts.dbt_release import build_dbt_release_from_files, dbt_release_dag_descriptors

    inputs = _materialized_inputs()
    descriptor = inputs[section][0]
    descriptor[mutation] = 0 if mutation == "bytes" else "foreign"
    with pytest.raises(ValueError, match="descriptor differs from materialized bytes"):
        build_dbt_release_from_files(**inputs, dag_specs=dbt_release_dag_descriptors(inputs["dag_files"]))


@pytest.mark.parametrize("action", ["missing", "extra"])
def test_materialized_release_requires_exact_verified_pack_membership(action):
    from dpone.contracts.dbt_release import build_dbt_release_from_files, dbt_release_dag_descriptors

    inputs = _materialized_inputs()
    if action == "missing":
        inputs["verified_packs"].clear()
    else:
        inputs["verified_packs"]["extra"] = (b"other", "sha256:" + "e" * 64, ())
    with pytest.raises(ValueError, match="verified pack membership differs"):
        build_dbt_release_from_files(**inputs, dag_specs=dbt_release_dag_descriptors(inputs["dag_files"]))


@pytest.mark.parametrize("action", ["missing", "extra", "duplicate"])
def test_materialized_release_requires_exact_unique_dag_descriptors(action):
    from dpone.contracts.dbt_release import build_dbt_release_from_files, dbt_release_dag_descriptors

    inputs = _materialized_inputs()
    descriptors = dbt_release_dag_descriptors(inputs["dag_files"])
    if action == "missing":
        descriptors.clear()
    elif action == "extra":
        descriptors.append({**descriptors[0], "id": "extra"})
    else:
        descriptors *= 2
    with pytest.raises(ValueError, match="DAG descriptor membership differs"):
        build_dbt_release_from_files(**inputs, dag_specs=descriptors)


def test_each_pack_trio_is_captured_after_its_own_verifier(monkeypatch):
    from dpone_airflow_pack import pack_identity

    from dpone.services.dbt_release_set_assembly import build_release_set

    inputs = _materialized_inputs()
    inputs.pop("verified_packs")
    inputs["pack_runtime_payload_ids"] = {"pack": ("runtime",)}
    inputs["pack_files"]["second"] = b"second pack"

    def verify(body):
        if body == b"second pack":
            inputs["pack_runtime_payload_ids"]["pack"] = ("replacement",)
        return "sha256:" + "d" * 64

    monkeypatch.setattr(pack_identity, "verify_pack_fingerprint", verify)
    release = build_release_set(**inputs)
    assert release["artifacts"]["workload_packs"][0]["runtime_payload_ids"] == ["runtime"]


def test_service_checks_wire_before_pack_verification(monkeypatch):
    from dpone_airflow_pack import pack_identity

    from dpone.services.dbt_release_set_assembly import build_release_set

    inputs = _materialized_inputs()
    inputs.pop("verified_packs")
    inputs["pack_runtime_payload_ids"] = {"pack": ("runtime",)}
    monkeypatch.setattr(pack_identity, "verify_pack_fingerprint", lambda _: pytest.fail("pack verification ran"))
    with pytest.raises(ValueError, match="unsupported dbt release wire contract"):
        build_release_set(**inputs, wire_contract="unknown")


def test_service_verifies_pack_before_materialized_runtime_descriptors(monkeypatch):
    from dpone_airflow_pack import pack_identity

    from dpone.services.dbt_release_set_assembly import build_release_set

    inputs = _materialized_inputs()
    inputs.pop("verified_packs")
    inputs["pack_runtime_payload_ids"] = {"pack": ("runtime",)}
    inputs["runtime_descriptors"][0]["bytes"] = 0

    def invalid_pack(_):
        raise ValueError("pack fingerprint rejected")

    monkeypatch.setattr(pack_identity, "verify_pack_fingerprint", invalid_pack)
    with pytest.raises(ValueError, match="^pack fingerprint rejected$"):
        build_release_set(**inputs)


@pytest.mark.parametrize("section,key", [("pack_files", "pack"), ("dag_files", "dag")])
def test_service_cannot_reuse_identity_after_verifier_replaces_bytes(monkeypatch, section, key):
    from dpone_airflow_pack import pack_identity

    from dpone.services.dbt_release_set_assembly import build_release_set

    inputs = _materialized_inputs()
    inputs.pop("verified_packs")
    inputs["pack_runtime_payload_ids"] = {"pack": ("runtime",)}

    def replace_after_verifying(body):
        assert body == b"pack bytes"
        inputs[section][key] = b"unverified replacement"
        return "sha256:" + "d" * 64

    monkeypatch.setattr(pack_identity, "verify_pack_fingerprint", replace_after_verifying)
    with pytest.raises(ValueError, match="release descriptor differs from materialized bytes"):
        build_release_set(**inputs)
