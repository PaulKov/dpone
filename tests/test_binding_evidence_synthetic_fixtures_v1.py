"""Freshly authored fixture vectors; no historical execution or certification."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import jsonschema
import pytest

from tests.support import binding_evidence_synthetic_fixtures_v1 as fixtures


@pytest.mark.parametrize("layout", [1, 2, 3])
@pytest.mark.parametrize("phase", ["red", "candidate"])
def test_synthetic_layout_vector_is_closed_and_explicit(layout: int, phase: str) -> None:
    schema = fixtures.schema(layout)
    vector = fixtures.vector(layout, phase)
    assert vector["fixture_kind"] == "authored_synthetic_binding_evidence"
    assert vector["original_wire_bytes_preserved"] is False
    assert vector["live_certification"] is False
    assert vector["origin_preimage"] == fixtures.ORIGIN_PREIMAGE
    document = vector["document"]
    assert (
        document["integration_base_commit"]
        == hashlib.sha1(
            b"dpone synthetic binding evidence fixture v1: integration base", usedforsecurity=False
        ).hexdigest()
    )
    assert document["certification_status"] == "UNVERIFIED"
    assert document["producer_contract_version"].endswith(f"-{layout}")
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.Draft202012Validator(schema).validate(document)
    assert fixtures.canonical(vector) == fixtures.canonical(json.loads(fixtures.canonical(vector)))


@pytest.mark.parametrize("layout", [1, 2, 3])
@pytest.mark.parametrize("mutation", ["extra", "base", "version", "certification", "denominator", "phase"])
def test_synthetic_schema_keeps_rejection_semantics(layout: int, mutation: str) -> None:
    document = deepcopy(fixtures.vector(layout, "red")["document"])
    if mutation == "extra":
        document["extra"] = True
    elif mutation == "base":
        document["integration_base_commit"] = "0" * 40
    elif mutation == "version":
        document["producer_contract_version"] = "dpone-postgres-mssql-r1-v3-binding-v2-evidence-4"
    elif mutation == "certification":
        document["certification_status"] = "PASS"
    elif mutation == "denominator":
        document["case_results"].pop()
    else:
        document["red_evidence_commit"] = "0" * 40
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(fixtures.schema(layout)).validate(document)


@pytest.mark.parametrize("layout", [True, False, 0, 4, "1", None])
def test_synthetic_fixture_rejects_nonexact_layout(layout: object) -> None:
    with pytest.raises(ValueError):
        fixtures.schema(layout)


@pytest.mark.parametrize("phase", ["green", "", 1, None])
def test_synthetic_fixture_rejects_nonexact_phase(phase: object) -> None:
    with pytest.raises(ValueError):
        fixtures.vector(1, phase)


def test_frozen_fixture_outputs_equal_producer_bytes() -> None:
    assert (
        Path(fixtures.__file__).resolve() == Path("tests/support/binding_evidence_synthetic_fixtures_v1.py").resolve()
    )
    for name, content in fixtures.outputs().items():
        assert (fixtures.DIRECTORY / name).read_bytes() == content


@pytest.mark.parametrize("mutation", ["unknown", "bool_version", "provenance", "changed_hash", "duplicate"])
def test_recipe_is_closed_and_bound(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str) -> None:
    document = json.loads((fixtures.DIRECTORY / "recipe.json").read_bytes())
    if mutation == "unknown":
        document["unknown"] = True
    elif mutation == "bool_version":
        document["recipe_version"] = True
    elif mutation == "provenance":
        document["original_wire_bytes_preserved"] = True
    raw = fixtures.canonical(document)
    if mutation == "duplicate":
        raw = raw.replace(b"{", b'{"recipe_version":1,', 1)
    (tmp_path / "recipe.json").write_bytes(raw)
    monkeypatch.setattr(fixtures, "DIRECTORY", tmp_path)
    if mutation != "changed_hash":
        monkeypatch.setattr(fixtures, "RECIPE_SHA256", hashlib.sha256(raw).hexdigest())
    with pytest.raises(ValueError):
        fixtures.schema(1)


def test_generation_is_equal_existing_or_conflict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "recipe.json").write_bytes((fixtures.DIRECTORY / "recipe.json").read_bytes())
    monkeypatch.setattr(fixtures, "DIRECTORY", tmp_path)
    fixtures.generate()
    initial = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    fixtures.generate()
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == initial
    target = tmp_path / "layout-1.schema.json"
    target.write_bytes(b"conflicting fixture")
    with pytest.raises(ValueError, match="fixture conflict"):
        fixtures.generate()
    assert target.read_bytes() == b"conflicting fixture"


@pytest.mark.parametrize("target", ["recipe.json", "layout-1.schema.json"])
def test_generator_rejects_symlink_targets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target: str) -> None:
    raw = (fixtures.DIRECTORY / "recipe.json").read_bytes()
    outside = tmp_path / "outside.json"
    outside.write_bytes(raw)
    directory = tmp_path / "fixtures"
    directory.mkdir()
    if target != "recipe.json":
        (directory / "recipe.json").write_bytes(raw)
    (directory / target).symlink_to(outside)
    monkeypatch.setattr(fixtures, "DIRECTORY", directory)
    with pytest.raises(ValueError):
        fixtures.generate()
    assert outside.read_bytes() == raw
