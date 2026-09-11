"""Strict local reader for the frozen DDA-01/DDA-05 envelope and retained proofs."""

from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

from .artifacts import canonical_json, digest, read_artifact
from .correctness import RECOVERY_CHECKS, SAMPLE_CHECKS
from .profiles import Dataset
from .runner import configuration, route_record

STATUSES = {"PASS", "FAIL", "SKIP", "UNVERIFIED"}


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("invalid_run_contract")


def _hash(value: Any, size: int = 64) -> None:
    _require(isinstance(value, str) and bool(re.fullmatch(f"[0-9a-f]{{{size}}}", value)))


def validate_metric(metric: dict[str, Any]) -> None:
    _require(set(metric) == {"value", "unit", "availability", "reason", "provenance"})
    _require(isinstance(metric["unit"], str) and bool(metric["unit"]))
    _require(isinstance(metric["provenance"], str) and bool(metric["provenance"]))
    if metric["availability"] == "unavailable":
        _require(metric["value"] is None and isinstance(metric["reason"], str) and bool(metric["reason"]))
    else:
        _require(metric["availability"] == "measured" and metric["reason"] is None)
        value = metric["value"]
        _require(type(value) in (int, float) and math.isfinite(value) and value >= 0)


def _receipt(
    root: Path, envelope: dict[str, Any], reference: dict[str, Any], scope: str, sample_id: str | None = None
) -> dict[str, Any]:
    value = read_artifact(root, reference)
    _require(value["schema_version"] == 1 and type(value["schema_version"]) is int)
    _require(value["kind"] == "native-delivery-correctness" and value["scope"] == scope)
    _require(value["status"] in STATUSES and value["execution"] in {"hermetic", "live"})
    for receipt_key, envelope_key in (
        ("subject_commit", "subject"),
        ("workload_sha256", "workload"),
        ("configuration_sha256", "configuration"),
        ("environment_sha256", "environment"),
    ):
        _require(value[receipt_key] == envelope[envelope_key]["commit" if envelope_key == "subject" else "sha256"])
    _require(value["route"] == envelope["route"])
    _require(isinstance(value["sample_id"], str) and bool(value["sample_id"]))
    if sample_id is not None:
        _require(value["sample_id"] == sample_id)
    fixture = value["fixture"]
    _require(type(fixture["rows"]) is int and fixture["rows"] >= 0 and isinstance(fixture["id"], str))
    _hash(fixture["sha256"])
    required = set(RECOVERY_CHECKS if scope == "failure_recovery" else SAMPLE_CHECKS)
    checks = value["checks"]
    _require({c["id"] for c in checks} == required and len(checks) == len(required))
    for item in checks:
        _require(item["status"] in STATUSES | {"N/A"})
        _require(
            item["method"]
            in {
                "exact_typed_multiset",
                "versioned_typed_digest",
                "transaction_fixture",
                "live_observation",
                "not_applicable",
            }
        )
        if item["status"] != "PASS":
            _require(isinstance(item["reason"], str) and bool(item["reason"]))
        else:
            _require(canonical_json(item["expected"]) == canonical_json(item["observed"]))
        if item["status"] == "N/A":
            _require(
                item["id"] == "outside_window_unchanged"
                and envelope["route"]["strategy"] == "full_refresh"
                and item["method"] == "not_applicable"
            )
        if item["id"] == "outside_window_unchanged" and envelope["route"]["strategy"] == "full_refresh":
            _require(item["status"] == "N/A")
        if (
            scope == "type_fidelity"
            and item["status"] == "PASS"
            and item["id"] in {"typed_content", "duplicate_multiplicity"}
        ):
            _require(item["method"] == "exact_typed_multiset")
        if value["status"] == "PASS":
            _require(item["status"] in {"PASS", "N/A"})
        if item["evidence"] is not None:
            observed = read_artifact(root, item["evidence"])
            for key in (
                "subject_commit",
                "workload_sha256",
                "configuration_sha256",
                "environment_sha256",
                "sample_id",
                "route",
                "execution",
            ):
                _require(canonical_json(observed[key]) == canonical_json(value[key]))
            _require(observed["scope"] == scope and canonical_json(observed["fixture"]) == canonical_json(fixture))
            _require(any(canonical_json(c) == canonical_json({**item, "evidence": None}) for c in observed["checks"]))
        elif item["status"] == "PASS":
            raise ValueError("missing_retained_observation")
    return value


def validate_run(envelope: dict[str, Any], root: Path) -> list[dict[str, Any]]:
    """Validate bytes/bindings and return eligible timed samples; never trust PASS alone."""
    _require(
        set(envelope)
        == {
            "schema_version",
            "kind",
            "producer",
            "subject",
            "route",
            "workload",
            "configuration",
            "environment",
            "samples",
            "fidelity_receipt",
            "recovery_receipt",
            "status",
            "limitations",
        }
    )
    _require(type(envelope["schema_version"]) is int and envelope["schema_version"] == 1)
    _require(envelope["kind"] == "native-delivery-run" and envelope["status"] in STATUSES)
    for key in ("producer", "subject"):
        _hash(envelope[key]["commit"], 40)
        _require(type(envelope[key]["dirty"]) is bool)
    _require(
        all(isinstance(envelope["producer"][key], str) and envelope["producer"][key] for key in ("name", "version"))
    )
    route = envelope["route"]
    _require(route_record(route["strategy"], route["mode"]) == route)
    workload = envelope["workload"]
    _require(isinstance(workload["id"], str) and bool(workload["id"]))
    for key, minimum in (("rows", 0), ("columns", 1), ("seed", 0)):
        _require(type(workload[key]) is int and workload[key] >= minimum)
    for key in ("workload", "configuration", "environment"):
        _hash(envelope[key]["sha256"])
    _require(configuration(envelope["configuration"]["limits"]) == envelope["configuration"])
    _require(Dataset(workload["id"], workload["rows"], workload["seed"]).envelope() == workload)
    environment = envelope["environment"]
    _require(digest({k: v for k, v in environment.items() if k != "sha256"}) == environment["sha256"])
    _hash(envelope["environment"]["target_layout_sha256"])
    _require(isinstance(envelope["limitations"], list) and all(isinstance(s, str) for s in envelope["limitations"]))
    fidelity = _receipt(root, envelope, envelope["fidelity_receipt"], "type_fidelity")
    recovery = _receipt(root, envelope, envelope["recovery_receipt"], "failure_recovery")
    eligible, seen = [], set()
    warmup = False
    for sample in envelope["samples"]:
        _require(isinstance(sample["id"], str) and sample["id"] not in seen)
        seen.add(sample["id"])
        _require(type(sample["is_warmup"]) is bool and sample["status"] in STATUSES)
        if sample["status"] != "PASS":
            _require(isinstance(sample["reason"], str) and bool(sample["reason"]))
        for metric in [sample["visibility_seconds"], sample["pipeline_seconds"], *sample["metrics"].values()]:
            validate_metric(metric)
        proof = _receipt(root, envelope, sample["correctness"], "sample", sample["id"])
        if sample["observations"] is not None:
            read_artifact(root, sample["observations"])
        good = sample["status"] == proof["status"] == "PASS" and all(
            sample[k]["availability"] == "measured" for k in ("visibility_seconds", "pipeline_seconds")
        )
        if good:
            _require(sample["visibility_seconds"]["value"] <= sample["pipeline_seconds"]["value"])
        if sample["is_warmup"]:
            warmup |= good and proof["execution"] == "live"
        elif good and warmup and proof["execution"] == "live":
            eligible.append(sample)
    live = all(proof["execution"] == "live" and proof["status"] == "PASS" for proof in (fidelity, recovery))
    clean = not envelope["subject"]["dirty"] and not envelope["producer"]["dirty"]
    return eligible if live and clean and len(eligible) >= 3 else []


def require_comparable(baseline: dict[str, Any], candidate: dict[str, Any]) -> None:
    """Reject workload, limits, server, resource or physical layout drift."""
    for key in ("workload", "configuration", "environment", "route"):
        _require(canonical_json(baseline[key]) == canonical_json(candidate[key]))
