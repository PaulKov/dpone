"""Offline v1 benchmark comparison; retained evidence is checked before claims.

Hashes detect accidental replacement, not the authenticity of self-authored
receipts. The caller must retain trusted real-row producer evidence for live use.
"""

from __future__ import annotations

import hashlib
import json
import os
import statistics
import tempfile
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from dpone.contracts.mssql_native_chunks import NativeChunkLimits
from dpone.contracts.native_delivery_observations import CAMPAIGN_SCHEMA, CHECKS_BY_SCOPE, RECEIPT_SCHEMA, RUN_SCHEMA


class BenchmarkInputError(ValueError):
    """Stable sanitized input failure; never include dataset or exception text."""


def content_sha256(content: bytes) -> str:
    """Hash retained bytes exactly, without reparsing or reserializing."""
    return hashlib.sha256(content).hexdigest()


def canonical_json(payload: Any) -> bytes:
    """Canonical UTF-8 JSON with finite numbers; usable by independent producers."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
        "utf-8"
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise BenchmarkInputError("duplicate_json_key")
        result[key] = value
    return result


def _validate(payload: Any, schema: dict[str, Any]) -> None:
    if not Draft202012Validator(schema).is_valid(payload):
        raise BenchmarkInputError("invalid_schema")


class _Artifacts:
    """Retained bytes and paths for hashing, identity checks, and output protection."""

    def __init__(self) -> None:
        self.bytes: dict[Path, bytes] = {}
        self.references: list[tuple[dict[str, Any], Path]] = []

    def read(self, path: Path) -> bytes:
        resolved = path.resolve(strict=True)
        content = resolved.read_bytes()
        if resolved in self.bytes and self.bytes[resolved] != content:
            raise BenchmarkInputError("artifact_changed")
        self.bytes[resolved] = content
        return content

    def json(self, path: Path) -> Any:
        try:
            payload = json.loads(self.read(path), object_pairs_hook=_unique_object)
            canonical_json(payload)
            return payload
        except (UnicodeError, ValueError, RecursionError):
            raise BenchmarkInputError("invalid_json") from None

    def reference(self, root: Path, ref: dict[str, Any]) -> Path:
        relative = Path(ref["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise BenchmarkInputError("artifact_path_escape")
        resolved = (root / relative).resolve(strict=True)
        if not resolved.is_relative_to(root.resolve()):
            raise BenchmarkInputError("artifact_path_escape")
        if content_sha256(self.read(resolved)) != ref["sha256"]:
            raise BenchmarkInputError("artifact_hash_mismatch")
        return resolved

    def export(self, path: Path, status: str) -> dict[str, Any]:
        reference = {
            "path": str(path.resolve()),
            "sha256": content_sha256(self.bytes[path.resolve()]),
            "status": status,
        }
        self.references.append((reference, path.resolve()))
        return reference

    def relativize(self, root: Path, input_roots: tuple[Path, Path], *, retain: bool) -> None:
        """Keep references beneath the report, copying evidence only when needed."""
        destinations = {}
        for source in input_roots:
            source = source.resolve()
            if source.is_relative_to(root):
                destinations[source] = source
                continue
            files = {str(p.relative_to(source)): data for p, data in self.bytes.items() if p.is_relative_to(source)}
            digest = content_sha256(canonical_json({name: content_sha256(data) for name, data in files.items()}))
            destination = root / ("native-delivery-evidence-" + digest)
            if retain:
                if destination.exists() or destination.is_symlink():
                    if destination.is_symlink() or not destination.is_dir():
                        raise BenchmarkInputError("retained_bundle_conflict")
                    for name, data in files.items():
                        target = destination / name
                        if target.resolve() != target.absolute() or target.read_bytes() != data:
                            raise BenchmarkInputError("retained_bundle_conflict")
                else:
                    with tempfile.TemporaryDirectory(dir=root, prefix=".native-delivery-") as temporary:
                        bundle = Path(temporary) / "bundle"
                        bundle.mkdir()
                        for name, data in files.items():
                            target = bundle / name
                            target.parent.mkdir(parents=True, exist_ok=True)
                            target.write_bytes(data)
                        os.rename(bundle, destination)
            destinations[source] = destination
        for reference, original in self.references:
            source = max((p for p in destinations if original.is_relative_to(p)), key=lambda p: len(p.parts))
            reference["path"] = str((destinations[source] / original.relative_to(source)).relative_to(root))


def _observed_check(
    store: _Artifacts, root: Path, ref: dict[str, Any], receipt: dict[str, Any], check: dict[str, Any]
) -> None:
    observed = store.json(store.reference(root, ref))
    identity = (
        "schema_version",
        "subject_commit",
        "workload_sha256",
        "configuration_sha256",
        "environment_sha256",
        "sample_id",
        "route",
        "execution",
    )
    if not isinstance(observed, dict) or observed.get("kind") != "native-delivery-live-observation":
        raise BenchmarkInputError("invalid_live_observation")
    if any(observed.get(key) != receipt[key] for key in identity):
        raise BenchmarkInputError("live_observation_identity_mismatch")
    if observed.get("status") != ref["status"] or not isinstance(observed.get("checks"), list):
        raise BenchmarkInputError("live_observation_status_mismatch")
    if {**check, "evidence": None} not in observed["checks"]:
        raise BenchmarkInputError("live_observation_check_mismatch")


def _receipt(
    store: _Artifacts, root: Path, ref: dict[str, Any], run: dict[str, Any], scope: str, sample_id: str | None = None
) -> str:
    receipt = store.json(store.reference(root, ref))
    _validate(receipt, RECEIPT_SCHEMA)
    bindings = {
        "subject_commit": run["subject"]["commit"],
        "route": run["route"],
        "scope": scope,
        **{f"{key}_sha256": run[key]["sha256"] for key in ("workload", "configuration", "environment")},
    }
    if sample_id is not None:
        bindings["sample_id"] = sample_id
    if any(receipt[key] != value for key, value in bindings.items()):
        raise BenchmarkInputError("receipt_identity_mismatch")
    checks = {check["id"]: check for check in receipt["checks"]}
    if len(checks) != len(receipt["checks"]):
        raise BenchmarkInputError("duplicate_check_id")
    statuses = [ref["status"], receipt["status"]]
    live_evidence = False
    for check in checks.values():
        evidence = check["evidence"]
        if evidence is not None:
            _observed_check(store, root, evidence, receipt, check)
        if check["status"] != "PASS" and not check["reason"]:
            raise BenchmarkInputError("check_reason_required")
        inapplicable = check["id"] == "outside_window_unchanged" and run["route"]["strategy"] == "full_refresh"
        if inapplicable:
            if check["status"] != "N/A" or check["method"] != "not_applicable":
                raise BenchmarkInputError("invalid_inapplicable_check")
            continue
        if check["status"] == "N/A" or check["method"] == "not_applicable":
            raise BenchmarkInputError("invalid_inapplicable_check")
        statuses.append(check["status"])
        if check["status"] == "PASS" and check["expected"] != check["observed"]:
            statuses.append("FAIL")
        if (
            scope == "type_fidelity"
            and check["id"] in {"typed_content", "duplicate_multiplicity"}
            and check["method"] != "exact_typed_multiset"
        ):
            statuses.append("UNVERIFIED")
        if evidence is None or evidence["status"] != "PASS":
            statuses.append("UNVERIFIED")
        elif check["method"] == "live_observation":
            live_evidence = True
    if not CHECKS_BY_SCOPE[scope] <= checks.keys():
        statuses.append("UNVERIFIED")
    if receipt["execution"] != "live" or (scope == "sample" and not live_evidence):
        statuses.append("UNVERIFIED")
    return _status(statuses)


def _status(statuses: list[str]) -> str:
    return "FAIL" if "FAIL" in statuses else "UNVERIFIED" if any(s != "PASS" for s in statuses) else "PASS"


def _run(store: _Artifacts, path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    run = store.json(path)
    _validate(run, RUN_SCHEMA)
    limits = run["configuration"]["limits"]
    if set(limits) != set(NativeChunkLimits.__dataclass_fields__):
        raise BenchmarkInputError("invalid_limits")
    try:
        NativeChunkLimits(**limits)
    except (TypeError, ValueError):
        raise BenchmarkInputError("invalid_limits") from None
    statuses = [run["status"]]
    if run["route"]["mode"] == "isolated_switch" and run["route"]["strategy"] != "partition_replace":
        raise BenchmarkInputError("invalid_switch_route")
    if run["producer"]["dirty"] or run["subject"]["dirty"]:
        statuses.append("UNVERIFIED")
    for scope, field in (("type_fidelity", "fidelity_receipt"), ("failure_recovery", "recovery_receipt")):
        statuses.append(_receipt(store, path.parent, run[field], run, scope))
    samples, ids, successful, eligible, warmup = [], set(), [], [], False
    for sample in run["samples"]:
        if sample["id"] in ids:
            raise BenchmarkInputError("duplicate_sample_id")
        ids.add(sample["id"])
        if sample["status"] != "PASS" and not sample["reason"]:
            raise BenchmarkInputError("sample_reason_required")
        correctness = _receipt(store, path.parent, sample["correctness"], run, "sample", sample["id"])
        status = _status([sample["status"], correctness])
        if sample["observations"] is not None:
            observed = store.json(store.reference(path.parent, sample["observations"]))
            if (
                not isinstance(observed, dict)
                or observed.get("schema_version") != 1
                or observed.get("kind") != "native-delivery-observations"
            ):
                raise BenchmarkInputError("invalid_observations")
            if observed.get("status") != "PASS" or sample["observations"]["status"] != "PASS":
                status = _status([status, "UNVERIFIED"])
        timing = [sample[key] for key in ("visibility_seconds", "pipeline_seconds")]
        measured = all(m["availability"] == "measured" and m["unit"] == "seconds" and m["value"] > 0 for m in timing)
        if measured and timing[1]["value"] < timing[0]["value"]:
            raise BenchmarkInputError("pipeline_precedes_visibility")
        if not measured:
            status = _status([status, "UNVERIFIED"])
        statuses.append(status)
        if sample["is_warmup"]:
            warmup = warmup or status == "PASS"
        elif sample["status"] == "PASS" and measured:
            successful.append(sample["visibility_seconds"]["value"])
            if status == "PASS" and warmup:
                eligible.append(sample["visibility_seconds"]["value"])
        samples.append(
            {
                "id": sample["id"],
                "is_warmup": sample["is_warmup"],
                "status": status,
                "correctness": sample["correctness"],
                "observations": sample["observations"],
            }
        )
    if len(eligible) < 3 or not warmup:
        statuses.append("UNVERIFIED")
    status = _status(statuses)
    return run, {
        "envelope": store.export(path, status),
        "status": status,
        "limitations": ["inspect_retained_receipts_and_sample_status"] if status != "PASS" else [],
        "samples": samples,
        "successful_samples": len(successful),
        "eligible_samples": len(eligible),
        "median_seconds": statistics.median(eligible) if status == "PASS" else None,
    }


def _campaign(store: _Artifacts, path: Path) -> tuple[list[str], dict[tuple[str, str], Any], bool]:
    payload = store.json(path)
    if not isinstance(payload, dict):
        raise BenchmarkInputError("invalid_schema")
    campaign = payload.get("kind") == "native-delivery-campaign"
    refs = []
    if campaign:
        _validate(payload, CAMPAIGN_SCHEMA)
        refs = [(store.reference(path.parent, ref), ref["status"]) for ref in payload["runs"]]
    else:
        refs = [(path, "PASS")]
    runs = {}
    for run_path, reference_status in refs:
        run, summary = _run(store, run_path)
        summary["status"] = _status([summary["status"], reference_status])
        key = (run["workload"]["id"], run["configuration"]["sha256"])
        if key in runs:
            raise BenchmarkInputError("duplicate_workload_case")
        runs[key] = (run, summary)
    declared = payload["workloads"] if campaign else [payload["workload"]["id"]]
    if any(key[0] not in declared for key in runs):
        raise BenchmarkInputError("undeclared_workload")
    if len({run["subject"]["commit"] for run, _ in runs.values()}) > 1:
        raise BenchmarkInputError("campaign_subject_drift")
    return declared, runs, campaign


def compare(baseline: Path, candidate: Path, *, output: Path | None = None, overwrite: bool = False) -> dict[str, Any]:
    """Validate v1 runs/campaigns and optionally atomically write a comparison.

    Schema, identity and file errors raise; missing eligible measurements yield
    UNVERIFIED. No SQL/network is used, and no thresholds are treated as results.
    """
    store = _Artifacts()
    before_ids, before, before_campaign = _campaign(store, baseline)
    after_ids, after, after_campaign = _campaign(store, candidate)
    if set(before_ids) != set(after_ids) or before_campaign != after_campaign:
        raise BenchmarkInputError("campaign_identity_drift")
    if not before_campaign and set(before) != set(after):
        raise BenchmarkInputError("workload_or_configuration_drift")
    limitations = [
        "hashes_are_integrity_not_live_authentication",
        "medians_only_no_p95",
        "structural_metrics_unverified",
    ]
    if not before_campaign:
        limitations.append("single_workload_only")
    missing = (
        set(before) != set(after)
        or set(before_ids) != {key[0] for key in before}
        or set(after_ids) != {key[0] for key in after}
    )
    workloads = []
    for key in sorted(set(before) & set(after)):
        left, left_summary = before[key]
        right, right_summary = after[key]
        for identity in ("route", "workload", "configuration", "environment"):
            if left[identity] != right[identity]:
                raise BenchmarkInputError(f"{identity}_drift")
        status = _status([left_summary["status"], right_summary["status"]])
        ratio = right_summary["median_seconds"] / left_summary["median_seconds"] if status == "PASS" else None
        workloads.append(
            {
                "id": key[0],
                "configuration_sha256": key[1],
                "baseline": left_summary,
                "candidate": right_summary,
                "ratio": ratio,
                "status": status,
            }
        )
    statuses = [w["status"] for w in workloads]
    if missing or not workloads:
        statuses.append("UNVERIFIED")
        limitations.append("missing_declared_cases")
    status = _status(statuses)
    if status == "PASS" and (
        not any(w["ratio"] <= 0.85 for w in workloads) or any(w["ratio"] > 1.05 for w in workloads)
    ):
        status = "FAIL"
    report = {
        "schema_version": 1,
        "kind": "native-delivery-comparison",
        "baseline": store.export(baseline, status),
        "candidate": store.export(candidate, status),
        "workloads": workloads,
        "thresholds": {"any_median_ratio_at_most": 0.85, "every_median_ratio_at_most": 1.05, "minimum_trials": 3},
        "structural_checks": {"status": "UNVERIFIED", "reason": "no_structural_receipt_contract"},
        "status": status,
        "limitations": limitations,
    }
    root = (
        output.parent.resolve()
        if output is not None
        else Path(os.path.commonpath([baseline.resolve().parent, candidate.resolve().parent]))
    )
    if output is not None:
        _check_output(output, store, overwrite=overwrite)
    store.relativize(root, (baseline.parent, candidate.parent), retain=output is not None)
    report["sha256"] = content_sha256(canonical_json(report))
    if output is not None:
        _write_report(output, report, store, overwrite=overwrite)
    return report


def _check_output(path: Path, store: _Artifacts, *, overwrite: bool) -> None:
    if (
        path.is_symlink()
        or path.resolve() in store.bytes
        or (path.exists() and any(path.samefile(p) for p in store.bytes))
    ):
        raise BenchmarkInputError("output_aliases_evidence")
    if path.exists() and not overwrite:
        raise BenchmarkInputError("output_exists_use_overwrite")
    for retained, content in store.bytes.items():
        if retained.read_bytes() != content:
            raise BenchmarkInputError("artifact_changed")


def _write_report(path: Path, report: dict[str, Any], store: _Artifacts, *, overwrite: bool) -> None:
    _check_output(path, store, overwrite=overwrite)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=".native-delivery-", delete=False
        ) as stream:
            temporary = stream.name
            stream.write(canonical_json(report) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        if overwrite:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)  # Atomic no-clobber publication, including concurrent writers.
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)
