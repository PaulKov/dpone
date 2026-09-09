"""Write one source-bound, diagnostic-only PR Gate shadow capacity receipt."""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path

from dpone.adapters.ci_shadow_reconciliation_github import GitHubReconciliationApi, GitHubReconciliationApiError
from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.contracts.ci_shadow_reconciliation import ReconciliationPolicyV2
from dpone.contracts.strict_json import StrictJsonError, strict_json_object
from dpone.services.ci.shadow_capacity import blocked_capacity_calibration, calibrate_capacity
from dpone.services.ci.shadow_capacity_archive import CapacityArchiveError
from dpone.services.ci.shadow_capacity_evidence import render_capacity_evidence
from dpone.services.ci.shadow_capacity_source import authenticate_capacity_source
from dpone.services.ci.shadow_capacity_transport import ArtifactTransportError, fetch_artifact_archive
from dpone.services.ci.shadow_observation_bundle import evidence_manifest
from dpone.services.ci.shadow_observation_bundle_verifier import verify_observation_bundle
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget
from dpone.services.ci.shadow_reconciliation_complete_observation import acquire_complete_observation
from dpone.services.ci.shadow_reconciliation_observation import ObservationError

_MANIFEST = Path(".agents/policy/ci-shadow-reconciliation-observation-bundle-v1.yml")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the closed probe; valid diagnostic evidence always exits one."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", type=Path, required=True, help="GitHub workflow_dispatch event JSON")
    parser.add_argument("--output", type=Path, required=True, help="New immutable diagnostic receipt path")
    args = parser.parse_args(argv)
    try:
        return _run(args.event, args.output)
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"capacity calibration could not produce evidence: {exc}", file=sys.stderr)
        return 70


def _run(event_path: Path, output: Path) -> int:
    event = _event(event_path)
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise ValueError("GITHUB_TOKEN is required for the authenticated capacity probe")
    repository = _mapping(event.get("repository"), "event repository")
    name = _string(repository.get("full_name"), "event repository name")
    source_root = Path.cwd().resolve()
    if not output.parent.is_dir() or output.parent.resolve() != output.resolve().parent:
        raise ValueError("output parent must be an existing non-symlink directory")
    policy = ReconciliationPolicyV2.fixed()
    budget = RequestBudget(policy=policy)
    provider = GitHubReconciliationApi(repository=name, token=token)
    source = authenticate_capacity_source(event, os.environ, provider=provider, budget=budget)
    entries, bundle_digest, manifest_sha256 = verify_observation_bundle(
        source_root / _MANIFEST,
        source_root=source_root,
        source_commit_sha=source.workflow_sha,
        provider=provider,
        budget=budget,
    )
    interval = policy.interval_for(_utc_now())
    try:
        calibration = calibrate_capacity(
            lambda: acquire_complete_observation(
                provider,
                archive_fetcher=lambda artifact_id: fetch_artifact_archive(
                    f"https://api.github.com/repos/{name}/actions/artifacts/{artifact_id}/zip",
                    budget=budget,
                    request=provider.request_artifact_archive,
                ),
                policy=policy,
                interval=interval,
                budget=budget,
                utc_clock=_utc_now,
            ),
            budget=budget,
            policy=policy,
            utc_clock=_utc_now,
        )
    except (ArtifactTransportError, CapacityArchiveError, GitHubReconciliationApiError, ObservationError, RuntimeError):
        calibration = blocked_capacity_calibration(
            budget=budget,
            observation_started_at=interval.observation_started_at,
            utc_clock=_utc_now,
        )
    manifest = evidence_manifest(entries, manifest_sha256=manifest_sha256)
    manifest_entries = manifest["entries"]
    if not isinstance(manifest_entries, list) or not all(isinstance(item, dict) for item in manifest_entries):
        raise RuntimeError("observation bundle manifest projection is malformed")
    evidence = render_capacity_evidence(
        calibration,
        source=source,
        policy=policy,
        observation_bundle_sha256=bundle_digest,
        observation_bundle_manifest_sha256=manifest_sha256,
        observation_bundle_entries=manifest_entries,
        execution_configuration=_execution_configuration(),
        interval={
            "scan_from": _timestamp(interval.scan_from),
            "safe_scan_through": _timestamp(interval.safe_scan_through),
            "observation_started_at": _timestamp(calibration.observation_started_at),
        },
    )
    payload = json.dumps(evidence, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    DescriptorPinnedCreateOnlyEvidenceWriter(output.parent.resolve()).write(output.name, payload)
    return 1


def _event(path: Path) -> Mapping[str, object]:
    try:
        return strict_json_object(path.read_bytes())
    except (OSError, StrictJsonError) as exc:
        raise ValueError("workflow event is not a strict JSON object") from exc


def _execution_configuration() -> dict[str, str]:
    label = os.environ.get("DPONE_CAPACITY_RUNNER_LABEL")
    architecture = os.environ.get("RUNNER_ARCH")
    if not label or not architecture:
        raise ValueError("capacity runner label and architecture must be explicitly bound by the workflow")
    return {"runner_label": label, "runner_arch": architecture, "python_version": platform.python_version()}


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} is malformed")
    return value


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} is malformed")
    return value


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)  # noqa: UP017


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
