"""Fail closed unless the latest exact-SHA release-candidate evidence is valid."""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from pathlib import Path
from typing import Any


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


codec = _load_sibling("dpone_release_candidate_codec_gate", "release_candidate_evidence_codec.py")
contract = _load_sibling("dpone_release_candidate_contract_gate", "release_candidate_evidence_contract.py")
archive = _load_sibling("dpone_release_candidate_archive_gate", "release_candidate_evidence_archive.py")
source = _load_sibling("dpone_release_candidate_source_gate", "release_candidate_evidence_source.py")


def _publication_receipt_identity(selected: Any) -> tuple[list[dict[str, Any]], str]:
    """Revalidate and canonically bind the paired provider-run projection."""

    paired_runs: list[dict[str, Any]] = []
    created_at = []
    for index, item in enumerate(selected.paired_publication_runs):
        if not isinstance(item, dict):
            raise ValueError(f"paired_publication_runs[{index}] must be a mapping")
        run = dict(item)
        field = f"paired_publication_runs[{index}]"
        codec.require_exact_keys(run, contract.PAIRED_PUBLICATION_RUN_KEYS, field=field)
        codec.require_string(run["workflow_path"], field=f"{field}.workflow_path")
        codec.require_positive_int(run["run_id"], field=f"{field}.run_id")
        codec.require_positive_int(run["run_attempt"], field=f"{field}.run_attempt")
        created_at.append(source.github.parse_timestamp(run["created_at"], field=f"{field}.created_at"))
        paired_runs.append(run)
    expected_paths = sorted(source.PUBLICATION_WORKFLOW_PATHS)
    if [run["workflow_path"] for run in paired_runs] != expected_paths:
        raise ValueError("paired publication receipt must contain the two approved workflows in path order")
    caller = [run for run in paired_runs if run["workflow_path"] == selected.publication_workflow_path]
    if len(caller) != 1 or (
        caller[0]["run_id"],
        caller[0]["run_attempt"],
    ) != (selected.publication_run_id, selected.publication_run_attempt):
        raise ValueError("current publication caller does not match its paired receipt identity")
    cutoff = source.github.parse_timestamp(selected.publication_cutoff, field="publication_cutoff")
    if cutoff != min(created_at):
        raise ValueError("publication cutoff is not the exact minimum paired creation time")
    pair_sha256 = codec.canonical_json_sha256(paired_runs)
    if (
        codec.require_digest(
            selected.publication_pair_sha256,
            field="publication_pair_sha256",
        )
        != pair_sha256
    ):
        raise ValueError("paired publication run digest does not match its canonical projection")
    return paired_runs, pair_sha256


def verify_release_candidate(
    *,
    repository: str,
    commit_sha: str,
    release: str,
    publication_workflow_path: str,
    publication_run_id: int,
    publication_run_attempt: int,
    adapter: Any,
) -> dict[str, Any]:
    """Select provider evidence, verify inner bytes, and emit credential-free identity."""

    selected = source.select_release_candidate_artifact(
        repository=repository,
        commit_sha=commit_sha,
        release=release,
        publication_workflow_path=publication_workflow_path,
        publication_run_id=publication_run_id,
        publication_run_attempt=publication_run_attempt,
        adapter=adapter,
    )
    verified = archive.verify_archive(
        selected.archive_bytes,
        repository=repository,
        commit_sha=commit_sha,
        release=release,
        run_id=selected.workflow_run_id,
        run_attempt=selected.workflow_run_attempt,
    )
    paired_runs, publication_pair_sha256 = _publication_receipt_identity(selected)
    return {
        "schema": "dpone.release_candidate_evidence_verification.v1",
        "status": "PASS",
        "decision": "GO",
        "repository": repository,
        "commit_sha": commit_sha,
        "release": release,
        "publication_workflow_path": selected.publication_workflow_path,
        "publication_run_id": selected.publication_run_id,
        "publication_run_attempt": selected.publication_run_attempt,
        "paired_publication_runs": paired_runs,
        "publication_cutoff": selected.publication_cutoff,
        "publication_pair_sha256": publication_pair_sha256,
        "profile": verified.profile,
        "check_run_id": selected.check_run_id,
        "check_suite_id": selected.check_suite_id,
        "workflow_run_id": selected.workflow_run_id,
        "workflow_run_attempt": selected.workflow_run_attempt,
        "job_id": selected.job_id,
        "artifact_id": selected.artifact_id,
        "artifact_name": selected.artifact_name,
        "artifact_digest": selected.artifact_digest,
        "artifact_size_bytes": selected.artifact_size_bytes,
        "archive_sha256": selected.archive_sha256,
        "archive_size_bytes": selected.archive_size_bytes,
        "manifest_sha256": verified.manifest_sha256,
        "pack_sha256": verified.pack_sha256,
        "receipt_sha256": verified.receipt_sha256,
        "source_chain_sha256": verified.source_chain_sha256,
        "binding_id": verified.binding_id,
        "source_count": verified.source_count,
        "blockers": [],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--commit-sha", default=os.environ.get("GITHUB_SHA"))
    parser.add_argument("--release", required=True)
    parser.add_argument("--publication-workflow-path", required=True, choices=sorted(source.PUBLICATION_WORKFLOW_PATHS))
    parser.add_argument("--publication-run-id", required=True, type=int)
    parser.add_argument("--publication-run-attempt", required=True, type=int)
    parser.add_argument("--github-token-env", default="GITHUB_TOKEN")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        token = os.environ.get(args.github_token_env)
        if not token:
            raise ValueError("approved GitHub token is required")
        payload = verify_release_candidate(
            repository=args.repository or "",
            commit_sha=args.commit_sha or "",
            release=args.release,
            publication_workflow_path=args.publication_workflow_path,
            publication_run_id=args.publication_run_id,
            publication_run_attempt=args.publication_run_attempt,
            adapter=source.GitHubReleaseCandidateEvidenceAdapter(token),
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        payload = {
            "schema": "dpone.release_candidate_evidence_verification.v1",
            "status": "FAIL",
            "decision": "NO-GO",
            "repository": args.repository or "",
            "commit_sha": args.commit_sha or "",
            "release": args.release,
            "publication_workflow_path": args.publication_workflow_path,
            "publication_run_id": args.publication_run_id,
            "publication_run_attempt": args.publication_run_attempt,
            "blockers": [{"code": "RELEASE_CANDIDATE_EVIDENCE_INVALID", "message": str(exc)}],
        }
    raw = codec.canonical_json_bytes(payload)
    if args.output.exists():
        print("DPONE_RELEASE_CANDIDATE_EVIDENCE_INVALID: gate output already exists", file=sys.stderr)
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(raw)
    print(raw.decode("utf-8"), end="")
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["verify_release_candidate"]
