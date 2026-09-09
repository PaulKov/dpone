"""Helpers and command runners for the Shape B evidence CLI."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
from argparse import Namespace
from pathlib import Path
from typing import Any


def _load_sibling(module_name: str, filename: str) -> Any:
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


canonical = _load_sibling("dpone_agent_release_canonical", "release_canonical.py")
lease = _load_sibling("dpone_agent_release_lease_service", "release_lease_service.py")
store_mod = _load_sibling("dpone_agent_release_evidence_store_b2", "release_evidence_store_b2.py")
attest = _load_sibling("dpone_agent_release_attest_draft", "release_attest_draft.py")
stage = _load_sibling("dpone_agent_release_stage_draft", "release_stage_draft.py")
authorize = _load_sibling("dpone_agent_release_authorize", "release_authorize.py")
snapshots = _load_sibling("dpone_agent_release_governance_snapshot", "release_governance_snapshot.py")
github = _load_sibling("dpone_agent_release_github_api", "release_github_api.py")


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"missing required environment variable: {name}")
    return value


def release_ids(args: Namespace) -> dict[str, Any]:
    tag_ref = args.tag if args.tag.startswith("refs/tags/") else f"refs/tags/{args.tag}"
    release_identity_id = canonical.sha256_id(
        "dpone.release.identity.v2",
        {
            "repository_id": args.repository_id,
            "tag": args.tag.removeprefix("refs/tags/"),
            "projects": [
                "dpone",
                "dpone-native-accel",
                "dpone-airflow-pack",
                "apache-airflow-providers-dpone",
            ],
        },
    )
    release_authority_id = canonical.sha256_id(
        "dpone.release.authority.v2",
        {
            "release_id": release_identity_id,
            "tag_object_sha": args.tag_object_sha,
            "peeled_commit_sha": args.peeled_commit_sha,
            "policy_sha256": args.policy_sha256,
            "protected_base_ref": "refs/heads/master",
        },
    )
    return {
        "tag_ref": tag_ref,
        "release_identity_id": release_identity_id,
        "release_authority_id": release_authority_id,
    }


def build_store(args: Namespace) -> Any:
    if args.dry_memory:
        return store_mod.InMemoryEvidenceStore()
    return store_mod.BackblazeB2EvidenceStore(
        store_mod.B2Credentials(
            key_id=require_env("B2_APPLICATION_KEY_ID"),
            application_key=require_env("B2_APPLICATION_KEY"),
            bucket_id=require_env("B2_BUCKET_ID"),
            bucket_name=require_env("B2_BUCKET_NAME"),
        )
    )


def producer(*, default_job: str) -> dict[str, Any]:
    workflow_path = ".github/workflows/release-controller.yml"
    workflow_ref = os.environ.get("GITHUB_WORKFLOW_REF", "")
    if "/.github/workflows/" in workflow_ref:
        workflow_path = ".github/workflows/" + workflow_ref.split("/.github/workflows/", 1)[1].split("@", 1)[0]
    return {
        "kind": "github_actions_job",
        "repository_id": os.environ.get("GITHUB_REPOSITORY_ID", "1305993853"),
        "workflow_id": os.environ.get("DPONE_CONTROLLER_WORKFLOW_ID", "316322127"),
        "workflow_path": workflow_path,
        "workflow_sha": os.environ.get("GITHUB_SHA", "0" * 40),
        "run_id": os.environ.get("GITHUB_RUN_ID", "0"),
        "run_attempt": int(os.environ.get("GITHUB_RUN_ATTEMPT", "1")),
        "job_name": os.environ.get("GITHUB_JOB", default_job),
        "environment": os.environ.get("DPONE_CONTROLLER_ENVIRONMENT", "none"),
    }


def run_acquire_lease(store: Any, ids: dict[str, Any], args: Namespace, prod: dict[str, Any], now: str) -> int:
    try:
        receipt = lease.acquire_publication_lease(
            store,
            release_identity_id=ids["release_identity_id"],
            release_authority_id=ids["release_authority_id"],
            repository_id=args.repository_id,
            tag_ref=ids["tag_ref"],
            attempt_seed={
                "run_id": int(str(prod["run_id"])),
                "run_attempt": int(str(prod["run_attempt"])),
            },
            producer=prod,
            ttl_seconds=args.ttl_seconds,
            now_utc=now,
            retention_days=args.retention_days,
        )
    except lease.LeaseConflictError as exc:
        print(json.dumps({"status": "CONFLICT", "error": str(exc)}, sort_keys=True))
        return 3
    print(json.dumps({"status": "LEASE_ACQUIRED", "receipt": receipt}, sort_keys=True))
    return 0


def run_capture_snapshot_a(store: Any, ids: dict[str, Any], args: Namespace, prod: dict[str, Any], now: str) -> int:
    api = github.GitHubApi(token=require_env(args.github_token_env))
    try:
        result = snapshots.append_governance_snapshot(
            store,
            api,
            label="A",
            owner=args.owner,
            repo=args.repo,
            release_identity_id=ids["release_identity_id"],
            release_authority_id=ids["release_authority_id"],
            repository_id=args.repository_id,
            tag_ref=ids["tag_ref"],
            producer=prod,
            now_utc=now,
            retention_days=args.retention_days,
            gap_seconds=args.snapshot_gap_seconds,
        )
    except snapshots.StreamPrerequisiteError as exc:
        print(json.dumps({"status": "PREREQUISITE", "error": str(exc)}, sort_keys=True))
        return 4
    except snapshots.SnapshotError as exc:
        print(json.dumps({"status": "SNAPSHOT_ERROR", "error": str(exc)}, sort_keys=True))
        return 6
    except snapshots.GitHubApiError as exc:
        print(json.dumps({"status": "GITHUB_ERROR", "error": str(exc)}, sort_keys=True))
        return 5
    print(json.dumps(result, sort_keys=True))
    return 0


def run_attest_draft_dry_run(store: Any, ids: dict[str, Any], args: Namespace, prod: dict[str, Any], now: str) -> int:
    distributions = [
        {
            "project": "dpone",
            "filename": f"dpone-{args.tag.removeprefix('refs/tags/')}-py3-none-any.whl",
            "size": 1,
            "sha256": "d" * 64,
        }
    ]
    try:
        result = attest.run_attest_and_draft_dry_run(
            store,
            release_identity_id=ids["release_identity_id"],
            release_authority_id=ids["release_authority_id"],
            repository_id=args.repository_id,
            tag_ref=ids["tag_ref"],
            producer=prod,
            now_utc=now,
            distributions=distributions,
            retention_days=args.retention_days,
        )
    except attest.StreamPrerequisiteError as exc:
        print(json.dumps({"status": "PREREQUISITE", "error": str(exc)}, sort_keys=True))
        return 4
    print(json.dumps(result, sort_keys=True))
    return 0


def run_stage_draft_live(store: Any, ids: dict[str, Any], args: Namespace, prod: dict[str, Any], now: str) -> int:
    subject_path: Path = args.subject_file
    bundle_path: Path = args.attestation_bundle
    if not subject_path.is_file():
        raise SystemExit(f"subject file not found: {subject_path}")
    if not bundle_path.is_file():
        raise SystemExit(f"attestation bundle not found: {bundle_path}")
    subject_bytes = subject_path.read_bytes()
    bundle_bytes = bundle_path.read_bytes()
    api = github.GitHubApi(token=require_env(args.github_token_env))
    attestation = {
        "digest": "sha256:" + hashlib.sha256(bundle_bytes).hexdigest(),
        "bundle_sha256": "sha256:" + hashlib.sha256(bundle_bytes).hexdigest(),
        "bundle_bytes": len(bundle_bytes),
        "url": args.attestation_url,
        "predicate_type": "https://slsa.dev/provenance/v1",
    }
    try:
        result = stage.run_stage_draft_live(
            store,
            api,
            owner=args.owner,
            repo=args.repo,
            release_identity_id=ids["release_identity_id"],
            release_authority_id=ids["release_authority_id"],
            repository_id=args.repository_id,
            tag_ref=ids["tag_ref"],
            producer=prod,
            now_utc=now,
            subject_filename=subject_path.name,
            subject_bytes=subject_bytes,
            attestation=attestation,
            retention_days=args.retention_days,
        )
    except stage.StreamPrerequisiteError as exc:
        print(json.dumps({"status": "PREREQUISITE", "error": str(exc)}, sort_keys=True))
        return 4
    except stage.GitHubApiError as exc:
        print(json.dumps({"status": "GITHUB_ERROR", "error": str(exc)}, sort_keys=True))
        return 5
    print(json.dumps(result, sort_keys=True))
    return 0


def run_authorize_publication(store: Any, ids: dict[str, Any], args: Namespace, prod: dict[str, Any], now: str) -> int:
    api = github.GitHubApi(token=require_env(args.github_token_env))
    try:
        result = authorize.run_authorize_publication(
            store,
            api,
            owner=args.owner,
            repo=args.repo,
            release_identity_id=ids["release_identity_id"],
            release_authority_id=ids["release_authority_id"],
            repository_id=args.repository_id,
            tag_ref=ids["tag_ref"],
            producer=prod,
            now_utc=now,
            retention_days=args.retention_days,
            snapshot_gap_seconds=args.snapshot_gap_seconds,
        )
    except authorize.StreamPrerequisiteError as exc:
        print(json.dumps({"status": "PREREQUISITE", "error": str(exc)}, sort_keys=True))
        return 4
    except authorize.AuthorizationError as exc:
        print(json.dumps({"status": "AUTHORIZATION_ERROR", "error": str(exc)}, sort_keys=True))
        return 6
    except authorize.GitHubApiError as exc:
        print(json.dumps({"status": "GITHUB_ERROR", "error": str(exc)}, sort_keys=True))
        return 5
    except RuntimeError as exc:
        print(json.dumps({"status": "STORE_ERROR", "error": str(exc)}, sort_keys=True))
        return 7
    print(json.dumps(result, sort_keys=True))
    return 0


def run_release_lease(store: Any, ids: dict[str, Any], args: Namespace, prod: dict[str, Any], now: str) -> int:
    try:
        receipt = lease.release_publication_lease(
            store,
            release_identity_id=ids["release_identity_id"],
            release_authority_id=ids["release_authority_id"],
            repository_id=args.repository_id,
            tag_ref=ids["tag_ref"],
            producer=prod,
            now_utc=now,
            reason=args.reason,
            retention_days=args.retention_days,
        )
    except lease.LeaseConflictError as exc:
        print(json.dumps({"status": "CONFLICT", "error": str(exc)}, sort_keys=True))
        return 3
    print(json.dumps({"status": "LEASE_RELEASED", "receipt": receipt}, sort_keys=True))
    return 0
