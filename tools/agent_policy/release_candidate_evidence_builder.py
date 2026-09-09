"""Build one closed pre-tag release-candidate evidence bundle from observed inputs."""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
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


codec = _load_sibling("dpone_release_candidate_codec_builder", "release_candidate_evidence_codec.py")
policy = _load_sibling("dpone_release_candidate_policy_builder", "release_candidate_evidence_policy.py")
validation = _load_sibling(
    "dpone_release_candidate_validation_builder",
    "release_candidate_evidence_validation.py",
)

MANIFEST_NAME = "release_candidate_evidence_manifest.json"
PACK_NAME = "release_candidate_evidence_pack.json"
RECEIPT_NAME = "release_candidate_evidence_receipt.json"
EXIT_NAME = "release_candidate_evidence_exit_code.txt"
SOURCE_PREFIX = "sources"
MANIFEST_SCHEMA = "dpone.release_candidate_evidence_manifest.v1"
PACK_SCHEMA = "dpone.release_candidate_evidence_pack.v1"
RECEIPT_SCHEMA = "dpone.release_candidate_evidence_receipt.v1"
CHAIN_SCHEMA = "dpone.release_candidate_evidence_source_chain.v1"
BINDING_DOMAIN = b"dpone.release_candidate_evidence_receipt.v1\0"


def build_bundle(
    *,
    root: Path,
    input_root: Path,
    output_dir: Path,
    repository: str,
    commit_sha: str,
    release: str,
    run_id: int,
    run_attempt: int,
) -> dict[str, Any]:
    """Validate observed inputs and create one non-overwritable authority directory."""

    _validate_identity(
        repository=repository,
        commit_sha=commit_sha,
        release=release,
        run_id=run_id,
        run_attempt=run_attempt,
    )
    if output_dir.exists():
        raise ValueError(f"authority output directory already exists: {output_dir}")

    source_payloads: dict[str, dict[str, Any]] = {}
    source_raw: dict[str, bytes] = {}
    for role, relative in sorted(policy.SOURCE_PATHS.items()):
        payload, raw = codec.read_strict_json(input_root / relative, field=role)
        source_payloads[role] = payload
        source_raw[role] = raw

    observations = {
        role: validation.validate_source(
            role,
            source_payloads[role],
            raw=source_raw[role],
            commit_sha=commit_sha,
            related_raw=source_raw,
        )
        for role in sorted(source_payloads)
    }
    exact_repository = observations["exact_commit_checks"]["repository"]
    merge_repository = observations["merge_receipt"]["repository"]
    if exact_repository != repository or merge_repository != repository:
        raise ValueError("required-check and merge-receipt repositories must match the candidate")
    package_versions = validation.validate_project_release(root, release=release)

    entries = [
        {
            "role": role,
            "path": f"{SOURCE_PREFIX}/{policy.SOURCE_PATHS[role]}",
            "size_bytes": len(source_raw[role]),
            "sha256": codec.sha256_bytes(source_raw[role]),
        }
        for role in sorted(source_raw)
    ]
    chain_projection = {
        "schema": CHAIN_SCHEMA,
        "policy_sha256": policy.POLICY_SHA256,
        "entries": entries,
    }
    chain_digest = codec.sha256_bytes(codec.canonical_json_bytes(chain_projection))
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "repository": repository,
        "commit_sha": commit_sha,
        "release": release,
        "profile": policy.PROFILE,
        "run_id": run_id,
        "run_attempt": run_attempt,
        "policy_sha256": policy.POLICY_SHA256,
        "entries": entries,
    }
    source_digests = {entry["role"]: entry["sha256"] for entry in entries}
    checklist = {name: "PASS" for name in sorted(policy.CHECKLIST_ROLES)}
    pack = {
        "schema": PACK_SCHEMA,
        "status": "PASS",
        "decision": "GO",
        "repository": repository,
        "commit_sha": commit_sha,
        "release": release,
        "profile": policy.PROFILE,
        "policy_sha256": policy.POLICY_SHA256,
        "source_chain_sha256": chain_digest,
        "required_roles": sorted(policy.SOURCE_PATHS),
        "source_digests": source_digests,
        "observations": observations,
        "checklist": checklist,
        "package_versions": package_versions,
        "blockers": [],
    }

    output_dir.mkdir(parents=True, exist_ok=False)
    try:
        for role, relative in sorted(policy.SOURCE_PATHS.items()):
            destination = output_dir / SOURCE_PREFIX / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("xb") as handle:
                handle.write(source_raw[role])
                handle.flush()
                os.fsync(handle.fileno())
        manifest_raw = codec.write_canonical_json(output_dir / MANIFEST_NAME, manifest)
        pack_raw = codec.write_canonical_json(output_dir / PACK_NAME, pack)
        receipt_without_binding = {
            "schema": RECEIPT_SCHEMA,
            "status": "PASS",
            "repository": repository,
            "commit_sha": commit_sha,
            "release": release,
            "profile": policy.PROFILE,
            "workflow_path": policy.WORKFLOW_PATH,
            "workflow_name": policy.WORKFLOW_NAME,
            "job_name": policy.JOB_NAME,
            "run_id": run_id,
            "run_attempt": run_attempt,
            "policy_sha256": policy.POLICY_SHA256,
            "manifest_sha256": codec.sha256_bytes(manifest_raw),
            "pack_sha256": codec.sha256_bytes(pack_raw),
            "source_chain_sha256": chain_digest,
        }
        receipt = {
            **receipt_without_binding,
            "binding_id": receipt_binding_id(receipt_without_binding),
        }
        receipt_raw = codec.write_canonical_json(output_dir / RECEIPT_NAME, receipt)
        with (output_dir / EXIT_NAME).open("xb") as handle:
            handle.write(b"0\n")
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        shutil.rmtree(output_dir, ignore_errors=True)
        raise
    return {
        "status": "PASS",
        "manifest_sha256": codec.sha256_bytes(manifest_raw),
        "pack_sha256": codec.sha256_bytes(pack_raw),
        "receipt_sha256": codec.sha256_bytes(receipt_raw),
        "source_chain_sha256": chain_digest,
        "source_count": len(entries),
    }


def receipt_binding_id(payload_without_binding: dict[str, Any]) -> str:
    """Return the domain-separated binding for a receipt without its binding field."""

    return codec.sha256_bytes(BINDING_DOMAIN + codec.canonical_json_bytes(payload_without_binding))


def _validate_identity(
    *,
    repository: str,
    commit_sha: str,
    release: str,
    run_id: int,
    run_attempt: int,
) -> None:
    if codec.REPOSITORY.fullmatch(repository) is None:
        raise ValueError("repository must use owner/name form")
    if codec.FULL_SHA.fullmatch(commit_sha) is None:
        raise ValueError("commit SHA must be full lowercase hexadecimal")
    if codec.RELEASE.fullmatch(release) is None:
        raise ValueError("release must use canonical stable form vX.Y.Z")
    codec.require_positive_int(run_id, field="run_id")
    codec.require_positive_int(run_attempt, field="run_attempt")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--commit-sha", default=os.environ.get("GITHUB_SHA"))
    parser.add_argument("--release", required=True)
    parser.add_argument("--run-id", type=int, default=os.environ.get("GITHUB_RUN_ID"))
    parser.add_argument("--run-attempt", type=int, default=os.environ.get("GITHUB_RUN_ATTEMPT"))
    args = parser.parse_args(argv)
    try:
        result = build_bundle(
            root=args.root,
            input_root=args.input_root,
            output_dir=args.output_dir,
            repository=args.repository or "",
            commit_sha=args.commit_sha or "",
            release=args.release,
            run_id=args.run_id,
            run_attempt=args.run_attempt,
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        print(f"DPONE_RELEASE_CANDIDATE_EVIDENCE_INVALID: {exc}", file=sys.stderr)
        return 1
    print(codec.canonical_json_bytes(result).decode("utf-8"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CHAIN_SCHEMA",
    "EXIT_NAME",
    "MANIFEST_NAME",
    "MANIFEST_SCHEMA",
    "PACK_NAME",
    "PACK_SCHEMA",
    "RECEIPT_NAME",
    "RECEIPT_SCHEMA",
    "SOURCE_PREFIX",
    "build_bundle",
    "receipt_binding_id",
]
