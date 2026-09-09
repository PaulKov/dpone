"""Verify the closed bytes of one provider-bound release-candidate artifact."""

from __future__ import annotations

import argparse
import importlib.util
import os
import stat
import sys
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
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


codec = _load_sibling("dpone_release_candidate_codec_archive", "release_candidate_evidence_codec.py")
policy = _load_sibling("dpone_release_candidate_policy_archive", "release_candidate_evidence_policy.py")
validation = _load_sibling(
    "dpone_release_candidate_validation_archive",
    "release_candidate_evidence_validation.py",
)
builder = _load_sibling("dpone_release_candidate_builder_archive", "release_candidate_evidence_builder.py")
contract = _load_sibling(
    "dpone_release_candidate_contract_archive",
    "release_candidate_evidence_contract.py",
)
resource_limits = _load_sibling(
    "dpone_release_candidate_resource_limits_archive",
    "artifact_resource_limits.py",
)


@dataclass(frozen=True, slots=True)
class VerifiedReleaseCandidateBundle:
    """Credential-free identity of verified inner evidence bytes."""

    repository: str
    commit_sha: str
    release: str
    profile: str
    run_id: int
    run_attempt: int
    manifest_sha256: str
    pack_sha256: str
    receipt_sha256: str
    source_chain_sha256: str
    binding_id: str
    source_count: int

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def verify_archive(
    archive: bytes,
    *,
    repository: str,
    commit_sha: str,
    release: str,
    run_id: int | None = None,
    run_attempt: int | None = None,
) -> VerifiedReleaseCandidateBundle:
    """Validate one bounded GitHub Actions artifact ZIP."""

    members = resource_limits.read_bounded_zip(archive, resource="release-candidate evidence artifact")
    expected_files = expected_file_names()
    expected_dirs = _expected_directory_names(expected_files)
    for member in members:
        if member.is_dir and member.filename not in expected_dirs:
            raise ValueError(f"release-candidate artifact contains unexpected directory {member.filename!r}")
    files = {member.filename: member.content for member in members if not member.is_dir}
    return verify_files(
        files,
        repository=repository,
        commit_sha=commit_sha,
        release=release,
        run_id=run_id,
        run_attempt=run_attempt,
    )


def verify_directory(
    directory: Path,
    *,
    repository: str,
    commit_sha: str,
    release: str,
    run_id: int | None = None,
    run_attempt: int | None = None,
) -> VerifiedReleaseCandidateBundle:
    """Validate the exact directory before provider upload."""

    if not directory.is_dir() or directory.is_symlink():
        raise ValueError("release-candidate authority directory is unavailable")
    files: dict[str, bytes] = {}
    for path in directory.rglob("*"):
        relative = path.relative_to(directory).as_posix()
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            continue
        if not stat.S_ISREG(mode) or path.is_symlink():
            raise ValueError(f"release-candidate authority contains unsafe file type {relative!r}")
        files[relative] = path.read_bytes()
    return verify_files(
        files,
        repository=repository,
        commit_sha=commit_sha,
        release=release,
        run_id=run_id,
        run_attempt=run_attempt,
    )


def verify_files(
    files: Mapping[str, bytes],
    *,
    repository: str,
    commit_sha: str,
    release: str,
    run_id: int | None = None,
    run_attempt: int | None = None,
) -> VerifiedReleaseCandidateBundle:
    """Validate one already bounded closed file mapping."""

    expected = expected_file_names()
    if frozenset(files) != expected:
        missing = sorted(expected - frozenset(files))
        extra = sorted(frozenset(files) - expected)
        raise ValueError(f"release-candidate artifact file set is invalid; missing={missing}, extra={extra}")
    if files[builder.EXIT_NAME] != b"0\n":
        raise ValueError("release-candidate producer exit code is not the exact success marker")

    manifest, manifest_raw = _canonical_object(files[builder.MANIFEST_NAME], field="manifest")
    pack, pack_raw = _canonical_object(files[builder.PACK_NAME], field="pack")
    receipt, receipt_raw = _canonical_object(files[builder.RECEIPT_NAME], field="receipt")
    codec.require_exact_keys(manifest, contract.MANIFEST_KEYS, field="manifest")
    codec.require_exact_keys(pack, contract.PACK_KEYS, field="pack")
    codec.require_exact_keys(receipt, contract.RECEIPT_KEYS, field="receipt")

    actual_run_id = codec.require_positive_int(manifest.get("run_id"), field="manifest.run_id")
    actual_attempt = codec.require_positive_int(manifest.get("run_attempt"), field="manifest.run_attempt")
    if run_id is not None and actual_run_id != run_id:
        raise ValueError("release-candidate manifest run ID does not match provider run")
    if run_attempt is not None and actual_attempt != run_attempt:
        raise ValueError("release-candidate manifest attempt does not match provider attempt")
    identity = {
        "repository": repository,
        "commit_sha": commit_sha,
        "release": release,
        "profile": policy.PROFILE,
        "policy_sha256": policy.POLICY_SHA256,
    }
    if manifest.get("schema") != builder.MANIFEST_SCHEMA or any(
        manifest.get(key) != value for key, value in identity.items()
    ):
        raise ValueError("release-candidate manifest identity is invalid")

    source_raw: dict[str, bytes] = {}
    entries = manifest.get("entries")
    if not isinstance(entries, list) or len(entries) != len(policy.SOURCE_PATHS):
        raise ValueError("release-candidate manifest entry count is invalid")
    normalized_entries: list[dict[str, Any]] = []
    for index, value in enumerate(entries):
        if not isinstance(value, dict):
            raise ValueError(f"manifest.entries[{index}] must be a mapping")
        codec.require_exact_keys(value, contract.ENTRY_KEYS, field=f"manifest.entries[{index}]")
        role = codec.require_string(value.get("role"), field=f"manifest.entries[{index}].role")
        if role not in policy.SOURCE_PATHS:
            raise ValueError(f"manifest contains unknown source role {role!r}")
        expected_path = f"{builder.SOURCE_PREFIX}/{policy.SOURCE_PATHS[role]}"
        if value.get("path") != expected_path:
            raise ValueError(f"manifest source path for {role} is invalid")
        raw = files[expected_path]
        if value.get("size_bytes") != len(raw) or value.get("sha256") != codec.sha256_bytes(raw):
            raise ValueError(f"manifest source digest or size for {role} is invalid")
        source_raw[role] = raw
        normalized_entries.append(dict(value))
    expected_roles = sorted(policy.SOURCE_PATHS)
    if [entry["role"] for entry in normalized_entries] != expected_roles:
        raise ValueError("release-candidate manifest entries are not binary-role sorted")

    chain_projection = {
        "schema": builder.CHAIN_SCHEMA,
        "policy_sha256": policy.POLICY_SHA256,
        "entries": normalized_entries,
    }
    chain_digest = codec.sha256_bytes(codec.canonical_json_bytes(chain_projection))
    source_payloads = {role: codec.strict_json_object(raw, field=f"source.{role}") for role, raw in source_raw.items()}
    observations = {
        role: validation.validate_source(
            role,
            source_payloads[role],
            raw=source_raw[role],
            commit_sha=commit_sha,
            related_raw=source_raw,
        )
        for role in expected_roles
    }
    if observations["exact_commit_checks"].get("repository") != repository:
        raise ValueError("exact required-check source repository is invalid")
    if observations["merge_receipt"].get("repository") != repository:
        raise ValueError("merge receipt source repository is invalid")

    expected_source_digests = {role: codec.sha256_bytes(source_raw[role]) for role in expected_roles}
    expected_checklist = {name: "PASS" for name in sorted(policy.CHECKLIST_ROLES)}
    version = release.removeprefix("v")
    expected_packages = {
        "apache-airflow-providers-dpone": version,
        "dpone": version,
        "dpone-airflow-pack": version,
        "dpone-native-accel": version,
    }
    expected_pack_values = {
        "schema": builder.PACK_SCHEMA,
        "status": "PASS",
        "decision": "GO",
        **identity,
        "source_chain_sha256": chain_digest,
        "required_roles": expected_roles,
        "source_digests": expected_source_digests,
        "observations": observations,
        "checklist": expected_checklist,
        "package_versions": expected_packages,
        "blockers": [],
    }
    if pack != expected_pack_values:
        raise ValueError("release-candidate pack is not the exact derived PASS projection")

    expected_receipt = {
        "schema": builder.RECEIPT_SCHEMA,
        "status": "PASS",
        "repository": repository,
        "commit_sha": commit_sha,
        "release": release,
        "profile": policy.PROFILE,
        "workflow_path": policy.WORKFLOW_PATH,
        "workflow_name": policy.WORKFLOW_NAME,
        "job_name": policy.JOB_NAME,
        "run_id": actual_run_id,
        "run_attempt": actual_attempt,
        "policy_sha256": policy.POLICY_SHA256,
        "manifest_sha256": codec.sha256_bytes(manifest_raw),
        "pack_sha256": codec.sha256_bytes(pack_raw),
        "source_chain_sha256": chain_digest,
    }
    if any(receipt.get(key) != value for key, value in expected_receipt.items()):
        raise ValueError("release-candidate receipt identity or digest projection is invalid")
    if receipt.get("binding_id") != builder.receipt_binding_id(expected_receipt):
        raise ValueError("release-candidate receipt binding ID is invalid")
    return VerifiedReleaseCandidateBundle(
        repository=repository,
        commit_sha=commit_sha,
        release=release,
        profile=policy.PROFILE,
        run_id=actual_run_id,
        run_attempt=actual_attempt,
        manifest_sha256=codec.sha256_bytes(manifest_raw),
        pack_sha256=codec.sha256_bytes(pack_raw),
        receipt_sha256=codec.sha256_bytes(receipt_raw),
        source_chain_sha256=chain_digest,
        binding_id=str(receipt["binding_id"]),
        source_count=len(source_raw),
    )


def expected_file_names() -> frozenset[str]:
    source_names = {f"{builder.SOURCE_PREFIX}/{relative}" for relative in policy.SOURCE_PATHS.values()}
    return frozenset(
        {
            builder.MANIFEST_NAME,
            builder.PACK_NAME,
            builder.RECEIPT_NAME,
            builder.EXIT_NAME,
            *source_names,
        }
    )


def _expected_directory_names(files: frozenset[str]) -> frozenset[str]:
    result: set[str] = set()
    for filename in files:
        parent = PurePosixPath(filename).parent
        while parent != PurePosixPath("."):
            result.add(f"{parent.as_posix().rstrip('/')}/")
            parent = parent.parent
    return frozenset(result)


def _canonical_object(raw: bytes, *, field: str) -> tuple[dict[str, Any], bytes]:
    payload = codec.strict_json_object(raw, field=field)
    if codec.canonical_json_bytes(payload) != raw:
        raise ValueError(f"{field} bytes are not canonical JSON plus LF")
    return payload, raw


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--commit-sha", default=os.environ.get("GITHUB_SHA"))
    parser.add_argument("--release", required=True)
    parser.add_argument("--run-id", type=int, default=os.environ.get("GITHUB_RUN_ID"))
    parser.add_argument("--run-attempt", type=int, default=os.environ.get("GITHUB_RUN_ATTEMPT"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        verified = verify_directory(
            args.directory,
            repository=args.repository or "",
            commit_sha=args.commit_sha or "",
            release=args.release,
            run_id=args.run_id,
            run_attempt=args.run_attempt,
        )
        payload = {
            "schema": "dpone.release_candidate_evidence_local_verification.v1",
            "status": "PASS",
            **verified.to_payload(),
        }
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        payload = {
            "schema": "dpone.release_candidate_evidence_local_verification.v1",
            "status": "FAIL",
            "message": str(exc),
        }
    raw = codec.canonical_json_bytes(payload)
    if args.output is not None:
        if args.output.exists():
            print("DPONE_RELEASE_CANDIDATE_EVIDENCE_INVALID: verification output already exists", file=sys.stderr)
            return 1
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(raw)
    print(raw.decode("utf-8"), end="")
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "VerifiedReleaseCandidateBundle",
    "expected_file_names",
    "verify_archive",
    "verify_directory",
    "verify_files",
]
