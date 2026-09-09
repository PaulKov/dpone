"""Construct runtime-image PASS certification from bounded, digest-bound evidence."""

from __future__ import annotations

import hashlib
import importlib
import json
import re
import stat
from pathlib import Path
from typing import Any
from uuid import uuid4

if __package__:
    from .runtime_image_certification_models import CertificationArtifacts, CertificationInputs
else:
    from runtime_image_certification_models import CertificationArtifacts, CertificationInputs  # type: ignore[no-redef]

CHECK_RECEIPT_SCHEMA = "dpone.runtime-image-check.v1"
CONTEXT_MANIFEST_SCHEMA = "dpone.runtime-image-context-manifest.v1"
MAX_CHECK_BYTES = 64 * 1024
MAX_VERIFICATION_BYTES = 16 * 1024 * 1024
MAX_SPDX_BYTES = 8 * 1024 * 1024
MAX_CONTEXT_BYTES = 64 * 1024 * 1024
MAX_CONTEXT_FILES = 20_000
CANONICAL_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
STABLE_SEMVER = re.compile(r"^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$")
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
IMAGE_NAME = re.compile(r"^ghcr\.io/[a-z0-9]+(?:[._/-][a-z0-9]+)*$")
SOURCE_REPOSITORY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*$")
POSITIVE_ID = re.compile(r"^[1-9]\d*$")
EXPECTED_DOCKERIGNORE_LINES = tuple(
    (
        "** !docker/ !docker/runtime/ !docker/runtime/Dockerfile !docker/runtime/Dockerfile.dockerignore "
        "!README.md !pyproject.toml !src/ !src/dpone/ !src/dpone/** !packages/ !packages/dpone-native-accel/ "
        "!packages/dpone-native-accel/README.md !packages/dpone-native-accel/pyproject.toml "
        "!packages/dpone-native-accel/src/ !packages/dpone-native-accel/src/** !packages/dpone-airflow-pack/ "
        "!packages/dpone-airflow-pack/README.md !packages/dpone-airflow-pack/pyproject.toml "
        "!packages/dpone-airflow-pack/src/ !packages/dpone-airflow-pack/src/** "
        "!packages/dbt-dpone/ !packages/dbt-dpone/dbt_project.yml !packages/dbt-dpone/macros/ "
        "!packages/dbt-dpone/macros/dpone_publish.sql "
        "!packages/dbt-dpone/macros/semantic_refresh_restore.sql "
        "!packages/dbt-dpone/macros/semantic_refresh_scope_merge.sql "
        "!packages/apache-airflow-providers-dpone/ !packages/apache-airflow-providers-dpone/README.md "
        "!packages/apache-airflow-providers-dpone/pyproject.toml !packages/apache-airflow-providers-dpone/src/ "
        "!packages/apache-airflow-providers-dpone/src/** **/.env **/.env.* **/AGENTS.md **/.agents/** "
        "**/.codex/** **/.git/** **/.gitignore **/__pycache__/ **/test_artifacts/**"
    ).split()
)
EXACT_CONTEXT_FILES = tuple(
    (
        "docker/runtime/Dockerfile docker/runtime/Dockerfile.dockerignore README.md pyproject.toml "
        "packages/dpone-native-accel/README.md packages/dpone-native-accel/pyproject.toml "
        "packages/dpone-airflow-pack/README.md packages/dpone-airflow-pack/pyproject.toml "
        "packages/dbt-dpone/dbt_project.yml packages/dbt-dpone/macros/dpone_publish.sql "
        "packages/dbt-dpone/macros/semantic_refresh_restore.sql "
        "packages/dbt-dpone/macros/semantic_refresh_scope_merge.sql "
        "packages/apache-airflow-providers-dpone/README.md packages/apache-airflow-providers-dpone/pyproject.toml"
    ).split()
)
RECURSIVE_CONTEXT_ROOTS = tuple(
    "src/dpone packages/dpone-native-accel/src packages/dpone-airflow-pack/src "
    "packages/apache-airflow-providers-dpone/src".split()
)
FORBIDDEN_PARTS = frozenset({".agents", ".codex", ".git", "__pycache__", "test_artifacts"})
PROVENANCE_PREDICATE = "https://slsa.dev/provenance/v1"
SBOM_PREDICATE = "https://spdx.dev/Document/v2.3"
OIDC_ISSUER = "https://token.actions.githubusercontent.com"


# Carries only sanitized machine-readable failure codes.
class CertificationEvidenceError(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


# Canonical JSON bytes are shared by stdout, persisted evidence, and hashing.
def render_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _strict(value: Any, keys: tuple[str, ...], code: str) -> dict[str, Any]:
    if type(value) is not dict or value.keys() != set(keys):
        raise CertificationEvidenceError(code)
    return value


# Reject duplicate keys, symlinks, empty files, and inputs over the caller's bound.
def read_bounded_json(path: Path, limit: int, code: str, *, include_raw: bool = False) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise CertificationEvidenceError(code)
            result[key] = value
        return result

    try:
        if path.is_symlink() or not path.is_file():
            raise OSError
        raw = path.read_bytes()
        if not raw or len(raw) > limit:
            raise OSError
        payload = json.loads(raw, object_pairs_hook=pairs)
        return (payload, raw) if include_raw else payload
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise CertificationEvidenceError(code) from None


def _sha256_file(path: Path) -> str:
    try:
        if path.is_symlink() or not path.is_file():
            raise OSError
        return f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}"
    except OSError:
        raise CertificationEvidenceError("CERTIFICATION_FILE_INVALID") from None


def _excluded(relative: Path) -> bool:
    return (
        bool(FORBIDDEN_PARTS.intersection(relative.parts))
        or relative.name in {"AGENTS.md", ".gitignore", ".env"}
        or relative.name.startswith(".env.")
    )


def _context_manifest(inputs: CertificationInputs) -> dict[str, Any]:
    try:
        root = inputs.context_root.resolve(strict=True)
        if (
            inputs.context_root.is_symlink()
            or inputs.dockerfile.is_symlink()
            or inputs.dockerignore.is_symlink()
            or inputs.dockerfile.resolve(strict=True) != root / "docker/runtime/Dockerfile"
            or inputs.dockerignore.resolve(strict=True) != root / "docker/runtime/Dockerfile.dockerignore"
            or inputs.dockerignore.read_text(encoding="utf-8") != "\n".join(EXPECTED_DOCKERIGNORE_LINES) + "\n"
        ):
            raise OSError
    except (OSError, UnicodeError):
        raise CertificationEvidenceError("DOCKER_CONTEXT_BOUNDARY_INVALID") from None

    paths = {root / relative for relative in EXACT_CONTEXT_FILES}
    for relative in RECURSIVE_CONTEXT_ROOTS:
        tree = root / relative
        if tree.is_symlink() or not tree.is_dir():
            raise CertificationEvidenceError("DOCKER_CONTEXT_BOUNDARY_INVALID")
        for candidate in tree.rglob("*"):
            relative_path = candidate.relative_to(root)
            if _excluded(relative_path):
                continue
            if candidate.is_symlink():
                raise CertificationEvidenceError("DOCKER_CONTEXT_SYMLINK_FORBIDDEN")
            if candidate.is_file():
                paths.add(candidate)
    if len(paths) > MAX_CONTEXT_FILES:
        raise CertificationEvidenceError("DOCKER_CONTEXT_LIMIT_EXCEEDED")

    files: list[dict[str, Any]] = []
    total = 0
    for path in sorted(paths, key=lambda item: item.relative_to(root).as_posix()):
        if path.is_symlink() or not path.is_file():
            raise CertificationEvidenceError("DOCKER_CONTEXT_BOUNDARY_INVALID")
        metadata = path.stat()
        total += metadata.st_size
        if total > MAX_CONTEXT_BYTES:
            raise CertificationEvidenceError("DOCKER_CONTEXT_LIMIT_EXCEEDED")
        files.append(
            {
                "path": path.relative_to(root).as_posix(),
                "mode": f"{stat.S_IMODE(metadata.st_mode):04o}",
                "size": metadata.st_size,
                "sha256": _sha256_file(path),
            }
        )
    return {"schema": CONTEXT_MANIFEST_SCHEMA, "dockerignore_sha256": _sha256_file(inputs.dockerignore), "files": files}


def _check_receipts(
    inputs: CertificationInputs,
    required_check_ids: tuple[str, ...],
) -> list[dict[str, str]]:
    if not required_check_ids or len(set(required_check_ids)) != len(required_check_ids):
        raise CertificationEvidenceError("CHECK_SET_INVALID")
    checks: list[dict[str, str]] = []
    for check_id in required_check_ids:
        receipt = _strict(
            read_bounded_json(inputs.checks_root / f"{check_id}.json", MAX_CHECK_BYTES, "CHECK_RECEIPT_INVALID"),
            ("schema", "id", "status", "subject_name", "subject_digest", "source_commit_sha"),
            "CHECK_RECEIPT_INVALID",
        )
        if (
            receipt["schema"] != CHECK_RECEIPT_SCHEMA
            or receipt["id"] != check_id
            or receipt["status"] != "PASS"
            or receipt["subject_name"] != inputs.image
            or receipt["subject_digest"] != inputs.digest
            or receipt["source_commit_sha"] != inputs.source_commit_sha
        ):
            raise CertificationEvidenceError("CHECK_RECEIPT_MISMATCH")
        checks.append({"id": check_id, "status": "PASS", "subject_digest": inputs.digest})
    return checks


def _find_string(value: Any, key: str) -> str | None:
    normalized = "".join(char.lower() for char in key if char.isalnum())
    if isinstance(value, dict):
        for candidate, nested in value.items():
            if "".join(char.lower() for char in str(candidate) if char.isalnum()) == normalized:
                return nested if isinstance(nested, str) and nested else None
        for nested in value.values():
            if found := _find_string(nested, key):
                return found
    elif isinstance(value, list):
        for nested in value:
            if found := _find_string(nested, key):
                return found
    return None


def _verified_statement(
    path: Path,
    *,
    inputs: CertificationInputs,
    predicate_type: str,
    expected_predicate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = read_bounded_json(path, MAX_VERIFICATION_BYTES, "ATTESTATION_VERIFICATION_INVALID")
    if not isinstance(payload, list) or not payload:
        raise CertificationEvidenceError("ATTESTATION_VERIFICATION_INVALID")
    expected_subject = inputs.digest.removeprefix("sha256:")
    expected_certificate = {
        "issuer": OIDC_ISSUER,
        "sourceRepositoryURI": f"https://github.com/{inputs.source_repository}",
        "sourceRepositoryDigest": inputs.source_commit_sha,
        "sourceRepositoryRef": inputs.source_ref,
        "subjectAlternativeName": f"https://github.com/{inputs.signer_workflow}@{inputs.source_ref}",
    }
    for entry in payload:
        result = entry.get("verificationResult") if isinstance(entry, dict) else None
        statement = result.get("statement") if isinstance(result, dict) else None
        signature = result.get("signature") if isinstance(result, dict) else None
        certificate = signature.get("certificate") if isinstance(signature, dict) else None
        subjects = statement.get("subject") if isinstance(statement, dict) else None
        timestamps = result.get("verifiedTimestamps") if isinstance(result, dict) else None
        subject_matches = isinstance(subjects, list) and any(
            isinstance(subject, dict)
            and subject.get("name") == inputs.image
            and isinstance(subject.get("digest"), dict)
            and subject["digest"].get("sha256") == expected_subject
            for subject in subjects
        )
        identity_matches = isinstance(certificate, dict) and all(
            _find_string(certificate, key) == value for key, value in expected_certificate.items()
        )
        if (
            isinstance(statement, dict)
            and statement.get("_type") == "https://in-toto.io/Statement/v1"
            and statement.get("predicateType") == predicate_type
            and subject_matches
            and identity_matches
            and isinstance(timestamps, list)
            and timestamps
            and (expected_predicate is None or statement.get("predicate") == expected_predicate)
        ):
            return statement
    raise CertificationEvidenceError("ATTESTATION_VERIFICATION_MISMATCH")


def _validate_inputs(inputs: CertificationInputs) -> None:
    patterns = (
        (inputs.image, IMAGE_NAME),
        (inputs.version, STABLE_SEMVER),
        (inputs.digest, CANONICAL_DIGEST),
        (inputs.source_commit_sha, FULL_SHA),
        (inputs.source_repository, SOURCE_REPOSITORY),
        (inputs.run_id, POSITIVE_ID),
        (inputs.run_attempt, POSITIVE_ID),
    )
    if (
        any(pattern.fullmatch(value) is None for value, pattern in patterns)
        or inputs.release_tag != f"v{inputs.version}"
        or inputs.source_ref != f"refs/tags/v{inputs.version}"
        or inputs.signer_workflow != f"{inputs.source_repository}/.github/workflows/runtime-image.yml"
        or inputs.platform != "linux/amd64"
    ):
        raise CertificationEvidenceError("CERTIFICATION_IDENTITY_INVALID")


# PASS is constructed only after all raw evidence has been read and validated.
def produce_certification(
    inputs: CertificationInputs,
    *,
    certification_schema: str,
    required_check_ids: tuple[str, ...],
) -> CertificationArtifacts:
    _validate_inputs(inputs)
    spdx_module = f"{__package__}.runtime_image_spdx" if __package__ else "runtime_image_spdx"
    spdx = importlib.import_module(spdx_module).load_validated_spdx(
        inputs.sbom,
        max_bytes=MAX_SPDX_BYTES,
        read_json=read_bounded_json,
        error_type=CertificationEvidenceError,
        error_code="SPDX_INVALID",
    )
    _verified_statement(
        inputs.provenance_verification,
        inputs=inputs,
        predicate_type=PROVENANCE_PREDICATE,
    )
    _verified_statement(
        inputs.sbom_verification,
        inputs=inputs,
        predicate_type=SBOM_PREDICATE,
        expected_predicate=spdx,
    )
    checks = _check_receipts(inputs, required_check_ids)
    context_manifest = _context_manifest(inputs)
    context_sha = f"sha256:{hashlib.sha256(render_json(context_manifest).encode()).hexdigest()}"
    certification = {
        "schema": certification_schema,
        "status": "PASS",
        "idempotency_key": f"{inputs.image}|{inputs.version}|{inputs.digest}",
        "release": {
            "version": inputs.version,
            "tag": inputs.release_tag,
            "source_commit_sha": inputs.source_commit_sha,
            "source_ref": inputs.source_ref,
        },
        "subject": {"name": inputs.image, "digest": inputs.digest, "platform": inputs.platform},
        "build": {
            "run_id": inputs.run_id,
            "run_attempt": inputs.run_attempt,
            "dockerfile_sha256": _sha256_file(inputs.dockerfile),
            "dockerignore_sha256": _sha256_file(inputs.dockerignore),
            "context_manifest_sha256": context_sha,
        },
        "checks": checks,
        "attestations": {
            "provenance": {"status": "PASS", "subject_digest": inputs.digest},
            "sbom": {"status": "PASS", "subject_digest": inputs.digest},
        },
    }
    return CertificationArtifacts(certification, context_manifest)


# Attempt-scoped evidence is append-only; an existing destination is a conflict.
def write_new(path: Path, rendered: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        path.hardlink_to(temporary)
    finally:
        temporary.unlink(missing_ok=True)


# Context evidence is committed first; canonical PASS certification is committed last.
def produce_and_write(policy: Any, inputs: CertificationInputs, output: Path) -> str:
    root = inputs.context_root.resolve(strict=True)
    destinations = (inputs.context_manifest_output, output)
    resolved = {path.resolve() for path in destinations}
    if (
        len(resolved) != 2
        or any(path.exists() or path.is_symlink() for path in destinations)
        or any(path.is_relative_to(root) for path in resolved)
    ):
        raise CertificationEvidenceError("CERTIFICATION_OUTPUT_INVALID")
    artifacts = produce_certification(
        inputs,
        certification_schema=policy.CERTIFICATION_SCHEMA,
        required_check_ids=policy.REQUIRED_CHECK_IDS,
    )
    identity = (inputs.image, inputs.version, inputs.digest, inputs.source_commit_sha, inputs.source_ref)
    certification = policy.validate_certification(
        artifacts.certification,
        expected=policy.CertificationIdentity(*identity),
    )
    rendered = render_json(certification.to_payload())
    write_new(inputs.context_manifest_output, render_json(artifacts.context_manifest))
    write_new(output, rendered)
    return rendered
