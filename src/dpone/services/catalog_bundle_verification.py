"""Integrity, signature, policy, and semantic verification for catalog bundles."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.services.catalog_bundle_content import validate_verified_bundle_content
from dpone.services.catalog_bundle_io import listed_regular_files, read_bundle_file
from dpone.services.catalog_supply_chain_support import (
    BUNDLE_SCHEMA,
    MAX_CONTROL_NODES,
    MAX_CONTROL_TOKENS,
    TRUST_POLICY_SCHEMA,
    BlobSignatureVerifier,
    CatalogBundleError,
    CatalogBundleVerification,
    CatalogBundleVerifyRequest,
    CosignVerificationPolicy,
    RecipeBundleSupportError,
    bundle_id,
    canonical_json_bytes,
    parse_bounded_json_mapping,
    parse_bounded_mapping,
    policy_fingerprint,
    read_project_file,
    sha256_bytes,
    validate_registered_schema,
)

_CONTROL_LIMIT = 4 * 1024 * 1024
_UTC = UTC


class CatalogBundleVerificationService:
    """Produce a safe, create-only receipt for one local immutable bundle."""

    def __init__(
        self,
        *,
        blob_verifier: BlobSignatureVerifier,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._blob_verifier = blob_verifier
        self._clock = clock or (lambda: datetime.now(_UTC))

    def verify(self, request: CatalogBundleVerifyRequest) -> CatalogBundleVerification:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("catalog verification clock must be timezone-aware")
        bundle_dir = Path(request.bundle_dir).resolve(strict=True)
        manifest_bytes = read_bundle_file(bundle_dir, "catalog-bundle.json")
        manifest_digest = sha256_bytes(manifest_bytes)
        policy_bytes = _read_control(Path(request.policy))
        trusted_root = _read_control(Path(request.trusted_root))
        sigstore_bundle = _read_control(Path(request.sigstore_bundle))
        try:
            manifest = _parse_manifest(manifest_bytes)
            policy = _parse_mapping(policy_bytes)
            _validate_schema(manifest, BUNDLE_SCHEMA)
            _validate_schema(policy, TRUST_POLICY_SCHEMA)
            identity, _ = _verify_manifest(bundle_dir, manifest)
            adapter_policy = _verify_policy(manifest, policy, trusted_root)
        except CatalogBundleError as exc:
            receipt = _receipt(
                decision="invalid",
                code=exc.code,
                message=str(exc),
                now=now,
                manifest_digest=manifest_digest,
                manifest=locals().get("manifest"),
                policy=locals().get("policy"),
            )
            _write_receipt(Path(request.output), receipt)
            return receipt
        signature = self._blob_verifier.verify_blob(
            blob=manifest_bytes,
            sigstore_bundle=sigstore_bundle,
            trusted_root=trusted_root,
            policy=adapter_policy,
        )
        if signature.status != "verified":
            unavailable = signature.status == "unverified"
            receipt = _receipt(
                decision="unverified" if unavailable else "invalid",
                code=(
                    "DPONE_CATALOG_VERIFIER_UNAVAILABLE" if unavailable else "DPONE_CATALOG_BUNDLE_SIGNATURE_INVALID"
                ),
                message=(
                    "The certified catalog verifier is unavailable."
                    if unavailable
                    else "The catalog signature or signer identity is invalid."
                ),
                now=now,
                manifest_digest=manifest_digest,
                manifest=manifest,
                policy=policy,
                verifier_version=signature.verifier_version,
            )
            _write_receipt(Path(request.output), receipt)
            return receipt
        try:
            _, artifacts = _verify_manifest(bundle_dir, manifest)
        except CatalogBundleError as exc:
            receipt = _receipt(
                decision="invalid",
                code=exc.code,
                message=str(exc),
                now=now,
                manifest_digest=manifest_digest,
                manifest=manifest,
                policy=policy,
                verifier_version=signature.verifier_version,
            )
            _write_receipt(Path(request.output), receipt)
            return receipt
        try:
            entrypoint = artifacts_by_path(manifest, artifacts)[str(manifest["entrypoint"])]
            validate_verified_bundle_content(
                kind=str(manifest["kind"]),
                entrypoint=entrypoint,
                artifacts={
                    str(item["logical_id"]): artifacts[index] for index, item in enumerate(manifest["artifacts"])
                },
                environment=_optional_text(manifest.get("environment")),
            )
        except (CatalogBundleError, KeyError, TypeError, IndexError):
            receipt = _receipt(
                decision="invalid",
                code="DPONE_CATALOG_CONTENT_INVALID",
                message="Verified catalog payload failed semantic validation.",
                now=now,
                manifest_digest=manifest_digest,
                manifest=manifest,
                policy=policy,
                verifier_version=signature.verifier_version,
            )
            _write_receipt(Path(request.output), receipt)
            return receipt
        receipt = CatalogBundleVerification(
            decision="verified",
            code="DPONE_CATALOG_BUNDLE_VERIFIED",
            bundle_id=identity,
            bundle_manifest_sha256=manifest_digest,
            kind=str(manifest["kind"]),
            publisher_id=str(manifest["publisher_id"]),
            environment=_optional_text(manifest.get("environment")),
            policy_fingerprint=policy_fingerprint(policy),
            verified_artifacts=len(artifacts),
            verifier_version=signature.verifier_version,
            verified_at=_utc_text(now),
        )
        _write_receipt(Path(request.output), receipt)
        return receipt


def _verify_manifest(bundle_dir: Path, manifest: Mapping[str, Any]) -> tuple[str, tuple[bytes, ...]]:
    identity = bundle_id(manifest)
    if manifest.get("bundle_id") != identity or bundle_dir.name != identity.replace(":", "-"):
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED", "Catalog bundle identity is invalid.")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED", "Catalog artifact list is invalid.")
    logical_ids: set[str] = set()
    paths: set[str] = set()
    artifact_bytes: list[bytes] = []
    expected = {"catalog-bundle.json", "_SUCCESS"}
    for raw in artifacts:
        if not isinstance(raw, Mapping):
            raise CatalogBundleError("DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED", "Catalog artifact is invalid.")
        logical_id, path = str(raw.get("logical_id", "")), str(raw.get("path", ""))
        if logical_id in logical_ids or path in paths or not path.startswith("payload/"):
            raise CatalogBundleError(
                "DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED", "Catalog artifact identity is duplicated."
            )
        logical_ids.add(logical_id)
        paths.add(path)
        content = read_bundle_file(bundle_dir, path)
        if raw.get("sha256") != sha256_bytes(content) or raw.get("size_bytes") != len(content):
            raise CatalogBundleError("DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED", "Catalog artifact digest is invalid.")
        artifact_bytes.append(content)
        expected.add(path)
    if manifest.get("entrypoint") not in paths or set(listed_regular_files(bundle_dir)) != expected:
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED", "Catalog payload inventory is invalid.")
    if read_bundle_file(bundle_dir, "_SUCCESS", max_bytes=256) != identity.encode() + b"\n":
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_INCOMPLETE", "Catalog bundle completion marker is invalid.")
    return identity, tuple(artifact_bytes)


def _verify_policy(
    manifest: Mapping[str, Any], policy: Mapping[str, Any], trusted_root: bytes
) -> CosignVerificationPolicy:
    if sha256_bytes(trusted_root) != policy.get("trusted_root_sha256"):
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED", "Pinned trusted root is unavailable.")
    if manifest.get("kind") not in policy.get("allowed_kinds", ()):
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_SIGNATURE_INVALID", "Catalog kind is not allowed.")
    if manifest.get("publisher_id") not in policy.get("allowed_publishers", ()):
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_SIGNATURE_INVALID", "Catalog publisher is not allowed.")
    environment = manifest.get("environment")
    allowed_environments = policy.get("allowed_environments")
    if environment is not None and isinstance(allowed_environments, list) and environment not in allowed_environments:
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_SIGNATURE_INVALID", "Catalog environment is not allowed.")
    verifier = policy["verifier"]
    return CosignVerificationPolicy(
        certificate_identity=str(policy["certificate_identity"]),
        certificate_oidc_issuer=str(policy["certificate_oidc_issuer"]),
        minimum_version=str(verifier["minimum_version"]),
        maximum_version_exclusive=str(verifier["maximum_version_exclusive"]),
        timeout_seconds=int(verifier["timeout_seconds"]),
    )


def artifacts_by_path(manifest: Mapping[str, Any], values: tuple[bytes, ...]) -> dict[str, bytes]:
    return {str(item["path"]): values[index] for index, item in enumerate(manifest["artifacts"])}


def _validate_schema(payload: Mapping[str, Any], kind: str) -> None:
    if not validate_registered_schema(payload, kind):
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED", "Catalog control schema is invalid.")


def _parse_mapping(content: bytes) -> dict[str, Any]:
    try:
        return parse_bounded_mapping(
            content,
            max_bytes=_CONTROL_LIMIT,
            max_tokens=MAX_CONTROL_TOKENS,
            max_nodes=MAX_CONTROL_NODES,
        )
    except RecipeBundleSupportError as exc:
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED", "Catalog control file is invalid.") from exc


def _parse_manifest(content: bytes) -> dict[str, Any]:
    try:
        return parse_bounded_json_mapping(
            content,
            max_bytes=_CONTROL_LIMIT,
            max_nodes=MAX_CONTROL_NODES,
        )
    except RecipeBundleSupportError as exc:
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED", "Catalog control file is invalid.") from exc


def _read_control(path: Path) -> bytes:
    try:
        return read_project_file(path.parent.resolve(strict=True), path.name, max_bytes=_CONTROL_LIMIT)
    except (RecipeBundleSupportError, OSError) as exc:
        raise CatalogBundleError(
            "DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED", "Catalog control file is unavailable."
        ) from exc


def _receipt(
    *,
    decision: str,
    code: str,
    message: str,
    now: datetime,
    manifest_digest: str,
    manifest: object,
    policy: object,
    verifier_version: str | None = None,
) -> CatalogBundleVerification:
    safe_manifest = manifest if isinstance(manifest, Mapping) else {}
    safe_policy = policy if isinstance(policy, Mapping) else {}
    return CatalogBundleVerification(
        decision=decision,
        code=code,
        bundle_id=_optional_text(safe_manifest.get("bundle_id")),
        bundle_manifest_sha256=manifest_digest,
        kind=_optional_text(safe_manifest.get("kind")),
        publisher_id=_optional_text(safe_manifest.get("publisher_id")),
        environment=_optional_text(safe_manifest.get("environment")),
        policy_fingerprint=policy_fingerprint(safe_policy),
        verified_artifacts=0,
        verifier_version=verifier_version,
        verified_at=_utc_text(now),
        errors=({"code": code, "message": message},),
    )


def _write_receipt(path: Path, receipt: CatalogBundleVerification) -> None:
    data = canonical_json_bytes(receipt.to_dict())
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if not path.is_symlink() and path.read_bytes() == data:
            return
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_CONFLICT", "Verification receipt already differs.")
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile("wb", dir=path.parent, prefix=f".{path.name}.", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _optional_text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _utc_text(value: datetime) -> str:
    return value.astimezone(_UTC).isoformat().replace("+00:00", "Z")


__all__ = ["CatalogBundleVerificationService"]
