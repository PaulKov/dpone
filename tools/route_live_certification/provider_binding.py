"""Provider artifact identity bound to one consolidated route manifest."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

from .contract import COMMIT_SHA_PATTERN, CONSOLIDATED_VERSION, PASS_STATUS, digest, fail
from .io_authority import json_object

PROVIDER_BINDING_VERSION = "dpone.route_live.postgres_mssql.provider_binding.v1"
_TAGGED_SHA256 = re.compile(r"sha256:[0-9a-f]{64}")


def build_provider_binding(
    *,
    manifest_path: Path,
    artifact_name: str,
    artifact_id: int,
    artifact_digest: str,
    commit_sha: str,
    workflow_run_id: int,
    workflow_run_attempt: int,
) -> dict[str, Any]:
    """Validate the consolidated manifest and bind its provider artifact."""

    if not artifact_name.strip() or any(character in artifact_name for character in "/\\\0"):
        fail("provider_binding.artifact_name_invalid")
    if type(artifact_id) is not int or artifact_id < 1:
        fail("provider_binding.artifact_id_invalid")
    if _TAGGED_SHA256.fullmatch(artifact_digest) is None:
        fail("provider_binding.artifact_digest_invalid")
    if COMMIT_SHA_PATTERN.fullmatch(commit_sha) is None:
        fail("provider_binding.commit_sha_invalid")
    if type(workflow_run_id) is not int or workflow_run_id < 1:
        fail("provider_binding.workflow_run_id_invalid")
    if type(workflow_run_attempt) is not int or workflow_run_attempt < 1:
        fail("provider_binding.workflow_run_attempt_invalid")
    manifest_bytes = manifest_path.read_bytes()
    manifest = json_object(manifest_bytes, code="provider_binding.manifest_invalid")
    required = {
        "schema_version": CONSOLIDATED_VERSION,
        "status": PASS_STATUS,
        "release_ready": True,
        "route": "postgres_mssql",
        "commit_sha": commit_sha,
        "workflow_run_id": str(workflow_run_id),
        "workflow_run_attempt": workflow_run_attempt,
    }
    if any(type(manifest.get(key)) is not type(value) or manifest.get(key) != value for key, value in required.items()):
        fail("provider_binding.manifest_identity_mismatch")
    semantic_digest = manifest.get("manifest_sha256")
    if not isinstance(semantic_digest, str) or re.fullmatch(r"[0-9a-f]{64}", semantic_digest) is None:
        fail("provider_binding.manifest_sha256_invalid")
    unsigned = dict(manifest)
    unsigned.pop("manifest_sha256", None)
    if digest(unsigned) != semantic_digest:
        fail("provider_binding.manifest_semantic_digest_mismatch")
    return {
        "schema_version": PROVIDER_BINDING_VERSION,
        "status": PASS_STATUS,
        "artifact_name": artifact_name,
        "artifact_id": artifact_id,
        "artifact_digest": artifact_digest,
        "commit_sha": commit_sha,
        "workflow_run_id": workflow_run_id,
        "workflow_run_attempt": workflow_run_attempt,
        "manifest_content_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "manifest_sha256": semantic_digest,
    }
