from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tools.agent_policy import runtime_image_certification as producer
from tools.agent_policy import runtime_image_promotion as promotion

IMAGE = "ghcr.io/paulkov/dpone-runtime"
VERSION = "0.73.2"
DIGEST = f"sha256:{'a' * 64}"
OTHER_DIGEST = f"sha256:{'b' * 64}"
SOURCE_SHA = "c" * 40
SOURCE_REF = f"refs/tags/v{VERSION}"
SOURCE_REPOSITORY = "PaulKov/dpone"
SIGNER_WORKFLOW = f"{SOURCE_REPOSITORY}/.github/workflows/runtime-image.yml"


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _spdx() -> dict[str, Any]:
    return {
        "spdxVersion": "SPDX-2.3",
        "SPDXID": "SPDXRef-DOCUMENT",
        "dataLicense": "CC0-1.0",
        "name": "dpone-runtime",
        "documentNamespace": "https://example.invalid/dpone-runtime",
        "documentDescribes": ["SPDXRef-Package-dpone"],
        "packages": [{"SPDXID": "SPDXRef-Package-dpone", "name": "dpone"}],
    }


def _verification(predicate_type: str, predicate: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "verificationResult": {
                "statement": {
                    "_type": "https://in-toto.io/Statement/v1",
                    "predicateType": predicate_type,
                    "subject": [{"name": IMAGE, "digest": {"sha256": DIGEST.removeprefix("sha256:")}}],
                    "predicate": predicate,
                },
                "signature": {
                    "certificate": {
                        "issuer": "https://token.actions.githubusercontent.com",
                        "sourceRepositoryURI": f"https://github.com/{SOURCE_REPOSITORY}",
                        "sourceRepositoryDigest": SOURCE_SHA,
                        "sourceRepositoryRef": SOURCE_REF,
                        "subjectAlternativeName": f"https://github.com/{SIGNER_WORKFLOW}@{SOURCE_REF}",
                    }
                },
                "verifiedTimestamps": [{"timestamp": "2026-07-19T00:00:00Z"}],
            }
        }
    ]


def _context(root: Path) -> tuple[Path, Path]:
    for relative in producer.EXACT_CONTEXT_FILES:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"content:{relative}\n", encoding="utf-8")
    for relative in producer.RECURSIVE_CONTEXT_ROOTS:
        path = root / relative / "module.py"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"value = {relative!r}\n", encoding="utf-8")
    dockerignore = root / "docker/runtime/Dockerfile.dockerignore"
    dockerignore.write_text("\n".join(producer.EXPECTED_DOCKERIGNORE_LINES) + "\n", encoding="utf-8")
    return root / "docker/runtime/Dockerfile", dockerignore


def _checks(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    for check_id in promotion.REQUIRED_CHECK_IDS:
        _write_json(
            root / f"{check_id}.json",
            {
                "schema": producer.CHECK_RECEIPT_SCHEMA,
                "id": check_id,
                "status": "PASS",
                "subject_name": IMAGE,
                "subject_digest": DIGEST,
                "source_commit_sha": SOURCE_SHA,
            },
        )
    return root


def _inputs(tmp_path: Path) -> producer.CertificationInputs:
    context_root = tmp_path / "context"
    dockerfile, dockerignore = _context(context_root)
    sbom = tmp_path / "evidence/runtime-image.spdx.json"
    provenance = tmp_path / "evidence/provenance-verification.json"
    sbom_verification = tmp_path / "evidence/sbom-verification.json"
    spdx = _spdx()
    _write_json(sbom, spdx)
    _write_json(provenance, _verification("https://slsa.dev/provenance/v1", {"buildDefinition": {}}))
    _write_json(sbom_verification, _verification("https://spdx.dev/Document/v2.3", spdx))
    return producer.CertificationInputs(
        image=IMAGE,
        version=VERSION,
        release_tag=f"v{VERSION}",
        source_commit_sha=SOURCE_SHA,
        source_ref=SOURCE_REF,
        source_repository=SOURCE_REPOSITORY,
        signer_workflow=SIGNER_WORKFLOW,
        digest=DIGEST,
        platform="linux/amd64",
        run_id="12345",
        run_attempt="2",
        dockerfile=dockerfile,
        dockerignore=dockerignore,
        context_root=context_root,
        context_manifest_output=tmp_path / "evidence/runtime-image-context-manifest.json",
        sbom=sbom,
        provenance_verification=provenance,
        sbom_verification=sbom_verification,
        checks_root=_checks(tmp_path / "evidence/checks"),
    )
