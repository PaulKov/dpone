from __future__ import annotations

import hashlib
import json
from pathlib import Path

from dpone.adapters.dbt_semantic_refresh_release_template import (
    ImmutableSemanticRefreshReleaseTemplateVerifier,
)
from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.contracts.dbt_semantic_refresh_activation import (
    SemanticRefreshReleaseTemplateSubject,
)
from tests.test_dbt_semantic_refresh_plan_compiler import _template_pack


def test_promoted_release_verifier_authenticates_exact_template(tmp_path: Path) -> None:
    release_root, subject = _release(tmp_path)

    assert ImmutableSemanticRefreshReleaseTemplateVerifier(release_root).verify(subject)


def test_promoted_release_verifier_rejects_wrong_release_and_refingerprinted_pack(
    tmp_path: Path,
) -> None:
    release_root, subject = _release(tmp_path)
    verifier = ImmutableSemanticRefreshReleaseTemplateVerifier(release_root)
    wrong_release = SemanticRefreshReleaseTemplateSubject(
        release_id="sha256:" + "f" * 64,
        template_pack_fingerprint=subject.template_pack_fingerprint,
        pre_release_bundle_sha256=subject.pre_release_bundle_sha256,
        package_artifacts_sha256=subject.package_artifacts_sha256,
    )
    assert verifier.verify(wrong_release) is False

    pack_path = next((release_root / "packs").glob("*.airflow-pack.json"))
    payload = json.loads(pack_path.read_text(encoding="utf-8"))
    payload["producer"] = "forged"
    pack_path.write_bytes(_json_bytes(payload))
    assert verifier.verify(subject) is False


def test_promoted_release_verifier_rejects_descriptor_path_traversal(tmp_path: Path) -> None:
    release_root, subject = _release(tmp_path)
    release_path = release_root / "release-set.json"
    release = json.loads(release_path.read_text(encoding="utf-8"))
    release["artifacts"]["workload_packs"][0]["path"] = "packs/../../foreign.json"
    release["release_id"] = compute_release_id(release)
    release_path.write_bytes(_json_bytes(release))
    traversed = SemanticRefreshReleaseTemplateSubject(
        release_id=release["release_id"],
        template_pack_fingerprint=subject.template_pack_fingerprint,
        pre_release_bundle_sha256=subject.pre_release_bundle_sha256,
        package_artifacts_sha256=subject.package_artifacts_sha256,
    )

    assert ImmutableSemanticRefreshReleaseTemplateVerifier(release_root).verify(traversed) is False


def _release(tmp_path: Path) -> tuple[Path, SemanticRefreshReleaseTemplateSubject]:
    release_root = tmp_path / "promoted-release"
    pack = _template_pack()
    pack_bytes = _json_bytes(pack)
    pack_path = release_root / "packs/semantic__daily_events.airflow-pack.json"
    pack_path.parent.mkdir(parents=True)
    pack_path.write_bytes(pack_bytes)
    descriptor = {
        "bytes": len(pack_bytes),
        "id": "semantic__daily_events",
        "pack_fingerprint": pack["pack_fingerprint"],
        "path": "packs/semantic__daily_events.airflow-pack.json",
        "runtime_payload_ids": list(pack["runtime_payload_ids"]),
        "sha256": "sha256:" + hashlib.sha256(pack_bytes).hexdigest(),
    }
    release = {
        "artifacts": {
            "canonical_schemas": [],
            "dag_specs": [],
            "runtime_payloads": [],
            "workload_packs": [descriptor],
        },
        "producer": {"dpone_version": "test", "wire_contract": "test"},
        "provenance": {
            "route_certifications": [],
            "selection_fingerprints": [],
            "source": "dpone dbt compile",
            "source_snapshot_sha256": "sha256:" + "a" * 64,
        },
        "release_id": "",
        "schema": "dpone.release-set.v2",
        "selection_authority": "dbt_cli",
        "selection_fingerprint": "sha256:" + "b" * 64,
    }
    release["release_id"] = compute_release_id(release)
    (release_root / "release-set.json").write_bytes(_json_bytes(release))
    semantic = pack["semantic_refresh"]
    return release_root, SemanticRefreshReleaseTemplateSubject(
        release_id=release["release_id"],
        template_pack_fingerprint=pack["pack_fingerprint"],
        pre_release_bundle_sha256=semantic["pre_release_bundle_sha256"],
        package_artifacts_sha256=semantic["package_artifacts_sha256"],
    )


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, allow_nan=False, ensure_ascii=False, sort_keys=True) + "\n").encode()
