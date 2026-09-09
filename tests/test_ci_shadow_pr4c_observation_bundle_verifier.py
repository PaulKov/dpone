from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path

import pytest
import yaml

from dpone.contracts.ci_shadow_reconciliation import ReconciliationPolicyV1
from dpone.services.ci.shadow_observation_bundle_verifier import (
    ObservationBundleVerificationError,
    verify_observation_bundle,
)
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / ".agents" / "policy" / "ci-shadow-reconciliation-observation-bundle-v1.yml"


class _GitProvider:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = files
        self.blobs = {
            hashlib.sha1(f"blob {len(content)}\0".encode() + content).hexdigest(): content  # noqa: S324
            for content in files.values()
        }
        self.tree_sha = "a" * 40

    def get_git_commit_tree(self, **_: object) -> bytes:
        return _json({"tree": {"sha": self.tree_sha}})

    def get_git_tree(self, **_: object) -> bytes:
        return _json(
            {
                "truncated": False,
                "tree": [
                    {
                        "path": path,
                        "mode": "100644",
                        "type": "blob",
                        "sha": hashlib.sha1(f"blob {len(content)}\0".encode() + content).hexdigest(),  # noqa: S324
                    }
                    for path, content in self.files.items()
                ],
            }
        )

    def get_git_blob(self, **_: object) -> bytes:
        blob_sha = _["blob_sha"]
        assert isinstance(blob_sha, str)
        content = self.blobs[blob_sha]
        return _json({"sha": blob_sha, "encoding": "base64", "content": base64.b64encode(content).decode()})


def _json(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def _budget() -> RequestBudget:
    return RequestBudget(policy=ReconciliationPolicyV1.fixed(), monotonic_clock=lambda: 0.0)


def _write_bundle(root: Path, content: bytes) -> Path:
    source = root / "src" / "dpone"
    source.mkdir(parents=True)
    (source / "example.py").write_bytes(content)
    digest = hashlib.sha256(content).hexdigest()
    manifest = root / "bundle.yml"
    manifest.write_text(
        "schema: dpone.ci-shadow-reconciliation-observation-bundle.v1\n"
        "domain: dpone.ci-shadow-reconciliation-observation-bundle.v1\n"
        "entries:\n"
        "  - path: src/dpone/example.py\n"
        "    mode: '100644'\n"
        f"    blob_sha256: sha256:{digest}\n",
        encoding="utf-8",
    )
    return manifest


def test_bundle_verifier_binds_manifest_git_blobs_and_checked_out_bytes(tmp_path: Path) -> None:
    content = b'"""trusted"""\n'
    manifest = _write_bundle(tmp_path, content)
    budget = _budget()
    provider = _GitProvider({"src/dpone/example.py": content, "bundle.yml": manifest.read_bytes()})

    entries, digest, manifest_sha256 = verify_observation_bundle(
        manifest,
        source_root=tmp_path,
        source_commit_sha="b" * 40,
        provider=provider,
        budget=budget,
    )

    assert entries[0].path == "src/dpone/example.py"
    assert digest.startswith("sha256:")
    assert manifest_sha256 == "sha256:" + hashlib.sha256(manifest.read_bytes()).hexdigest()
    assert budget.counters()["git_object_requests"] == 4


def test_bundle_verifier_rejects_local_bytes_that_do_not_match_the_immutable_blob(tmp_path: Path) -> None:
    manifest = _write_bundle(tmp_path, b"local\n")

    provider = _GitProvider({"src/dpone/example.py": b"remote\n", "bundle.yml": manifest.read_bytes()})
    with pytest.raises(ObservationBundleVerificationError, match="does not match"):
        verify_observation_bundle(
            manifest,
            source_root=tmp_path,
            source_commit_sha="b" * 40,
            provider=provider,
            budget=_budget(),
        )


def test_checked_in_observation_bundle_has_current_exact_local_bytes() -> None:
    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    assert isinstance(manifest, dict)
    entries = manifest["entries"]
    assert isinstance(entries, list)
    paths = [entry["path"] for entry in entries]
    assert paths == sorted(paths)
    for entry in entries:
        assert isinstance(entry, dict)
        path = ROOT / entry["path"]
        assert path.is_file()
        assert entry["blob_sha256"] == "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
