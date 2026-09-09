from __future__ import annotations

import hashlib
import json

import pytest

from dpone.manifest.compatibility_candidate import CandidateManifestError, build_manifest


def test_build_manifest_is_canonical_and_binds_inventory_digest() -> None:
    files = {
        "dpone-0.74.28-py3-none-any.whl": b"dpone",
        "dpone_airflow_pack-0.74.28-py3-none-any.whl": b"pack",
        "apache_airflow_providers_dpnone-0.74.28-py3-none-any.whl": b"provider",
    }
    with pytest.raises(CandidateManifestError, match="distribution"):
        build_manifest(
            files,
            expected_versions={
                "dpone": "0.74.28",
                "dpone-airflow-pack": "0.74.28",
                "apache-airflow-providers-dpone": "0.74.28",
            },
        )

    files["apache_airflow_providers_dpone-0.74.28-py3-none-any.whl"] = files.pop(
        "apache_airflow_providers_dpnone-0.74.28-py3-none-any.whl"
    )
    manifest = build_manifest(
        files,
        expected_versions={
            "dpone": "0.74.28",
            "dpone-airflow-pack": "0.74.28",
            "apache-airflow-providers-dpone": "0.74.28",
        },
    )
    projection = {key: value for key, value in manifest.items() if key != "inventory_digest"}
    assert (
        manifest["inventory_digest"]
        == "sha256:"
        + hashlib.sha256(
            json.dumps(projection, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest()
    )
    assert [entry["filename"] for entry in manifest["entries"]] == sorted(files)


def test_build_manifest_rejects_wrong_cardinality() -> None:
    with pytest.raises(CandidateManifestError, match="exactly three"):
        build_manifest({}, expected_versions={})
