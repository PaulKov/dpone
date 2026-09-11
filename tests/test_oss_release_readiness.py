from __future__ import annotations

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _pyproject() -> dict[str, Any]:
    return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def test_pyproject_declares_public_oss_metadata() -> None:
    project = _pyproject()["project"]

    assert project["name"] == "dpone"
    assert project["version"] == "0.77.1"
    assert project["license"] == "Apache-2.0"
    assert project["authors"] == [{"name": "PaulKov"}]
    assert project["maintainers"] == [{"name": "PaulKov"}]
    assert project["urls"]["Repository"] == "https://github.com/PaulKov/dpone"
    assert project["urls"]["Issues"] == "https://github.com/PaulKov/dpone/issues"
    assert "License :: OSI Approved :: Apache Software License" in project["classifiers"]
    assert "Typing :: Typed" in project["classifiers"]


def test_pyproject_exposes_public_vault_and_full_extras() -> None:
    extras = _pyproject()["project"]["optional-dependencies"]

    assert extras["vault"] == ["vault-kv-client>=0.1.0,<0.2.0"]
    assert extras["accel"] == ["dpone-native-accel==0.77.1"]
    assert "vault-client" not in "\n".join(extras["vault"])

    full = set(extras["full"])
    expected = {
        "psycopg[binary]==3.3.4",
        "clickhouse-connect==0.6.22",
        "clickhouse-driver==0.2.10",
        "clickhouse-cityhash==1.0.2.5",
        "google-cloud-bigquery==3.42.2",
        "google-cloud-storage==3.12.1",
        "boto3>=1.34,<2",
        "azure-identity>=1.13,<2",
        "azure-storage-blob>=12.19,<13",
        "pandas==2.1.4",
        "numpy==1.26.4",
        "vault-kv-client>=0.1.0,<0.2.0",
        "google-ads>=28.0.0,<29.0.0",
    }
    assert expected <= full
    assert extras["s3"] == ["boto3>=1.34,<2"]
    assert extras["azure"] == ["azure-identity>=1.13,<2", "azure-storage-blob>=12.19,<13"]
    assert set(extras["object_storage"]) == {
        "boto3>=1.34,<2",
        "azure-identity>=1.13,<2",
        "azure-storage-blob>=12.19,<13",
        "google-cloud-storage==3.12.1",
    }


def test_package_config_and_lock_do_not_depend_on_private_example_travel_registry() -> None:
    checked_files = [ROOT / "pyproject.toml", ROOT / "uv.lock"]
    for path in checked_files:
        text = path.read_text(encoding="utf-8")
        assert "gitlab.example.com" not in text
        assert "vault-client" not in text
        assert "vault-kv-client" in text


def test_oss_baseline_documents_exist() -> None:
    for relative_path in [
        "LICENSE",
        "CONTRIBUTING.md",
        "SECURITY.md",
        "CHANGELOG.md",
        ".github/ISSUE_TEMPLATE/bug_report.yml",
        ".github/ISSUE_TEMPLATE/feature_request.yml",
        ".github/pull_request_template.md",
    ]:
        assert (ROOT / relative_path).is_file(), relative_path


def test_certification_suite_documentation_contract_exists() -> None:
    required_docs = [
        "docs/certification-suite.md",
        "docs/developer-certification-suite.md",
        "docs/architecture.md",
        "docs/developer-ci-cd.md",
    ]
    for relative_path in required_docs:
        assert (ROOT / relative_path).is_file(), relative_path

    user_doc = (ROOT / "docs/certification-suite.md").read_text(encoding="utf-8")
    developer_doc = (ROOT / "docs/developer-certification-suite.md").read_text(encoding="utf-8")
    architecture_doc = (ROOT / "docs/architecture.md").read_text(encoding="utf-8")

    assert "dpone ops certification-suite" in user_doc
    assert "Runbook" in user_doc
    assert "CertificationArtifactReader" in developer_doc
    assert "Do not add certification-suite business logic to `ops_cmd.py`" in developer_doc
    assert "Operational evidence lane" in architecture_doc


def test_observability_documentation_contract_exists() -> None:
    required_docs = [
        "docs/observability.md",
        "docs/developer-observability.md",
        "docs/architecture.md",
        "docs/developer-ci-cd.md",
    ]
    for relative_path in required_docs:
        assert (ROOT / relative_path).is_file(), relative_path

    user_doc = (ROOT / "docs/observability.md").read_text(encoding="utf-8")
    developer_doc = (ROOT / "docs/developer-observability.md").read_text(encoding="utf-8")
    architecture_doc = (ROOT / "docs/architecture.md").read_text(encoding="utf-8")
    ci_doc = (ROOT / "docs/developer-ci-cd.md").read_text(encoding="utf-8")

    assert "dpone observability metrics-export" in user_doc
    assert "Prometheus" in user_doc
    assert "OpenTelemetry" in user_doc
    assert "Runbook" in user_doc
    assert "RuntimeMetricsExportService" in developer_doc
    assert "Do not add observability business logic to `ops_cmd.py`" in developer_doc
    assert "dpone.observability.*" in architecture_doc
    assert "test_artifacts/observability/" in ci_doc


def test_cdc_replay_documentation_contract_exists() -> None:
    required_docs = [
        "docs/cdc.md",
        "docs/developer-cdc.md",
        "docs/architecture.md",
        "docs/production-readiness.md",
    ]
    for relative_path in required_docs:
        assert (ROOT / relative_path).is_file(), relative_path

    user_doc = (ROOT / "docs/cdc.md").read_text(encoding="utf-8")
    developer_doc = (ROOT / "docs/developer-cdc.md").read_text(encoding="utf-8")
    architecture_doc = (ROOT / "docs/architecture.md").read_text(encoding="utf-8")
    readiness_doc = (ROOT / "docs/production-readiness.md").read_text(encoding="utf-8")

    assert "dpone cdc replay-plan" in user_doc
    assert "CDCEventIdentityService" in user_doc
    assert "CDCCommitGate" in user_doc
    assert "Replay runbook" in user_doc
    assert "dpone.runtime.cdc.identity" in developer_doc
    assert "dpone.readiness.cdc_replay" in developer_doc
    assert "Do not put reader logic, replay policy, or idempotency logic into command" in developer_doc
    assert "dpone.runtime.cdc.identity" in architecture_doc
    assert "dpone.readiness.cdc_replay" in architecture_doc
    assert "dpone cdc replay-plan" in readiness_doc


def test_object_storage_documentation_contract_exists() -> None:
    required_docs = [
        "docs/object-storage-staging.md",
        "docs/developer-object-storage.md",
        "docs/architecture.md",
        "docs/production-readiness.md",
        "docs/reference/extras.md",
    ]
    for relative_path in required_docs:
        assert (ROOT / relative_path).is_file(), relative_path

    user_doc = (ROOT / "docs/object-storage-staging.md").read_text(encoding="utf-8")
    developer_doc = (ROOT / "docs/developer-object-storage.md").read_text(encoding="utf-8")
    architecture_doc = (ROOT / "docs/architecture.md").read_text(encoding="utf-8")
    readiness_doc = (ROOT / "docs/production-readiness.md").read_text(encoding="utf-8")
    extras_doc = (ROOT / "docs/reference/extras.md").read_text(encoding="utf-8")

    assert "S3, Google Cloud Storage, Azure Blob Storage" in user_doc
    assert "ObjectStorageChunkWindow" in user_doc
    assert "ObjectStorageStagingManifest" in user_doc
    assert "Runbook" in user_doc
    assert "dpone.storage.models" in developer_doc
    assert "ObjectStorageChunkWindow" in developer_doc
    assert "Do not couple sources or sinks directly to `boto3`" in developer_doc
    assert "dpone.storage.*" in architecture_doc
    assert "Object storage staging" in readiness_doc
    assert "dpone[object_storage]" in extras_doc


def test_supply_chain_documentation_contract_exists() -> None:
    required_docs = [
        "docs/supply-chain.md",
        "docs/developer-supply-chain.md",
        "docs/architecture.md",
        "docs/developer-ci-cd.md",
    ]
    for relative_path in required_docs:
        assert (ROOT / relative_path).is_file(), relative_path

    user_doc = (ROOT / "docs/supply-chain.md").read_text(encoding="utf-8")
    developer_doc = (ROOT / "docs/developer-supply-chain.md").read_text(encoding="utf-8")
    architecture_doc = (ROOT / "docs/architecture.md").read_text(encoding="utf-8")
    ci_doc = (ROOT / "docs/developer-ci-cd.md").read_text(encoding="utf-8")

    assert "dpone supply-chain attest" in user_doc
    assert "SBOM" in user_doc
    assert "provenance" in user_doc
    assert "Runbook" in user_doc
    assert "dpone.supply_chain" in developer_doc
    assert "Do not put supply-chain business logic into command modules" in developer_doc
    assert "dpone.supply_chain.*" in architecture_doc
    assert "test_artifacts/supply-chain/" in ci_doc


def test_readme_is_pypi_and_github_first() -> None:
    text = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "pip install dpone" in text
    assert 'pip install "dpone[full]"' in text
    assert "https://github.com/PaulKov/dpone" in text
    assert "https://img.shields.io/pypi/v/dpone.svg" in text
    assert "img.shields.io/badge/pypi-v" not in text
    assert "GitLab Package Registry" not in text
    assert "gitlab.example.com" not in text
    assert "data-platform-dpone" not in text
    assert "example_travel" not in text


def test_release_runbook_gates_archive_contents_and_fresh_local_candidates() -> None:
    text = (ROOT / "docs/release.md").read_text(encoding="utf-8")

    assert "tools/agent_policy/package_archive_gate.py dist/*.whl dist/*.tar.gz" in text
    assert "/tmp/dpone-release-smoke/bin/pip install dist/*.whl" in text
    assert "/tmp/dpone-release-smoke/bin/pip check" in text


def test_public_docs_entrypoints_do_not_route_users_to_legacy_gitlab_release_flow() -> None:
    for relative_path in ["docs/README.md", "docs/release.md", "docs/ci-cd.md"]:
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "GitLab Package Registry" not in text
        assert "gitlab.example.com" not in text
        assert "data-platform-dpone" not in text


def test_public_release_guides_do_not_pin_stale_example_versions() -> None:
    for relative_path in [
        "docs/release.md",
        "docs/release-evidence.md",
        "docs/live-certification.md",
        "docs/ops-cli.md",
        "docs/operational-control-plane.md",
        "docs/cli-reference.md",
        "docs/observability.md",
        "docs/supply-chain.md",
    ]:
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        for stale_token in ("v0.7.1", "0.7.1", "v0.8.0", "0.8.0"):
            assert stale_token not in text, f"{relative_path} contains {stale_token}"


def test_github_actions_cover_ci_and_pypi_release() -> None:
    ci = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    release = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8"))

    ci_steps = "\n".join(
        str(step) for job in ci["jobs"].values() if isinstance(job, dict) for step in job.get("steps", [])
    )
    assert ci["jobs"]["quality-preflight"]["env"]["DPONE_TEST_USE_INSTALLED_PACKAGE"] == "1"
    assert "uv sync --locked --all-extras" in ci_steps
    assert "uv run ruff check ." in ci_steps
    assert "uv run ruff format --check ." in ci_steps
    assert "uv run mypy --config-file mypy.ini" in ci_steps
    assert 'uv run pytest -p tools.ci_quality_shards -m "not integration_live" -n auto --dist loadfile' in ci_steps
    assert "--cov=src/dpone" in ci_steps
    assert "--cov=packages/dpone-airflow-pack/src/dpone_airflow_pack" in ci_steps
    assert "uv run coverage xml" in ci_steps
    assert "uv build" in ci_steps
    assert "uv build packages/apache-airflow-providers-dpone --out-dir dist" in ci_steps
    assert "coverage.xml" in ci_steps

    release_on = release[True]
    assert release_on["push"]["tags"] == ["v*.*.*"]
    assert "id-token" not in release["permissions"]
    assert release["jobs"]["build"]["permissions"]["contents"] == "read"
    assert release["jobs"]["publish"]["permissions"]["contents"] == "read"
    assert "id-token" not in release["jobs"]["publish"]["permissions"]
    assert "environment" not in release["jobs"]["publish"]
    assert release["jobs"]["verify-public-bytes"]["permissions"]["contents"] == "read"
    assert release["jobs"]["github-release"]["permissions"]["contents"] == "write"
    assert "id-token" not in release["jobs"]["github-release"]["permissions"]

    build_steps = "\n".join(str(step) for step in release["jobs"]["build"]["steps"])
    publish_steps = "\n".join(str(step) for step in release["jobs"]["publish"]["steps"])
    release_steps = "\n".join(str(step) for step in release["jobs"]["github-release"]["steps"])
    assert "uv build" in build_steps
    assert "uv build packages/apache-airflow-providers-dpone --out-dir dist" in build_steps
    assert "uv run twine check dist/*" in build_steps
    assert "pypa/gh-action-pypi-publish" not in publish_steps
    assert "dpone-release-controller/.github/workflows/pypi-release.yml" in publish_steps
    assert "softprops/action-gh-release" in release_steps
    assert "actions/checkout@" not in release_steps
    assert "release-candidates" in build_steps
    for job_id in ("publish", "verify-public-bytes", "github-release"):
        downloads = [
            step
            for step in release["jobs"][job_id]["steps"]
            if isinstance(step, dict)
            and step.get("name") == "Download immutable release candidates"
            and str(step.get("uses", "")).startswith("actions/download-artifact@")
        ]
        assert len(downloads) == 1
        inputs = downloads[0]["with"]
        assert inputs["artifact-ids"] == "${{ needs.build.outputs.candidates_artifact_id }}"
        assert inputs["digest-mismatch"] == "error"
        assert "name" not in inputs
