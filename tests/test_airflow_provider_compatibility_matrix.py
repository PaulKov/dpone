from __future__ import annotations

import importlib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]

EXPECTED_AIRFLOW_MATRIX = (
    ("2.10.5", "3.11", "10.1.0", "", "compatibility"),
    ("2.10.5", "3.12", "10.1.0", "", "compatibility"),
    ("2.11.0", "3.11", "10.5.0", "4.7.0", "compatibility"),
    ("2.11.0", "3.12", "10.5.0", "4.7.0", "compatibility"),
    ("3.2.0", "3.11", "10.14.0", "4.7.0", "primary"),
    ("3.2.0", "3.12", "10.14.0", "4.7.0", "primary"),
    ("3.3.0", "3.11", "10.20.0", "4.7.0", "latest"),
    ("3.3.0", "3.12", "10.20.0", "4.7.0", "latest"),
)


def test_airflow_compatibility_workflow_uses_exact_supported_matrix() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/airflow-pack-compat.yml").read_text(encoding="utf-8"))
    include = workflow["jobs"]["airflow-compat"]["strategy"]["matrix"]["include"]

    actual = tuple(
        (
            str(row["airflow-version"]),
            str(row["python-version"]),
            str(row["cncf-provider-version"]),
            str(row.get("mssql-provider-version") or ""),
            str(row["support"]),
        )
        for row in include
    )

    assert actual == EXPECTED_AIRFLOW_MATRIX


def test_airflow_compatibility_docs_publish_exact_supported_matrix() -> None:
    docs = (ROOT / "docs/compatibility.md").read_text(encoding="utf-8")

    for airflow, python, cncf_provider, _mssql_provider, support in EXPECTED_AIRFLOW_MATRIX:
        assert f"| `{airflow}` | `{python}` | `{cncf_provider}` | `{support}` |" in docs
    assert 'requires_airflow: ">=2.10,<3.4"' in docs
    assert "apache-airflow-providers-microsoft-mssql==4.7.0" in docs
    assert "Airflow >= 2.11" in docs


def test_airflow_matrix_gates_installed_provider_parse_slo_and_uploads_evidence() -> None:
    workflow_path = ROOT / ".github/workflows/airflow-pack-compat.yml"
    workflow = workflow_path.read_text(encoding="utf-8")

    for required in (
        "tools/airflow_provider_parse_benchmark.py",
        "--cold-samples 30",
        "--warm-samples 30",
        '--source-commit "$source_commit"',
        'source_commit="$(git rev-parse HEAD)"',
        '--airflow-version "${{ matrix.airflow-version }}"',
        '--python-version "${{ matrix.python-version }}"',
        '--support "${{ matrix.support }}"',
        '--constraint "$install_constraints_file"',
        "tools/airflow_constraints_override.py",
        "constraints-provider-override.json",
        "AIRFLOW_INSTALL_CONSTRAINTS_SHA256",
        '--constraints "$AIRFLOW_CONSTRAINTS_URL"',
        '--constraints-sha256 "$AIRFLOW_CONSTRAINTS_SHA256"',
        '--cncf-provider-version "${{ matrix.cncf-provider-version }}"',
        "--candidate-manifest test_artifacts/airflow-provider-parse/candidate-manifest.json",
        '--candidate-manifest-sha256 "$manifest_sha256"',
        "build_candidate_manifest",
        "airflow-parse-slo-${{ matrix.airflow-version }}-py${{ matrix.python-version }}",
        "if-no-files-found: error",
        "uv pip check --python .venv-airflow/bin/python",
        "constraints-${{ matrix.airflow-version }}/constraints-${{ matrix.python-version }}.txt",
        "dist-airflow/*.whl",
        "runtime-wheel-smoke:",
        "dist-runtime/dpone-*.whl",
        "dpone airflow runtime-init-fetch --help",
        "dpone airflow runtime-pack-exec --help",
        'test "$status" -eq 2',
        'grep -q "Traceback (most recent call last)"',
        "apache-airflow-providers-microsoft-mssql==${{ matrix.mssql-provider-version }}",
        "tests/test_airflow_mssql_provider_asset_uri.py",
        "Install microsoft-mssql AIP-60 provider cell (Airflow >= 2.11)",
        "Install outside Airflow constraints",
        "Prove Airflow 3.3 provider 10.19 fails closed without durable KPO",
        'metadata.version("apache-airflow-providers-cncf-kubernetes") == "10.19.0"',
        '"durable" not in inspect.signature(KubernetesPodOperator.__init__).parameters',
    ):
        assert required in workflow


def test_airflow_helm_artifact_is_structural_and_never_uploads_rendered_manifests() -> None:
    workflow = (ROOT / ".github/workflows/airflow-pack-compat.yml").read_text(encoding="utf-8")

    assert 'python" -m dpone_airflow_pack.cli_helm_evidence' in workflow
    assert 'render_dir="${RUNNER_TEMP}/dpone-airflow-renders"' in workflow
    assert "> test_artifacts/airflow-helm/airflow-2.10-chart-1.19.0.evidence.json" in workflow
    assert "> test_artifacts/airflow-helm/airflow-3.2-chart-1.22.0.evidence.json" in workflow
    assert "> test_artifacts/airflow-helm/airflow-2.10-chart-1.19.0.yaml" not in workflow
    assert "> test_artifacts/airflow-helm/airflow-3.2-chart-1.22.0.yaml" not in workflow
    assert "rendered Kubernetes manifests must never be persisted as CI evidence" in workflow


def test_every_supported_airflow_matrix_cell_is_a_required_status_check() -> None:
    access = importlib.import_module("tools.agent_policy.governance_policy_access")
    policy = yaml.safe_load((ROOT / ".agents/policy/github-branch-protection.yml").read_text(encoding="utf-8"))
    required = set(access.required_context_names(policy))
    matrix_checks = {f"Airflow {airflow} / py{python}" for airflow, python, _, _, _ in EXPECTED_AIRFLOW_MATRIX}
    runtime_wheel_checks = {"Runtime wheel smoke / py3.11", "Runtime wheel smoke / py3.12"}

    assert matrix_checks <= required
    assert runtime_wheel_checks <= required
