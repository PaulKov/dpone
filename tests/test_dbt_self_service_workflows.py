from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEV_WORKFLOW = ROOT / ".github" / "workflows" / "dbt-self-service-dev.yml"
DEV_ACTIVATION_WORKFLOW = ROOT / ".github" / "workflows" / "dbt-self-service-dev-activation.yml"
DEV_EVIDENCE_WORKFLOW = ROOT / ".github" / "workflows" / "dbt-self-service-dev-evidence.yml"
PROD_WORKFLOW = ROOT / ".github" / "workflows" / "dbt-self-service-prod.yml"
MIRROR_WORKFLOW = ROOT / ".github" / "workflows" / "dbt-self-service-open-prod-pr.yml"
AIRFLOW_COMPAT_WORKFLOW = ROOT / ".github" / "workflows" / "airflow-pack-compat.yml"
COSMOS_COEXISTENCE_PROBE = ROOT / "tools" / "dbt_self_service" / "cosmos_coexistence_probe.py"
PLATFORM_WORKFLOW_DOC = ROOT / "docs" / "dbt-self-service-platform-workflows.md"


def _workflow(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _workflow_mapping(path: Path) -> dict[str, object]:
    payload = yaml.safe_load(_workflow(path))
    assert isinstance(payload, dict)
    return payload


def _workflow_call_inputs(payload: dict[str, object]) -> dict[str, object]:
    events = payload.get("on", payload.get(True))
    assert isinstance(events, dict)
    workflow_call = events["workflow_call"]
    assert isinstance(workflow_call, dict)
    inputs = workflow_call["inputs"]
    assert isinstance(inputs, dict)
    return inputs


def _fenced_block_after(text: str, marker: str) -> str:
    """Return the first fenced text block following a unique documentation marker."""

    tail = text.split(marker, maxsplit=1)[1]
    return tail.split("```text", maxsplit=1)[1].split("```", maxsplit=1)[0]


def test_platform_workflow_docs_separate_environment_and_repository_authority() -> None:
    docs = _workflow(PLATFORM_WORKFLOW_DOC)
    dev_environment = _fenced_block_after(
        docs,
        "values only as protected `development` environment variables:",
    )
    dev_repository = _fenced_block_after(
        docs,
        "Define the independent identity and signer allowlist",
    )
    prod_environment = _fenced_block_after(
        docs,
        "values only as protected `production` environment variables:",
    )
    prod_repository = _fenced_block_after(
        docs,
        "Define the independent promotion and signer allowlists",
    )

    for block in (dev_environment, prod_environment):
        assert "DPONE_DBT_TOOLING_VERSION" in block
        assert "DPONE_DBT_EXPECTED_CURRENT_DEPLOYMENT_ID" in block
        assert "DPONE_DBT_ARTIFACT_REGISTRY_SCOPE_ID" in block
        assert "DPONE_PROMOTION_IDENTITY" not in block
        assert "DPONE_DBT_ALLOWED_PROMOTER" not in block

    for block in (dev_repository, prod_repository):
        assert "DPONE_PROMOTION_IDENTITY" in block
        assert "DPONE_DBT_ALLOWED_PROMOTER" in block
        assert "DPONE_DBT_TRUSTED_SIGNER_WORKFLOW" in block
        assert "DPONE_DBT_EXPECTED_CURRENT_DEPLOYMENT_ID" not in block
        assert "DPONE_DBT_ARTIFACT_REGISTRY_SCOPE_ID" not in block


def test_dev_workflow_builds_one_reproducible_release_and_review_report() -> None:
    workflow = _workflow(DEV_WORKFLOW)
    payload = _workflow_mapping(DEV_WORKFLOW)
    jobs = payload["jobs"]
    assert isinstance(jobs, dict)
    build = jobs["build"]
    attest = jobs["attest"]
    assert isinstance(build, dict)
    assert isinstance(attest, dict)

    assert "workflow_call:" in workflow
    assert '"dpone[dbt-mssql]==${DPONE_TRUSTED_VERSION}"' in workflow
    assert ("DPONE_TRUSTED_VERSION: ${{ vars.DPONE_DBT_TOOLING_VERSION }}") in workflow
    assert "differs from repository/organization DPONE_DBT_TOOLING_VERSION" in workflow
    assert workflow.count("dpone dbt compile") == 2
    assert "diff --recursive --brief" in workflow
    assert "dpone dbt render-ci-report" in workflow
    assert "dpone dbt write-release-checksums" in workflow
    assert "release-subjects.sha256" in workflow
    assert "release-attestation.sigstore.jsonl" in workflow
    assert workflow.count("actions/attest@59d89421af93a897026c735860bf21b6eb4f7b26") == 2
    assert "source-commit=${GITHUB_SHA}" in workflow
    assert "source-ref=${GITHUB_REF}" in workflow
    assert "actions/attest@59d89421af93a897026c735860bf21b6eb4f7b26" in workflow
    assert "id-token: write" in workflow
    assert "attestations: write" in workflow
    assert build["permissions"] == {"contents": "read"}
    assert attest["permissions"] == {
        "contents": "read",
        "id-token": "write",
        "attestations": "write",
        "artifact-metadata": "write",
    }
    assert "actions/checkout@" in str(build["steps"])
    assert "actions/attest@" not in str(build["steps"])
    assert "actions/checkout@" not in str(attest["steps"])
    assert "dbt parse" not in str(attest["steps"])
    assert "dpone dbt compile" not in str(attest["steps"])
    assert "dpone dbt materialize-release" not in workflow
    assert "dbt-profiles-yml:" in workflow
    assert "chmod(0o600)" in workflow
    assert "DPONE_CI_ROOT: ${{ github.workspace }}/.dpone-ci/dbt" in workflow
    assert 'git ls-files --error-unmatch "${DPONE_PROJECT_DIR}/package-lock.yml"' in workflow
    assert 'git status --porcelain=v1 --untracked-files=all -- "${DPONE_PROJECT_DIR}/package-lock.yml"' in workflow


def test_prod_workflow_validates_downloaded_release_without_rebuild() -> None:
    workflow = _workflow(PROD_WORKFLOW)
    payload = _workflow_mapping(PROD_WORKFLOW)
    inputs = _workflow_call_inputs(payload)
    jobs = payload["jobs"]
    assert isinstance(jobs, dict)
    promote = jobs["promote"]
    assert isinstance(promote, dict)
    environment = promote["environment"]
    env = promote["env"]
    assert environment == "production"
    assert isinstance(env, dict)

    assert "workflow_call:" in workflow
    assert env["PYTHONNOUSERSITE"] == "1"
    assert env["PYTHONPATH"] == ""
    assert "repository: ${{ inputs.release-repository }}" in workflow
    assert "run-id: ${{ inputs.release-run-id }}" in workflow
    assert "github-token: ${{ secrets.release-artifact-token }}" in workflow
    assert "dbt deps --project-dir" in workflow
    assert "dpone dbt verify-promotion" in workflow
    assert '--descriptor-path "${PROMOTION_DESCRIPTOR_PATH}"' in workflow
    assert "--expected-dev-evidence-ref" in workflow
    assert "gh attestation verify" in workflow
    assert "dpone dbt verify-release-checksums" in workflow
    assert "dpone dbt verify-dev-evidence-integrity" in workflow
    assert "DPONE_DBT_TRUSTED_EVIDENCE_WORKFLOW" in workflow
    assert "evidence-subjects.sha256" in workflow
    assert "--expected-dev-evidence-subject-sha256" in workflow
    assert "--expected-dev-evidence-producer-workflow" in workflow
    assert "--expected-dev-evidence-source-commit" in workflow
    assert "dpone dbt materialize-release" in workflow
    assert "dpone dbt compile" not in workflow
    assert ('"apache-airflow-providers-dpone==${DPONE_TRUSTED_VERSION}"') in workflow
    assert ("DPONE_TRUSTED_VERSION: ${{ vars.DPONE_DBT_TOOLING_VERSION }}") in workflow
    assert '"dpone[dbt-mssql]==${DPONE_TRUSTED_VERSION}"' in workflow
    assert 'git ls-files --error-unmatch "${DPONE_PROJECT_DIR}/package-lock.yml"' in workflow
    assert 'git status --porcelain=v1 --untracked-files=all -- "${DPONE_PROJECT_DIR}/package-lock.yml"' in workflow
    assert "AIRFLOW_BUNDLE_REF: git:${{ github.sha }}" in workflow
    assert "unsupported Airflow/Python pair" in workflow
    assert "3.3.0:3.12" in workflow
    assert "DPONE_ALLOWED_PROMOTER: ${{ vars.DPONE_DBT_ALLOWED_PROMOTER }}" in workflow
    assert '--allowed-promoter "${DPONE_ALLOWED_PROMOTER}"' in workflow
    assert '--allowed-promoter "${DPONE_PROMOTED_BY}"' not in workflow
    assert "trusted repository variable DPONE_DBT_ALLOWED_PROMOTER is required" in workflow
    assert "DPONE_DBT_TRUSTED_SIGNER_DIGEST" in workflow
    assert '--signer-digest "${DPONE_TRUSTED_SIGNER_DIGEST}"' in workflow
    assert "DPONE_ARTIFACT_REGISTRY_SCOPE_ID: ${{ vars.DPONE_DBT_ARTIFACT_REGISTRY_SCOPE_ID }}" in workflow
    assert '--expected-registry-scope-id "${DPONE_ARTIFACT_REGISTRY_SCOPE_ID}"' in workflow
    assert "--publication-mode exact" in workflow
    assert '--source-digest "${DPONE_RELEASE_SOURCE_COMMIT}"' in workflow
    assert '--source-ref "${DPONE_RELEASE_SOURCE_REF}"' in workflow
    assert '--source-digest "${DPONE_DEV_EVIDENCE_SOURCE_COMMIT_EXPECTED}"' in workflow
    assert '--source-ref "${DPONE_DEV_EVIDENCE_SOURCE_REF}"' in workflow
    assert "activation-runner:" not in workflow
    assert "scheduler-cache-root:" not in workflow
    assert "approval-environment:" not in workflow
    assert "environment: production" in workflow
    assert "runs-on: [self-hosted, dpone-dbt-prod]" in workflow
    assert "DPONE_CACHE_ROOT: /var/lib/dpone/airflow/prod/.dpone-cache" in workflow
    assert "DPONE_ENVIRONMENT: prod" in workflow
    assert '--signer-digest "${DPONE_TRUSTED_SIGNER_DIGEST}"' in workflow
    assert '--signer-digest "${DPONE_TRUSTED_EVIDENCE_SIGNER_DIGEST}"' in workflow
    assert "dpone airflow verify-attestation" in workflow
    assert "--expected-trust-policy-sha256" in workflow
    assert "--expected-trust-tier production" in workflow
    assert "--artifact-attestation-bundle" in workflow
    assert "DPONE_ARTIFACT_ATTESTATION_REQUIRED" not in workflow
    assert "runtime-attestation-verifier:" not in workflow
    assert "allow-unverified-runtime:" not in workflow
    assert "trust-policy-path" not in inputs
    assert "trust-policy-sha256" not in inputs
    assert env["DPONE_TRUST_POLICY_PATH"] == "platform/runtime-artifact-trust-policy.json"
    assert env["DPONE_TRUST_POLICY_SHA256"] == "${{ vars.DPONE_DBT_RUNTIME_TRUST_POLICY_SHA256 }}"
    assert "expected-current-deployment-id" not in inputs
    assert env["DPONE_EXPECTED_CURRENT"] == ("${{ vars.DPONE_DBT_EXPECTED_CURRENT_DEPLOYMENT_ID }}")
    assert "candidate.is_symlink()" in workflow
    assert "resolved.is_relative_to(root)" in workflow


def test_prod_workflows_scope_exact_dev_activation_to_every_verification_step() -> None:
    cases = (
        (PROD_WORKFLOW, "promote", "${{ inputs.expected-dev-activation-id }}"),
        (MIRROR_WORKFLOW, "open", "${{ inputs.dev-activation-id }}"),
    )
    for path, job_name, expected in cases:
        payload = _workflow_mapping(path)
        jobs = payload["jobs"]
        assert isinstance(jobs, dict)
        job = jobs[job_name]
        assert isinstance(job, dict)
        env = job["env"]
        assert isinstance(env, dict)
        assert env["EXPECTED_DEV_ACTIVATION_ID"] == expected
        assert "EXPECTED_DEV_ACTIVATION_ID" in _workflow(path)


def test_dbt_workflows_include_hidden_ci_artifacts() -> None:
    for path in (DEV_WORKFLOW, DEV_ACTIVATION_WORKFLOW, DEV_EVIDENCE_WORKFLOW, PROD_WORKFLOW):
        payload = _workflow_mapping(path)
        jobs = payload["jobs"]
        assert isinstance(jobs, dict)
        for job in jobs.values():
            assert isinstance(job, dict)
            steps = job.get("steps", [])
            assert isinstance(steps, list)
            for step in steps:
                assert isinstance(step, dict)
                if not str(step.get("uses", "")).startswith("actions/upload-artifact@"):
                    continue
                with_args = step.get("with")
                assert isinstance(with_args, dict)
                if ".dpone-ci" in str(with_args.get("path", "")):
                    assert with_args.get("include-hidden-files") is True, path


def test_dev_evidence_attestation_job_is_source_free() -> None:
    payload = _workflow_mapping(DEV_EVIDENCE_WORKFLOW)
    jobs = payload["jobs"]
    assert isinstance(jobs, dict)
    finalize = jobs["finalize"]
    attest = jobs["attest"]
    assert isinstance(finalize, dict)
    assert isinstance(attest, dict)

    assert finalize["permissions"] == {"actions": "read", "contents": "read"}
    assert "actions/checkout@" in str(finalize["steps"])
    assert "actions/attest@" not in str(finalize["steps"])
    assert attest["permissions"] == {
        "actions": "read",
        "artifact-metadata": "write",
        "attestations": "write",
        "contents": "read",
        "id-token": "write",
    }
    serialized = str(attest["steps"])
    assert "actions/checkout@" not in serialized
    assert "actions/setup-python@" not in serialized
    assert "pip install" not in serialized
    assert "dpone " not in serialized
    assert "dbt " not in serialized


def test_dev_activation_uses_attested_release_and_audited_cas() -> None:
    workflow = _workflow(DEV_ACTIVATION_WORKFLOW)
    payload = _workflow_mapping(DEV_ACTIVATION_WORKFLOW)
    inputs = _workflow_call_inputs(payload)
    jobs = payload["jobs"]
    assert isinstance(jobs, dict)
    activate = jobs["activate"]
    assert isinstance(activate, dict)
    env = activate["env"]
    assert isinstance(env, dict)

    assert "workflow_call:" in workflow
    assert env["PYTHONNOUSERSITE"] == "1"
    assert env["PYTHONPATH"] == ""
    assert "gh attestation verify" in workflow
    assert "dpone dbt verify-release-checksums" in workflow
    assert "dpone dbt materialize-release" in workflow
    assert "dpone dbt compile" not in workflow
    assert "--trust-tier non_production" in workflow
    assert ('"apache-airflow-providers-dpone==${DPONE_TRUSTED_VERSION}"') in workflow
    assert "runs-on: [self-hosted, dpone-dbt-dev]" in workflow
    assert "DPONE_CACHE_ROOT: /var/lib/dpone/airflow/dev/.dpone-cache" in workflow
    assert "AIRFLOW_BUNDLE_REF: git:${{ github.sha }}" in workflow
    assert "unsupported Airflow/Python pair" in workflow
    assert (
        "https://raw.githubusercontent.com/apache/airflow/"
        "constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    ) in workflow
    assert '--constraint "${CONSTRAINT_URL}"' in workflow
    assert ".venv-dpone/bin/python -m pip check" in workflow
    assert ".venv-airflow/bin/python -m pip check" in workflow
    assert 'echo "${GITHUB_WORKSPACE}/.venv-dpone/bin" >> "${GITHUB_PATH}"' in workflow
    assert ".venv-airflow/bin/python -c" in workflow
    assert "3.3.0:3.12" in workflow
    assert "DPONE_ALLOWED_PROMOTER: ${{ vars.DPONE_DBT_ALLOWED_PROMOTER }}" in workflow
    assert '--allowed-promoter "${DPONE_ALLOWED_PROMOTER}"' in workflow
    assert '--allowed-promoter "${DPONE_PROMOTED_BY}"' not in workflow
    assert "trusted repository variable DPONE_DBT_ALLOWED_PROMOTER is required" in workflow
    assert '--source-digest "${DPONE_RELEASE_SOURCE_COMMIT}"' in workflow
    assert '--source-ref "${DPONE_RELEASE_SOURCE_REF}"' in workflow
    assert "github-attestation-receipt:sha256:" in workflow
    assert "--artifact-attestation-bundle" in workflow
    assert "DPONE_ARTIFACT_REGISTRY_SCOPE_ID: ${{ vars.DPONE_DBT_ARTIFACT_REGISTRY_SCOPE_ID }}" in workflow
    assert '--expected-registry-scope-id "${DPONE_ARTIFACT_REGISTRY_SCOPE_ID}"' in workflow
    assert "--publication-mode exact" in workflow
    assert "runtime-attestation-verification.json" in workflow
    assert "expected-current-deployment-id" not in inputs
    assert ("DPONE_EXPECTED_CURRENT: ${{ vars.DPONE_DBT_EXPECTED_CURRENT_DEPLOYMENT_ID }}") in workflow

    attest = workflow.index("gh attestation verify")
    integrity = workflow.index("dpone dbt verify-release-checksums")
    materialize = workflow.index("dpone dbt materialize-release")
    build = workflow.index("dpone airflow build")
    parse_smoke = workflow.index("load_dpone_dags")
    publish = workflow.index("dpone airflow publish")
    promote = workflow.index("dpone airflow cache-sync")
    receipt = workflow.index("dpone airflow cache-recovery-plan")
    assert attest < integrity < materialize < build < parse_smoke < publish < promote < receipt


def test_airflow_matrix_installs_candidate_dbt_activation_tree_and_smokes_runtime_wheel() -> None:
    workflow = _workflow(AIRFLOW_COMPAT_WORKFLOW)

    assert "Install the exact candidate activation dependency sets" in workflow
    assert workflow.count('"${core_wheels[0]}[dbt-mssql]"') == 2
    assert "pack_wheels=(dist-airflow/dpone_airflow_pack-*.whl)" in workflow
    assert workflow.count('"${pack_wheels[0]}"') == 2
    assert "uv venv .venv-activation" in workflow
    assert "uv pip install --python .venv-activation/bin/python" in workflow
    assert "uv pip check --python .venv-activation/bin/python" in workflow
    assert "uv pip check --python .venv-airflow/bin/python" in workflow
    assert ".venv-runtime/bin/dpone dbt execute-pack --help" in workflow
    assert "Run real dbt offline parse and selection from installed wheels" in workflow
    assert ".venv-runtime/bin/dbt --quiet --no-use-colors parse" in workflow
    assert ".venv-runtime/bin/dbt --quiet --no-use-colors ls" in workflow
    assert "unit_test.dpone_runtime_fixture.orders.orders_from_ephemeral_parent" in workflow
    assert "tests/test_dbt_manifest_schema_validation.py" in workflow
    assert "tests/test_dbt_publish_schema_contracts.py" in workflow
    assert "tests/test_dbt_project_bundle.py" in workflow
    assert "tests/test_dbt_run_results_schema_validation.py" in workflow
    assert "tests/test_dbt_runtime_execution.py" in workflow
    assert "tests/test_dbt_subprocess_supervision.py" in workflow


def test_airflow_candidate_binds_wheels_and_helm_values_in_one_verified_artifact() -> None:
    workflow = _workflow(AIRFLOW_COMPAT_WORKFLOW)

    for profile in ("airflow-cache-values-2.10.yaml", "airflow-cache-values-3.2.yaml"):
        assert f"cp docs/examples/{profile} dist-airflow/" in workflow
        assert f"-f dist-airflow/{profile}" in workflow
        assert f"sha256sum dist-airflow/{profile}" in workflow
    assert "sha256sum ./*.whl airflow-cache-values-*.yaml > SHA256SUMS" in workflow
    assert 'test "$(cat dist-airflow/SOURCE_COMMIT)" = "${DPONE_CANDIDATE_SHA}"' in workflow
    assert 'test "$(cat dist-runtime/SOURCE_COMMIT)" = "${DPONE_CANDIDATE_SHA}"' in workflow


def test_prod_workflow_preserves_provider_smoke_and_audited_cas_order() -> None:
    workflow = _workflow(PROD_WORKFLOW)

    attestation = workflow.index("gh attestation verify")
    integrity = workflow.index("dpone dbt verify-release-checksums")
    verify = workflow.index("dpone dbt verify-promotion")
    evidence_attestation = workflow.index('"${DPONE_DEV_EVIDENCE_ROOT}/evidence-subjects.sha256"')
    dev_evidence = workflow.index("dpone dbt verify-dev-evidence-integrity")
    materialize = workflow.index("dpone dbt materialize-release")
    build = workflow.index("dpone airflow build")
    parse_smoke = workflow.index("load_dpone_dags")
    runtime_attestation_gate = workflow.index("dpone airflow verify-attestation")
    publish = workflow.index("dpone airflow publish")
    promote = workflow.index("dpone airflow cache-sync")
    activation_receipt = workflow.index("dpone airflow cache-recovery-plan")

    assert (
        attestation
        < integrity
        < runtime_attestation_gate
        < evidence_attestation
        < dev_evidence
        < verify
        < materialize
        < build
        < parse_smoke
        < publish
        < promote
        < activation_receipt
    )
    assert "--expected-current-deployment-id" in workflow
    assert "--expect-current-absent" in workflow
    assert "--allowed-promoter" in workflow
    assert "promoted-by:" not in workflow
    assert "DPONE_PROMOTED_BY: ${{ vars.DPONE_PROMOTION_IDENTITY }}" in workflow
    assert "DPONE_ALLOWED_PROMOTER: ${{ vars.DPONE_DBT_ALLOWED_PROMOTER }}" in workflow
    assert '--allowed-promoter "${DPONE_ALLOWED_PROMOTER}"' in workflow
    assert '--allowed-promoter "${DPONE_PROMOTED_BY}"' not in workflow
    assert (
        "https://raw.githubusercontent.com/apache/airflow/"
        "constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    ) in workflow
    assert '--constraint "${CONSTRAINT_URL}"' in workflow
    assert ".venv-dpone/bin/python -m pip check" in workflow
    assert ".venv-airflow/bin/python -m pip check" in workflow
    assert 'echo "${GITHUB_WORKSPACE}/.venv-dpone/bin" >> "${GITHUB_PATH}"' in workflow
    assert ".venv-airflow/bin/python -c" in workflow
    assert "trusted repository variable DPONE_PROMOTION_IDENTITY is required" in workflow
    assert "trusted repository variable DPONE_DBT_ALLOWED_PROMOTER is required" in workflow
    assert "DPONE_DBT_TRUSTED_SIGNER_WORKFLOW" in workflow
    assert "trusted repository variable DPONE_DBT_TRUSTED_SIGNER_WORKFLOW is required" in workflow
    assert "protected production-environment variable DPONE_DBT_ARTIFACT_REGISTRY_SCOPE_ID is required" in workflow
    assert "--attestation-ref" in workflow
    assert "attestation-ref:" not in workflow
    assert "github-attestation-receipt:sha256:" in workflow
    assert "environment: production" in workflow
    assert "runs-on: [self-hosted, dpone-dbt-prod]" in workflow
    assert "trusted prod cache root must be absolute" in workflow
    assert "DPONE_DBT_TRUSTED_SIGNER_DIGEST" in workflow
    assert "DPONE_DBT_TRUSTED_EVIDENCE_SIGNER_DIGEST" in workflow
    upload = workflow[workflow.index("- name: Upload promotion evidence") :]
    assert "if: ${{ !cancelled() }}" in upload


def test_prod_mirror_workflow_uses_attested_bytes_and_opens_bot_pr() -> None:
    workflow = _workflow(MIRROR_WORKFLOW)

    assert "workflow_call:" in workflow
    assert "repository: ${{ inputs.prod-repository }}" in workflow
    assert "run-id: ${{ inputs.release-run-id }}" in workflow
    assert "gh attestation verify" in workflow
    assert "dpone dbt verify-release-checksums" in workflow
    assert "dpone dbt verify-dev-evidence-integrity" in workflow
    assert "DPONE_DBT_TRUSTED_EVIDENCE_WORKFLOW" in workflow
    assert "--dev-evidence-subject-sha256" in workflow
    assert "--dev-evidence-producer-workflow" in workflow
    assert "--expected-dev-evidence-subject-sha256" in workflow
    assert "dpone dbt prepare-prod-mirror" in workflow
    assert "dpone dbt verify-promotion" in workflow
    assert workflow.count("--signer-digest") == 2
    assert workflow.count("--source-digest") == 2
    assert workflow.count("--source-ref") == 2
    assert "git add --" in workflow
    assert "git push --force-with-lease" in workflow
    assert "gh pr create" in workflow
    assert "dpone dbt compile" not in workflow

    attest = workflow.index("gh attestation verify")
    integrity = workflow.index("dpone dbt verify-release-checksums")
    evidence_attestation = workflow.index('"${DPONE_DEV_EVIDENCE_ROOT}/evidence-subjects.sha256"')
    dev_evidence = workflow.index("dpone dbt verify-dev-evidence-integrity")
    prepare = workflow.index("dpone dbt prepare-prod-mirror")
    verify = workflow.index("dpone dbt verify-promotion")
    push = workflow.index("git push --force-with-lease")
    pull_request = workflow.index("gh pr create")
    assert attest < integrity < evidence_attestation < dev_evidence < prepare < verify < push < pull_request


def test_dev_evidence_workflow_attests_semantically_verified_bundle() -> None:
    workflow = _workflow(DEV_EVIDENCE_WORKFLOW)

    assert "workflow_call:" in workflow
    assert "runs-on: [self-hosted, dpone-dbt-dev]" in workflow
    assert "source-evidence-root:" not in workflow
    assert "airflow-api-url:" not in workflow
    assert "airflow-api-version:" not in workflow
    assert ("DPONE_EVIDENCE_SOURCE_ROOT: ${{ vars.DPONE_DBT_EVIDENCE_EXPORT_ROOT }}") in workflow
    assert ("DPONE_AIRFLOW_API_URL: ${{ vars.DPONE_DBT_AIRFLOW_API_URL }}") in workflow
    assert ("DPONE_AIRFLOW_API_VERSION: ${{ vars.DPONE_DBT_AIRFLOW_API_VERSION }}") in workflow
    assert "protected development-environment variable DPONE_DBT_EVIDENCE_EXPORT_ROOT is required" in workflow
    assert "evidence export root must not be inside GITHUB_WORKSPACE" in workflow
    assert "evidence export root must not be a symlink" in workflow
    assert "gh attestation verify" in workflow
    assert "dpone dbt verify-release-checksums" in workflow
    assert "dpone dbt run-dev-evidence-campaign" in workflow
    assert "dpone dbt finalize-dev-evidence" in workflow
    assert "evidence-subjects.sha256" in workflow
    assert "actions/attest@59d89421af93a897026c735860bf21b6eb4f7b26" in workflow
    assert "id-token: write" in workflow
    assert "attestations: write" in workflow
    assert "artifact-metadata: write" in workflow
    assert "evidence-artifact-name:" in workflow
    assert "name: ${{ inputs.evidence-artifact-name }}" in workflow
    assert "DPONE_DBT_TRUSTED_SIGNER_DIGEST" in workflow
    assert '--signer-digest "${DPONE_TRUSTED_RELEASE_SIGNER_DIGEST}"' in workflow
    assert '--source-digest "${DPONE_RELEASE_SOURCE_COMMIT}"' in workflow
    assert '--source-ref "${DPONE_RELEASE_SOURCE_REF}"' in workflow
    assert "source-commit=${GITHUB_SHA}" in workflow
    assert "source-ref=${GITHUB_REF}" in workflow
    assert "DPONE_FINALIZER_REPOSITORY: ${{ github.repository }}" in workflow
    assert "DPONE_FINALIZER_WORKFLOW: ${{ github.workflow_ref }}" in workflow
    assert "DPONE_FINALIZER_SOURCE_COMMIT: ${{ github.workflow_sha }}" in workflow
    assert '--producer-repository "${DPONE_FINALIZER_REPOSITORY}"' in workflow
    assert '--producer-workflow "${DPONE_FINALIZER_WORKFLOW}"' in workflow
    assert '--source-commit "${DPONE_FINALIZER_SOURCE_COMMIT}"' in workflow

    release_attestation = workflow.index("gh attestation verify")
    release_integrity = workflow.index("dpone dbt verify-release-checksums")
    finalize = workflow.index("dpone dbt finalize-dev-evidence")
    upload = workflow.index("Upload immutable dev evidence")
    download = workflow.index("Download immutable dev evidence")
    evidence_attestation = workflow.index("Attest complete dev evidence checksum subject")
    assert release_attestation < release_integrity < finalize < upload < download < evidence_attestation


def test_cosmos_coexistence_is_optional_pinned_and_topology_neutral() -> None:
    workflow = _workflow(AIRFLOW_COMPAT_WORKFLOW)
    probe = _workflow(COSMOS_COEXISTENCE_PROBE)

    assert workflow.count('cosmos-version: "1.15.0"') == 2
    assert '"astronomer-cosmos==${{ matrix.cosmos-version }}"' in workflow
    assert "if: matrix.cosmos-version != ''" in workflow
    assert "tools/dbt_self_service/cosmos_coexistence_probe.py" in workflow
    assert "cosmos_coexistence_probe" in probe
    assert "dpone_coexistence_probe" in probe
    assert "from cosmos import DbtDag" in probe
    assert "from airflow.providers.dpone import load_dpone_dags" in probe
    assert "cache_write_lease(cache)" in probe
    assert "manifest_path=" in probe
    assert "dbt_project_path=" in probe
    assert "safe_mode" in probe
    assert "bag.dags.get(" in probe
    assert "load_dpone_dags(globals()" in probe


def test_reusable_dbt_workflows_allowlist_shell_touched_inputs() -> None:
    for path in (
        DEV_WORKFLOW,
        DEV_ACTIVATION_WORKFLOW,
        DEV_EVIDENCE_WORKFLOW,
        PROD_WORKFLOW,
        MIRROR_WORKFLOW,
    ):
        workflow = _workflow(path)
        assert "rejected by allowlist" in workflow
        assert "python - <<'PY'" in workflow
