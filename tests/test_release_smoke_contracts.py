from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any

import yaml
from tools.agent_policy.pypi_verifier_closure import ORDINARY_RELEASE_FILES

ROOT = Path(__file__).resolve().parents[1]


def _workflow(name: str) -> dict[str, Any]:
    payload = yaml.safe_load((ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _job(workflow: dict[str, Any], name: str) -> dict[str, Any]:
    job = workflow["jobs"][name]
    assert isinstance(job, dict)
    return job


def _step(job: dict[str, Any], name: str) -> dict[str, Any]:
    for value in job["steps"]:
        if isinstance(value, dict) and value.get("name") == name:
            return value
    raise AssertionError(f"missing workflow step: {name}")


def _run(step: dict[str, Any]) -> str:
    value = step.get("run")
    assert isinstance(value, str)
    return value


def _needs(job: dict[str, Any]) -> set[str]:
    value = job.get("needs", [])
    if isinstance(value, str):
        return {value}
    assert isinstance(value, list)
    return {str(item) for item in value}


def _steps(job: dict[str, Any]) -> list[dict[str, Any]]:
    return [step for step in job.get("steps", []) if isinstance(step, dict)]


def _assert_final_bytes_authority_mutation(
    job: dict[str, Any],
    *,
    mutation_name: str,
) -> None:
    steps = _steps(job)
    mutation_index = next(index for index, step in enumerate(steps) if step.get("name") == mutation_name)
    assert mutation_index >= 2
    byte_fence = steps[mutation_index - 2]
    authority_fence = steps[mutation_index - 1]
    assert byte_fence.get("if") is None
    assert byte_fence.get("continue-on-error") is None
    assert authority_fence.get("if") is None
    assert authority_fence.get("continue-on-error") is None

    byte_command = _run(byte_fence)
    assert 'python "${VERIFIER_DIR}/pypi_prepublication_gate.py"' in byte_command
    assert "--candidate-inventory test_artifacts/release-candidates/candidate-inventory.json" in byte_command
    assert "--dist-dir dist" in byte_command
    assert byte_fence["env"]["VERIFIER_DIR"] == "${{ runner.temp }}/release-verifier"

    authority_command = _run(authority_fence)
    commit_index = authority_command.index("release_commit_artifact_gate.py")
    merge_index = authority_command.index("release_merge_receipt_gate.py")
    candidate_index = authority_command.index("release_candidate_evidence_gate.py")
    assert commit_index < merge_index < candidate_index
    assert "--authorization-report" in authority_command
    assert "--authorization-ruleset-report" in authority_command
    assert "exact_commit_checks.json" in authority_command
    assert "release_ruleset_snapshot.json" in authority_command
    assert authority_fence["env"]["VERIFIER_DIR"] == "${{ runner.temp }}/release-verifier"


def test_release_verifier_is_sealed_before_every_dependency_and_candidate() -> None:
    workflow = _workflow("release.yml")
    verifier = _job(workflow, "verifier")
    steps = _steps(verifier)

    assert _needs(verifier) == set()
    assert verifier["permissions"] == {"contents": "read"}
    assert verifier["outputs"] == {
        "artifact_id": "${{ steps.verifier-upload.outputs.artifact-id }}",
        "artifact_digest": "${{ steps.verifier-upload.outputs.artifact-digest }}",
        "artifact_name": "${{ steps.verifier-stage.outputs.artifact_name }}",
        "manifest_sha256": "${{ steps.verifier-stage.outputs.manifest_sha256 }}",
        "producer_run_attempt": "${{ steps.verifier-stage.outputs.producer_run_attempt }}",
    }
    assert [step.get("name") for step in steps] == [
        "Checkout exact release commit",
        "Stage exact stdlib-only release verifier",
        "Upload immutable closed release verifier",
    ]
    assert steps[0]["with"] == {
        "ref": "${{ github.sha }}",
        "fetch-depth": 1,
        "persist-credentials": False,
    }
    stage = _run(steps[1])
    assert steps[1]["id"] == "verifier-stage"
    assert "PYTHONDONTWRITEBYTECODE=1 python tools/agent_policy/pypi_verifier_closure.py" in stage
    assert "--profile ordinary-release" in stage
    assert "uv " not in stage and "pip install" not in stage
    assert "artifact_name=release-verifier-" in stage
    assert "producer_run_attempt=" in stage
    upload = steps[2]
    assert upload["id"] == "verifier-upload"
    assert upload["with"] == {
        "name": "release-verifier-${{ github.run_id }}-${{ github.run_attempt }}",
        "path": "${{ runner.temp }}/release-verifier/",
        "if-no-files-found": "error",
        "retention-days": 90,
        "overwrite": False,
    }
    assert _needs(_job(workflow, "preflight")) == {"verifier"}


def test_release_verifier_consumers_authenticate_and_read_back_exact_artifact() -> None:
    workflow = _workflow("release.yml")
    workflow_raw = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")

    for job_name in ("preflight", "attest", "publish", "github-release"):
        job = _job(workflow, job_name)
        assert "verifier" in _needs(job)
        steps = _steps(job)
        names = [step.get("name") for step in steps]
        identity_name = "Verify immutable release verifier artifact provider identity"
        download_name = "Download immutable closed release verifier"
        readback_name = "Validate closed release verifier before first execution"
        identity_index = names.index(identity_name)
        download_index = names.index(download_name)
        readback_index = names.index(readback_name)
        assert identity_index + 1 == download_index
        assert download_index + 1 == readback_index

        if job_name == "preflight":
            assert names.index("Checkout complete release history") < identity_index

        identity = steps[identity_index]
        assert identity["env"] == {
            "EXPECTED_ARTIFACT_ID": "${{ needs.verifier.outputs.artifact_id }}",
            "EXPECTED_ARTIFACT_DIGEST": "${{ needs.verifier.outputs.artifact_digest }}",
            "EXPECTED_ARTIFACT_NAME": "${{ needs.verifier.outputs.artifact_name }}",
            "EXPECTED_PRODUCER_RUN_ATTEMPT": "${{ needs.verifier.outputs.producer_run_attempt }}",
            "EXPECTED_RELEASE_COMMIT": "${{ github.sha }}",
            "GITHUB_TOKEN": "${{ github.token }}",
            "RECEIPT": identity["env"]["RECEIPT"],
        }
        identity_command = _run(identity)
        for fragment in (
            "/actions/artifacts/{artifact_id}",
            '"expired": False',
            '"run_id": int(os.environ["GITHUB_RUN_ID"])',
            '"head_sha": os.environ["EXPECTED_RELEASE_COMMIT"]',
            "EXPECTED_PRODUCER_RUN_ATTEMPT",
        ):
            assert fragment in identity_command

        download = steps[download_index]
        assert download["with"] == {
            "artifact-ids": "${{ needs.verifier.outputs.artifact_id }}",
            "digest-mismatch": "error",
            "path": "${{ runner.temp }}/release-verifier",
        }
        readback = steps[readback_index]
        assert readback["env"] == {
            "EXPECTED_MANIFEST_SHA256": "${{ needs.verifier.outputs.manifest_sha256 }}",
            "VERIFIER_DIR": "${{ runner.temp }}/release-verifier",
        }
        readback_command = _run(readback)
        for fragment in (
            "dpone.pypi_verifier_closure.v1",
            "object_pairs_hook",
            "contains a duplicate key",
            "hashlib.sha256(raw).hexdigest()",
            "closed release verifier tree is not exact",
        ):
            assert fragment in readback_command

        first_execution = next(
            index
            for index, step in enumerate(steps)
            if index > readback_index and '"${VERIFIER_DIR}/' in str(step.get("run", ""))
        )
        assert readback_index < first_execution
        intervening = steps[readback_index + 1 : first_execution]
        assert all(
            not str(step.get("uses", "")).startswith(
                ("actions/checkout@", "astral-sh/setup-uv@", "actions/setup-python@")
            )
            and "pip install" not in str(step.get("run", ""))
            and "uv sync" not in str(step.get("run", ""))
            for step in intervening
        )

    assert "release-preflight/release-candidate-verifier" not in workflow_raw


def test_release_workflow_gates_exact_tag_commit_before_build() -> None:
    workflow = _workflow("release.yml")
    preflight = _job(workflow, "preflight")
    build = _job(workflow, "build")

    checkout = _step(preflight, "Checkout complete release history")
    assert checkout["with"]["fetch-depth"] == 0
    assert preflight["permissions"] == {
        "actions": "read",
        "checks": "read",
        "contents": "read",
        "statuses": "read",
    }
    preflight_deps = _run(_step(preflight, "Install preflight dependencies"))
    assert 'uv pip install --system "pyyaml==6.0.3" "jsonschema==4.26.0"' in preflight_deps
    identity = _run(_step(preflight, "Validate tag, commit, package versions, and changelog"))
    source_hygiene = _run(_step(preflight, "Certify frozen source for tenant hygiene"))
    exact_checks = _run(_step(preflight, "Verify live required checks for exact release commit"))
    assert "tools/agent_policy/release_identity_gate.py" in identity
    assert "tools/agent_policy/tenant_hygiene.py source" in source_hygiene
    assert "TENANT_HYGIENE_POLICY" in source_hygiene
    assert "tenant-hygiene-source.json" in source_hygiene
    assert "--remote-ref origin/master" in identity
    assert 'python -I -S "${VERIFIER_DIR}/release_commit_gate.py"' in exact_checks
    assert 'python -I -S "${VERIFIER_DIR}/release_merge_receipt_gate.py"' in exact_checks
    assert 'python -I -S "${VERIFIER_DIR}/release_ruleset_snapshot.py"' in exact_checks
    assert exact_checks.count("--sealed-authority") == 2
    assert "--policy" not in exact_checks
    assert "--repo-root" not in exact_checks
    assert '--commit-sha "${RELEASE_COMMIT}"' in exact_checks
    assert "--required-check-report test_artifacts/release-preflight/exact_commit_checks.json" in exact_checks
    assert "test_artifacts/release-preflight/exact_commit_merge_receipt.json" in exact_checks
    assert _needs(build) == {"preflight"}


def test_ci_builds_and_checks_the_formal_airflow_provider() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert "uv build packages/apache-airflow-providers-dpone --out-dir dist" in workflow
    assert "dpone docs check-airflow-public-contracts --format json" in workflow
    assert "dpone docs update-airflow-public-contract-reference --check" in workflow
    assert "airflow-v1-public-contract-report.json" in workflow


def test_runtime_image_context_delivers_only_the_closed_dbt_package() -> None:
    dockerignore = (ROOT / "docker/runtime/Dockerfile.dockerignore").read_text(encoding="utf-8").splitlines()

    assert "!packages/dbt-dpone/**" not in dockerignore
    for relative in (
        "dbt_project.yml",
        "macros/dpone_publish.sql",
        "macros/semantic_refresh_restore.sql",
        "macros/semantic_refresh_scope_merge.sql",
    ):
        assert f"!packages/dbt-dpone/{relative}" in dockerignore


def test_release_workflow_builds_and_verifies_complete_candidate_set() -> None:
    workflow = _workflow("release.yml")
    build = _job(workflow, "build")
    verify = _job(workflow, "verify-public-bytes")
    github_release = _job(workflow, "github-release")

    build_command = _run(_step(build, "Build all release distributions"))
    for command in (
        "uv build",
        "uv build packages/dpone-native-accel --out-dir dist",
        "uv build packages/dpone-airflow-pack --out-dir dist",
        "uv build packages/apache-airflow-providers-dpone --out-dir dist",
        "rm -f dist/.gitignore",
    ):
        assert command in build_command
    inventory_step = _step(build, "Validate exact candidate inventory")
    inventory = _run(inventory_step)
    assert "tools/pypi_release_smoke_dist.py" in inventory
    assert "--inventory-only" in inventory
    assert "--expected-version" in inventory
    assert "candidate-inventory.json" in inventory
    archive_gate = _run(_step(build, "Reject repository-internal archive members"))
    assert "tools/agent_policy/package_archive_gate.py" in archive_gate
    assert "dist/*.whl dist/*.tar.gz" in archive_gate
    archive_hygiene = _run(_step(build, "Certify built archives for tenant hygiene"))
    assert "tools/agent_policy/tenant_hygiene.py archive" in archive_hygiene
    assert "TENANT_HYGIENE_POLICY" in archive_hygiene
    assert "tenant-hygiene-archives.json" in archive_hygiene
    local_smoke = _run(_step(build, "Smoke exact local candidate wheels"))
    assert "pip install dist/*.whl" in local_smoke
    assert '"dpone[full,accel]==${RELEASE_VERSION}"' in local_smoke
    assert "pip check" in local_smoke
    assert "tools/package_smoke.py" in local_smoke
    for package in (
        "dpone",
        "dpone-native-accel",
        "dpone-airflow-pack",
        "apache-airflow-providers-dpone",
    ):
        assert package in local_smoke
    step_names = [step.get("name") for step in build["steps"] if isinstance(step, dict)]
    assert step_names.index("Validate exact candidate inventory") < step_names.index(
        "Reject repository-internal archive members"
    )
    assert local_smoke.index('"dpone[full,accel]==${RELEASE_VERSION}"') < local_smoke.index("pip check")
    verification = _run(_step(verify, "Verify exact PyPI wheel and sdist identities"))
    assert "tools/pypi_release_smoke_dist.py" in verification
    assert "--dist-dir dist" in verification
    assert '--expected-version "${RELEASE_VERSION}"' in verification
    assert "--install-smoke" in verification
    assert "--dpone-install-extra accel" in verification
    assert _needs(verify) == {"preflight", "build", "attest", "publish"}
    assert _needs(github_release) == {
        "verifier",
        "preflight",
        "build",
        "attest",
        "publish",
        "verify-public-bytes",
    }
    build_step_names = [step.get("name") for step in build["steps"] if isinstance(step, dict)]
    assert build_step_names.index("Reject repository-internal archive members") < build_step_names.index(
        "Upload immutable release candidates"
    )
    assert build_step_names.index("Smoke exact local candidate wheels") < build_step_names.index(
        "Upload immutable release candidates"
    )
    github_authority = _run(_step(github_release, "Prove fresh live authority immediately before GitHub Release"))
    assert "git/ref/tags/{os.environ['RELEASE_TAG']}" in github_authority
    assert "git/tags/{ref_object['sha']}" in github_authority
    assert all(
        not str(step.get("uses", "")).startswith("actions/checkout@")
        for step in github_release["steps"]
        if isinstance(step, dict)
    )


def test_release_workflow_keeps_build_and_controller_handoff_privileges_separate() -> None:
    workflow = _workflow("release.yml")
    build = _job(workflow, "build")
    publish = _job(workflow, "publish")
    raw = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")

    assert build["permissions"] == {"contents": "read"}
    assert "environment" not in publish
    assert publish["permissions"] == {
        "actions": "read",
        "checks": "read",
        "contents": "read",
        "statuses": "read",
    }
    assert publish.get("env") is None
    assert all(
        not str(step.get("uses", "")).startswith("actions/checkout@")
        for step in publish["steps"]
        if isinstance(step, dict)
    )
    assert "PYPI_API_TOKEN" not in raw
    assert "steps.publisher.outputs.mode" not in raw
    publish_contract = yaml.safe_dump(publish, sort_keys=True)
    assert "secrets." not in publish_contract
    assert "twine upload" not in publish_contract
    assert "pypa/gh-action-pypi-publish" not in publish_contract
    fresh_authority = _run(_step(publish, "Prove fresh release authority before controller handoff"))
    assert "git/ref/tags/{os.environ['RELEASE_TAG']}" in fresh_authority
    assert "git/tags/{ref_object['sha']}" in fresh_authority
    assert "annotated release tag" in fresh_authority
    staged_sources = {item.source for item in ORDINARY_RELEASE_FILES}
    for module in (
        "pypi_core_metadata.py",
        "pypi_prepublication_codec.py",
        "pypi_prepublication_contract.py",
        "pypi_prepublication_gate.py",
        "pypi_prepublication_pypi.py",
        "pypi_prepublication_source.py",
    ):
        assert f"tools/{module}" in staged_sources
    step_names = [step.get("name") for step in publish["steps"] if isinstance(step, dict)]
    provider_gate = "Prove fresh release authority before controller handoff"
    public_gate = "Prove safe public PyPI prepublication state"
    assert step_names.index(public_gate) + 1 == step_names.index(provider_gate)
    assert step_names.index(provider_gate) + 1 == step_names.index("Confirm controller is the sole PyPI publisher")
    public_gate_step = _step(publish, public_gate)
    assert public_gate_step["env"] == {
        "RELEASE_COMMIT": "${{ github.sha }}",
        "RELEASE_TAG": "${{ github.ref_name }}",
        "RELEASE_VERSION": "${{ needs.preflight.outputs.version }}",
        "VERIFIER_DIR": "${{ runner.temp }}/release-verifier",
    }
    public_gate_command = _run(public_gate_step)
    for fragment in (
        'python "${VERIFIER_DIR}/pypi_prepublication_gate.py"',
        "--candidate-inventory test_artifacts/release-candidates/candidate-inventory.json",
        "--dist-dir dist",
        '--expected-version "${RELEASE_VERSION}"',
        '--repository "${GITHUB_REPOSITORY}"',
        '--commit-sha "${RELEASE_COMMIT}"',
        '--release "${RELEASE_TAG}"',
        '--workflow-path ".github/workflows/release.yml"',
        '--workflow-run-id "${GITHUB_RUN_ID}"',
        '--workflow-run-attempt "${GITHUB_RUN_ATTEMPT}"',
        "--output test_artifacts/release-pre-publication/pypi_prepublication_gate.json",
    ):
        assert fragment in public_gate_command
    handoff = _step(publish, "Confirm controller is the sole PyPI publisher")
    assert "dpone-release-controller/.github/workflows/pypi-release.yml" in _run(handoff)


def test_release_workflow_attests_before_publish_and_retains_evidence() -> None:
    workflow = _workflow("release.yml")
    local_evidence = _job(workflow, "build-supply-chain-evidence")
    attest = _job(workflow, "attest")
    publish = _job(workflow, "publish")

    assert _needs(local_evidence) == {"preflight", "build"}
    assert _needs(attest) == {"verifier", "preflight", "build", "build-supply-chain-evidence"}
    assert _needs(publish) == {"verifier", "preflight", "build", "attest"}
    assert local_evidence["permissions"] == {"contents": "read"}
    install_candidate = _run(_step(local_evidence, "Install exact candidate dpone wheel"))
    assert "--find-links dist" in install_candidate
    assert "dist/dpone-*.whl" in install_candidate
    local_attest = _run(_step(local_evidence, "Build dpone supply-chain evidence"))
    assert "--project-root" in local_attest
    assert "dist/dpone-*.tar.gz" in local_attest
    assert "actions/checkout@" not in local_attest
    assert attest["permissions"] == {
        "actions": "read",
        "attestations": "write",
        "checks": "read",
        "contents": "read",
        "id-token": "write",
        "statuses": "read",
    }
    assert all(
        not str(step.get("uses", "")).startswith("actions/checkout@")
        for job in (local_evidence, attest)
        for step in job["steps"]
        if isinstance(step, dict)
    )
    attest_action = _step(attest, "Attest release artifacts")
    assert re.fullmatch(r"actions/attest@[0-9a-f]{40}", attest_action["uses"])
    assert "dist/*.whl" in attest_action["with"]["subject-path"]
    assert "dist/*.tar.gz" in attest_action["with"]["subject-path"]
    verify_command = _run(_step(attest, "Verify release artifact attestations"))
    assert "gh attestation verify" in verify_command
    assert '--source-digest "${RELEASE_COMMIT}"' in verify_command

    attempt_suffix = "${{ github.run_id }}-${{ github.run_attempt }}"
    expected = {
        (
            "verifier",
            "Upload immutable closed release verifier",
            f"release-verifier-{attempt_suffix}",
        ),
        ("preflight", "Upload release preflight evidence", f"release-preflight-{attempt_suffix}"),
        ("build", "Upload immutable release candidates", "release-candidates"),
        (
            "build-supply-chain-evidence",
            "Upload local supply-chain evidence",
            "release-supply-chain-local",
        ),
        (
            "attest",
            "Upload release attestation evidence",
            f"release-attestation-evidence-{attempt_suffix}",
        ),
        ("publish", "Upload pre-publication evidence", f"release-pre-publication-{attempt_suffix}"),
        (
            "verify-public-bytes",
            "Upload PyPI resolver smoke evidence",
            f"pypi-resolver-smoke-{attempt_suffix}",
        ),
        (
            "github-release",
            "Upload GitHub Release publication evidence",
            f"release-github-publication-evidence-{attempt_suffix}",
        ),
    }
    for job_name, step_name, artifact_name in expected:
        upload = _step(_job(workflow, job_name), step_name)
        if job_name in {"verifier", "build", "build-supply-chain-evidence"}:
            assert "if" not in upload
        else:
            assert upload["if"] == "always()"
        assert upload["with"]["name"] == artifact_name
        assert upload["with"]["if-no-files-found"] == "error"
        assert upload["with"]["retention-days"] == 90
        assert upload["with"]["overwrite"] is False


def test_release_mutations_finish_with_exact_bytes_then_fresh_authority() -> None:
    workflow = _workflow("release.yml")

    for job_name, mutation_name in (
        ("attest", "Attest release artifacts"),
        ("github-release", "Create GitHub Release"),
    ):
        _assert_final_bytes_authority_mutation(
            _job(workflow, job_name),
            mutation_name=mutation_name,
        )


def test_release_workflow_uses_locked_release_tools() -> None:
    workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")

    assert "uv run twine check dist/*" in workflow
    assert "uv tool run twine" not in workflow


def test_runtime_dockerfile_includes_native_transfer_toolchain() -> None:
    dockerfile = (ROOT / "docker/runtime/Dockerfile").read_text(encoding="utf-8")
    required_fragments = [
        "python:3.12-slim-bookworm",
        "AS runtime-base",
        "AS airflow-runtime",
        "msodbcsql18",
        "mssql-tools18",
        "clickhouse-client",
        "      git \\",
        "DPONE_PACKAGE_SPEC",
        "DPONE_ACCEL_PACKAGE_SPEC",
        "AIRFLOW_PACKAGE_SPEC",
        "AIRFLOW_PROVIDER_PACKAGE_SPEC",
        "dpone --version",
        "bcp -v",
        "sqlcmd -?",
        "clickhouse-client --version",
        'GH_CLI_VERSION="2.93.0"',
        "GH_CLI_AMD64_SHA256=",
        "GH_CLI_ARM64_SHA256=",
        'COSIGN_VERSION="3.0.4"',
        "COSIGN_AMD64_SHA256=",
        "COSIGN_ARM64_SHA256=",
        "cosign-linux-${architecture}",
        "10dab2fd2170b5aa0d5c0673a9a2793304960220b314f6a873bf39c2f08287aa",
        "c12fc6150195758ec0b1aeb1aade3381a1d3a299584982b66543f22bab04535b",
        "sha256sum --check --strict",
        "gh version",
        "cosign version --json",
        "DPONE_IMAGE_VERSION",
        "org.opencontainers.image.version",
        "org.opencontainers.image.revision",
    ]
    for fragment in required_fragments:
        assert fragment in dockerfile
    assert dockerfile.index("      git \\") < dockerfile.index('&& dpkg -i "${gh_deb}"')


def test_runtime_dockerfile_installs_release_from_fresh_pypi_resolver() -> None:
    dockerfile = (ROOT / "docker/runtime/Dockerfile").read_text(encoding="utf-8")
    required_fragments = [
        "--no-cache-dir",
        "--index-url https://pypi.org/simple",
        "--retries 10",
        "--timeout 60",
        "/opt/dpone-src",
        '"${DPONE_PACKAGE_SPEC}"',
    ]
    for fragment in required_fragments:
        assert fragment in dockerfile


def test_runtime_docker_build_context_is_deny_by_default() -> None:
    dockerignore = (ROOT / "docker/runtime/Dockerfile.dockerignore").read_text(encoding="utf-8").splitlines()

    assert dockerignore[0] == "**"
    for required_allowlist_entry in (
        "!docker/runtime/Dockerfile.dockerignore",
        "!docker/runtime/Dockerfile",
        "!pyproject.toml",
        "!src/dpone/**",
        "!packages/dpone-native-accel/src/**",
        "!packages/dpone-airflow-pack/src/**",
        "!packages/apache-airflow-providers-dpone/src/**",
    ):
        assert required_allowlist_entry in dockerignore
    for required_secret_exclusion in (
        "**/.env",
        "**/.env.*",
        "**/AGENTS.md",
        "**/.agents/**",
        "**/.codex/**",
        "**/.git/**",
        "**/.gitignore",
        "**/__pycache__/",
        "**/test_artifacts/**",
    ):
        assert required_secret_exclusion in dockerignore


def test_runtime_image_workflow_separates_preflight_certification_and_promotion_permissions() -> None:
    workflow = _workflow("runtime-image.yml")
    trigger = workflow.get("on", workflow.get(True))
    assert isinstance(trigger, dict)
    pull_request = trigger["pull_request"]
    assert isinstance(pull_request, dict)
    watched_paths = set(pull_request["paths"])
    assert {
        "tools/agent_policy/runtime_image_certification.py",
        "tools/agent_policy/runtime_image_promotion.py",
        "tools/agent_policy/runtime_image_registry.py",
        "tools/pypi_candidate_inventory.py",
        "tests/agent_policy/test_runtime_image_*.py",
        "docs/schemas/release/runtime-image-*.schema.json",
    } <= watched_paths
    development = _job(workflow, "development-build")
    preflight = _job(workflow, "release-preflight")
    build = _job(workflow, "build-runtime-candidate")
    push = _job(workflow, "push-attest-runtime-candidate")
    certify = _job(workflow, "certify-runtime-candidate")
    promote = _job(workflow, "promote-certified-image")
    collect = _job(workflow, "collect-runtime-image-evidence")

    assert development["if"] == "${{ github.event_name != 'push' }}"
    assert development["permissions"] == {"contents": "read"}
    assert preflight["if"] == "${{ github.event_name == 'push' && github.ref_type == 'tag' }}"
    assert preflight["permissions"] == {
        "actions": "read",
        "checks": "read",
        "contents": "read",
        "statuses": "read",
    }
    assert _needs(build) == {"release-preflight"}
    assert build["permissions"] == {"contents": "read"}
    assert _needs(push) == {"release-preflight", "build-runtime-candidate"}
    assert push["permissions"] == {
        "actions": "read",
        "attestations": "write",
        "checks": "read",
        "contents": "read",
        "id-token": "write",
        "packages": "write",
    }
    assert not any(
        isinstance(step, dict) and str(step.get("uses", "")).startswith("actions/checkout@")
        for step in push.get("steps", [])
    )
    assert _needs(certify) == {"release-preflight", "push-attest-runtime-candidate"}
    assert certify["permissions"] == {"contents": "read"}
    assert _needs(promote) == {
        "release-preflight",
        "certify-runtime-candidate",
        "build-runtime-candidate",
    }
    assert promote["permissions"] == {
        "actions": "read",
        "checks": "read",
        "contents": "read",
        "packages": "write",
    }
    assert not any(
        isinstance(step, dict) and str(step.get("uses", "")).startswith("actions/checkout@")
        for step in promote.get("steps", [])
    )
    assert promote["concurrency"] == {
        "group": "runtime-image-ghcr-promotion",
        "cancel-in-progress": False,
    }
    assert _needs(collect) == {
        "release-preflight",
        "build-runtime-candidate",
        "push-attest-runtime-candidate",
        "certify-runtime-candidate",
        "promote-certified-image",
    }
    assert collect["if"] == ("${{ always() && github.event_name == 'push' && github.ref_type == 'tag' }}")
    assert collect["permissions"] == {"actions": "read", "contents": "read"}
    all_run_scripts = "\n".join(
        str(step.get("run", ""))
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        if isinstance(step, dict)
    )
    assert "${{ inputs.package_spec }}" not in all_run_scripts
    assert "${{ inputs.version }}" not in all_run_scripts
    assert "${{ inputs.push_image }}" not in all_run_scripts
    selector = _run(_step(development, "Resolve safe non-publishing inputs"))
    assert "Manual runtime-image publication is forbidden" in selector


def test_runtime_image_candidate_is_digest_only_and_fully_certified_before_promotion() -> None:
    workflow = _workflow("runtime-image.yml")
    preflight = _job(workflow, "release-preflight")
    build = _job(workflow, "build-runtime-candidate")
    push = _job(workflow, "push-attest-runtime-candidate")
    certify = _job(workflow, "certify-runtime-candidate")
    promote = _job(workflow, "promote-certified-image")

    identity = _run(_step(preflight, "Validate runtime release identity"))
    identity_environment = _step(preflight, "Validate runtime release identity")["env"]
    assert "uv run python tools/agent_policy/release_identity_gate.py" in identity
    assert "uv run python tools/agent_policy/release_commit_gate.py" in identity
    assert "uv run python tools/agent_policy/release_merge_receipt_gate.py" in identity
    assert "--required-check-report test_artifacts/runtime-image/exact-commit-checks.json" in identity
    assert "\npython tools/agent_policy/release_identity_gate.py" not in identity
    assert "\npython tools/agent_policy/release_commit_gate.py" not in identity
    assert "\npython tools/agent_policy/release_merge_receipt_gate.py" not in identity
    assert identity_environment["GITHUB_TOKEN"] == "${{ github.token }}"

    candidate = _run(_step(build, "Build exact runtime candidate distributions"))
    assert "uv build packages/apache-airflow-providers-dpone --out-dir dist" in candidate
    public_identity = _run(_step(build, "Verify exact public wheel and sdist identities"))
    assert "pypi_release_smoke_dist.py" in public_identity
    assert "--expected-version" in public_identity

    crane_install = _step(push, "Install crane")
    crane_install_run = _run(crane_install)
    assert crane_install["env"]["CRANE_VERSION"] == "0.20.3"
    assert crane_install["env"]["CRANE_SHA256"] == ("36c67a932f489b3f2724b64af90b599a8ef2aa7b004872597373c0ad694dc059")
    assert "sha256sum --check -" in crane_install_run
    assert '--output "${archive}"' in crane_install_run
    assert "| tar " not in crane_install_run

    local_build = _run(_step(build, "Build local runtime candidate image and OCI layout"))
    assert "docker buildx build" in local_build
    assert "type=oci,dest=${RUNTIME_EVIDENCE_DIR}/oci-layout,name=${IMAGE_NAME},tar=false" in local_build
    assert 'mkdir -p "${RUNTIME_EVIDENCE_DIR}/checks" "${RUNTIME_EVIDENCE_DIR}/oci-layout"' not in _run(
        _step(build, "Create runtime evidence directory")
    )
    assert "push-by-digest=true" not in local_build
    assert "push=true" not in local_build
    for forbidden_alias in ('"${IMAGE_NAME}:sha-', '"${IMAGE_NAME}:latest"'):
        assert forbidden_alias not in local_build

    pinned_smoke = _run(_step(build, "Smoke runtime image before privileged push"))
    assert 'image="${IMAGE_NAME}:${RUNTIME_VERSION}"' in pinned_smoke
    for command in (
        "--version",
        "-m pip check",
        "bcp",
        "sqlcmd",
        "clickhouse-client",
        "cosign",
        "runtime native-accel doctor --format json",
        "runtime-init-fetch --help",
        "runtime-pack-exec --help",
    ):
        assert command in pinned_smoke
    for check_id in (
        "installed-dpone-version",
        "dpone-version",
        "pip-check",
        "bcp",
        "sqlcmd",
        "clickhouse-client",
        "cosign",
        "native-accel-doctor",
        "airflow-runtime-init-fetch-help",
        "airflow-runtime-pack-exec-help",
    ):
        assert f"record_pass {check_id}" in pinned_smoke
    assert 'schema: "dpone.runtime-image-check.v1"' in pinned_smoke
    assert (
        _step(build, "Smoke runtime image before privileged push")["env"]["RUNTIME_EVIDENCE_DIR"]
        == "${{ runner.temp }}/dpone-runtime-image"
    )

    sbom = _step(build, "Generate runtime image SBOM")
    assert re.fullmatch(r"anchore/sbom-action@[0-9a-f]{40}", sbom["uses"])

    push_run = _run(_step(push, "Push digest-only runtime candidate"))
    assert "crane push" in push_run
    assert "--image-refs" in push_run
    assert "image-refs.txt" in push_run
    assert "@(sha256:[0-9a-f]{64})" in push_run
    assert 'digest="$(crane push' not in push_run
    assert 'subject="${IMAGE_NAME}@${digest}"' in push_run
    assert 'docker pull "${subject}"' in push_run
    assert 'docker run --rm --entrypoint cosign "${subject}" version --json' in push_run
    assert '"${RUNTIME_EVIDENCE_DIR}/checks/cosign.json"' in push_run
    for name in ("Attest runtime image provenance", "Attest runtime image SBOM"):
        action = _step(push, name)
        assert re.fullmatch(r"actions/attest@[0-9a-f]{40}", action["uses"])
        assert action["with"]["subject-digest"] == "${{ steps.candidate.outputs.digest }}"

    push_steps = [step.get("name") for step in push["steps"] if isinstance(step, dict)]
    verify_index = push_steps.index("Verify runtime image attestations")
    assert push_steps.index("Push digest-only runtime candidate") < verify_index
    assert push_steps.index("Attest runtime image provenance") < verify_index
    assert push_steps.index("Attest runtime image SBOM") < verify_index
    verification = _run(_step(push, "Verify runtime image attestations"))
    assert "gh attestation verify" in verification
    assert "oci://${IMAGE_NAME}@${IMAGE_DIGEST}" in verification
    assert '--signer-workflow "${GITHUB_REPOSITORY}/.github/workflows/runtime-image.yml"' in verification
    assert verification.count("--signer-workflow") == 2

    certify_steps = [step.get("name") for step in certify["steps"] if isinstance(step, dict)]
    receipt_index = certify_steps.index("Derive PASS runtime image certification from verified receipts")
    upload_index = certify_steps.index("Upload immutable runtime image certification")
    assert receipt_index < upload_index
    receipt = _run(_step(certify, "Derive PASS runtime image certification from verified receipts"))
    assert "uv run python tools/agent_policy/runtime_image_promotion.py certify" in receipt
    assert "\npython tools/agent_policy/runtime_image_promotion.py certify" not in receipt
    assert "--checks-root" in receipt
    assert "--provenance-verification" in receipt
    assert "--sbom-verification" in receipt
    assert "--source-repository" in receipt
    assert "--signer-workflow" in receipt
    assert "write-certification" not in receipt
    assert "runtime-image-certification.raw.json" not in receipt

    certification_upload = _step(certify, "Upload immutable runtime image certification")
    assert certification_upload["id"] == "certification-upload"
    assert certification_upload["with"]["name"] == "${{ steps.certification.outputs.artifact_name }}"
    assert certification_upload["with"]["path"] == "${{ runner.temp }}/dpone-runtime-image/"
    assert certification_upload["with"].get("overwrite") is not True
    assert certify["outputs"] == {
        "certification_artifact_digest": "${{ steps.certification-upload.outputs.artifact-digest }}",
        "certification_artifact_id": "${{ steps.certification-upload.outputs.artifact-id }}",
        "certification_artifact_name": "${{ steps.certification.outputs.artifact_name }}",
        "certification_run_attempt": "${{ steps.certification.outputs.run_attempt }}",
        "digest": "${{ needs.push-attest-runtime-candidate.outputs.digest }}",
    }

    artifact_identity = _step(promote, "Verify immutable runtime image certification artifact identity")
    artifact_identity_run = _run(artifact_identity)
    assert "actions/artifacts/${CERTIFICATION_ARTIFACT_ID}" in artifact_identity_run
    assert 'expected_name="runtime-image-certification-${GITHUB_RUN_ID}-${CERTIFICATION_RUN_ATTEMPT}"' in (
        artifact_identity_run
    )
    assert "CERTIFICATION_ARTIFACT_DIGEST" in artifact_identity_run
    assert "certification-artifact-identity.json" in artifact_identity_run
    certification_download = _step(promote, "Download immutable runtime image certification")
    assert certification_download["with"]["artifact-ids"] == (
        "${{ needs.certify-runtime-candidate.outputs.certification_artifact_id }}"
    )
    assert certification_download["with"]["digest-mismatch"] == "error"

    promotion_steps = [step.get("name") for step in promote["steps"] if isinstance(step, dict)]
    assert promotion_steps.index("Validate runtime image certification") < promotion_steps.index(
        "Reconcile certified runtime image aliases"
    )
    validation_run = _run(_step(promote, "Validate runtime image certification"))
    assert "--certification test_artifacts/runtime-image/runtime-image-certification.json" in validation_run
    assert "--source-commit" in validation_run
    assert "--source-ref" in validation_run
    assert "PYTHONPATH=" in validation_run
    promote_run = _run(_step(promote, "Reconcile certified runtime image aliases"))
    assert "runtime_image_promotion.py" in promote_run
    assert "--source-commit" in promote_run
    assert "--source-ref" in promote_run
    assert "imagetools create" not in "\n".join(
        _run(step) for step in push["steps"] if isinstance(step, dict) and isinstance(step.get("run"), str)
    )
    assert "--latest-alias" not in promote_run

    publication_upload = _step(promote, "Upload runtime image publication evidence")
    assert publication_upload["if"] == "always()"
    assert "${{ github.run_id }}" in publication_upload["with"]["name"]
    assert "${{ github.run_attempt }}" in publication_upload["with"]["name"]
    assert publication_upload["with"].get("overwrite") is not True


def test_runtime_image_is_linked_from_docs_nav_and_installation() -> None:
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    installation = (ROOT / "docs/getting-started/installation.md").read_text(encoding="utf-8")
    docs_index = (ROOT / "docs/index.md").read_text(encoding="utf-8")
    runtime_doc = (ROOT / "docs/runtime-image.md").read_text(encoding="utf-8")
    assert "runtime-image.md" in mkdocs
    assert "Runtime Docker image" in installation
    assert "Runtime Docker image" in docs_index
    assert "ghcr.io/paulkov/dpone-runtime" in runtime_doc


def test_docs_homepage_source_version_matches_project_version_without_claiming_publication() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    expected_version = pyproject["project"]["version"]
    docs_index = (ROOT / "docs/index.md").read_text(encoding="utf-8")

    match = re.search(r"Current source version v(?P<version>[0-9]+\.[0-9]+\.[0-9]+)", docs_index)

    assert match is not None
    assert match.group("version") == expected_version
    assert "Latest release v" not in docs_index


def test_first_airflow_dag_separates_published_pins_from_exact_candidate_wheels() -> None:
    first_dag = (ROOT / "docs/getting-started/first-airflow-dag.md").read_text(encoding="utf-8")

    assert "dpone==${DPONE_VERSION}" in first_dag
    assert "apache-airflow-providers-dpone==${DPONE_VERSION}" in first_dag
    assert "dist-airflow/SOURCE_COMMIT" in first_dag
    assert "sha256sum --check SHA256SUMS" in first_dag
    assert "dist-airflow/dpone-*.whl" in first_dag
    assert "dist-airflow/dpone_airflow_pack-*.whl" in first_dag
    assert "dist-airflow/apache_airflow_providers_dpone-*.whl" in first_dag
    assert "--upgrade apache-airflow-providers-dpone" not in first_dag
