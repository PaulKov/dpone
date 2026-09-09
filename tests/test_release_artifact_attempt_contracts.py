from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
ATTEMPT_SUFFIX = "${{ github.run_id }}-${{ github.run_attempt }}"


def _workflow(name: str) -> dict[str, Any]:
    payload = yaml.safe_load((ROOT / ".github/workflows" / name).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _job(workflow: dict[str, Any], name: str) -> dict[str, Any]:
    job = workflow["jobs"][name]
    assert isinstance(job, dict)
    return job


def _step(job: dict[str, Any], name: str) -> dict[str, Any]:
    matches = [step for step in job.get("steps", []) if isinstance(step, dict) and step.get("name") == name]
    assert len(matches) == 1
    return matches[0]


def _assert_id_download(step: dict[str, Any], artifact_id: str) -> None:
    inputs = step.get("with")
    assert isinstance(inputs, dict)
    assert inputs.get("artifact-ids") == artifact_id
    assert inputs.get("digest-mismatch") == "error"
    assert "name" not in inputs


def test_release_rerun_artifact_names_preserve_immutable_rebuild_tripwires() -> None:
    workflow = _workflow("release.yml")
    expected = {
        ("verifier", "Upload immutable closed release verifier"): f"release-verifier-{ATTEMPT_SUFFIX}",
        ("preflight", "Upload release preflight evidence"): f"release-preflight-{ATTEMPT_SUFFIX}",
        ("build", "Upload immutable release candidates"): "release-candidates",
        ("build-supply-chain-evidence", "Upload local supply-chain evidence"): "release-supply-chain-local",
        (
            "attest",
            "Upload release attestation evidence",
        ): f"release-attestation-evidence-{ATTEMPT_SUFFIX}",
        ("publish", "Upload pre-publication evidence"): f"release-pre-publication-{ATTEMPT_SUFFIX}",
        (
            "verify-public-bytes",
            "Upload PyPI resolver smoke evidence",
        ): f"pypi-resolver-smoke-{ATTEMPT_SUFFIX}",
        (
            "github-release",
            "Upload GitHub Release publication evidence",
        ): f"release-github-publication-evidence-{ATTEMPT_SUFFIX}",
    }

    for (job_name, step_name), artifact_name in expected.items():
        inputs = _step(_job(workflow, job_name), step_name)["with"]
        assert inputs["name"] == artifact_name
        assert inputs["if-no-files-found"] == "error"
        assert inputs["retention-days"] == 90
        assert inputs["overwrite"] is False


def test_release_consumers_use_exact_upstream_artifact_ids_and_digests() -> None:
    workflow = _workflow("release.yml")
    expected = {
        ("preflight", "Download immutable closed release verifier"): ("${{ needs.verifier.outputs.artifact_id }}"),
        ("build-supply-chain-evidence", "Download immutable release candidates"): (
            "${{ needs.build.outputs.candidates_artifact_id }}"
        ),
        ("attest", "Download immutable closed release verifier"): ("${{ needs.verifier.outputs.artifact_id }}"),
        ("attest", "Download immutable release candidates"): ("${{ needs.build.outputs.candidates_artifact_id }}"),
        ("attest", "Download local supply-chain evidence"): (
            "${{ needs.build-supply-chain-evidence.outputs.supply_chain_artifact_id }}"
        ),
        ("attest", "Download immutable release preflight authority"): (
            "${{ needs.preflight.outputs.evidence_artifact_id }}"
        ),
        ("publish", "Download immutable closed release verifier"): ("${{ needs.verifier.outputs.artifact_id }}"),
        ("publish", "Download immutable release candidates"): ("${{ needs.build.outputs.candidates_artifact_id }}"),
        ("publish", "Download immutable release preflight authority"): (
            "${{ needs.preflight.outputs.evidence_artifact_id }}"
        ),
        ("verify-public-bytes", "Download immutable release candidates"): (
            "${{ needs.build.outputs.candidates_artifact_id }}"
        ),
        ("github-release", "Download immutable release candidates"): (
            "${{ needs.build.outputs.candidates_artifact_id }}"
        ),
        ("github-release", "Download release attestation evidence"): (
            "${{ needs.attest.outputs.attestation_artifact_id }}"
        ),
        ("github-release", "Download immutable closed release verifier"): ("${{ needs.verifier.outputs.artifact_id }}"),
        ("github-release", "Download immutable release preflight authority"): (
            "${{ needs.preflight.outputs.evidence_artifact_id }}"
        ),
    }

    for (job_name, step_name), artifact_id in expected.items():
        _assert_id_download(_step(_job(workflow, job_name), step_name), artifact_id)


def test_audit_policy_freezes_fixed_and_attempt_scoped_artifact_identities() -> None:
    policy = yaml.safe_load((ROOT / ".agents/policy/workflow-security.yml").read_text(encoding="utf-8"))
    assert isinstance(policy, dict)
    required = policy["required_audit_artifacts"]
    attempt_scoped = {
        f"{base}-{ATTEMPT_SUFFIX}"
        for base in {
            "release-verifier",
            "release-preflight",
            "release-attestation-evidence",
            "release-pre-publication",
            "release-github-publication-evidence",
            "pypi-resolver-smoke",
        }
    }
    fixed_rebuild_tripwires = {"release-candidates", "release-supply-chain-local"}

    assert set(required["release.yml"]) == fixed_rebuild_tripwires | attempt_scoped
    assert all(contract["retention_days"] == 90 for contract in required["release.yml"].values())
    assert f"runtime-image-evidence-{ATTEMPT_SUFFIX}" in required["runtime-image.yml"]


def test_runtime_uploads_are_attempt_scoped_or_content_bound_and_never_overwrite() -> None:
    workflow = _workflow("runtime-image.yml")
    uploads = [
        step
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        if isinstance(step, dict) and str(step.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    assert uploads
    assert all(step.get("with", {}).get("overwrite") is False for step in uploads)
    development = _step(_job(workflow, "development-build"), "Upload runtime image evidence")
    assert development["if"] == "always()"
    assert development["with"]["name"] == f"runtime-image-evidence-{ATTEMPT_SUFFIX}"


def test_runtime_release_jobs_chain_provider_artifact_ids_and_digests() -> None:
    workflow = _workflow("runtime-image.yml")
    expected_outputs = {
        "release-preflight": {
            "evidence_artifact_id": "${{ steps.preflight-upload.outputs.artifact-id }}",
            "evidence_artifact_digest": "${{ steps.preflight-upload.outputs.artifact-digest }}",
        },
        "build-runtime-candidate": {
            "oci_artifact_id": "${{ steps.oci-upload.outputs.artifact-id }}",
            "oci_artifact_digest": "${{ steps.oci-upload.outputs.artifact-digest }}",
            "build_artifact_id": "${{ steps.build-upload.outputs.artifact-id }}",
            "build_artifact_digest": "${{ steps.build-upload.outputs.artifact-digest }}",
            "tools_artifact_id": "${{ steps.tools-upload.outputs.artifact-id }}",
            "tools_artifact_digest": "${{ steps.tools-upload.outputs.artifact-digest }}",
        },
        "push-attest-runtime-candidate": {
            "evidence_artifact_id": "${{ steps.push-upload.outputs.artifact-id }}",
            "evidence_artifact_digest": "${{ steps.push-upload.outputs.artifact-digest }}",
        },
        "certify-runtime-candidate": {
            "certification_artifact_id": "${{ steps.certification-upload.outputs.artifact-id }}",
            "certification_artifact_digest": "${{ steps.certification-upload.outputs.artifact-digest }}",
        },
        "promote-certified-image": {
            "evidence_artifact_id": "${{ steps.publication-upload.outputs.artifact-id }}",
            "evidence_artifact_digest": "${{ steps.publication-upload.outputs.artifact-digest }}",
        },
    }

    for job_name, required_outputs in expected_outputs.items():
        outputs = _job(workflow, job_name)["outputs"]
        assert required_outputs.items() <= outputs.items()

    expected_downloads = {
        ("push-attest-runtime-candidate", "Download OCI layout candidate"): (
            "${{ needs.build-runtime-candidate.outputs.oci_artifact_id }}"
        ),
        ("push-attest-runtime-candidate", "Download runtime image build evidence"): (
            "${{ needs.build-runtime-candidate.outputs.build_artifact_id }}"
        ),
        ("push-attest-runtime-candidate", "Download closed release-candidate verifier bundle"): (
            "${{ needs.release-preflight.outputs.evidence_artifact_id }}"
        ),
        ("certify-runtime-candidate", "Download runtime image push and attestation evidence"): (
            "${{ needs.push-attest-runtime-candidate.outputs.evidence_artifact_id }}"
        ),
        ("promote-certified-image", "Download immutable runtime image certification"): (
            "${{ needs.certify-runtime-candidate.outputs.certification_artifact_id }}"
        ),
        ("promote-certified-image", "Download runtime image tool bundle"): (
            "${{ needs.build-runtime-candidate.outputs.tools_artifact_id }}"
        ),
        ("promote-certified-image", "Download closed release-candidate verifier bundle"): (
            "${{ needs.release-preflight.outputs.evidence_artifact_id }}"
        ),
    }
    for (job_name, step_name), artifact_id in expected_downloads.items():
        _assert_id_download(_step(_job(workflow, job_name), step_name), artifact_id)


def test_runtime_mutation_freshness_is_preceded_by_annotated_tag_recheck() -> None:
    workflow = _workflow("runtime-image.yml")
    cases = (
        (
            "push-attest-runtime-candidate",
            "Recheck annotated tag identity before GHCR publication",
            "Reverify release candidate immediately before GHCR publication",
        ),
        (
            "promote-certified-image",
            "Recheck annotated tag identity before GHCR alias promotion",
            "Reverify release candidate immediately before GHCR alias promotion",
        ),
    )

    for job_name, recheck_name, reverify_name in cases:
        steps = [step for step in _job(workflow, job_name)["steps"] if isinstance(step, dict)]
        reverify_index = next(index for index, step in enumerate(steps) if step.get("name") == reverify_name)
        assert reverify_index > 0
        recheck = steps[reverify_index - 1]
        assert recheck.get("name") == recheck_name
        assert recheck.get("if") is None
        assert recheck.get("continue-on-error") is None
        assert recheck["env"] == {
            "GH_TOKEN": "${{ github.token }}",
            "RELEASE_COMMIT": "${{ github.sha }}",
            "RELEASE_TAG": "${{ github.ref_name }}",
        }
        command = recheck["run"]
        for invariant in (
            "git/ref/tags/${RELEASE_TAG}",
            '"${object_type}" != "tag"',
            "git/tags/${object_sha}",
            '"${tag_object_type}" != "commit"',
            '"${peeled}" != "${RELEASE_COMMIT}"',
        ):
            assert invariant in command


def test_runtime_candidate_push_uses_single_descriptor_digest_destination() -> None:
    workflow = _workflow("runtime-image.yml")
    push = _step(_job(workflow, "push-attest-runtime-candidate"), "Push digest-only runtime candidate")
    command = push["run"]

    assert "(.manifests | length) == 1" in command
    assert "then .manifests[0].digest" in command
    assert 'error("OCI layout must contain exactly one image descriptor")' in command
    assert 'if [[ ! "${candidate_digest}" =~ ^sha256:[0-9a-f]{64}$ ]]' in command
    assert 'destination="${IMAGE_NAME}@${candidate_digest}"' in command
    assert command.count("crane push \\") == 1
    invocation = command.split("crane push \\", 1)[1].split('--image-refs "${refs_file}"', 1)[0]
    assert '"${destination}"' in invocation
    assert '"${IMAGE_NAME}"' not in invocation
    assert ":latest" not in invocation
    assert 'destination="${IMAGE_NAME}"' not in command
    assert 'destination="${IMAGE_NAME}:latest"' not in command
    assert 'if [[ "${digest}" != "${candidate_digest}" || "$(cat "${refs_file}")" != "${destination}" ]]' in command


def test_runtime_collector_fails_closed_on_phase_or_artifact_download_incompleteness() -> None:
    workflow = _workflow("runtime-image.yml")
    collect = _job(workflow, "collect-runtime-image-evidence")
    assert collect["if"] == ("${{ always() && github.event_name == 'push' && github.ref_type == 'tag' }}")
    assert set(collect["needs"]) == {
        "release-preflight",
        "build-runtime-candidate",
        "push-attest-runtime-candidate",
        "certify-runtime-candidate",
        "promote-certified-image",
    }
    expected_downloads = {
        "Download runtime image preflight evidence": "${{ needs.release-preflight.outputs.evidence_artifact_id }}",
        "Download runtime image build evidence": "${{ needs.build-runtime-candidate.outputs.build_artifact_id }}",
        "Download runtime image push evidence": "${{ needs.push-attest-runtime-candidate.outputs.evidence_artifact_id }}",
        "Download runtime image certification evidence": (
            "${{ needs.certify-runtime-candidate.outputs.certification_artifact_id }}"
        ),
        "Download runtime image publication evidence": (
            "${{ needs.promote-certified-image.outputs.evidence_artifact_id }}"
        ),
    }
    for step_name, artifact_id in expected_downloads.items():
        download = _step(collect, step_name)
        assert download["if"].startswith("${{ always() && ")
        assert "continue-on-error" not in download
        _assert_id_download(download, artifact_id)

    outcome = _step(collect, "Record truthful runtime image attempt outcome")
    assert outcome["if"] == "always()"
    outcome_env = outcome["env"]
    assert set(outcome_env) == {
        "BUILD_RESULT",
        "PUSH_RESULT",
        "CERTIFICATION_RESULT",
        "PREFLIGHT_RESULT",
        "PROMOTION_RESULT",
        "PREFLIGHT_DOWNLOAD",
        "BUILD_DOWNLOAD",
        "PUSH_DOWNLOAD",
        "CERTIFICATION_DOWNLOAD",
        "PROMOTION_DOWNLOAD",
    }
    command = outcome["run"]
    for variable in outcome_env:
        assert f'"${{{variable}}}"' in command
    assert command.count('== "success"') == len(outcome_env)

    final_upload = _step(collect, "Upload attempt-scoped runtime image evidence")
    assert final_upload["if"] == "always()"
    assert final_upload["with"]["name"] == f"runtime-image-evidence-{ATTEMPT_SUFFIX}"
    assert final_upload["with"]["overwrite"] is False
    terminal = _step(collect, "Fail incomplete runtime image attempt")
    assert terminal["if"] == "always()"
    assert terminal["run"] == "jq -e '.status == \"PASS\"' test_artifacts/runtime-image-final/attempt.json"
