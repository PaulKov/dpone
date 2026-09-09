from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from tests.agent_policy._release_candidate_evidence_helpers import (
    ARTIFACT_ID,
    CHECK_RUN_ID,
    CHECK_SUITE_ID,
    COMMIT_SHA,
    JOB_ID,
    RELEASE,
    REPOSITORY,
    ROOT,
    RUN_ATTEMPT,
    RUN_ID,
    digest,
    load_module,
    policy,
    write_valid_sources,
    zip_bytes,
)
from tests.agent_policy._release_candidate_evidence_provider_helpers import (
    FakeReleaseEvidenceAdapter,
)

builder = load_module(
    "dpone_release_candidate_evidence_gate_builder_test",
    "tools/agent_policy/release_candidate_evidence_builder.py",
)
gate = load_module(
    "dpone_release_candidate_evidence_gate_test",
    "tools/agent_policy/release_candidate_evidence_gate.py",
)

CREATED_AT = "2026-08-13T09:59:00Z"
COMPLETED_AT = "2026-08-13T10:00:00Z"
PUBLICATION_CREATED_AT = "2026-08-13T10:01:00Z"
PUBLICATION_UPDATED_AT = "2026-08-13T10:01:30Z"
PUBLICATION_RUN_ID = 901
PUBLICATION_RUN_ATTEMPT = 3
PUBLICATION_WORKFLOW_PATH = ".github/workflows/release.yml"
PARTNER_PUBLICATION_RUN_ID = 902
PARTNER_PUBLICATION_WORKFLOW_PATH = ".github/workflows/runtime-image.yml"
DOWNLOAD_URL = f"https://api.github.test/repos/{REPOSITORY}/actions/artifacts/{ARTIFACT_ID}/zip"


def _authority_archive(tmp_path: Path, *, run_id: int = RUN_ID) -> bytes:
    inputs = tmp_path / f"inputs-{run_id}"
    output = tmp_path / f"bundle-{run_id}"
    write_valid_sources(inputs)
    builder.build_bundle(
        root=ROOT,
        input_root=inputs,
        output_dir=output,
        repository=REPOSITORY,
        commit_sha=COMMIT_SHA,
        release=RELEASE,
        run_id=run_id,
        run_attempt=RUN_ATTEMPT,
    )
    members = sorted(
        (
            path.relative_to(output).as_posix(),
            path.read_bytes(),
        )
        for path in output.rglob("*")
        if path.is_file()
    )
    return zip_bytes(members)


def _adapter(raw: bytes) -> FakeReleaseEvidenceAdapter:
    check = {
        "id": CHECK_RUN_ID,
        "name": policy.JOB_NAME,
        "head_sha": COMMIT_SHA,
        "status": "completed",
        "conclusion": "success",
        "completed_at": COMPLETED_AT,
        "app": {"id": policy.APP_ID, "slug": policy.APP_SLUG},
        "check_suite": {"id": CHECK_SUITE_ID},
    }
    run = {
        "id": RUN_ID,
        "name": f"{policy.WORKFLOW_NAME} {RELEASE} at {COMMIT_SHA}",
        "event": "workflow_dispatch",
        "head_sha": COMMIT_SHA,
        "head_branch": "master",
        "status": "completed",
        "conclusion": "success",
        "check_suite_id": CHECK_SUITE_ID,
        "run_attempt": RUN_ATTEMPT,
        "repository": {"full_name": REPOSITORY},
        "path": f"{policy.WORKFLOW_PATH}@refs/heads/master",
        "created_at": CREATED_AT,
        "updated_at": COMPLETED_AT,
    }
    publication_run = {
        "id": PUBLICATION_RUN_ID,
        "event": "push",
        "head_sha": COMMIT_SHA,
        "head_branch": RELEASE,
        "status": "in_progress",
        "conclusion": None,
        "run_attempt": PUBLICATION_RUN_ATTEMPT,
        "repository": {"full_name": REPOSITORY},
        "path": f"{PUBLICATION_WORKFLOW_PATH}@refs/tags/{RELEASE}",
        "created_at": PUBLICATION_CREATED_AT,
        "updated_at": PUBLICATION_UPDATED_AT,
    }
    partner_publication_run = {
        **publication_run,
        "id": PARTNER_PUBLICATION_RUN_ID,
        "path": f"{PARTNER_PUBLICATION_WORKFLOW_PATH}@refs/tags/{RELEASE}",
    }
    job = {
        "id": JOB_ID,
        "name": policy.JOB_NAME,
        "head_sha": COMMIT_SHA,
        "run_id": RUN_ID,
        "run_attempt": RUN_ATTEMPT,
        "status": "completed",
        "conclusion": "success",
        "check_run_url": f"https://api.github.com/repos/{REPOSITORY}/check-runs/{CHECK_RUN_ID}",
    }
    artifact = {
        "id": ARTIFACT_ID,
        "name": policy.artifact_name(COMMIT_SHA, RUN_ID, RUN_ATTEMPT),
        "expired": False,
        "created_at": COMPLETED_AT,
        "workflow_run": {"id": RUN_ID, "head_sha": COMMIT_SHA},
        "digest": digest(raw),
        "size_in_bytes": len(raw),
        "archive_download_url": DOWNLOAD_URL,
    }
    return FakeReleaseEvidenceAdapter(
        check_runs=[check],
        workflow_runs=[dict(run)],
        publication_workflow_runs={
            PUBLICATION_WORKFLOW_PATH: [dict(publication_run)],
            PARTNER_PUBLICATION_WORKFLOW_PATH: [dict(partner_publication_run)],
        },
        runs={PUBLICATION_RUN_ID: publication_run, RUN_ID: run},
        jobs={(RUN_ID, RUN_ATTEMPT): [job]},
        artifacts={RUN_ID: [artifact]},
        downloads={DOWNLOAD_URL: raw},
    )


def _verify(adapter: FakeReleaseEvidenceAdapter) -> dict[str, Any]:
    return gate.verify_release_candidate(
        repository=REPOSITORY,
        commit_sha=COMMIT_SHA,
        release=RELEASE,
        publication_workflow_path=PUBLICATION_WORKFLOW_PATH,
        publication_run_id=PUBLICATION_RUN_ID,
        publication_run_attempt=PUBLICATION_RUN_ATTEMPT,
        adapter=adapter,
    )


def _cli_args(output: Path) -> list[str]:
    return [
        "--repository",
        REPOSITORY,
        "--commit-sha",
        COMMIT_SHA,
        "--release",
        RELEASE,
        "--publication-workflow-path",
        PUBLICATION_WORKFLOW_PATH,
        "--publication-run-id",
        str(PUBLICATION_RUN_ID),
        "--publication-run-attempt",
        str(PUBLICATION_RUN_ATTEMPT),
        "--output",
        str(output),
    ]


def test_gate_binds_provider_and_inner_authority_end_to_end(tmp_path: Path) -> None:
    raw = _authority_archive(tmp_path)

    result = _verify(_adapter(raw))

    assert result["status"] == "PASS"
    assert result["decision"] == "GO"
    assert result["repository"] == REPOSITORY
    assert result["commit_sha"] == COMMIT_SHA
    assert result["release"] == RELEASE
    assert result["publication_workflow_path"] == PUBLICATION_WORKFLOW_PATH
    assert result["publication_run_id"] == PUBLICATION_RUN_ID
    assert result["publication_run_attempt"] == PUBLICATION_RUN_ATTEMPT
    assert result["paired_publication_runs"] == [
        {
            "workflow_path": PUBLICATION_WORKFLOW_PATH,
            "run_id": PUBLICATION_RUN_ID,
            "run_attempt": PUBLICATION_RUN_ATTEMPT,
            "created_at": PUBLICATION_CREATED_AT,
        },
        {
            "workflow_path": PARTNER_PUBLICATION_WORKFLOW_PATH,
            "run_id": PARTNER_PUBLICATION_RUN_ID,
            "run_attempt": PUBLICATION_RUN_ATTEMPT,
            "created_at": PUBLICATION_CREATED_AT,
        },
    ]
    assert all(frozenset(run) == gate.contract.PAIRED_PUBLICATION_RUN_KEYS for run in result["paired_publication_runs"])
    assert result["publication_cutoff"] == PUBLICATION_CREATED_AT
    assert result["publication_pair_sha256"] == gate.codec.canonical_json_sha256(result["paired_publication_runs"])
    assert result["profile"] == policy.PROFILE
    assert result["check_run_id"] == CHECK_RUN_ID
    assert result["check_suite_id"] == CHECK_SUITE_ID
    assert result["workflow_run_id"] == RUN_ID
    assert result["workflow_run_attempt"] == RUN_ATTEMPT
    assert result["job_id"] == JOB_ID
    assert result["job_id"] != result["check_run_id"]
    assert result["artifact_id"] == ARTIFACT_ID
    assert result["artifact_digest"] == digest(raw)
    assert result["archive_sha256"] == digest(raw)
    assert result["source_count"] == len(policy.SOURCE_PATHS)
    assert result["blockers"] == []
    assert "archive_bytes" not in result


def test_gate_never_falls_back_when_latest_provider_workflow_run_is_red(tmp_path: Path) -> None:
    adapter = _adapter(_authority_archive(tmp_path))
    adapter.runs[RUN_ID]["conclusion"] = "failure"

    with pytest.raises(ValueError, match="latest.*run identity or result"):
        _verify(adapter)


@pytest.mark.parametrize(
    "run_name",
    [
        policy.WORKFLOW_NAME,
        f"{policy.WORKFLOW_NAME} v999.0.0 at {COMMIT_SHA}",
        f"{policy.WORKFLOW_NAME} {RELEASE} at {'f' * 40}",
    ],
)
def test_gate_rejects_provider_run_name_not_bound_to_release_and_commit(
    tmp_path: Path,
    run_name: str,
) -> None:
    adapter = _adapter(_authority_archive(tmp_path))
    adapter.runs[RUN_ID]["name"] = run_name

    with pytest.raises(ValueError, match="latest.*run identity or result"):
        _verify(adapter)


def test_provider_valid_bytes_with_wrong_inner_run_are_rejected(tmp_path: Path) -> None:
    raw = _authority_archive(tmp_path, run_id=RUN_ID + 1)

    with pytest.raises(ValueError, match="manifest run ID"):
        _verify(_adapter(raw))


def test_provider_digest_does_not_make_malformed_inner_archive_authoritative() -> None:
    raw = zip_bytes([("manual-pass.json", b'{"status":"PASS"}\n')])

    with pytest.raises(ValueError, match="file set is invalid"):
        _verify(_adapter(raw))


def test_cli_failure_is_nonzero_and_writes_a_fail_closed_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output = tmp_path / "verification.json"
    token = "never-serialize-this-token"
    monkeypatch.setenv("GITHUB_TOKEN", token)
    monkeypatch.setattr(
        gate.source,
        "GitHubReleaseCandidateEvidenceAdapter",
        lambda _token: FakeReleaseEvidenceAdapter(
            check_runs=[],
            publication_workflow_runs=(adapter := _adapter(b"unused")).publication_workflow_runs,
            runs={PUBLICATION_RUN_ID: adapter.runs[PUBLICATION_RUN_ID]},
        ),
    )

    exit_code = gate.main(_cli_args(output))

    payload = json.loads(output.read_bytes())
    captured = capsys.readouterr()
    assert exit_code == 1
    assert payload["status"] == "FAIL"
    assert payload["decision"] == "NO-GO"
    assert payload["blockers"][0]["code"] == "RELEASE_CANDIDATE_EVIDENCE_INVALID"
    assert token not in output.read_text(encoding="utf-8")
    assert token not in captured.out
    assert token not in captured.err


@pytest.mark.parametrize(
    "required_option",
    [
        "--publication-workflow-path",
        "--publication-run-id",
        "--publication-run-attempt",
    ],
)
def test_cli_requires_provider_publication_identity(
    tmp_path: Path,
    required_option: str,
) -> None:
    args = _cli_args(tmp_path / "verification.json")
    option_index = args.index(required_option)
    del args[option_index : option_index + 2]

    with pytest.raises(SystemExit) as exc_info:
        gate.main(args)

    assert exc_info.value.code == 2


def test_cli_rejects_unapproved_publication_workflow_path(tmp_path: Path) -> None:
    args = _cli_args(tmp_path / "verification.json")
    args[args.index(PUBLICATION_WORKFLOW_PATH)] = ".github/workflows/attacker.yml"

    with pytest.raises(SystemExit) as exc_info:
        gate.main(args)

    assert exc_info.value.code == 2


def test_cli_success_writes_exact_credential_free_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "verification.json"
    token = "never-serialize-this-token"
    adapter = _adapter(_authority_archive(tmp_path))
    monkeypatch.setenv("GITHUB_TOKEN", token)
    monkeypatch.setattr(
        gate.source,
        "GitHubReleaseCandidateEvidenceAdapter",
        lambda observed: adapter if observed == token else None,
    )

    exit_code = gate.main(_cli_args(output))

    payload = json.loads(output.read_bytes())
    assert exit_code == 0
    assert payload["status"] == "PASS"
    assert token not in output.read_text(encoding="utf-8")


def test_cli_does_not_overwrite_an_existing_verification_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "verification.json"
    output.write_text("operator-owned\n", encoding="utf-8")
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setattr(
        gate.source,
        "GitHubReleaseCandidateEvidenceAdapter",
        lambda _token: _adapter(_authority_archive(tmp_path)),
    )

    exit_code = gate.main(_cli_args(output))

    assert exit_code == 1
    assert output.read_text(encoding="utf-8") == "operator-owned\n"
