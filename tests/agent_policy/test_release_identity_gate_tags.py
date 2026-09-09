from __future__ import annotations

from typing import Any

import pytest
from tests.agent_policy._release_identity_gate_helpers import (
    COMMIT_SHA,
    codes,
    evaluate,
    release_identity,
    successful_git,
)


def test_accepts_annotated_tag_exact_commit_and_synchronized_packages(
    tmp_path: Any,
    monkeypatch: Any,
) -> None:
    report = evaluate(tmp_path, monkeypatch)

    assert report.status == "PASS"
    assert report.version == "1.2.3"
    assert report.commit_sha == COMMIT_SHA
    assert report.remote_ref == "origin/master"
    assert report.protected_base_sha == "c" * 40
    assert report.policy_sha256
    assert report.package_versions == {
        "apache-airflow-providers-dpone": "1.2.3",
        "dpone": "1.2.3",
        "dpone-airflow-pack": "1.2.3",
        "dpone-native-accel": "1.2.3",
    }
    assert report.blockers == ()


def test_rejects_caller_selected_head_as_protected_base(tmp_path: Any, monkeypatch: Any) -> None:
    report = evaluate(tmp_path, monkeypatch, remote_ref="HEAD")
    assert report.status == "FAIL"
    assert "RELEASE_BASE_REF_POLICY_MISMATCH" in codes(report)


@pytest.mark.parametrize(
    ("tag", "code"),
    [
        ("1.2.3", "RELEASE_TAG_INVALID"),
        ("v1.2", "RELEASE_TAG_INVALID"),
        ("v1.2.3-rc.1", "RELEASE_TAG_INVALID"),
        ("v01.2.3", "RELEASE_TAG_INVALID"),
        ("v1.2.3/../../main", "RELEASE_TAG_INVALID"),
    ],
)
def test_rejects_noncanonical_release_tags(
    tmp_path: Any,
    monkeypatch: Any,
    tag: str,
    code: str,
) -> None:
    report = evaluate(tmp_path, monkeypatch, tag=tag)

    assert codes(report) == [code]


def test_rejects_lightweight_tag(tmp_path: Any, monkeypatch: Any) -> None:
    def lightweight(command: tuple[str, ...], cwd: Any) -> Any:
        result = successful_git(command, cwd)
        if command[1:3] == ("cat-file", "-t"):
            return release_identity.CommandResult(returncode=0, stdout="commit\n")
        return result

    report = evaluate(tmp_path, monkeypatch, runner=lightweight)

    assert "RELEASE_TAG_NOT_ANNOTATED" in codes(report)


def test_rejects_tag_that_does_not_point_to_workflow_commit(tmp_path: Any, monkeypatch: Any) -> None:
    def different_commit(command: tuple[str, ...], cwd: Any) -> Any:
        result = successful_git(command, cwd)
        if command[1:3] == ("rev-list", "-n"):
            return release_identity.CommandResult(returncode=0, stdout=f"{'b' * 40}\n")
        return result

    report = evaluate(tmp_path, monkeypatch, runner=different_commit)

    assert "RELEASE_TAG_COMMIT_MISMATCH" in codes(report)


def test_rejects_commit_outside_remote_master(tmp_path: Any, monkeypatch: Any) -> None:
    def divergent(command: tuple[str, ...], cwd: Any) -> Any:
        result = successful_git(command, cwd)
        if command[1:3] == ("merge-base", "--is-ancestor"):
            return release_identity.CommandResult(returncode=1, stdout="")
        return result

    report = evaluate(tmp_path, monkeypatch, runner=divergent)

    assert "RELEASE_COMMIT_NOT_ON_BASE" in codes(report)
