from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.release_rc_finalizer import ReleaseRcFinalizerService


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _green_artifact(path: Path, *, summary: str) -> Path:
    return _write_json(path, {"passed": True, "summary": summary, "blockers": []})


def _merge_train(path: Path, *, broken: bool = False, failed_check: bool = False) -> Path:
    second_base = "codex/not-the-previous-head" if broken else "codex/route-bootstrap-doctor"
    check = {
        "name": "Build GitHub Pages documentation",
        "status": "COMPLETED",
        "conclusion": "FAILURE" if failed_check else "SUCCESS",
        "details_url": "https://github.com/PaulKov/dpone/actions/runs/1",
    }
    return _write_json(
        path,
        {
            "schema_version": "dpone.release_rc_merge_train.v1",
            "base_branch": "codex/route-certify-release-automation",
            "head_branch": "codex/route-conformance-lab",
            "pull_requests": [
                {
                    "number": 75,
                    "title": "Add generic route bootstrap doctor",
                    "base_ref": "codex/route-certify-release-automation",
                    "head_ref": "codex/route-bootstrap-doctor",
                    "state": "OPEN",
                    "merge_state": "CLEAN",
                    "is_draft": False,
                    "checks": [check],
                    "url": "https://github.com/PaulKov/dpone/pull/75",
                },
                {
                    "number": 76,
                    "title": "Add route conformance lab",
                    "base_ref": second_base,
                    "head_ref": "codex/route-conformance-lab",
                    "state": "OPEN",
                    "merge_state": "CLEAN",
                    "is_draft": False,
                    "checks": [
                        {
                            "name": "Deploy GitHub Pages documentation",
                            "status": "COMPLETED",
                            "conclusion": "SKIPPED",
                            "details_url": "https://github.com/PaulKov/dpone/actions/runs/2",
                        }
                    ],
                    "url": "https://github.com/PaulKov/dpone/pull/76",
                },
            ],
        },
    )


def _artifacts(tmp_path: Path) -> dict[str, Path]:
    return {
        "route_release_finalizer": _green_artifact(
            tmp_path / "evidence" / "route_release_finalizer.json",
            summary="route release finalizer ready",
        ),
        "release_evidence_pack": _green_artifact(
            tmp_path / "evidence" / "release_evidence_pack.json",
            summary="release evidence pack ready",
        ),
        "pre_release_checklist": _green_artifact(
            tmp_path / "evidence" / "pre_release_checklist.json",
            summary="pre-release checklist complete",
        ),
    }


def test_release_rc_finalizer_accepts_clean_merge_train_version_and_evidence(tmp_path: Path) -> None:
    report = ReleaseRcFinalizerService().finalize(
        output_dir=tmp_path / "rc-final",
        release="v0.10.0",
        previous_release="v0.9.0",
        package_version="0.10.0",
        merge_train_json=_merge_train(tmp_path / "merge_train.json"),
        artifacts=_artifacts(tmp_path),
        required_artifacts=("route_release_finalizer", "release_evidence_pack", "pre_release_checklist"),
    )
    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))

    assert report.passed is True
    assert report.level == "rc_ready"
    assert report.score == 100.0
    assert payload["schema_version"] == "dpone.release_rc_finalizer.v1"
    assert payload["release"] == "v0.10.0"
    assert payload["previous_release"] == "v0.9.0"
    assert payload["package_version"] == "0.10.0"
    assert payload["mode"] == "pre_merge"
    assert payload["merge_train"]["base_branch"] == "codex/route-certify-release-automation"
    assert payload["merge_train"]["head_branch"] == "codex/route-conformance-lab"
    assert [item["number"] for item in payload["merge_train"]["pull_requests"]] == [75, 76]
    assert {check["name"] for check in payload["checks"]} >= {
        "version_increment",
        "package_version",
        "merge_train_chain",
        "pull_request_checks",
        "release_artifacts",
    }
    assert Path(payload["markdown_path"]).exists()


def test_release_rc_finalizer_blocks_broken_chain_and_failed_checks(tmp_path: Path) -> None:
    report = ReleaseRcFinalizerService().finalize(
        output_dir=tmp_path / "rc-final",
        release="v0.10.0",
        previous_release="v0.9.0",
        package_version="0.10.0",
        merge_train_json=_merge_train(tmp_path / "merge_train.json", broken=True, failed_check=True),
        artifacts=_artifacts(tmp_path),
    )

    assert report.passed is False
    assert report.level == "blocked"
    assert "merge_train.chain_broken" in report.blockers
    assert "pull_request.75.check_failed:Build GitHub Pages documentation" in report.blockers


def test_release_rc_finalizer_blocks_version_mismatch_and_missing_evidence(tmp_path: Path) -> None:
    artifacts = _artifacts(tmp_path)
    artifacts.pop("release_evidence_pack")

    report = ReleaseRcFinalizerService().finalize(
        output_dir=tmp_path / "rc-final",
        release="v0.10.0",
        previous_release="v0.9.0",
        package_version="0.9.0",
        merge_train_json=_merge_train(tmp_path / "merge_train.json"),
        artifacts=artifacts,
        required_artifacts=("route_release_finalizer", "release_evidence_pack"),
    )

    assert report.passed is False
    assert "release.package_version_mismatch" in report.blockers
    assert "release_evidence_pack.missing" in report.blockers


def test_release_rc_finalizer_post_merge_requires_merged_prs(tmp_path: Path) -> None:
    report = ReleaseRcFinalizerService().finalize(
        output_dir=tmp_path / "rc-final",
        release="v0.10.0",
        previous_release="v0.9.0",
        package_version="0.10.0",
        merge_train_json=_merge_train(tmp_path / "merge_train.json"),
        artifacts=_artifacts(tmp_path),
        mode="post_merge",
    )

    assert report.passed is False
    assert "pull_request.75.not_merged" in report.blockers
