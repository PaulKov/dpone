from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.release_rc_collector import ReleaseRcCollectorService


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _gh_pr(path: Path, *, number: int, base: str, head: str, conclusion: str = "SUCCESS") -> Path:
    return _write_json(
        path,
        {
            "number": number,
            "title": f"PR {number}",
            "baseRefName": base,
            "headRefName": head,
            "state": "OPEN",
            "mergeStateStatus": "CLEAN",
            "isDraft": False,
            "url": f"https://github.com/PaulKov/dpone/pull/{number}",
            "statusCheckRollup": [
                {
                    "__typename": "CheckRun",
                    "name": "Build GitHub Pages documentation",
                    "status": "COMPLETED",
                    "conclusion": conclusion,
                    "detailsUrl": f"https://github.com/PaulKov/dpone/actions/runs/{number}",
                }
            ],
        },
    )


def test_release_rc_collector_writes_merge_train_and_finalizer_inputs(tmp_path: Path) -> None:
    report = ReleaseRcCollectorService().collect(
        output_dir=tmp_path / "rc-collect",
        release="v0.10.0",
        previous_release="v0.9.0",
        package_version="0.10.0",
        base_branch="codex/route-certify-release-automation",
        head_branch="codex/route-conformance-lab",
        pull_request_json=(
            _gh_pr(
                tmp_path / "pr-75.json",
                number=75,
                base="codex/route-certify-release-automation",
                head="codex/route-bootstrap-doctor",
            ),
            _gh_pr(
                tmp_path / "pr-76.json",
                number=76,
                base="codex/route-bootstrap-doctor",
                head="codex/route-conformance-lab",
            ),
        ),
        artifacts={
            "route_release_finalizer": "test_artifacts/release/v0.10.0/route_release_finalizer.json",
            "release_evidence_pack": "test_artifacts/release/v0.10.0/release_evidence_pack.json",
        },
        required_artifacts=("route_release_finalizer", "release_evidence_pack"),
        finalizer_output_dir="test_artifacts/release/v0.10.0/release-rc-finalizer",
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    train = json.loads(Path(report.merge_train_path).read_text(encoding="utf-8"))
    inputs = json.loads(Path(report.inputs_path).read_text(encoding="utf-8"))

    assert report.passed is True
    assert payload["schema_version"] == "dpone.release_rc_collect.v1"
    assert payload["merge_train_path"].endswith("merge_train.json")
    assert train["schema_version"] == "dpone.release_rc_merge_train.v1"
    assert train["base_branch"] == "codex/route-certify-release-automation"
    assert train["head_branch"] == "codex/route-conformance-lab"
    assert [item["number"] for item in train["pull_requests"]] == [75, 76]
    assert train["pull_requests"][0]["checks"][0]["details_url"].endswith("/75")
    assert inputs["finalizer_command"][:3] == ["uv", "run", "dpone"]
    assert "release-rc-finalize" in inputs["finalizer_command"]
    assert "--merge-train-json" in inputs["finalizer_command"]
    assert Path(report.markdown_path).read_text(encoding="utf-8").startswith("# Release RC collector")


def test_release_rc_collector_blocks_missing_pull_request_inputs(tmp_path: Path) -> None:
    report = ReleaseRcCollectorService().collect(
        output_dir=tmp_path / "rc-collect",
        release="v0.10.0",
        previous_release="v0.9.0",
        package_version="0.10.0",
        base_branch="master",
        head_branch="codex/release",
        pull_request_json=(),
        artifacts={},
        required_artifacts=("route_release_finalizer",),
    )

    assert report.passed is False
    assert "release_rc_collect.pull_requests_missing" in report.blockers


def test_release_rc_collector_surfaces_chain_blockers(tmp_path: Path) -> None:
    report = ReleaseRcCollectorService().collect(
        output_dir=tmp_path / "rc-collect",
        release="v0.10.0",
        previous_release="v0.9.0",
        package_version="0.10.0",
        base_branch="codex/a",
        head_branch="codex/c",
        pull_request_json=(
            _gh_pr(tmp_path / "pr-1.json", number=1, base="codex/a", head="codex/b"),
            _gh_pr(tmp_path / "pr-2.json", number=2, base="codex/not-b", head="codex/c"),
        ),
        artifacts={},
        required_artifacts=("route_release_finalizer",),
    )

    assert report.passed is False
    assert "merge_train.chain_broken" in report.blockers
