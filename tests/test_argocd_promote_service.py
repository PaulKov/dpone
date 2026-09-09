from __future__ import annotations

import json
import subprocess
from pathlib import Path

from dpone.services.ci.argocd_promote import (
    PromoteSnapshotSettings,
    build_promote_branch_name,
    load_snapshot_values,
    promote_snapshot_to_argocd,
)
from dpone.services.ci.gitlab_mr import GitLabMrClient
from dpone.services.ci.yaml_update import SNAPSHOT_ROOT_KEY, update_airflow_dev_snapshot_file


def _git(args: list[str], cwd: Path) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        check=True,
        text=True,
        capture_output=True,
    )
    return completed.stdout.strip()


def test_load_snapshot_values_prefers_file_then_env(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / "snapshot.env"
    env_file.write_text(
        "DPONE_PACKAGE_SPEC=dpone==1.2.3.dev45\nDPONE_SNAPSHOT_VERSION=1.2.3.dev45\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DPONE_SNAPSHOT_SHA", "abc1234")
    values = load_snapshot_values(env_file)
    assert values.package_spec == "dpone==1.2.3.dev45"
    assert values.snapshot_version == "1.2.3.dev45"
    assert values.snapshot_sha == "abc1234"


def test_build_promote_branch_name() -> None:
    assert (
        build_promote_branch_name("auto/dpone-airflow-dev", "dpone==1.4.3.dev8123")
        == "auto/dpone-airflow-dev/1.4.3.dev8123"
    )


def test_update_airflow_dev_snapshot_file_creates_expected_payload(tmp_path: Path) -> None:
    target = tmp_path / ".helm" / "overrides" / "airflow-dev-dpone-snapshot.yaml"
    result = update_airflow_dev_snapshot_file(target, "dpone==1.2.3.dev4")
    assert result.changed is True
    assert result.created is True
    payload = json.loads(json.dumps(result.payload))
    assert payload[SNAPSHOT_ROOT_KEY]["packageSpec"] == "dpone==1.2.3.dev4"
    assert target.exists()


def test_promote_snapshot_to_argocd_pushes_branch_and_updates_file(tmp_path: Path) -> None:
    remote = tmp_path / "argocd-remote.git"
    work = tmp_path / "seed"
    work.mkdir()
    _git(["init", "--bare", str(remote)], cwd=tmp_path)

    _git(["init"], cwd=work)
    _git(["config", "user.email", "test@example.com"], cwd=work)
    _git(["config", "user.name", "tester"], cwd=work)
    (work / "README.md").write_text("seed\n", encoding="utf-8")
    _git(["add", "README.md"], cwd=work)
    _git(["commit", "-m", "seed"], cwd=work)
    _git(["branch", "-M", "master"], cwd=work)
    _git(["remote", "add", "origin", str(remote)], cwd=work)
    _git(["push", "-u", "origin", "master"], cwd=work)

    settings = PromoteSnapshotSettings(
        package_spec="dpone==1.2.3.dev77",
        snapshot_version="1.2.3.dev77",
        snapshot_sha="abc1234",
        repo_url=str(remote),
        repo_dir=None,
        target_branch="master",
        values_file=".helm/overrides/airflow-dev-dpone-snapshot.yaml",
        branch_prefix="auto/dpone-airflow-dev",
        push_user=None,
        push_token=None,
        git_author_name="dpone-ci-bot",
        git_author_email="dpone-ci-bot@local",
        commit_message=None,
        mr_title=None,
        mr_description=None,
        mr_labels=("dpone",),
        project_id=None,
        api_url=None,
        api_token=None,
        open_mr=False,
        draft_mr=False,
        remove_source_branch=False,
        target_dir="/opt/airflow/.dpone-pkgs",
        install_mode="snapshot",
        enabled=True,
        dry_run=False,
    )
    result = promote_snapshot_to_argocd(settings)
    assert result.changed is True
    assert result.branch_name.endswith("1.2.3.dev77")

    inspect = tmp_path / "inspect"
    _git(["clone", "--branch", result.branch_name, str(remote), str(inspect)], cwd=tmp_path)
    payload_path = inspect / ".helm" / "overrides" / "airflow-dev-dpone-snapshot.yaml"
    assert payload_path.exists()
    assert "dpone==1.2.3.dev77" in payload_path.read_text(encoding="utf-8")


def test_promote_snapshot_to_argocd_dry_run(tmp_path: Path) -> None:
    repo_dir = tmp_path / "argocd-main"
    repo_dir.mkdir()
    settings = PromoteSnapshotSettings(
        package_spec="dpone==1.2.3.dev88",
        snapshot_version="1.2.3.dev88",
        snapshot_sha=None,
        repo_url=None,
        repo_dir=repo_dir,
        target_branch="master",
        values_file=".helm/overrides/airflow-dev-dpone-snapshot.yaml",
        branch_prefix="auto/dpone-airflow-dev",
        push_user=None,
        push_token=None,
        git_author_name="dpone-ci-bot",
        git_author_email="dpone-ci-bot@local",
        commit_message=None,
        mr_title=None,
        mr_description=None,
        mr_labels=(),
        project_id=None,
        api_url=None,
        api_token=None,
        open_mr=False,
        draft_mr=False,
        remove_source_branch=False,
        target_dir="/opt/airflow/.dpone-pkgs",
        install_mode="snapshot",
        enabled=True,
        dry_run=True,
    )
    result = promote_snapshot_to_argocd(settings)
    assert result.dry_run is True
    assert result.changed is True
    assert result.values_file == repo_dir / ".helm/overrides/airflow-dev-dpone-snapshot.yaml"


def test_gitlab_mr_client_ensure_merge_request(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    class DummyResponse:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self):
            return self._payload

    class DummySession:
        def __init__(self) -> None:
            self.headers = {}

        def get(self, url, params=None, timeout=None):
            calls.append(("GET", url))
            return DummyResponse([])

        def post(self, url, json=None, timeout=None):
            calls.append(("POST", url))
            return DummyResponse({"iid": 11, "web_url": "https://gitlab/mr/11"})

    client = GitLabMrClient(api_url="https://gitlab.example/api/v4", private_token="token", session=DummySession())
    result = client.ensure_merge_request(
        project_id="123",
        source_branch="feature/x",
        target_branch="master",
        title="chore: promote",
        description="desc",
        labels=("dpone", "snapshot"),
    )
    assert result.web_url == "https://gitlab/mr/11"
    assert result.existed is False
    assert [kind for kind, _ in calls] == ["GET", "POST"]
