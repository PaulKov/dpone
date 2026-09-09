from __future__ import annotations

import hashlib
import os
import subprocess
from dataclasses import dataclass, replace
from pathlib import Path

import pytest
import yaml
from tests.agent_policy._ci_shadow_history_fixtures import commit_files, integration_repository

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = "test_artifacts/agent-policy/dpone-ci-shadow-pr3b-implementation.yml"
SPEC_PATH = "docs/feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md"
ADR_PATH = "docs/adr/0037-immutable-agent-pr-merge-closure.md"
CONTRACT = ROOT / CONTRACT_PATH

BASE = "b3200e89ef375eeead7213e2c13d7c0fb33ff659"
INTEGRATION_BASE = "4f2b924230ae90eb32a82db9e3510338eafa07ef"
INTEGRATION_BASE_TREE = "efbb5fc284c58ddf26ab1d64459f7f2e439af6de"
REVIEWED_HEAD = "d0fcf7d99d79b85ea5a2681166db59645c50d7ce"
INTEGRATION_COMMIT = "0250d99a6c3b1344aebbe80a54455feda25a730d"
INTEGRATION_TREE = "6e9eb6485c46933a8d90a00e6f9ffdc4a8599314"
INTEGRATION_METHOD = "merge"
RETIRED_IMPLEMENTATION_TASK_BLOB = "ee7a69b5b8ecb23be5bff783df96303a1320c8c8"

MERGE_RECEIPT_RUN_ID = 31777266874
MERGE_RECEIPT_RUN_ATTEMPT = 1
MERGE_RECEIPT_ARTIFACT_ID = 9210346362
MERGE_RECEIPT_ARTIFACT_DIGEST = "sha256:2e01d33a642afaaff2c1b0d799f5f34f8ab544b7b0157bbb658fdd96f441d231"
MERGE_RECEIPT_ARTIFACT_SIZE_BYTES = 15974
MERGE_RECEIPT_ARCHIVE_SHA256 = "2e01d33a642afaaff2c1b0d799f5f34f8ab544b7b0157bbb658fdd96f441d231"
MERGE_RECEIPT_FILE_SHA256 = "c9b607200641afbda7718678d505354b27351a028a908b4ad46477b1e5526fff"
MERGE_RECEIPT_BINDING_ID = "sha256:2098e24e3ee21d33a9e889a4cc09e4115544bbd1e83040f6da5957fe6cc15e79"
MERGE_RECEIPT_CHECK_RUN_ID = 94695293503
MERGE_RECEIPT_CHECK_APP_ID = 15368


@dataclass(frozen=True)
class IntegrationIdentity:
    """Immutable Git identities that delimit a synthetic implementation."""

    base: str
    base_tree: str
    reviewed_head: str
    integration_commit: str
    integration_tree: str
    method: str


def _payload() -> dict[str, object]:
    parsed = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
    assert isinstance(parsed, dict)
    return parsed


def _git_output(*args: str, root: Path) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, "GIT_NO_REPLACE_OBJECTS": "1"},
    ).stdout.strip()


def _assert_integration_identity(
    identity: IntegrationIdentity,
    *,
    tip: str,
    root: Path,
) -> None:
    """Bind a merge or squash integration without consulting ambient diffs."""

    assert identity.method in {"merge", "squash"}
    assert _git_output("rev-parse", f"{identity.base}^{{commit}}", root=root) == identity.base
    assert _git_output("rev-parse", f"{identity.reviewed_head}^{{commit}}", root=root) == identity.reviewed_head
    assert (
        _git_output("rev-parse", f"{identity.integration_commit}^{{commit}}", root=root) == identity.integration_commit
    )
    assert _git_output("rev-parse", f"{identity.base}^{{tree}}", root=root) == identity.base_tree
    assert _git_output("rev-parse", f"{identity.reviewed_head}^{{tree}}", root=root) == identity.integration_tree
    assert _git_output("rev-parse", f"{identity.integration_commit}^{{tree}}", root=root) == identity.integration_tree
    assert _git_output("merge-base", identity.base, identity.reviewed_head, root=root) == identity.base

    parents = tuple(_git_output("show", "-s", "--format=%P", identity.integration_commit, root=root).split())
    expected = (identity.base, identity.reviewed_head) if identity.method == "merge" else (identity.base,)
    assert parents == expected
    assert _git_output("merge-base", identity.integration_commit, tip, root=root) == identity.integration_commit


def _changed_paths(
    base: str,
    scope_head: str,
    *,
    root: Path,
) -> set[str]:
    output = _git_output(
        "diff",
        "--no-renames",
        "--name-only",
        "--diff-filter=ACDMRT",
        base,
        scope_head,
        root=root,
    )
    return {line for line in output.splitlines() if line}


def _covered(path: str, boundaries: set[str]) -> bool:
    return any(path == boundary or path.startswith(f"{boundary}/") for boundary in boundaries)


def test_pr3b_archived_contract_documents_approved_receipt_identity() -> None:
    """Check retained metadata, not the availability or truth of old hosted receipts."""

    payload = _payload()
    raw_task = CONTRACT.read_bytes()
    blob_identity = hashlib.sha1(f"blob {len(raw_task)}\0".encode() + raw_task, usedforsecurity=False).hexdigest()
    assert blob_identity == RETIRED_IMPLEMENTATION_TASK_BLOB
    specification = (ROOT / SPEC_PATH).read_text(encoding="utf-8")
    metadata = specification.split("## Executive summary", maxsplit=1)[0]

    assert payload["base_commit"] == BASE
    assert metadata.count("- Status: APPROVED") == 1
    assert metadata.count("- Public-output amendment status: APPROVED") == 1
    assert "- Status: RESEARCHED" not in metadata
    assert "- Public-output amendment status: RESEARCHED" not in metadata
    assert specification.count("[x] Maintainer changed public-output amendment status to `APPROVED` after") == 1

    dependencies = payload["dependencies"]
    assert isinstance(dependencies, list)
    joined = " ".join(str(item) for item in dependencies)
    assert "post-merge run 31510473308" in joined
    assert "artifact 9108834368" in joined
    assert "raw receipt SHA-256 1eb177ac319b371f45daf4980f42a10786a77b4fdc72d66cab33f605087df944" in joined
    assert "sha256:58f1b8499ad55d9794346d0bba927667cec3354e789d4dff4a568aff121648c6" in joined
    assert "PASS: PR 539 squash receipt identity verified" in joined
    assert (
        f"PR 540 reviewed head {REVIEWED_HEAD} from exact base {INTEGRATION_BASE} "
        f"with base tree {INTEGRATION_BASE_TREE} was {INTEGRATION_METHOD}-integrated as "
        f"{INTEGRATION_COMMIT} with tree {INTEGRATION_TREE}."
    ) in dependencies
    assert (
        f"Provider post-merge run {MERGE_RECEIPT_RUN_ID} attempt {MERGE_RECEIPT_RUN_ATTEMPT} produced PASS "
        f"artifact {MERGE_RECEIPT_ARTIFACT_ID} named agent-pr-receipt with provider digest "
        f"{MERGE_RECEIPT_ARTIFACT_DIGEST} and size {MERGE_RECEIPT_ARTIFACT_SIZE_BYTES} bytes, archive SHA-256 "
        f"sha256:{MERGE_RECEIPT_ARCHIVE_SHA256}, "
        f"agent_pr_merge_receipt.json SHA-256 sha256:{MERGE_RECEIPT_FILE_SHA256}, and binding_id "
        f"{MERGE_RECEIPT_BINDING_ID}; exact-C check-run {MERGE_RECEIPT_CHECK_RUN_ID} used GitHub Actions App "
        f"id {MERGE_RECEIPT_CHECK_APP_ID}, external_id "
        f"agent-pr-merge-closure:{MERGE_RECEIPT_RUN_ID}:{MERGE_RECEIPT_RUN_ATTEMPT}, "
        "and conclusion success."
    ) in dependencies


def test_pr3b_implementation_contract_has_disjoint_writer_and_integrator_ownership() -> None:
    payload = _payload()
    owned = set(payload["owned_paths"])
    integrator_owned = set(payload["integrator_owned_paths"])
    forbidden = set(payload["forbidden_paths"])

    assert payload["integrator"] == payload["shared_file_owner"] == "/root"
    assert owned.isdisjoint(integrator_owned)
    assert {
        "tests/agent_policy/test_workflow_privilege_parser.py",
        "tests/agent_policy/test_workflow_privilege_graph.py",
        "tests/agent_policy/test_workflow_privilege_profiles.py",
        "tests/agent_policy/test_workflow_security_privileged_cli.py",
        "docs/ci-cd.md",
        "docs/cicd/runbooks.md",
        "docs/testing/index.md",
        "docs/agent-security-mapping.md",
    } <= owned
    assert {
        "tools/agent_policy/workflow_privilege_contracts.py",
        "tools/agent_policy/workflow_privilege_service.py",
        "tools/agent_policy/workflow_security_privileged.py",
        "tools/agent_policy/workflow_security.py",
        ".agents/policy/workflow-security-privileged.yml",
        "evals/agent/workflow-security-privileged-report.schema.json",
        ".github/workflows/ci.yml",
        ".github/workflows/codeql.yml",
        ".github/codeql/codeql-config.yml",
        "test_artifacts/agent-policy/pr3b-semantic-privilege-certification.json",
        "test_artifacts/agent-policy/agent_governance_gate.json",
        "tests/test_ci_shadow_pr3b_docs_contracts.py",
        "CHANGELOG.md",
        "docs/quality-metrics.md",
    } <= integrator_owned
    assert all(_covered(path, forbidden) for path in integrator_owned)


@pytest.mark.parametrize("method", ["merge", "squash"])
def test_pr3b_synthetic_integration_binds_identity_and_frozen_scope(tmp_path: Path, method: str) -> None:
    """Exercise the original identity/blob/scope guarantees in a disposable Git DAG."""

    payload = _payload()
    allowed = set(payload["owned_paths"]) | set(payload["integrator_owned_paths"])
    frozen_paths = (
        SPEC_PATH,
        ADR_PATH,
        "test_artifacts/agent-policy/dpone-ci-shadow-pr3b-spec.yml",
        "test_artifacts/agent-policy/dpone-ci-shadow-pr3b-public-output-amendment.yml",
        ".github/workflows/agent-pr-receipt.yml",
    )
    root = tmp_path / method
    history = integration_repository(
        root,
        frozen_files={path: (ROOT / path).read_text(encoding="utf-8") for path in frozen_paths},
        changed_files={"tools/agent_policy/workflow_privilege_service.py": "# Synthetic reviewed implementation.\n"},
        method=method,
    )
    identity = IntegrationIdentity(
        history.base,
        history.base_tree,
        history.reviewed_head,
        history.integration_commit,
        history.integration_tree,
        method,
    )
    _assert_integration_identity(identity, tip="HEAD", root=root)
    assert _git_output("rev-parse", f"{history.base}^", root=root) == history.parent
    for path, blob in history.frozen_blobs.items():
        assert _git_output("rev-parse", f"{history.integration_commit}:{path}", root=root) == blob

    changed = _changed_paths(history.base, history.integration_commit, root=root)

    assert changed == {"tools/agent_policy/workflow_privilege_service.py"}
    assert all(_covered(path, allowed) for path in changed)
    assert not any(path.startswith(("src/", "packages/")) for path in changed)
    assert {
        SPEC_PATH,
        ADR_PATH,
        ".github/workflows/agent-pr-receipt.yml",
        "pyproject.toml",
        "uv.lock",
    }.isdisjoint(changed)

    tampered = commit_files(root, "synthetic later authority tamper", {SPEC_PATH: "Status: RESEARCHED\n"})
    with pytest.raises(AssertionError):
        assert _git_output("rev-parse", f"{tampered}:{SPEC_PATH}", root=root) == history.frozen_blobs[SPEC_PATH]
    assert (
        _git_output("rev-parse", f"{history.integration_commit}:{SPEC_PATH}", root=root)
        == (history.frozen_blobs[SPEC_PATH])
    )
    _assert_integration_identity(identity, tip=tampered, root=root)
    assert _changed_paths(history.base, history.integration_commit, root=root) == changed


def test_frozen_scope_survives_adversarial_future_commits(tmp_path: Path) -> None:
    for method in ("merge", "squash"):
        root = tmp_path / method
        root.mkdir()
        _git_output("init", root=root)
        _git_output("config", "user.name", "Contract Test", root=root)
        _git_output("config", "user.email", "contract-test@example.invalid", root=root)

        (root / "base.txt").write_text("base\n", encoding="utf-8")
        _git_output("add", "base.txt", root=root)
        _git_output("commit", "-m", "base", root=root)
        base = _git_output("rev-parse", "HEAD", root=root)
        base_tree = _git_output("rev-parse", "HEAD^{tree}", root=root)

        _git_output("switch", "-c", "reviewed", root=root)
        (root / "allowed.txt").write_text("reviewed\n", encoding="utf-8")
        _git_output("add", "allowed.txt", root=root)
        _git_output("commit", "-m", "reviewed", root=root)
        reviewed = _git_output("rev-parse", "HEAD", root=root)
        reviewed_tree = _git_output("rev-parse", "HEAD^{tree}", root=root)

        _git_output("switch", "--detach", base, root=root)
        _git_output("switch", "-c", "integration", root=root)
        if method == "merge":
            _git_output("merge", "--no-ff", "--no-edit", reviewed, root=root)
        else:
            _git_output("merge", "--squash", reviewed, root=root)
            _git_output("commit", "-m", "squash integration", root=root)
        integration = _git_output("rev-parse", "HEAD", root=root)

        future_path = root / "src/future.py"
        future_path.parent.mkdir()
        future_path.write_text("unsafe = True\n", encoding="utf-8")
        (root / "allowed.txt").write_text("tampered\n", encoding="utf-8")
        _git_output("add", "src/future.py", "allowed.txt", root=root)
        _git_output("commit", "-m", "future tamper", root=root)
        (root / "allowed.txt").write_text("reviewed\n", encoding="utf-8")
        _git_output("add", "allowed.txt", root=root)
        _git_output("commit", "-m", "future byte restoration", root=root)
        tip = _git_output("rev-parse", "HEAD", root=root)

        identity = IntegrationIdentity(base, base_tree, reviewed, integration, reviewed_tree, method)
        _assert_integration_identity(identity, tip=tip, root=root)
        assert _changed_paths(base, integration, root=root) == {"allowed.txt"}
        assert _changed_paths(base, tip, root=root) == {"allowed.txt", "src/future.py"}
        assert _git_output("log", "--format=%H", f"{integration}..{tip}", "--", "allowed.txt", root=root)

        with pytest.raises(AssertionError):
            _assert_integration_identity(
                replace(identity, method="squash" if method == "merge" else "merge"), tip=tip, root=root
            )
        with pytest.raises(AssertionError):
            _assert_integration_identity(replace(identity, integration_tree="0" * 40), tip=tip, root=root)
        with pytest.raises(AssertionError):
            _assert_integration_identity(replace(identity, integration_commit=tip), tip=tip, root=root)


def test_pr3b_implementation_contract_freezes_fail_closed_and_compatibility_gates() -> None:
    payload = _payload()
    criteria = " ".join(payload["acceptance_criteria"])
    checks = payload["required_checks"]
    stops = " ".join(payload["stop_conditions"])

    assert "graph-derived route has complete route-specific authority evidence" in criteria
    assert "ordinary internal failures map to the one fixed umbrella error" in criteria
    assert "BaseException subclasses propagate unchanged" in criteria
    assert "pre-split base quality job and legacy CodeQL config remain reproducible RED fixtures" in criteria
    assert "full non-live xdist suite" in " ".join(checks["broad"])
    assert "run the standalone scan twice with byte-identical text and JSON output" in " ".join(checks["broad"])
    broad = " ".join(checks["broad"])
    assert "checksum-pinned actionlint 1.7.12" in broad
    assert "actionlint -no-color -format '{{json .}}' -shellcheck '' -pyflakes ''" in broad
    assert "stdout exactly [] followed by one newline, and empty stderr" in broad
    assert "pr3b-semantic-privilege-certification.json" in broad
    assert "workflow_security_privileged.py --root . --format json" in broad
    assert "workflow-security-privileged-report.schema.json" in broad
    assert "require status PASS" in broad
    assert "cmp it byte-for-byte with a second fresh invocation and the tracked artifact" in broad
    assert "Mocked or local policy tests never count" in " ".join(checks["live"])
    assert "new dependency" in stops
    assert "fixture widening" in stops
    assert "new required check" in stops
