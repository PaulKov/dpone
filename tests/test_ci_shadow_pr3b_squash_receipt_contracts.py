from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from tests.ci_shadow_pr3b_squash_receipt_support import (
    RECEIPT_SCHEMA,
    SPEC_PATH,
    TASK_PATH,
    binding_id,
    commit,
    git,
    receipt_script,
    run_receipt_check,
    squash_repository,
)


def test_public_output_amendment_squash_receipt_check_is_copyable() -> None:
    script = receipt_script()
    completed = subprocess.run(["bash", "-n"], input=script, capture_output=True, text=True, check=False)

    assert completed.returncode == 0
    assert completed.stdout == completed.stderr == ""
    assert "<APPROVED" not in script and "<OBSERVED" not in script and "<DOWNLOADED" not in script
    for expected in (
        "unverified()",
        "identity_fail()",
        '.integration_method == "squash"',
        ".reviewed_head_tree == $reviewed_tree",
        ".base_parent_sha == $base_parent",
        ".integration_tree == $integration_tree",
        ".integration_tree == .reviewed_head_tree",
        "Path(sys.argv[1]).read_bytes()",
        "python -I -c",
        'b"\\x00" in raw',
        "GIT_NO_REPLACE_OBJECTS=1",
        "GIT_GRAFT_FILE=/dev/null",
        "GIT_SHALLOW_FILE=/dev/null",
        "--is-shallow-repository",
        "pr-merge-receipt.schema.json",
        "Draft202012Validator",
        'receipt["binding_id"]',
        'git cat-file commit "${INTEGRATION_COMMIT}"',
        "^$/ { body = 1 }",
        '[[ "${PARENT_COUNT}" -eq 1 ]]',
        "merge receipt does not contain complete PASS evidence",
        "receipt subject or integration method disagrees",
        'git merge-base --is-ancestor "${BASE_PARENT}" "${REVIEWED_HEAD}"',
        "git log --first-parent --reverse --format=%H --diff-filter=A",
        "clarification lifecycle is not exactly APPROVED",
        "PASS: PR 539 squash receipt identity verified",
    ):
        assert expected in script
    assert script.count("!body { print }") == 2
    assert script.count("^$/ { body = 1 }") == 2


def test_squash_receipt_command_accepts_exact_local_and_receipt_identity(tmp_path: Path) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    schema = json.loads(RECEIPT_SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(receipt)
    assert receipt["binding_id"] == binding_id(receipt)

    completed = run_receipt_check(root, reviewed, integration, receipt)

    assert completed.returncode == 0
    assert completed.stdout == "PASS: PR 539 squash receipt identity verified\n"
    assert completed.stderr == ""


@pytest.mark.parametrize("large_message_commit", ("integration", "reviewed"))
def test_squash_receipt_command_drains_large_commit_messages(
    tmp_path: Path,
    large_message_commit: str,
) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    tree = str(receipt["integration_tree"])
    base = str(receipt["base_parent_sha"])
    created = subprocess.run(
        ["git", "commit-tree", tree, "-p", base],
        cwd=root,
        input="large receipt identity message\n" + "x" * 1_000_000,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if large_message_commit == "integration":
        integration = created
        receipt["integration_commit_sha"] = created
    else:
        reviewed = created
        receipt["reviewed_head_sha"] = created
    receipt["binding_id"] = binding_id(receipt)

    completed = run_receipt_check(root, reviewed, integration, receipt)

    assert completed.returncode == 0
    assert completed.stdout == "PASS: PR 539 squash receipt identity verified\n"
    assert completed.stderr == ""


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("protected_base_ref", "release"),
        ("integration_method", "merge"),
        ("reviewed_head_sha", "f" * 40),
        ("reviewed_head_tree", "f" * 40),
        ("integration_tree", "f" * 40),
        ("base_parent_sha", "f" * 40),
    ),
)
def test_squash_receipt_command_rejects_forged_identity(
    tmp_path: Path,
    field: str,
    replacement: str,
) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    receipt[field] = replacement
    receipt["binding_id"] = binding_id(receipt)

    completed = run_receipt_check(root, reviewed, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    expected = (
        "FAIL: receipt subject or integration method disagrees\n"
        if field in {"protected_base_ref", "integration_method", "reviewed_head_sha"}
        else "FAIL: receipt and local Git identity disagree\n"
    )
    assert completed.stderr == expected


def test_intrinsic_method_mismatch_precedes_missing_reviewed_object(tmp_path: Path) -> None:
    root, _reviewed, integration, receipt = squash_repository(tmp_path)
    missing_reviewed = "f" * 40
    receipt["integration_method"] = "merge"
    receipt["reviewed_head_sha"] = missing_reviewed
    receipt["binding_id"] = binding_id(receipt)

    completed = run_receipt_check(root, missing_reviewed, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "FAIL: receipt subject or integration method disagrees\n"


def test_squash_receipt_command_rejects_self_consistent_fake_trees(tmp_path: Path) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    receipt["reviewed_head_tree"] = receipt["integration_tree"] = "f" * 40
    receipt["binding_id"] = binding_id(receipt)

    completed = run_receipt_check(root, reviewed, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "FAIL: receipt and local Git identity disagree\n"


def test_squash_receipt_command_rejects_researched_integration(tmp_path: Path) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path, approved=False)

    completed = run_receipt_check(root, reviewed, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "FAIL: clarification lifecycle is not exactly APPROVED\n"


def test_squash_receipt_command_rejects_unrelated_reviewed_head(tmp_path: Path) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    reviewed_tree = git(root, "rev-parse", f"{reviewed}^{{tree}}")
    unrelated_reviewed = git(root, "commit-tree", reviewed_tree, "-m", "unrelated reviewed head")
    receipt["reviewed_head_sha"] = unrelated_reviewed
    receipt["binding_id"] = binding_id(receipt)

    completed = run_receipt_check(root, unrelated_reviewed, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "FAIL: reviewed head does not descend from the integration base parent\n"


def test_squash_receipt_command_ignores_replace_ref_ancestry(tmp_path: Path) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    reviewed_tree = git(root, "rev-parse", f"{reviewed}^{{tree}}")
    base = str(receipt["base_parent_sha"])
    unrelated = git(root, "commit-tree", reviewed_tree, "-m", "unrelated reviewed head")
    replacement = git(root, "commit-tree", reviewed_tree, "-p", base, "-m", "forged replacement ancestry")
    git(root, "replace", unrelated, replacement)
    receipt["reviewed_head_sha"] = unrelated
    receipt["binding_id"] = binding_id(receipt)

    completed = run_receipt_check(root, unrelated, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "FAIL: reviewed head does not descend from the integration base parent\n"


def test_squash_receipt_command_rejects_local_graft_history(tmp_path: Path) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    reviewed_tree = git(root, "rev-parse", f"{reviewed}^{{tree}}")
    unrelated = git(root, "commit-tree", reviewed_tree, "-m", "unrelated reviewed head")
    grafts = Path(git(root, "rev-parse", "--git-path", "info/grafts"))
    if not grafts.is_absolute():
        grafts = root / grafts
    grafts.parent.mkdir(parents=True, exist_ok=True)
    grafts.write_text(f"{unrelated} {receipt['base_parent_sha']}\n", encoding="utf-8")
    receipt["reviewed_head_sha"] = unrelated
    receipt["binding_id"] = binding_id(receipt)

    completed = run_receipt_check(root, unrelated, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "UNVERIFIED: local Git graft history is not admissible\n"


def test_history_view_is_pinned_after_admissibility_check(tmp_path: Path) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    reviewed_tree = git(root, "rev-parse", f"{reviewed}^{{tree}}")
    unrelated = git(root, "commit-tree", reviewed_tree, "-m", "unrelated reviewed head")
    receipt["reviewed_head_sha"] = unrelated
    receipt["binding_id"] = binding_id(receipt)
    fake_bin = root / ".receipt-test-bin"
    fake_bin.mkdir()
    (fake_bin / "git").write_text(
        "#!/bin/sh\n"
        'if [ "$1" = rev-parse ] && [ "$2" = --is-shallow-repository ]; then\n'
        '  "$RECEIPT_TEST_REAL_GIT" "$@"\n'
        "  status=$?\n"
        "  mkdir -p .git/info\n"
        '  printf "%s %s\\n" "$RECEIPT_TEST_REVIEWED" "$RECEIPT_TEST_GRAFT_PARENT" >.git/info/grafts\n'
        "  exit $status\n"
        "fi\n"
        'exec "$RECEIPT_TEST_REAL_GIT" "$@"\n',
        encoding="utf-8",
    )
    (fake_bin / "git").chmod(0o755)

    completed = run_receipt_check(
        root,
        unrelated,
        integration,
        receipt,
        extra_env={
            "RECEIPT_TEST_REVIEWED": unrelated,
            "RECEIPT_TEST_GRAFT_PARENT": str(receipt["base_parent_sha"]),
        },
    )

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "FAIL: reviewed head does not descend from the integration base parent\n"


def test_squash_receipt_command_rejects_actual_tree_mismatch(tmp_path: Path) -> None:
    root, reviewed, _integration, receipt = squash_repository(tmp_path)
    base = str(receipt["base_parent_sha"])
    git(root, "checkout", "--quiet", "-b", "mismatched-integration", base)
    git(root, "checkout", reviewed, "--", SPEC_PATH, TASK_PATH)
    mismatched_integration = commit(root, "mismatched integration", {"extra.txt": "tree drift\n"})
    receipt["integration_commit_sha"] = mismatched_integration
    receipt["integration_tree"] = git(root, "rev-parse", f"{mismatched_integration}^{{tree}}")
    receipt["changed_paths"].append("extra.txt")
    receipt["binding_id"] = binding_id(receipt)

    completed = run_receipt_check(root, reviewed, mismatched_integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "FAIL: receipt and local Git identity disagree\n"
