from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from tests.ci_shadow_pr3b_squash_receipt_support import (
    RECEIPT_SCHEMA,
    RECEIPT_SCHEMA_PATH,
    SPEC_PATH,
    TASK_PATH,
    binding_id,
    commit,
    complete_receipt,
    git,
    run_receipt_check,
    squash_repository,
)


def _remove_loose_object(root: Path, object_id: str) -> None:
    relative = git(root, "rev-parse", "--git-path", f"objects/{object_id[:2]}/{object_id[2:]}")
    object_path = Path(relative)
    if not object_path.is_absolute():
        object_path = root / object_path
    assert object_path.is_file()
    object_path.unlink()


@pytest.mark.parametrize("raw_receipt", ("", "   \n", "{not-json", "[]", "null", "{}\n{}\n"))
def test_squash_receipt_command_reports_unreadable_json_without_tool_diagnostics(
    tmp_path: Path,
    raw_receipt: str,
) -> None:
    root, reviewed, integration, _receipt = squash_repository(tmp_path)

    completed = run_receipt_check(root, reviewed, integration, raw_receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "UNVERIFIED: receipt JSON is unreadable\n"


@pytest.mark.parametrize("position", ("prefix", "infix", "suffix", "invalid_utf8"))
def test_squash_receipt_command_rejects_non_json_raw_bytes(
    tmp_path: Path,
    position: str,
) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    raw = json.dumps(receipt, separators=(",", ":")).encode("utf-8")
    if position == "prefix":
        corrupted = b"\x00" + raw
    elif position == "infix":
        corrupted = raw.replace(b'"status":"PASS"', b'"status":\x00"PASS"', 1)
    elif position == "suffix":
        corrupted = raw + b"\x00"
    else:
        corrupted = raw.replace(b'"warnings":[]', b'"warnings":["\xff"]', 1)

    completed = run_receipt_check(root, reviewed, integration, corrupted)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "UNVERIFIED: receipt JSON is unreadable\n"


@pytest.mark.parametrize("shadow_location", ("checkout", "pythonpath"))
def test_squash_receipt_schema_validation_ignores_untracked_python_shadowing(
    tmp_path: Path,
    shadow_location: str,
) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    for field in ("schema_version", "producer", "errors", "warnings"):
        receipt.pop(field)
    shadow_root = root if shadow_location == "checkout" else root / "hostile-pythonpath"
    shadow_root.mkdir(exist_ok=True)
    marker = root / f"{shadow_location}-jsonschema-imported"
    (shadow_root / "jsonschema.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text('imported', encoding='utf-8')\n"
        "class Draft202012Validator:\n"
        "    @staticmethod\n"
        "    def check_schema(_schema): pass\n"
        "    def __init__(self, _schema): pass\n"
        "    def validate(self, _payload): pass\n",
        encoding="utf-8",
    )
    extra_env = {"PYTHONPATH": str(shadow_root)} if shadow_location == "pythonpath" else None

    completed = run_receipt_check(root, reviewed, integration, receipt, extra_env=extra_env)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "UNVERIFIED: receipt PASS schema or binding is incomplete\n"
    assert not marker.exists()


@pytest.mark.parametrize(
    "missing_field",
    ("binding_id", "source_receipt", "producer", "errors", "warnings", "reviewed_head_tree"),
)
def test_squash_receipt_command_reports_incomplete_pass_schema_as_unverified(
    tmp_path: Path,
    missing_field: str,
) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    receipt.pop(missing_field)

    completed = run_receipt_check(root, reviewed, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "UNVERIFIED: receipt PASS schema or binding is incomplete\n"


def test_squash_receipt_command_maps_readable_failure_receipt_to_unverified(tmp_path: Path) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    receipt.update(
        {
            "status": "FAIL",
            "integration_method": None,
            "reviewed_head_sha": None,
            "reviewed_head_tree": None,
            "base_parent_sha": None,
            "integration_tree": None,
            "errors": ["source evidence unavailable"],
        }
    )

    completed = run_receipt_check(root, reviewed, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "UNVERIFIED: merge receipt does not contain complete PASS evidence\n"


def test_squash_receipt_command_rejects_wrong_binding_and_duplicate_keys(tmp_path: Path) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    wrong_binding = dict(receipt)
    wrong_binding["binding_id"] = "sha256:" + "f" * 64
    duplicate_status = json.dumps(receipt).replace("{", '{"status":"PASS",', 1)

    wrong_binding_result = run_receipt_check(root, reviewed, integration, wrong_binding)
    duplicate_result = run_receipt_check(root, reviewed, integration, duplicate_status)

    assert wrong_binding_result.returncode == duplicate_result.returncode == 1
    assert wrong_binding_result.stdout == duplicate_result.stdout == ""
    assert wrong_binding_result.stderr == "UNVERIFIED: receipt PASS schema or binding is incomplete\n"
    assert duplicate_result.stderr == "UNVERIFIED: receipt JSON is unreadable\n"


def test_squash_receipt_command_rejects_zero_parent_before_parent_resolution(tmp_path: Path) -> None:
    root = tmp_path / "root-integration"
    root.mkdir()
    git(root, "init", "--quiet")
    git(root, "config", "user.name", "PR3B Receipt Test")
    git(root, "config", "user.email", "pr3b@example.invalid")
    integration = commit(
        root,
        "root integration",
        {
            RECEIPT_SCHEMA_PATH: RECEIPT_SCHEMA.read_text(encoding="utf-8"),
            SPEC_PATH: (
                "- Public-output amendment status: APPROVED\n"
                "- [x] Maintainer changed public-output amendment status to `APPROVED` after\n"
                "  reviewing the exact amendment head.\n"
            ),
            TASK_PATH: "task: approved\n",
        },
    )
    integration_tree = git(root, "rev-parse", f"{integration}^{{tree}}")
    receipt = complete_receipt(
        reviewed=integration,
        reviewed_tree=integration_tree,
        base="f" * 40,
        integration=integration,
        integration_tree=integration_tree,
        changed_paths=[SPEC_PATH, TASK_PATH],
    )

    completed = run_receipt_check(root, integration, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "FAIL: integration commit is not one-parent squash\n"


def test_squash_receipt_command_rejects_two_parent_merge(tmp_path: Path) -> None:
    root, reviewed, _integration, receipt = squash_repository(tmp_path)
    reviewed_tree = str(receipt["reviewed_head_tree"])
    base = str(receipt["base_parent_sha"])
    integration = git(
        root,
        "commit-tree",
        reviewed_tree,
        "-p",
        base,
        "-p",
        reviewed,
        "-m",
        "two-parent integration",
    )
    receipt["integration_commit_sha"] = integration
    receipt["integration_tree"] = reviewed_tree
    receipt["binding_id"] = binding_id(receipt)

    completed = run_receipt_check(root, reviewed, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "FAIL: integration commit is not one-parent squash\n"


def test_header_parent_mismatch_precedes_missing_parent_object(tmp_path: Path) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    actual_parent = str(receipt["base_parent_sha"])
    receipt["base_parent_sha"] = "f" * 40
    receipt["binding_id"] = binding_id(receipt)
    _remove_loose_object(root, actual_parent)

    completed = run_receipt_check(root, reviewed, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "FAIL: receipt and local Git identity disagree\n"


def test_reviewed_tree_mismatch_precedes_missing_reviewed_tree_object(tmp_path: Path) -> None:
    root, original_reviewed, integration, receipt = squash_repository(tmp_path)
    base = str(receipt["base_parent_sha"])
    git(root, "checkout", "--quiet", "-b", "different-reviewed", base)
    git(root, "checkout", original_reviewed, "--", SPEC_PATH, TASK_PATH)
    reviewed = commit(root, "different reviewed tree", {"extra.txt": "tree drift\n"})
    actual_reviewed_tree = git(root, "rev-parse", f"{reviewed}^{{tree}}")
    assert actual_reviewed_tree != receipt["reviewed_head_tree"]
    receipt["reviewed_head_sha"] = reviewed
    receipt["binding_id"] = binding_id(receipt)
    _remove_loose_object(root, actual_reviewed_tree)

    completed = run_receipt_check(root, reviewed, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "FAIL: receipt and local Git identity disagree\n"


@pytest.mark.parametrize("integration_fault", ("two_parent", "researched", "task_reuse"))
def test_integration_failures_precede_missing_reviewed_object(
    tmp_path: Path,
    integration_fault: str,
) -> None:
    root, reviewed, integration, receipt = squash_repository(
        tmp_path,
        approved=integration_fault != "researched",
        prior_task_history=integration_fault == "task_reuse",
    )
    if integration_fault == "two_parent":
        tree = str(receipt["integration_tree"])
        base = str(receipt["base_parent_sha"])
        integration = git(root, "commit-tree", tree, "-p", base, "-p", reviewed, "-m", "merge")
        receipt["integration_commit_sha"] = integration
    missing_reviewed = "f" * 40
    receipt["reviewed_head_sha"] = missing_reviewed
    receipt["binding_id"] = binding_id(receipt)

    completed = run_receipt_check(root, missing_reviewed, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    expected = {
        "two_parent": "FAIL: integration commit is not one-parent squash\n",
        "researched": "FAIL: clarification lifecycle is not exactly APPROVED\n",
        "task_reuse": "FAIL: integration commit is not the canonical task introduction\n",
    }
    assert completed.stderr == expected[integration_fault]


def test_squash_receipt_command_reports_shallow_parent_as_unverified(tmp_path: Path) -> None:
    source, reviewed, integration, receipt = squash_repository(tmp_path / "source")
    shallow = tmp_path / "shallow"
    subprocess.run(
        ["git", "clone", "--quiet", "--depth", "1", f"file://{source}", str(shallow)],
        check=True,
        capture_output=True,
        text=True,
    )
    git(shallow, "fetch", "--quiet", "--depth", "1", "origin", "reviewed:reviewed-evidence")

    completed = run_receipt_check(shallow, reviewed, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "UNVERIFIED: complete non-shallow repository history is required\n"


def test_squash_receipt_command_rejects_hidden_task_reuse_in_shallow_history(tmp_path: Path) -> None:
    source, reviewed, integration, receipt = squash_repository(
        tmp_path / "source",
        prior_task_history=True,
    )
    complete = run_receipt_check(source, reviewed, integration, receipt)
    assert complete.returncode == 1
    assert complete.stdout == ""
    assert complete.stderr == "FAIL: integration commit is not the canonical task introduction\n"

    shallow = tmp_path / "shallow"
    subprocess.run(
        ["git", "clone", "--quiet", "--depth", "2", f"file://{source}", str(shallow)],
        check=True,
        capture_output=True,
        text=True,
    )
    git(shallow, "fetch", "--quiet", "--depth", "2", "origin", "reviewed:reviewed-evidence")

    completed = run_receipt_check(shallow, reviewed, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "UNVERIFIED: complete non-shallow repository history is required\n"


def test_squash_receipt_command_rejects_missing_spec_without_git_diagnostics(tmp_path: Path) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path, include_spec=False)

    completed = run_receipt_check(root, reviewed, integration, receipt)

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert completed.stderr == "FAIL: approved clarification specification is unavailable\n"


def test_squash_receipt_command_reports_missing_local_or_file_evidence(tmp_path: Path) -> None:
    root, reviewed, integration, receipt = squash_repository(tmp_path)
    missing_head = "f" * 40
    receipt["reviewed_head_sha"] = missing_head
    receipt["binding_id"] = binding_id(receipt)

    missing_object = run_receipt_check(root, missing_head, integration, receipt)
    missing_file = run_receipt_check(root, reviewed, integration, None, receipt_name="missing.json")

    assert missing_object.returncode == missing_file.returncode == 1
    assert missing_object.stdout == missing_file.stdout == ""
    assert missing_object.stderr == "UNVERIFIED: reviewed head commit is unavailable locally\n"
    assert missing_file.stderr == "UNVERIFIED: receipt file is missing\n"
