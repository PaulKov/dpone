"""Exact review dispatch and immutable Git source acquisition contracts."""

import hashlib
from types import SimpleNamespace

import pytest
from tools.agent_policy.public_clean_candidate import Git, review_blob
from tools.agent_policy.public_clean_policy import Budget
from tools.agent_policy.public_clean_receipts import GateError, apply_review

from tests.test_public_clean_gate import GateProject


def inputs():
    binding = {"tree": "tree", "parent": "parent", "metadata_digest": "metadata"}
    finding = {"label": "fixture.py", "content_digest": "content", "code": "CREDENTIAL_SHAPE", "offset": 4}
    entry = {
        **finding,
        "justification": "Reviewed exact annotation",
        "synthetic": False,
        "classification": "NON_CREDENTIAL_SYNTAX",
        "context_evidence": {"role": "annotation"},
    }
    review = {
        "schema": "dpone.public-clean-review.v1",
        "binding": binding,
        "policy_digest": "policy",
        "scanner_identity": "scanner",
        "reviewer": "reviewer",
        "approved": True,
        "exceptions": [entry],
    }
    policy = SimpleNamespace(digest="policy", reviewers={"reviewer"})
    return binding, finding, entry, review, policy


def test_classification_requires_injected_exact_context_validation():
    binding, finding, entry, review, policy = inputs()
    with pytest.raises(GateError):
        apply_review(review, binding, policy, "scanner", [finding])
    seen = []
    assert apply_review(review, binding, policy, "scanner", [finding], seen.append) == []
    assert seen == [entry]


@pytest.mark.parametrize(
    "change",
    [
        {"classification": "OTHER"},
        {"synthetic": True},
        {"context_evidence": None},
        {"offset": True},
        {"code": "DATABASE_IDENTIFIER"},
        {"extra": "invalid"},
    ],
)
def test_invalid_combinations_never_reach_validator(change):
    binding, finding, entry, review, policy = inputs()
    entry.update(change)

    def unexpected(_entry):
        pytest.fail("invalid review reached context validator")

    with pytest.raises(GateError):
        apply_review(review, binding, policy, "scanner", [finding], unexpected)


def test_context_failure_cannot_clear_finding():
    binding, finding, entry, review, policy = inputs()

    def reject(_entry):
        raise GateError("REVIEW_CONTEXT_INVALID")

    with pytest.raises(GateError, match="Public-clean"):
        apply_review(review, binding, policy, "scanner", [finding], reject)


def test_legacy_synthetic_contract_does_not_invoke_validator():
    binding, finding, entry, review, policy = inputs()
    del entry["classification"]
    del entry["context_evidence"]
    entry["synthetic"] = True

    def unexpected(_entry):
        pytest.fail("legacy exception invoked new validator")

    assert apply_review(review, binding, policy, "scanner", [finding], unexpected) == []


def test_review_blob_reads_frozen_git_tree_not_working_file(tmp_path):
    project = GateProject(tmp_path)
    raw = b"def run(token: bytes): pass\n"
    project.put("fixture.py", raw)
    project.git("add", ".")
    tree = project.git("write-tree").strip()
    project.put("fixture.py", "untrusted working-tree replacement")
    digest = hashlib.sha256(raw).hexdigest()
    git = Git(project.root, Budget())
    assert review_blob(git, tree, "fixture.py", digest) == raw
    for label in ["metadata:fixture.py", "*", "../fixture.py", "fixture.py!member"]:
        with pytest.raises(GateError):
            review_blob(git, tree, label, digest)
    with pytest.raises(GateError):
        review_blob(git, tree, "fixture.py", "0" * 64)


@pytest.mark.parametrize("missing", ["classification", "context_evidence"])
def test_incomplete_optional_pair_rejected(missing):
    binding, finding, entry, review, policy = inputs()
    del entry[missing]
    with pytest.raises(GateError):
        apply_review(review, binding, policy, "scanner", [finding], lambda _entry: None)


def test_duplicate_exact_exception_rejected():
    binding, finding, entry, review, policy = inputs()
    review["exceptions"].append(dict(entry))
    with pytest.raises(GateError):
        apply_review(review, binding, policy, "scanner", [finding], lambda _entry: None)


def test_literal_glob_filename_and_nonregular_tree_entries(tmp_path):
    project = GateProject(tmp_path)
    raw = b"def run(token: bytes,): pass\n"
    project.put("literal[one]*.py", raw)
    (project.root / "alias.py").symlink_to("literal[one]*.py")
    project.git("add", ".")
    tree = project.git("write-tree").strip()
    git = Git(project.root, Budget())
    digest = hashlib.sha256(raw).hexdigest()
    assert review_blob(git, tree, "literal[one]*.py", digest) == raw
    with pytest.raises(GateError):
        review_blob(git, tree, "alias.py", digest)
    with pytest.raises(GateError):
        review_blob(git, tree, "literal*", digest)


def classified_project(tmp_path):
    import json

    from tools.agent_policy.public_clean_policy import load_policy
    from tools.agent_policy.public_clean_syntax import build_syntax_evidence

    project = GateProject(tmp_path)
    raw = b"def f(password: bytes,):\n    pass\n"
    project.put("fixture.py", raw)
    project.git("add", ".")
    code, summary, first = project.run()
    assert (code, summary["status"]) == (2, "BLOCKED")
    findings = json.loads(first.read_text())["findings"]
    assert len(findings) == 1 and findings[0]["code"] == "CREDENTIAL_SHAPE"
    entry = {key: findings[0][key] for key in ("label", "content_digest", "code", "offset")}
    entry.update(classification="NON_CREDENTIAL_SYNTAX", synthetic=False, justification="Exact annotation proof")
    policy = load_policy(json.loads(project.policy.read_text()))
    entry["context_evidence"] = build_syntax_evidence(entry, raw, policy, "python_annotation")
    return project, first, entry


def test_real_classified_candidate_and_commit_use_same_evidence(tmp_path):
    import json

    project, first, entry = classified_project(tmp_path)
    code, summary, approved = project.run(review=project.review(first, exceptions=[entry]))
    assert (code, summary["status"]) == (0, "PASS")
    metadata = json.loads(project.metadata.read_text())
    tree = project.git("write-tree").strip()
    commit = project.git("commit-tree", tree, "-p", metadata["parent"], data=metadata["message"].encode()).strip()
    code, summary, verified = project.run("commit", commit_sha=commit, candidate_receipt=approved)
    assert (code, summary["status"]) == (0, "PASS")
    assert json.loads(approved.read_text())["counts"] == json.loads(verified.read_text())["counts"]


@pytest.mark.parametrize("mutation", ["context", "source", "scanner", "role"])
def test_classified_proof_mutation_never_publishes_success(tmp_path, mutation):
    project, first, entry = classified_project(tmp_path)
    if mutation == "source":
        project.put("fixture.py", "def f(password: bytes = b'example-value'):\n    pass\n")
        project.git("add", ".")
    elif mutation == "role":
        entry["context_evidence"]["role"] = "runtime_value"
    else:
        field = "context_sha256" if mutation == "context" else "scanner_identity"
        entry["context_evidence"][field] = "0" * 64
    code, summary, receipt = project.run(review=project.review(first, exceptions=[entry]))
    assert code != 0 and summary["status"] != "PASS"
    assert not receipt.exists()
