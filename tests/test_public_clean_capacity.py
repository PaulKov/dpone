"""Capacity, exact-occurrence and resource boundary contracts for the local gate."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.test_public_clean_gate import GateProject


@pytest.fixture
def project(tmp_path: Path) -> GateProject:
    return GateProject(tmp_path)


@pytest.mark.parametrize("occurrences", [100_001, 10_001, 100_000])
def test_capacity_full_candidate_and_commit(project: GateProject, occurrences: int) -> None:
    project.put("orders.txt", "table=orders\n" * occurrences)
    project.git("add", ".")
    code, summary, first = project.run()
    if occurrences > 100_000:
        assert (code, summary["status"], first.exists()) == (3, "UNABLE_TO_CERTIFY", False)
        assert summary["findings"][0]["code"] == "FINDINGS_LIMIT"
        return
    assert (code, summary["status"]) == (2, "BLOCKED")
    findings = json.loads(first.read_text())["findings"]
    assert len(findings) == occurrences
    exceptions = [
        {
            **{k: f[k] for k in ("label", "content_digest", "code", "offset")},
            "justification": "Synthetic capacity fixture",
            "synthetic": True,
        }
        for f in findings
    ]
    code, summary, receipt = project.run(review=project.review(first, exceptions=exceptions))
    assert (code, summary["status"]) == (0, "PASS")
    metadata = json.loads(project.metadata.read_text())
    commit = project.git(
        "commit-tree", project.git("write-tree").strip(), "-p", metadata["parent"], data=metadata["message"].encode()
    ).strip()
    code, summary, _ = project.run("commit", commit_sha=commit, candidate_receipt=receipt)
    assert (code, summary["status"]) == (0, "PASS")


@pytest.mark.parametrize("extra", [0, 1])
def test_capacity_json_exact_byte_boundary(project: GateProject, extra: int) -> None:
    from tools.agent_policy.public_clean_receipts import GateError, canonical, read_json, write_receipt

    size = 64 * 1024**2
    payload = {"value": "x" * (size - len(canonical({"value": ""})) - 1 + extra)}
    target = project.private / "large.json"
    if extra:
        with pytest.raises(GateError, match="cannot be certified"):
            write_receipt(target, project.root, payload)
        assert not target.exists()
        target.write_bytes(b" " * (size + 1))
        target.chmod(0o600)
        with pytest.raises(GateError):
            read_json(target, project.root)
    else:
        write_receipt(target, project.root, payload)
        assert (target.stat().st_size, read_json(target, project.root)) == (size, payload)


@pytest.mark.parametrize("mutation", ["valid", "duplicate", "repeated", "unhashable", "overflow"])
def test_capacity_exact_occurrence_review(project: GateProject, mutation: str) -> None:
    from tools.agent_policy.public_clean_policy import load_policy
    from tools.agent_policy.public_clean_receipts import GateError, apply_review

    policy = load_policy(json.loads(project.policy.read_text()))
    binding = {"tree": "tree", "parent": "parent", "metadata_digest": "metadata"}
    findings = [
        {"label": "fixture", "content_digest": "digest", "code": "DATABASE_IDENTIFIER", "offset": n}
        for n in range(100_000)
    ]
    exceptions = [{**f, "justification": "Synthetic fixture", "synthetic": True} for f in findings]
    review = {
        "schema": "dpone.public-clean-review.v1",
        "binding": binding,
        "policy_digest": policy.digest,
        "scanner_identity": "scanner",
        "reviewer": policy.reviewers[0],
        "approved": True,
        "exceptions": exceptions,
    }
    if mutation == "duplicate":
        findings.append(findings[0].copy())
    if mutation == "repeated":
        exceptions[-1] = exceptions[0].copy()
    if mutation == "unhashable":
        exceptions[0]["label"] = []
    if mutation == "overflow":
        exceptions.append(exceptions[0].copy())
    if mutation == "valid":
        assert apply_review(review, binding, policy, "scanner", findings) == []
        assert len(findings) == 100_000
        review["exceptions"] = [exceptions[2], exceptions[0]]
        assert apply_review(review, binding, policy, "scanner", findings) == [findings[1], *findings[3:]]
    else:
        with pytest.raises(GateError) as error:
            apply_review(review, binding, policy, "scanner", findings)
        if mutation == "overflow":
            assert error.value.code == "REVIEW_INVALID"


@pytest.mark.parametrize("count", [10_000, 10_001])
def test_capacity_ticket_positions_remain_independent(project: GateProject, count: int) -> None:
    from tools.agent_policy.public_clean_policy import Budget, Scanner, load_policy
    from tools.agent_policy.public_clean_receipts import GateError

    scanner = Scanner(load_policy(json.loads(project.policy.read_text())), Budget())
    if count > 10_000:
        with pytest.raises(GateError) as error:
            scanner.scan("fixture", b"xSYNTH1 " * count)
        assert error.value.code == "FINDINGS_LIMIT"
    else:
        scanner.scan("fixture", b"xSYNTH1 " * count)
        assert not any(f.code == "PROTECTED_TICKET" for f in scanner.findings)


@pytest.mark.parametrize("extra", [0, 1])
def test_capacity_full_gate_serialized_receipt_boundary(project: GateProject, extra: int) -> None:
    from tools.agent_policy.public_clean_receipts import canonical, digest

    project.put("orders.txt", "table=orders\n")
    project.git("add", ".")
    _, _, first = project.run()
    payload = json.loads(first.read_text())
    finding = payload["findings"][0]
    exception = {
        **{key: finding[key] for key in ("label", "content_digest", "code", "offset")},
        "justification": "x",
        "synthetic": True,
    }
    review_path = project.review(first, exceptions=[exception])
    review = json.loads(review_path.read_text())
    payload.update(status="PASS", review=review, review_digest=digest(review))
    review["exceptions"][0]["justification"] += "x" * (64 * 1024**2 - len(canonical(payload)) - 1 + extra)
    review_path.write_bytes(canonical(review))
    assert review_path.stat().st_size <= 64 * 1024**2
    code, summary, receipt = project.run(review=review_path)
    if extra:
        assert (code, summary["status"]) == (3, "UNABLE_TO_CERTIFY")
        assert not receipt.exists() and summary["findings"][0]["code"] == "RECEIPT_LIMIT"
    else:
        assert (code, summary["status"]) == (0, "PASS")
        assert receipt.stat().st_size == 64 * 1024**2


@pytest.mark.parametrize("field", ["label", "content_digest", "code", "offset"])
@pytest.mark.parametrize("value", ["changed", [], {}, True])
def test_exact_key_mutation_never_waives(project: GateProject, field: str, value: object) -> None:
    from tools.agent_policy.public_clean_policy import load_policy
    from tools.agent_policy.public_clean_receipts import GateError, apply_review

    policy = load_policy(json.loads(project.policy.read_text()))
    binding = {"tree": "tree", "parent": "parent", "metadata_digest": "metadata"}
    finding = {"label": "fixture", "content_digest": "digest", "code": "DATABASE_IDENTIFIER", "offset": 1}
    exception = {**finding, "justification": "Synthetic fixture", "synthetic": True, field: value}
    review = {
        "schema": "dpone.public-clean-review.v1",
        "binding": binding,
        "policy_digest": policy.digest,
        "scanner_identity": "scanner",
        "reviewer": policy.reviewers[0],
        "approved": True,
        "exceptions": [exception],
    }
    with pytest.raises(GateError):
        apply_review(review, binding, policy, "scanner", [finding])
    review["exceptions"] = []
    assert apply_review(review, binding, policy, "scanner", [finding, finding.copy()]) == [finding, finding]


@pytest.mark.parametrize(
    "field", ["protected_terms", "protected_ticket_prefixes", "allowed_hosts", "allowed_identities", "reviewers"]
)
@pytest.mark.parametrize("extra", [0, 1])
def test_policy_list_capacity_is_unchanged(project: GateProject, field: str, extra: int) -> None:
    from tools.agent_policy.public_clean_policy import load_policy
    from tools.agent_policy.public_clean_receipts import GateError

    policy = json.loads(project.policy.read_text())
    pattern = (
        "fixture{}.example.org"
        if field == "allowed_hosts"
        else "fixture{}@example.org"
        if field == "allowed_identities"
        else "fixture{}"
    )
    policy[field] = [pattern.format(n) for n in range(10_000 + extra)]
    if extra:
        with pytest.raises(GateError) as error:
            load_policy(policy)
        assert error.value.code == "POLICY_INVALID"
    else:
        assert len(getattr(load_policy(policy), field)) == 10_000


def test_scanner_capacity_accumulates_and_ticket_budget_resets(project: GateProject) -> None:
    from tools.agent_policy.public_clean_policy import Budget, Scanner, load_policy
    from tools.agent_policy.public_clean_receipts import GateError

    scanner = Scanner(load_policy(json.loads(project.policy.read_text())), Budget())
    for label in ["first", "second"]:
        scanner.scan(label, b"SYNTH1 " * 6000)
    assert len(scanner.findings) == 12_000
    for offset in range(88_000):
        scanner.add("DATABASE_IDENTIFIER", "fixture", offset)
    assert len(scanner.findings) == 100_000
    with pytest.raises(GateError) as error:
        scanner.add("DATABASE_IDENTIFIER", "fixture", 88_000)
    assert error.value.code == "FINDINGS_LIMIT"
    assert len(scanner.findings) == 100_000


@pytest.mark.parametrize("field", ["label", "justification"])
def test_large_receipt_credential_tail_is_never_published(field: str) -> None:
    from tools.agent_policy.public_clean_policy import Budget, validate_receipt_credentials
    from tools.agent_policy.public_clean_receipts import GateError, canonical

    payload = {field: "x" * (64 * 1024**2 - 200) + " password=synthetic-only-value"}
    with pytest.raises(GateError) as error:
        validate_receipt_credentials(canonical(payload), Budget())
    assert error.value.code == "RECEIPT_SENSITIVE"


@pytest.mark.parametrize("repetitions", [1, 2])
def test_overlapping_host_matchers_keep_one_reviewable_occurrence(project: GateProject, repetitions: int) -> None:
    project.put("orders.py", 'url = f"https://api.example.org{path}"\n' * repetitions)
    project.git("add", ".")
    code, summary, first = project.run()
    assert (code, summary["status"]) == (2, "BLOCKED")
    findings = json.loads(first.read_text())["findings"]
    hosts = [item for item in findings if item["code"] == "UNAPPROVED_HOST"]
    assert len(hosts) == repetitions
    keys = {(item["label"], item["content_digest"], item["code"], item["offset"]) for item in findings}
    assert len(keys) == len(findings)
    exceptions = [
        {
            **{key: item[key] for key in ("label", "content_digest", "code", "offset")},
            "justification": "Synthetic source-format fixture; no runtime hostname assertion",
            "synthetic": True,
        }
        for item in findings
    ]
    code, summary, _ = project.run(review=project.review(first, exceptions=exceptions))
    assert (code, summary["status"]) == (0, "PASS")


@pytest.mark.parametrize("suffix, expected", [("/orders", 0), ("{path}", 1), (".unknown", 1)])
def test_overlapping_host_allowlist_does_not_hide_a_longer_unknown_host(
    project: GateProject, suffix: str, expected: int
) -> None:
    policy = json.loads(project.policy.read_text())
    policy["allowed_hosts"].append("api.example.org")
    project.write("policy.json", policy)
    project.put("orders.txt", "https://api.example.org" + suffix)
    project.git("add", ".")
    code, summary, first = project.run()
    assert code == 2
    assert summary["status"] == ("BLOCKED" if expected else "REVIEW_REQUIRED")
    hosts = [item for item in json.loads(first.read_text())["findings"] if item["code"] == "UNAPPROVED_HOST"]
    assert len(hosts) == expected
