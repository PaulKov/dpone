"""Black-box privacy gate contracts using isolated, synthetic Git repositories."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

GATE = Path(__file__).resolve().parents[1] / "tools/agent_policy/public_clean_gate.py"
IDENTITY = "reviewer@example.org"
TERM = "ExampleSecretMarker"


class GateProject:
    """Own a destination and external private inputs, never source repositories."""

    def __init__(self, root: Path) -> None:
        self.root = root / "destination"
        self.private = root / "private"
        self.root.mkdir()
        self.private.mkdir(mode=0o700)
        self.env = {
            **os.environ,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_AUTHOR_NAME": "Public Reviewer",
            "GIT_AUTHOR_EMAIL": IDENTITY,
            "GIT_COMMITTER_NAME": "Public Reviewer",
            "GIT_COMMITTER_EMAIL": IDENTITY,
            "GIT_AUTHOR_DATE": "1700000000 +0000",
            "GIT_COMMITTER_DATE": "1700000000 +0000",
        }
        self.git("init", "-q")
        self.put("orders.txt", "orders\n")
        self.git("add", ".")
        self.git("commit", "-qm", "Initial public example")
        self.policy = self.write(
            "policy.json",
            {
                "schema": "dpone.public-clean-policy.v1",
                "protected_terms": [TERM],
                "protected_ticket_prefixes": ["SYNTH"],
                "allowed_hosts": ["example.org"],
                "allowed_identities": [IDENTITY],
                "reviewers": [IDENTITY],
            },
        )
        self.metadata = self.write(
            "metadata.json",
            {
                "message": "Add public orders\n",
                "author_name": "Public Reviewer",
                "author_email": IDENTITY,
                "author_date": "1700000000 +0000",
                "committer_name": "Public Reviewer",
                "committer_email": IDENTITY,
                "committer_date": "1700000000 +0000",
                "parent": self.git("rev-parse", "HEAD").strip(),
            },
        )
        self.calls = 0

    def git(self, *args: str, data: bytes | None = None) -> str:
        return subprocess.run(
            ["git", "-C", str(self.root), *args],
            input=data,
            capture_output=True,
            check=True,
            env=self.env,
        ).stdout.decode()

    def put(self, name: str, content: str | bytes) -> Path:
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content.encode() if isinstance(content, str) else content)
        return path

    def write(self, name: str, payload: dict[str, Any]) -> Path:
        path = self.private / name
        path.write_text(json.dumps(payload), encoding="utf-8")
        path.chmod(0o600)
        return path

    def run(self, mode: str = "candidate", **options: Any) -> tuple[int, dict, Path]:
        self.calls += 1
        receipt = options.pop("receipt", self.private / f"receipt-{self.calls}.json")
        values = {"root": self.root, "policy": self.policy, "receipt": receipt}
        if mode == "candidate":
            values["metadata"] = self.metadata
        values.update(options)
        command = [sys.executable, str(GATE), mode]
        for key, value in values.items():
            command.extend(["--" + key.replace("_", "-"), str(value)])
        result = subprocess.run(command, capture_output=True, env=self.env, timeout=45)
        assert result.stderr == b"", "CLI must redact stderr inside its failure boundary"
        summary = json.loads(result.stdout)
        encoded = json.dumps(summary)
        for value in (TERM, str(self.root), str(self.private), IDENTITY):
            assert value not in encoded
        assert set(summary) <= {"schema", "mode", "status", "complete", "counts", "findings"}
        for finding in summary.get("findings", []):
            assert set(finding) <= {"item", "code"}
        return result.returncode, summary, Path(receipt)

    def review(self, receipt: Path, **changes: Any) -> Path:
        candidate = json.loads(receipt.read_text())
        payload = {
            "schema": "dpone.public-clean-review.v1",
            "binding": {key: candidate["binding"][key] for key in ("tree", "parent", "metadata_digest")},
            "policy_digest": candidate["policy_digest"],
            "scanner_identity": candidate["scanner_identity"],
            "reviewer": IDENTITY,
            "approved": True,
            "exceptions": [],
        }
        payload.update(changes)
        return self.write(f"review-{self.calls}.json", payload)


@pytest.fixture
def project(tmp_path: Path) -> GateProject:
    return GateProject(tmp_path)


def test_review_required_then_exact_commit_verified(project: GateProject) -> None:
    code, summary, first = project.run()
    assert (code, summary["status"]) == (2, "REVIEW_REQUIRED")
    review = project.review(first)
    code, summary, approved = project.run(review=review)
    assert (code, summary["status"]) == (0, "PASS")

    assert approved.stat().st_mode & 0o777 == 0o600
    metadata = json.loads(project.metadata.read_text())
    tree = project.git("write-tree").strip()
    commit = project.git("commit-tree", tree, "-p", metadata["parent"], data=metadata["message"].encode()).strip()
    code, summary, _ = project.run("commit", commit_sha=commit, candidate_receipt=approved)
    assert (code, summary["status"]) == (0, "PASS")


@pytest.mark.parametrize(
    "variant", [TERM.lower(), "ＥxampleSecretMarker", "Example-Secret_Marker", "Example\u200bSecretMarker"]
)
def test_unicode_case_separator_variants_block(project: GateProject, variant: str) -> None:
    project.put("customers.txt", variant)
    project.git("add", ".")
    code, summary, _ = project.run()
    assert (code, summary["status"]) == (2, "BLOCKED")
    assert "PROTECTED_TERM" in {item["code"] for item in summary["findings"]}


@pytest.mark.parametrize("field", ["message", "author_name", "author_email", "committer_name", "committer_email"])
def test_all_proposed_metadata_scanned(project: GateProject, field: str) -> None:
    metadata = json.loads(project.metadata.read_text())
    metadata[field] = TERM + ("@example.org" if field.endswith("email") else "")
    project.write("metadata.json", metadata)
    code, summary, _ = project.run()
    assert code in (2, 3)
    assert summary["status"] != "PASS"


@pytest.mark.parametrize("surface", ["path", "unchanged_blob", "ignored", "tag", "deleted", "renamed"])
def test_discovery_covers_historical_and_working_surfaces(project: GateProject, surface: str) -> None:
    mode = "history"
    options: dict[str, str] = {}
    if surface == "path":
        project.put(TERM + ".txt", "orders")
        project.git("add", ".")
        mode = "candidate"
    elif surface == "unchanged_blob":
        project.put("orders.txt", TERM)
        project.git("add", ".")
        project.git("commit", "-qm", "Update orders")
        metadata = json.loads(project.metadata.read_text())
        metadata["parent"] = project.git("rev-parse", "HEAD").strip()
        project.write("metadata.json", metadata)
        mode = "candidate"
    elif surface == "ignored":
        project.put(".gitignore", "generated/\n")
        project.put("generated/orders.txt", TERM)
        mode = "worktree"
    elif surface == "tag":
        project.git("tag", "-a", "example-tag", "-m", TERM)
    else:
        project.put("customers.txt", TERM)
        project.git("add", ".")
        project.git("commit", "-qm", "Add customer example")
        project.git("rm", "customers.txt") if surface == "deleted" else project.git(
            "mv", "customers.txt", "clients.txt"
        )
        project.git("commit", "-qm", "Update customer example")
        mode = "source-delta"
        options["commit_sha"] = project.git("rev-parse", "HEAD").strip()
    code, summary, _ = project.run(mode, **options)
    assert (code, summary["status"]) == (2, "BLOCKED")


@pytest.mark.parametrize("mode", ["history", "worktree", "source-delta"])
def test_clean_discovery_is_not_commit_clearance(project: GateProject, mode: str) -> None:
    options = {"commit_sha": project.git("rev-parse", "HEAD").strip()} if mode == "source-delta" else {}
    code, summary, receipt = project.run(mode, **options)
    assert (code, summary["status"]) == (0, "SCAN_COMPLETE")
    code, summary, _ = project.run(
        "commit", commit_sha=project.git("rev-parse", "HEAD").strip(), candidate_receipt=receipt
    )
    assert code in (2, 3)
    assert summary["status"] != "PASS"


@pytest.mark.parametrize(
    "value",
    [
        "172.20.1.9",
        "person@unknown.example",
        "https://unknown.example/orders",
        "/home/example/orders",
        "SYNTH-321",
        "password=example-value",
    ],
)
def test_generic_private_shapes_block(project: GateProject, value: str) -> None:
    project.put("customers.txt", value)
    project.git("add", ".")
    code, summary, _ = project.run()
    assert (code, summary["status"]) == (2, "BLOCKED")
    assert summary["findings"]


def test_protected_finding_cannot_be_waived(project: GateProject) -> None:
    project.put("customers.txt", TERM)
    project.git("add", ".")
    _, _, first = project.run()
    findings = json.loads(first.read_text())["findings"]
    exceptions = [
        {
            **{key: item[key] for key in ("label", "content_digest", "code", "offset")},
            "justification": "Synthetic public test input",
            "synthetic": True,
        }
        for item in findings
    ]
    code, summary, _ = project.run(review=project.review(first, exceptions=exceptions))
    assert code in (2, 3)
    assert summary["status"] != "PASS"


def test_reviewed_generic_exception_is_bound_to_one_occurrence(project: GateProject) -> None:
    project.put("customers.txt", "172.20.1.9\n172.20.1.9\n")
    project.git("add", ".")
    _, _, first = project.run()
    findings = json.loads(first.read_text())["findings"]
    assert len(findings) == 2
    exceptions = [
        {
            **{key: item[key] for key in ("label", "content_digest", "code", "offset")},
            "justification": "Synthetic public test input",
            "synthetic": True,
        }
        for item in findings
    ]
    code, summary, _ = project.run(review=project.review(first, exceptions=exceptions[:1]))
    assert (code, summary["status"]) == (2, "BLOCKED")
    code, summary, _ = project.run(review=project.review(first, exceptions=exceptions))
    assert (code, summary["status"]) == (0, "PASS")


def test_nested_annotated_tag_scans_inner_raw_message(project: GateProject) -> None:
    project.git("tag", "-a", "inner-example", "-m", TERM)
    project.git("tag", "-a", "outer-example", "inner-example", "-m", "Public outer tag")
    project.git("tag", "-d", "inner-example")
    code, summary, _ = project.run("history")
    assert (code, summary["status"]) == (2, "BLOCKED")
    assert "PROTECTED_TERM" in {item["code"] for item in summary["findings"]}


@pytest.mark.parametrize("field", ["message", "author_name", "committer_name", "author_email"])
def test_blocked_metadata_receipt_never_retains_raw_value(project: GateProject, field: str) -> None:
    metadata = json.loads(project.metadata.read_text())
    value = "password=synthetic-example-value"
    if field.endswith("email"):
        value += "@example.org"
    metadata[field] = value
    project.write("metadata.json", metadata)
    code, summary, receipt = project.run()
    assert (code, summary["status"]) == (2, "BLOCKED")
    assert value not in receipt.read_text()
    assert "metadata" not in json.loads(receipt.read_text())["binding"]


def test_sorted_metadata_waivers_survive_identical_commit(project: GateProject) -> None:
    metadata = json.loads(project.metadata.read_text())
    metadata["author_name"] = "Example 172.20.1.9"
    metadata["message"] = "Example 172.20.1.8\n"
    project.env["GIT_AUTHOR_NAME"] = metadata["author_name"]
    project.metadata.write_text(json.dumps(metadata, sort_keys=True))
    code, summary, first = project.run()
    assert (code, summary["status"]) == (2, "BLOCKED")
    findings = json.loads(first.read_text())["findings"]
    assert len(findings) == 2
    assert all(item["code"] == "PRIVATE_ADDRESS" for item in findings)
    exceptions = [
        {
            **{key: item[key] for key in ("label", "content_digest", "code", "offset")},
            "justification": "Synthetic fixture",
            "synthetic": True,
        }
        for item in findings
    ]
    code, summary, receipt = project.run(review=project.review(first, exceptions=exceptions))
    assert (code, summary["status"]) == (0, "PASS")
    commit = project.git(
        "commit-tree", project.git("write-tree").strip(), "-p", metadata["parent"], data=metadata["message"].encode()
    ).strip()
    code, summary, _ = project.run("commit", commit_sha=commit, candidate_receipt=receipt)
    assert (code, summary["status"]) == (0, "PASS")


@pytest.mark.parametrize("address", ["fd00::7", "fe80::7", "::1", "127.0.0.7", "169.254.1.7"])
def test_nonpublic_network_ranges_are_findings(project: GateProject, address: str) -> None:
    project.put("orders.txt", address)
    project.git("add", ".")
    code, summary, _ = project.run()
    assert (code, summary["status"]) == (2, "BLOCKED")
    assert "PRIVATE_ADDRESS" in {item["code"] for item in summary["findings"]}


@pytest.mark.parametrize("arguments", [["--help"], ["candidate", "--help"]])
def test_static_help_is_non_disclosing_json(arguments: list[str]) -> None:
    result = subprocess.run([sys.executable, str(GATE), *arguments], capture_output=True, check=True)
    assert result.stderr == b""
    payload = json.loads(result.stdout)
    assert payload["schema"] == "dpone.public-clean-help.v1"
    assert "candidate" in payload["commands"]
    assert payload["guide"] == "docs/public-clean-migration.md"


def test_operator_review_and_commit_programs_execute(project: GateProject) -> None:
    import shutil

    source = GATE.parent
    destination = project.root / "tools/agent_policy"
    destination.mkdir(parents=True)
    for path in [*source.glob("public_clean_*.py"), source / "tenant_hygiene.py"]:
        shutil.copy2(path, destination / path.name)
    _, summary, _ = project.run(receipt=project.private / "candidate.json")
    assert summary["status"] == "REVIEW_REQUIRED"
    doc = (GATE.parents[2] / "docs/public-clean-migration.md").read_text()
    programs = re.findall(r"```bash\nuv run python - <<'PYCODE'\n(.*?)\nPYCODE\n```", doc, re.S)
    assert len(programs) == 2
    environment = {
        **project.env,
        "PRIVATE_DIR": str(project.private),
        "PUBLIC_ROOT": str(project.root),
        "PUBLIC_REVIEWER": IDENTITY,
    }
    for program in programs:
        result = subprocess.run(
            [sys.executable, "-B", "-c", program], cwd=project.root, env=environment, capture_output=True
        )
        assert result.returncode == 0, result.stderr.decode()
    receipt = json.loads((project.private / "verified-commit.json").read_text())
    assert receipt["status"] == "PASS"
    assert project.git("rev-parse", "HEAD^").strip() == receipt["binding"]["parent"]


def test_credential_shaped_locator_is_not_copied_to_receipt(project: GateProject) -> None:
    token = "ghp_" + "a" * 30
    project.put(token + ".txt", "orders\n")
    project.git("add", ".")
    code, summary, receipt = project.run()
    assert (code, summary["status"]) == (3, "UNABLE_TO_CERTIFY")
    assert not receipt.exists()


@pytest.mark.parametrize("failure", ["missing", "timeout"])
def test_operator_helpers_redact_failures(project: GateProject, failure: str) -> None:

    doc = (GATE.parents[2] / "docs/public-clean-migration.md").read_text()
    programs = re.findall(r"```bash\nuv run python - <<'PYCODE'\n(.*?)\nPYCODE\n```", doc, re.S)
    marker = "synthetic-private-directory-marker"
    environment = {**project.env, "PRIVATE_DIR": str(project.private / marker), "PUBLIC_ROOT": str(project.root)}
    injection = (
        (
            "import subprocess\ndef fail(*a, **k):\n    raise subprocess.TimeoutExpired('" + marker + "', 1)\n"
            "import tools.agent_policy.public_clean_receipts as receipts\nreceipts.read_json = fail\n"
        )
        if failure == "timeout"
        else ""
    )
    for program in programs:
        result = subprocess.run(
            [sys.executable, "-B", "-c", injection + program],
            cwd=GATE.parents[2],
            env=environment,
            capture_output=True,
        )
        assert result.returncode != 0
        assert marker.encode() not in result.stdout + result.stderr
        assert b"Private migration helper failed" in result.stderr
