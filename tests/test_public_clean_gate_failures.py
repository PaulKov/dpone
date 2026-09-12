"""Fail-closed privacy CLI checks; all adversarial payloads are synthetic."""

from __future__ import annotations

import io
import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from tests.test_public_clean_gate import GATE, TERM, GateProject


@pytest.fixture
def project(tmp_path: Path) -> GateProject:
    return GateProject(tmp_path)


@pytest.mark.parametrize(
    "payload",
    [b"{}", b"{", b"\xff", b'{"schema":"one","schema":"two"}', b'{"value":NaN}'],
)
def test_malformed_policy_fails_closed(project: GateProject, payload: bytes) -> None:
    project.policy.write_bytes(payload)
    code, summary, _ = project.run()
    assert (code, summary["status"]) == (3, "UNABLE_TO_CERTIFY")


@pytest.mark.parametrize("change", ["empty_terms", "unknown", "missing", "empty_reviewers"])
def test_invalid_policy_contract(project: GateProject, change: str) -> None:
    payload = json.loads(project.policy.read_text())
    if change == "unknown":
        payload["allow_all"] = True
    elif change == "missing":
        del payload["allowed_hosts"]
    else:
        payload["protected_terms" if change == "empty_terms" else "reviewers"] = []
    project.write("policy.json", payload)
    code, summary, _ = project.run()
    assert (code, summary["status"]) == (3, "UNABLE_TO_CERTIFY")


@pytest.mark.parametrize("target", ["policy", "metadata"])
def test_symlink_private_input_rejected(project: GateProject, target: str) -> None:
    path = getattr(project, target)
    original = path.read_bytes()
    path.unlink()
    real = project.private / "real.json"
    real.write_bytes(original)
    path.symlink_to(real)
    code, summary, _ = project.run()
    assert (code, summary["status"]) == (3, "UNABLE_TO_CERTIFY")


@pytest.mark.parametrize("unsafe", ["repository", "existing", "symlink"])
def test_receipt_output_safety(project: GateProject, unsafe: str) -> None:
    path = project.private / "output.json"
    if unsafe == "repository":
        path = project.root / "output.json"
    elif unsafe == "existing":
        path.write_text("preserve")
    else:
        target = project.private / "target.json"
        target.write_text("preserve")
        path.symlink_to(target)
    code, summary, _ = project.run(receipt=path)
    assert (code, summary["status"]) == (3, "UNABLE_TO_CERTIFY")
    if unsafe == "repository":
        assert not path.exists()
    else:
        assert path.read_text() == "preserve"


@pytest.mark.parametrize("mutation", ["tree", "metadata", "policy", "reviewer", "approval", "scanner"])
def test_review_cannot_clear_changed_candidate(project: GateProject, mutation: str) -> None:
    _, _, first = project.run()
    review = project.review(first)
    if mutation == "tree":
        project.put("orders.txt", "changed orders\n")
        project.git("add", ".")
    elif mutation == "metadata":
        metadata = json.loads(project.metadata.read_text())
        metadata["message"] = "Different message\n"
        project.write("metadata.json", metadata)
    elif mutation == "policy":
        policy = json.loads(project.policy.read_text())
        policy["protected_terms"].append("AnotherSyntheticMarker")
        project.write("policy.json", policy)
    else:
        payload = json.loads(review.read_text())
        key, value = {
            "reviewer": ("reviewer", "unknown@example.org"),
            "approval": ("approved", False),
            "scanner": ("scanner_identity", "0" * 64),
        }[mutation]
        payload[key] = value
        review.write_text(json.dumps(payload))
    code, summary, _ = project.run(review=review)
    assert code in (2, 3)
    assert summary["status"] != "PASS"


@pytest.mark.parametrize("mutation", ["message", "author_date", "tree", "extra_header"])
def test_commit_receipt_rejects_actual_identity_mismatch(project: GateProject, mutation: str) -> None:
    _, _, first = project.run()
    _, approved, receipt = project.run(review=project.review(first))
    assert approved["status"] == "PASS"
    metadata = json.loads(project.metadata.read_text())
    if mutation == "tree":
        project.put("orders.txt", "different\n")
        project.git("add", ".")
    tree = project.git("write-tree").strip()
    author_date = "1700000001 +0000" if mutation == "author_date" else metadata["author_date"]
    headers = [
        f"tree {tree}",
        f"parent {metadata['parent']}",
        f"author {metadata['author_name']} <{metadata['author_email']}> {author_date}",
        f"committer {metadata['committer_name']} <{metadata['committer_email']}> {metadata['committer_date']}",
    ]
    if mutation == "extra_header":
        headers.append("encoding UTF-8")
    message = "Different message\n" if mutation == "message" else metadata["message"]
    raw = ("\n".join(headers) + "\n\n" + message).encode()
    commit = project.git("hash-object", "-t", "commit", "-w", "--stdin", data=raw).strip()
    code, summary, _ = project.run("commit", commit_sha=commit, candidate_receipt=receipt)
    assert code in (2, 3)
    assert summary["status"] != "PASS"


@pytest.mark.parametrize(
    "kind",
    ["compiled", "binary", "archive_term", "archive_path", "traversal", "duplicate", "ratio", "trailing", "nested"],
)
def test_binary_archive_inspection_never_silently_passes(project: GateProject, kind: str) -> None:
    if kind == "compiled":
        filename, raw = "generated.pyc", b"\xa7\r\r\n\x00" + TERM.encode()
    elif kind == "binary":
        filename, raw = "generated.bin", b"\x00\xff\x01"
    else:
        filename = "orders.zip"
        content = io.BytesIO()
        with zipfile.ZipFile(content, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            name = "../orders.txt" if kind == "traversal" else "orders.txt"
            if kind == "archive_path":
                name = TERM + ".txt"
            value = TERM.encode() if kind == "archive_term" else b"orders"
            if kind == "ratio":
                value = b"a" * 2_000_000
            if kind == "nested":
                inner = io.BytesIO()
                with zipfile.ZipFile(inner, "w") as nested:
                    nested.writestr("orders.txt", b"orders")
                name, value = "nested.zip", inner.getvalue()
            archive.writestr(name, value)
            if kind == "duplicate":
                with pytest.warns(UserWarning):
                    archive.writestr(name, value)
        raw = content.getvalue() + (b"unparsed" if kind == "trailing" else b"")
    project.put(filename, raw)
    project.git("add", ".")
    code, summary, _ = project.run()
    assert code in (2, 3)
    assert summary["status"] in ("BLOCKED", "UNABLE_TO_CERTIFY")


def test_bootstrap_import_error_is_redacted(tmp_path: Path) -> None:
    """Run an isolated entrypoint with unavailable siblings, not implementation mocks."""
    entry = tmp_path / "public_clean_gate.py"
    entry.write_bytes(GATE.read_bytes())
    result = subprocess.run([sys.executable, str(entry), "history"], capture_output=True, timeout=10)
    assert result.returncode == 3
    assert result.stderr == b""
    payload = json.loads(result.stdout)
    assert payload["status"] == "UNABLE_TO_CERTIFY"
    assert str(tmp_path) not in result.stdout.decode()
    assert "Traceback" not in result.stdout.decode()


@pytest.mark.parametrize("kind", ["symlink", "oversize_policy", "unknown_metadata", "empty_index", "invalid_args"])
def test_input_boundaries_are_closed(project: GateProject, kind: str) -> None:
    if kind == "symlink":
        (project.root / "customers.txt").symlink_to(project.private / "metadata.json")
        project.git("add", ".")
    elif kind == "oversize_policy":
        project.policy.write_bytes(b" " * (1024 * 1024 + 1))
    elif kind == "unknown_metadata":
        metadata = json.loads(project.metadata.read_text())
        metadata["signature"] = "unsupported"
        project.write("metadata.json", metadata)
    elif kind == "empty_index":
        project.git("rm", "orders.txt")
    code, summary, _ = project.run(unknown_option="value") if kind == "invalid_args" else project.run()
    assert code in (2, 3)
    assert summary["status"] in ("BLOCKED", "UNABLE_TO_CERTIFY")


def test_worktree_second_inventory_rejects_previously_scanned_file_mutation(
    project: GateProject, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tools.agent_policy import public_clean_worktree as worktree
    from tools.agent_policy.public_clean_candidate import Git
    from tools.agent_policy.public_clean_policy import Budget, Scanner, load_policy
    from tools.agent_policy.public_clean_receipts import GateError

    project.put("z.txt", "customers\n")
    original = worktree.inspect_content
    mutated = False

    def inspect_and_mutate(label: str, raw: bytes, scanner: Scanner) -> None:
        nonlocal mutated
        original(label, raw, scanner)
        if label == "z.txt":
            project.put("orders.txt", "changed after scanning\n")
            mutated = True

    monkeypatch.setattr(worktree, "inspect_content", inspect_and_mutate)
    budget = Budget()
    scanner = Scanner(load_policy(json.loads(project.policy.read_text())), budget)
    with pytest.raises(GateError) as raised:
        worktree.scan_worktree(Git(project.root, budget), scanner)
    assert raised.value.code == "WORKTREE_CHANGED"
    assert mutated


@pytest.mark.parametrize("behavior", ["hang", "overflow"])
def test_git_process_timeout_and_output_limits_terminate_child(
    project: GateProject, monkeypatch: pytest.MonkeyPatch, behavior: str
) -> None:
    import os
    import shlex
    import time

    from tools.agent_policy.public_clean_candidate import Git
    from tools.agent_policy.public_clean_policy import Budget
    from tools.agent_policy.public_clean_receipts import GateError

    executables = project.private / "bin"
    executables.mkdir()
    marker = project.private / "pid"
    fake = executables / "git"
    fake.write_text(
        "#!/bin/sh\n"
        f"printf '%s' \"$$\" > {shlex.quote(str(marker))}\n"
        + ("printf '%s' '" + "x" * 4096 + "'\n" if behavior == "overflow" else "")
        + "exec /bin/sleep 60\n"
    )
    fake.chmod(0o700)
    monkeypatch.setenv("PATH", str(executables))
    start = time.monotonic()
    code = "GIT_TIMEOUT" if behavior == "hang" else "GIT_OUTPUT_LIMIT"
    with pytest.raises(GateError) as raised:
        Git(project.root, Budget(), timeout=2).run("rev-parse", "HEAD", limit=32)
    assert raised.value.code == code
    assert time.monotonic() - start < 3
    assert marker.exists(), "The real child must have started for this test to exercise termination"
    with pytest.raises(ProcessLookupError):
        os.kill(int(marker.read_text()), 0)


def test_hanging_bootstrap_import_hits_redacted_deadline(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    import builtins
    import signal
    import time

    from tools.agent_policy.public_clean_gate import main

    original_import, original_timer = builtins.__import__, signal.setitimer

    def imported(name, *args, **kwargs):
        if name == "tools.agent_policy.public_clean_candidate":
            time.sleep(10)
        return original_import(name, *args, **kwargs)

    def timer(which, seconds, interval=0):
        return original_timer(which, min(seconds, 0.05), interval)

    monkeypatch.setattr(builtins, "__import__", imported)
    monkeypatch.setattr(signal, "setitimer", timer)
    started = time.monotonic()
    assert main(["candidate"]) == 3
    assert time.monotonic() - started < 1
    output = capsys.readouterr()
    assert output.err == ""
    assert json.loads(output.out)["findings"] == [{"item": 0, "code": "SCAN_TIMEOUT"}]


def test_missing_promisor_blob_never_fetches_or_writes_objects(project: GateProject) -> None:
    from tools.agent_policy.public_clean_candidate import Git
    from tools.agent_policy.public_clean_policy import Budget
    from tools.agent_policy.public_clean_receipts import GateError

    donor = project.private / "donor"
    subprocess.run(
        ["git", "clone", "--no-local", str(project.root), str(donor)], check=True, capture_output=True, env=project.env
    )
    oid = project.git("rev-parse", "HEAD:orders.txt").strip()
    project.git("config", "remote.origin.url", str(donor))
    project.git("config", "remote.origin.promisor", "true")
    project.git("config", "remote.origin.partialclonefilter", "blob:none")
    project.git("config", "extensions.partialClone", "origin")
    objects = project.root / ".git/objects"
    (objects / oid[:2] / oid[2:]).unlink()
    before = {str(path.relative_to(objects)): path.read_bytes() for path in objects.rglob("*") if path.is_file()}
    with pytest.raises(GateError) as raised:
        Git(project.root, Budget()).blob(oid)
    assert raised.value.code == "GIT_UNAVAILABLE"
    after = {str(path.relative_to(objects)): path.read_bytes() for path in objects.rglob("*") if path.is_file()}
    assert after == before


def test_receipt_ancestor_substitution_never_writes_into_replacement(
    project: GateProject, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tools.agent_policy import public_clean_receipts as receipts

    parent = project.private / "receipts"
    parent.mkdir(mode=0o700)
    pinned = project.private / "pinned"
    replacement = project.private / "replacement"
    replacement.mkdir(mode=0o700)
    original_open = receipts.os.open
    swapped = False

    def substitute(path, flags, *args, **kwargs):
        nonlocal swapped
        if str(path).startswith(".public-clean-") and not swapped:
            parent.rename(pinned)
            parent.symlink_to(replacement, target_is_directory=True)
            swapped = True
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(receipts.os, "open", substitute)
    with pytest.raises((OSError, receipts.GateError)):
        receipts.write_receipt(parent / "receipt.json", project.root, {"status": "PASS"})
    assert swapped
    assert list(replacement.iterdir()) == []
    assert list(pinned.iterdir()) == []


@pytest.mark.parametrize("failure", ["file_sync", "directory_sync", "cleanup", "post_sync"])
def test_receipt_atomic_publication_failure_semantics(
    project: GateProject, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    from tools.agent_policy import public_clean_receipts as receipts

    path = project.private / "receipt.json"
    sync = receipts.os.fsync
    unlink = receipts.os.unlink
    calls = 0

    def injected_sync(descriptor):
        nonlocal calls
        calls += 1
        if calls == {"file_sync": 1, "directory_sync": 2, "post_sync": 3}.get(failure):
            raise OSError("Synthetic sync failure")
        return sync(descriptor)

    def injected_unlink(name, *args, **kwargs):
        if failure == "cleanup" and str(name).startswith(".public-clean-"):
            raise OSError("Synthetic cleanup failure")
        return unlink(name, *args, **kwargs)

    monkeypatch.setattr(receipts.os, "fsync", injected_sync)
    monkeypatch.setattr(receipts.os, "unlink", injected_unlink)
    payload = {"status": "PASS", "complete": True}
    if failure in {"file_sync", "directory_sync"}:
        with pytest.raises(OSError):
            receipts.write_receipt(path, project.root, payload)
        assert not path.exists()
    else:
        receipts.write_receipt(path, project.root, payload)
        assert json.loads(path.read_text()) == payload
        assert path.stat().st_mode & 0o777 == 0o600
