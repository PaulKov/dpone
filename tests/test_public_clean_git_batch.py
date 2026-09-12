"""Real pipe protocol faults and exact local Git acquisition compatibility."""

import hashlib
import os
import subprocess
import sys
import time
from contextlib import contextmanager

import pytest
from tools.agent_policy import public_clean_git_batch as transport
from tools.agent_policy.public_clean_candidate import Git, scan_candidate, scan_tree
from tools.agent_policy.public_clean_policy import Budget, Policy, Scanner
from tools.agent_policy.public_clean_receipts import GateError


def oid(raw):
    return hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()


@pytest.fixture
def fake(monkeypatch):
    children = []
    original = subprocess.Popen

    def install(body):
        code = "import sys,os,time\nrequest=sys.stdin.buffer.readline().strip()\n" + body

        def factory(command, **kwargs):
            child = original([sys.executable, "-u", "-c", code], **kwargs)
            children.append(child)
            return child

        monkeypatch.setattr(transport, "Popen", factory)
        return children

    yield install
    assert all(child.poll() is not None for child in children)
    assert all(child.stdin.closed and child.stdout.closed for child in children)


def response(raw):
    return oid(raw).encode() + b" blob " + str(len(raw)).encode() + b"\n" + raw + b"\n"


@pytest.mark.parametrize("raw", [b"", b"test", b"\0\xff\n\r\x80"])
def test_binary_fragmented(fake, tmp_path, raw):
    fake("for byte in " + repr(response(raw)) + ": os.write(1,bytes([byte]))\nsys.stdin.buffer.read()\n")
    with transport.GitBatch(tmp_path, Budget(), timeout=2) as batch:
        assert batch.blob(oid(raw)) == raw


@pytest.mark.parametrize(
    "kind",
    [
        "missing",
        "wrong_oid",
        "wrong_type",
        "leading_zero",
        "negative",
        "plus",
        "header_long",
        "oversize",
        "terminator",
        "wrong_hash",
        "truncated",
        "extra",
    ],
)
def test_protocol_faults(fake, tmp_path, kind):
    raw = b"test"
    digest = oid(raw).encode()
    data = response(raw)
    if kind == "missing":
        data = digest + b" missing\n"
    if kind == "wrong_oid":
        data = b"0" * 40 + b" blob 4\ntest\n"
    if kind == "wrong_type":
        data = digest + b" commit 4\ntest\n"
    if kind == "leading_zero":
        data = digest + b" blob 04\ntest\n"
    if kind == "negative":
        data = digest + b" blob -4\n"
    if kind == "plus":
        data = digest + b" blob +4\ntest\n"
    if kind == "header_long":
        data = b"x" * 129
    if kind == "oversize":
        data = digest + b" blob " + str(transport.MAX_SOURCE_BLOB_BYTES + 1).encode() + b"\n"
    if kind == "terminator":
        data = data[:-1] + b"x"
    if kind == "wrong_hash":
        data = digest + b" blob 4\nfail\n"
    if kind == "truncated":
        data = digest + b" blob 4\nte"
    if kind == "extra":
        data += b"extra"
    ending = "" if kind == "truncated" else "sys.stdin.buffer.read()\n"
    children = fake("os.write(1," + repr(data) + ")\n" + ending)
    with pytest.raises(GateError):
        with transport.GitBatch(tmp_path, Budget(), timeout=2) as batch:
            batch.blob(oid(raw))
    assert children[0].poll() is not None
    with pytest.raises(GateError):
        batch.blob(oid(raw))


@pytest.mark.parametrize("kind", ["header", "body", "write", "shutdown"])
def test_stalls(fake, tmp_path, monkeypatch, kind):
    code = "time.sleep(5)\n"
    if kind == "body":
        code = "os.write(1," + repr(oid(b"test").encode() + b" blob 4\n") + ")\ntime.sleep(5)\n"
    if kind == "shutdown":
        code = "os.write(1," + repr(response(b"test")) + ")\nsys.stdin.buffer.read()\ntime.sleep(5)\n"
    fake(code)
    started = time.monotonic()
    with pytest.raises(GateError):
        with transport.GitBatch(tmp_path, Budget(), timeout=0.15) as batch:
            if kind == "write":

                def block(*args):
                    raise BlockingIOError()

                monkeypatch.setattr(transport.os, "write", block)
            batch.blob(oid(b"test"))
    assert time.monotonic() - started < 3


@pytest.mark.parametrize("kind", ["extra", "nonzero"])
def test_success_shutdown_is_verified(fake, tmp_path, kind):
    last = 'os.write(1,b"extra")' if kind == "extra" else "sys.exit(7)"
    fake("os.write(1," + repr(response(b"test")) + ")\nsys.stdin.buffer.read()\n" + last + "\n")
    with pytest.raises(GateError):
        with transport.GitBatch(tmp_path, Budget(), timeout=2) as batch:
            assert batch.blob(oid(b"test")) == b"test"


def test_partial_request_write(fake, tmp_path, monkeypatch):
    fake("os.write(1," + repr(response(b"test")) + ")\nsys.stdin.buffer.read()\n")
    original = os.write
    with transport.GitBatch(tmp_path, Budget(), timeout=2) as batch:
        monkeypatch.setattr(transport.os, "write", lambda fd, data: original(fd, data[:1]))
        assert batch.blob(oid(b"test")) == b"test"


def test_shared_budget_at_shutdown(fake, tmp_path):
    fake("os.write(1," + repr(response(b"test")) + ")\nsys.stdin.buffer.read()\n")
    budget = Budget()
    with pytest.raises(GateError):
        with transport.GitBatch(tmp_path, budget, timeout=2) as batch:
            batch.blob(oid(b"test"))
            budget.deadline = time.monotonic() - 1


def test_cancellation_preserved(fake, tmp_path):
    fake("time.sleep(5)\n")
    error = KeyboardInterrupt("synthetic cancellation")
    with pytest.raises(KeyboardInterrupt) as caught:
        with transport.GitBatch(tmp_path, Budget(), timeout=2):
            raise error
    assert caught.value is error


def test_invalid_and_closed(fake, tmp_path):
    fake("time.sleep(5)\n")
    with pytest.raises(GateError):
        with transport.GitBatch(tmp_path, Budget(), timeout=2) as batch:
            batch.blob("not-an-object")
    with pytest.raises(GateError):
        batch.__enter__()


def repository(tmp_path):
    subprocess.run(
        ["git", "init", "-q", str(tmp_path)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    for name, raw in [
        ("a.txt", b"password=synthetic-example\n"),
        ("b.txt", b"password=synthetic-example\n"),
        ("empty.txt", b""),
        ("unicode.txt", "Пример\n".encode()),
    ]:
        (tmp_path / name).write_bytes(raw)
    env = os.environ.copy()
    env.update(
        GIT_AUTHOR_NAME="Fixture",
        GIT_AUTHOR_EMAIL="fixture@example.invalid",
        GIT_COMMITTER_NAME="Fixture",
        GIT_COMMITTER_EMAIL="fixture@example.invalid",
    )
    subprocess.run(["git", "-C", str(tmp_path), "add", "."], env=env, check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "-c", "commit.gpgsign=false", "commit", "-qm", "synthetic fixture"],
        env=env,
        check=True,
    )
    return Git(tmp_path, Budget())


def test_actual_git_and_scan_cache(tmp_path, monkeypatch):
    git = repository(tmp_path)
    tree = git.run("rev-parse", "HEAD^{tree}").decode().strip()
    listing = git.run("ls-tree", "-rz", tree)
    object_ids = [line.split(b"\t")[0].split()[2].decode() for line in listing.rstrip(b"\0").split(b"\0")]
    with transport.GitBatch(tmp_path, Budget()) as batch:
        assert [batch.blob(x) for x in object_ids] == [git.blob(x) for x in object_ids]
    policy = Policy("a" * 64, (), (), (), (), ())
    old = Scanner(policy, Budget())
    new = Scanner(policy, Budget())
    import tools.agent_policy.public_clean_candidate as candidate

    @contextmanager
    def standalone(root, budget, timeout):
        yield Git(root, budget, timeout)

    with monkeypatch.context() as patch:
        patch.setattr(candidate, "GitBatch", standalone)
        binding_old = scan_candidate(Git(tmp_path, old.budget), old)
    binding_new = scan_candidate(Git(tmp_path, new.budget), new)
    assert binding_old == binding_new
    assert old.findings and old.findings == new.findings
    assert (old.budget.items, old.budget.bytes) == (new.budget.items, new.budget.bytes)
    calls = []
    original = transport.GitBatch.blob

    def counted(self, value):
        calls.append(value)
        return original(self, value)

    monkeypatch.setattr(transport.GitBatch, "blob", counted)
    scan_tree(git, Scanner(policy, Budget()), tree)
    assert len(calls) == len(set(object_ids))


def test_spawn_error_sanitized(tmp_path, monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("synthetic child-start failure")

    monkeypatch.setattr(transport, "Popen", fail)
    with pytest.raises(GateError):
        with transport.GitBatch(tmp_path, Budget()):
            pass


def test_offline_environment(monkeypatch):
    monkeypatch.setenv("GIT_DIR", "/synthetic/unrelated")
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    command, environment = transport.git_invocation("cat-file", "--batch")
    assert "GIT_DIR" not in environment and "GIT_CONFIG_COUNT" not in environment
    assert environment["GIT_NO_LAZY_FETCH"] == "1" and environment["GIT_ALLOW_PROTOCOL"] == ""
    assert environment["GIT_CONFIG_GLOBAL"] == os.devnull
    assert environment["GIT_NO_REPLACE_OBJECTS"] == "1"
    assert command[-2:] == ["cat-file", "--batch"] and "protocol.allow=never" in command


def test_one_outstanding_request(fake, tmp_path):
    fake("time.sleep(5)\n")
    with pytest.raises(GateError):
        with transport.GitBatch(tmp_path, Budget()) as batch:
            batch._request.acquire()
            try:
                batch.blob(oid(b"test"))
            finally:
                batch._request.release()
