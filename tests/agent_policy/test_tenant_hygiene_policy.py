"""Policy behavior uses fictional identifiers, never a production deny list."""

from __future__ import annotations

import json
from ipaddress import IPv4Address
from pathlib import Path

import pytest
from tests.agent_policy.test_tenant_hygiene import _commit, _load_module, _wheel

hygiene = _load_module()


def policy(tmp_path: Path, **overrides: object) -> Path:
    value = {
        "schema": "dpone.tenant-hygiene-policy.v2",
        "terms": ["ExampleTenant", "ПримерКлиента"],
        "jira_prefixes": ["ABC"],
        "detectors": ["rfc1918"],
        **overrides,
    }
    path = tmp_path / "policy.json"
    path.write_text("#!dpone.tenant-hygiene-policy.v2\n" + json.dumps(value), encoding="utf-8")
    return path


@pytest.mark.parametrize(
    "content",
    [
        "EXAMPLE-TENANT",
        "example_tenant",
        "Example.Tenant",
        "example/tenant",
        "example\\tenant",
        "ПрИмЕр_КлИеНтА",
        "a-b-c_123",
        "ABC-1",
        str(IPv4Address(0x0A000001)),
        str(IPv4Address(0xAC100001)),
        str(IPv4Address(0xAC1FFFFF)),
        str(IPv4Address(0xC0A80001)),
        "cloud_project=example-tenant-prod",
        "application_id=org.exampletenant.app",
        "\x00/Users/person/EXAMPLE_TENANT/work/task.py\x00",
    ],
)
def test_structured_policy_detects_protected_values(tmp_path: Path, content: str) -> None:
    archive = _wheel(tmp_path / "input.whl", {"module.py": content.encode()})
    report = hygiene.evaluate_archives(paths=[archive], policy_path=policy(tmp_path))
    assert report.status == "FAIL"
    assert content not in hygiene.render_report(report)


@pytest.mark.parametrize(
    "content",
    [
        "172.15.255.255",
        "172.32.0.0",
        "192.169.1.1",
        "127.0.0.1",
        "192.0.2.1",
        "10.999.1.1",
        "110.0.0.1",
        "1.10.0.0.1",
        "ABC-",
        "XABC-1",
        "ABC-1x",
    ],
)
def test_structured_policy_avoids_unrelated_values(tmp_path: Path, content: str) -> None:
    archive = _wheel(tmp_path / "input.whl", {"module.py": content.encode()})
    assert hygiene.evaluate_archives(paths=[archive], policy_path=policy(tmp_path)).status == "PASS"


@pytest.mark.parametrize("size", [1, 2, 3, 7, 64])
def test_structured_match_is_independent_of_archive_read_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    size: int,
) -> None:
    monkeypatch.setattr(hygiene, "READ_CHUNK_BYTES", size)
    archive = _wheel(tmp_path / "input.whl", {"module.py": "xx ПрИмЕр---КлИеНтА".encode()})
    assert hygiene.evaluate_archives(paths=[archive], policy_path=policy(tmp_path)).status == "FAIL"


@pytest.mark.parametrize(
    "changes",
    [
        {"schema": "unknown"},
        {"terms": []},
        {"terms": ["___"]},
        {"terms": "abc"},
        {"detectors": ["arbitrary"]},
        {"extra": "hidden"},
        {"jira_prefixes": [".*"]},
    ],
)
def test_invalid_structured_policy_fails_without_disclosure(tmp_path: Path, changes: dict) -> None:
    archive = _wheel(tmp_path / "input.whl", {"module.py": b"clean"})
    report = hygiene.evaluate_archives(paths=[archive], policy_path=policy(tmp_path, **changes))
    assert report.status == "UNABLE_TO_CERTIFY"
    assert report.findings[0].code == "DPONE_HYGIENE_POLICY_INVALID"


def test_source_and_archive_share_normalization_and_path_redaction(tmp_path: Path) -> None:
    repo, sha = _commit(tmp_path, {"EXAMPLE-TENANT/file.py": b"clean"})
    report = hygiene.evaluate_source(root=repo, commit_sha=sha, policy_path=policy(tmp_path))
    assert report.status == "FAIL"
    assert {f.path for f in report.findings} == {"$PROTECTED_PATH"}


def test_invalid_bytes_remain_a_barrier(tmp_path: Path) -> None:
    archive = _wheel(tmp_path / "input.whl", {"module.py": b"Example\xffTenant"})
    assert hygiene.evaluate_archives(paths=[archive], policy_path=policy(tmp_path)).status == "PASS"


def test_legacy_policy_keeps_exact_case_sensitive_literals(tmp_path: Path) -> None:
    path = tmp_path / "legacy.txt"
    path.write_text("ExampleTenant\n")
    archive = _wheel(tmp_path / "input.whl", {"module.py": b"exampletenant"})
    assert hygiene.evaluate_archives(paths=[archive], policy_path=path).status == "PASS"


@pytest.mark.parametrize("literal", ["{example}", '{"schema":"literal"}', "[literal]"])
def test_legacy_brace_literals_remain_literal(tmp_path: Path, literal: str) -> None:
    path = tmp_path / "legacy.txt"
    path.write_text(literal)
    archive = _wheel(tmp_path / "input.whl", {"module.py": literal.encode()})
    assert hygiene.evaluate_archives(paths=[archive], policy_path=path).status == "FAIL"


@pytest.mark.parametrize(
    "payload",
    [
        "#!dpone.tenant-hygiene-policy.v3\n{}",
        "#!dpone.tenant-hygiene-policy.v2\n{",
        "#!dpone.tenant-hygiene-policy.v2\n[]",
        '#!dpone.tenant-hygiene-policy.v2\n{"schema":"secret","schema":"duplicate"}',
    ],
)
def test_version_header_fails_closed(tmp_path: Path, payload: str) -> None:
    path = tmp_path / "invalid.txt"
    path.write_text(payload)
    archive = _wheel(tmp_path / "input.whl", {"module.py": b"clean"})
    report = hygiene.evaluate_archives(paths=[archive], policy_path=path)
    assert report.status == "UNABLE_TO_CERTIFY"
    assert "secret" not in hygiene.render_report(report)


def test_unicode_casefold_expansion(tmp_path: Path) -> None:
    archive = _wheel(tmp_path / "input.whl", {"module.py": "STRASSE".encode()})
    assert hygiene.evaluate_archives(paths=[archive], policy_path=policy(tmp_path, terms=["Straße"])).status == "FAIL"


@pytest.mark.parametrize("separator", [" ", "\t", "\r", "\n", "_", "-", ".", "/", "\\"])
def test_separator_normalization(tmp_path: Path, separator: str) -> None:
    archive = _wheel(tmp_path / "input.whl", {"module.py": ("Example" + separator * 100 + "Tenant").encode()})
    assert hygiene.evaluate_archives(paths=[archive], policy_path=policy(tmp_path)).status == "FAIL"


@pytest.mark.parametrize("changes", [{"terms": ["a" * 257]}, {"terms": ["x"] * 1025}, {"jira_prefixes": [1]}])
def test_structured_policy_resource_limits(tmp_path: Path, changes: dict) -> None:
    archive = _wheel(tmp_path / "input.whl", {"module.py": b"clean"})
    assert (
        hygiene.evaluate_archives(paths=[archive], policy_path=policy(tmp_path, **changes)).status
        == "UNABLE_TO_CERTIFY"
    )
