"""Exact syntax roles reject runtime values and stale contextual authority."""

import hashlib

import pytest
from tools.agent_policy.public_clean_policy import _RULES, Policy
from tools.agent_policy.public_clean_receipts import GateError
from tools.agent_policy.public_clean_syntax import build_syntax_evidence, verify_non_credential_syntax

POLICY = Policy("a" * 64, (), (), (), (), ())
CASES = [
    ("python_annotation", "tests/example.py", "def f(password: bytes,):\n    pass\n"),
    (
        "schema_field_declaration",
        "schema.json",
        '{"$schema":"https://json-schema.org/draft/2020-12/schema","properties":{"password": true}}',
    ),
    (
        "permission_declaration",
        ".github/workflows/check.yml",
        "on: push\npermissions:\n  id-token: write\njobs:\n  check:\n    runs-on: ubuntu-latest\n    steps: []\n",
    ),
]


def entry(raw, label):
    text = raw.decode()
    match = next(m for code, pattern in _RULES if code == "CREDENTIAL_SHAPE" for m in pattern.finditer(text))
    return {
        "label": label,
        "offset": match.start(),
        "code": "CREDENTIAL_SHAPE",
        "content_digest": hashlib.sha256(raw).hexdigest(),
        "synthetic": False,
        "classification": "NON_CREDENTIAL_SYNTAX",
    }


@pytest.mark.parametrize("role,label,text", CASES)
def test_positive(role, label, text):
    raw = text.encode()
    row = entry(raw, label)
    row["context_evidence"] = build_syntax_evidence(row, raw, POLICY, role)
    verify_non_credential_syntax(row, raw, POLICY)


@pytest.mark.parametrize(
    "text",
    [
        'def f(password: bytes = b"example"):\n pass\n',
        'password: bytes = b"example"\n',
        "bytes = str\ndef f(password: bytes,):\n pass\n",
        'def f(password: "bytes"):\n pass\n',
        "# password: bytes\n",
        'value = "password: bytes"\n',
        "def bytes(): pass\ndef f(password: bytes,): pass\n",
        "def f(bytes, password: bytes): pass\n",
    ],
)
def test_annotation_reject(text):
    raw = text.encode()
    row = entry(raw, "x.py")
    with pytest.raises(GateError):
        build_syntax_evidence(row, raw, POLICY, "python_annotation")


@pytest.mark.parametrize("change", ["path", "payload", "enum", "duplicate", "alias", "nested"])
def test_permission_reject(change):
    role, label, text = CASES[2]
    if change == "path":
        label = "metadata.yml"
    if change == "payload":
        text = text.replace("permissions:", "env:")
    if change == "enum":
        text = text.replace("write", "example")
    if change == "duplicate":
        text = text.replace("id-token: write", "id-token: write\n  id-token: none")
    if change == "alias":
        text = text.replace("permissions:", "permissions: &p")
    if change == "nested":
        text = text.replace("permissions:\n  id-token: write", "env:\n  permissions:\n    id-token: write")
    raw = text.encode()
    row = entry(raw, label)
    with pytest.raises(GateError):
        build_syntax_evidence(row, raw, POLICY, role)


@pytest.mark.parametrize("change", ["payload", "default", "duplicate", "root", "schema-value"])
def test_schema_reject(change):
    role, label, text = CASES[1]
    if change == "payload":
        text = text.replace("properties", "default")
    if change == "default":
        text = text.replace("true", '"example"')
    if change == "duplicate":
        text = text.replace('"password": true', '"password": true,"password": false')
    if change == "root":
        text = text.replace("2020-12", "unknown")
    if change == "schema-value":
        text = text.replace("true", "null")
    raw = text.encode()
    row = entry(raw, label)
    with pytest.raises(GateError):
        build_syntax_evidence(row, raw, POLICY, role)


@pytest.mark.parametrize(
    "field",
    [
        "role",
        "end",
        "context_sha256",
        "parser_identity",
        "scanner_identity",
        "policy_digest",
        "source_sha256",
        "unknown",
    ],
)
def test_evidence_tamper(field):
    role, label, text = CASES[0]
    raw = text.encode()
    row = entry(raw, label)
    row["context_evidence"] = build_syntax_evidence(row, raw, POLICY, role)
    row["context_evidence"][field] = "changed"
    with pytest.raises(GateError):
        verify_non_credential_syntax(row, raw, POLICY)


@pytest.mark.parametrize("delta", [-1, 1])
def test_partial_span(delta):
    role, label, text = CASES[0]
    raw = text.encode()
    row = entry(raw, label)
    row["offset"] += delta
    with pytest.raises(GateError):
        build_syntax_evidence(row, raw, POLICY, role)


def test_unicode_offset():
    raw = "# Пример\ndef f(password: bytes,): pass\n".encode()
    row = entry(raw, "x.py")
    row["context_evidence"] = build_syntax_evidence(row, raw, POLICY, "python_annotation")
    verify_non_credential_syntax(row, raw, POLICY)


def test_protected():
    role, label, text = CASES[0]
    raw = text.encode()
    row = entry(raw, label)
    policy = Policy("b" * 64, ("password",), (), (), (), ())
    with pytest.raises(GateError):
        build_syntax_evidence(row, raw, policy, role)


@pytest.mark.parametrize(
    "text",
    [
        "change_globals()\ndef f(password: bytes,): pass\n",
        "@decorate\nclass Fields:\n password: bytes\n",
        "match value:\n case {**bytes}: pass\ndef f(password: bytes,): pass\n",
    ],
)
def test_additional_shadowing(text):
    raw = text.encode()
    row = entry(raw, "x.py")
    with pytest.raises(GateError):
        build_syntax_evidence(row, raw, POLICY, "python_annotation")


@pytest.mark.parametrize(
    "field,value",
    [("offset", True), ("offset", -1), ("code", "OTHER"), ("content_digest", "stale"), ("label", "../x.py")],
)
def test_source_identity(field, value):
    role, label, text = CASES[0]
    raw = text.encode()
    row = entry(raw, label)
    row[field] = value
    with pytest.raises(GateError):
        build_syntax_evidence(row, raw, POLICY, role)


@pytest.mark.parametrize(
    "text",
    [
        "on: push\npermissions: [\n id-token: write\njobs: {}",
        "on: push\npermissions:\n id-token: write\njobs: {}\nextra: !custom value",
    ],
)
def test_yaml_invalid(text):
    raw = text.encode()
    row = entry(raw, CASES[2][1])
    with pytest.raises(GateError):
        build_syntax_evidence(row, raw, POLICY, CASES[2][0])


@pytest.mark.parametrize("value", [True, 0, None])
def test_synthetic_conflict(value):
    role, label, text = CASES[0]
    raw = text.encode()
    row = entry(raw, label)
    row["context_evidence"] = build_syntax_evidence(row, raw, POLICY, role)
    row["synthetic"] = value
    with pytest.raises(GateError):
        verify_non_credential_syntax(row, raw, POLICY)


@pytest.mark.parametrize("kind", ["bytes", "depth", "nodes"])
def test_input_limits(kind):
    text = "password: bytes\n"
    if kind == "bytes":
        text = "#" + ("x" * 262144) + "\n" + text
    if kind == "depth":
        text = "x=" + ("(" * 65) + "0" + (")" * 65) + "\n" + text
    if kind == "nodes":
        text = ("x=0\n" * 4000) + text
    raw = text.encode()
    row = entry(raw, "x.py")
    with pytest.raises(GateError):
        build_syntax_evidence(row, raw, POLICY, "python_annotation")


@pytest.mark.parametrize("keyword", ["$defs", "dependentSchemas", "prefixItems"])
def test_wrong_dialect_keyword(keyword):
    import json

    child = {"properties": {"password": True}}
    value = [child] if keyword == "prefixItems" else {"child": child}
    raw = json.dumps({"$schema": "http://json-schema.org/draft-07/schema#", keyword: value}).encode()
    row = entry(raw, "schema.json")
    with pytest.raises(GateError):
        build_syntax_evidence(row, raw, POLICY, "schema_field_declaration")


@pytest.mark.parametrize("change", ["invalid-type", "nested-dialect", "unknown-keyword"])
def test_schema_formal_consumer(change):
    import json

    root = {"$schema": "https://json-schema.org/draft/2020-12/schema", "properties": {"password": True}}
    if change == "invalid-type":
        root["type"] = 123
    if change == "nested-dialect":
        root = {"$schema": root["$schema"], "$defs": {"child": root.copy()}}
    if change == "unknown-keyword":
        root = {"$schema": root["$schema"], "runtime_payload": {"properties": root["properties"]}}
    raw = json.dumps(root).encode()
    row = entry(raw, "schema.json")
    with pytest.raises(GateError):
        build_syntax_evidence(row, raw, POLICY, "schema_field_declaration")


def test_extensionless_metaschema_asset_mutation(tmp_path, monkeypatch):
    import jsonschema_specifications
    from tools.agent_policy.public_clean_syntax import _parser

    package = tmp_path / "package"
    package.mkdir()
    init = package / "__init__.py"
    init.write_text("# synthetic package identity\n")
    schemas = package / "schemas"
    schemas.mkdir()
    asset = schemas / "metaschema"
    asset.write_text('{"type":"object"}')
    monkeypatch.setattr(jsonschema_specifications, "__file__", str(init))
    before = _parser("schema_field_declaration")
    asset.write_text('{"type":"string"}')
    assert before != _parser("schema_field_declaration")
