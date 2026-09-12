"""Re-derive exact non-credential syntax evidence; never grant review authority."""

import _json
import ast
import hashlib
import json
import json.scanner
import platform
import sys
import tokenize
from pathlib import Path, PurePosixPath
from types import ModuleType

from tools.agent_policy.public_clean_receipts import GateError, scanner_identity

ROLES = frozenset({"permission_declaration", "python_annotation", "schema_field_declaration"})


def _hash(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _module_path(module: ModuleType) -> Path:
    filename = getattr(module, "__file__", None)
    return Path(filename if isinstance(filename, str) else sys.executable)


def _parser(role: str) -> dict:
    # Builtin parser accelerators are bound by the exact interpreter binary.
    modules = [ast, tokenize] if role == "python_annotation" else [json, json.decoder, json.scanner, _json]
    if role == "permission_declaration":
        import yaml
        from yaml import composer, constructor, error, events, loader, nodes, parser, reader, resolver, scanner, tokens

        modules = [yaml, composer, constructor, error, events, loader, nodes, parser, reader, resolver, scanner, tokens]
    dependencies = {}
    if role == "schema_field_declaration":
        import attr
        import attrs
        import jsonschema
        import jsonschema_specifications
        import referencing
        import rpds

        for package in (attr, attrs, jsonschema, jsonschema_specifications, referencing, rpds):
            root = _module_path(package).parent
            for source in sorted(root.rglob("*")):
                if (
                    source.is_file()
                    and "__pycache__" not in source.parts
                    and (
                        source.suffix in {".py", ".json", ".so", ".pyd"}
                        or package is jsonschema_specifications
                        and source.is_relative_to(root / "schemas")
                    )
                ):
                    dependencies[package.__name__ + "/" + source.relative_to(root).as_posix()] = _hash(
                        source.read_bytes()
                    )
    return {
        "validator_dependencies": dependencies,
        "implementation": sys.implementation.name,
        "version": platform.python_version(),
        "executable_sha256": _hash(Path(sys.executable).read_bytes()),
        "modules": {module.__name__: _hash(_module_path(module).read_bytes()) for module in modules},
    }


def build_syntax_evidence(entry: dict, raw: bytes, policy, role: str) -> dict:
    """Construct deterministic context evidence for later independent exact review."""
    from tools.agent_policy import public_clean_policy as scanner

    if type(role) is not str or role not in ROLES or type(raw) is not bytes or len(raw) > 256 * 1024:
        raise GateError("SYNTAX_INPUT_INVALID")
    label = entry.get("label")
    offset = entry.get("offset")
    if type(label) is not str or type(offset) is not int or offset < 0:
        raise GateError("SYNTAX_IDENTITY_INVALID")
    path = PurePosixPath(label)
    if path.is_absolute() or ".." in path.parts or str(path) != label:
        raise GateError("SYNTAX_IDENTITY_INVALID")
    if entry.get("code") != "CREDENTIAL_SHAPE" or entry.get("content_digest") != _hash(raw):
        raise GateError("SYNTAX_SOURCE_MISMATCH")
    parser_before = _parser(role)
    try:
        text = raw.decode("utf-8")
        ends = {
            m.end()
            for code, pattern in scanner._RULES
            if code == "CREDENTIAL_SHAPE"
            for m in pattern.finditer(text)
            if m.start() == offset
        }
        if len(ends) != 1:
            raise GateError("SYNTAX_SPAN_INVALID")
        end = ends.pop()
        check = scanner.Scanner(policy, scanner.Budget())
        check.scan(label, raw)
        if any(row.code in {"PROTECTED_TERM", "PROTECTED_TICKET"} for row in check.findings):
            raise GateError("SYNTAX_PROTECTED")
        if role == "python_annotation" and path.suffix == ".py":
            from tools.agent_policy.public_clean_syntax_python import annotation_context

            begin, finish = annotation_context(text, offset, end)
        elif role == "schema_field_declaration" and path.suffix == ".json":
            from tools.agent_policy.public_clean_syntax_schema import schema_context

            begin, finish = schema_context(text, offset, end)
        elif role == "permission_declaration":
            import yaml
            from tools.agent_policy.public_clean_syntax_permission import permission_context

            try:
                begin, finish = permission_context(text, label, offset, end)
            except yaml.YAMLError as exc:
                raise GateError("SYNTAX_YAML_INVALID") from exc
        else:
            raise GateError("SYNTAX_ROLE_LOCATION")
    except GateError:
        raise
    except (ValueError, TypeError, SyntaxError, IndexError, KeyError, RecursionError) as exc:
        raise GateError("SYNTAX_UNPROVEN") from exc
    if parser_before != _parser(role):
        raise GateError("SYNTAX_PARSER_CHANGED")
    return {
        "schema": "dpone.non-credential-syntax.v1",
        "role": role,
        "source_sha256": _hash(raw),
        "label": label,
        "offset": offset,
        "end": end,
        "context_start": begin,
        "context_end": finish,
        "context_sha256": _hash(text[begin:finish].encode()),
        "policy_digest": policy.digest,
        "scanner_identity": scanner_identity(),
        "parser_identity": parser_before,
    }


def verify_non_credential_syntax(entry: dict, raw: bytes, policy) -> None:
    """Reject unsupported roles, ambiguous syntax, stale or malformed exact evidence."""
    evidence = entry.get("context_evidence")
    if (
        entry.get("classification") != "NON_CREDENTIAL_SYNTAX"
        or entry.get("synthetic") is not False
        or type(evidence) is not dict
    ):
        raise GateError("SYNTAX_REVIEW_INVALID")
    role = evidence.get("role")
    if not isinstance(role, str):
        raise GateError("SYNTAX_REVIEW_INVALID")
    actual = build_syntax_evidence(entry, raw, policy, role)
    try:
        equal = json.dumps(evidence, sort_keys=True, allow_nan=False) == json.dumps(
            actual, sort_keys=True, allow_nan=False
        )
    except (ValueError, TypeError) as exc:
        raise GateError("SYNTAX_EVIDENCE_MISMATCH") from exc
    if not equal:
        raise GateError("SYNTAX_EVIDENCE_MISMATCH")
