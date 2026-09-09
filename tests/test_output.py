from __future__ import annotations

from argparse import Namespace

from dpone.output import OutputFormat, dumps_json, dumps_yaml, resolve_output_format


def test_resolve_output_format_prefers_output_over_format() -> None:
    args = Namespace(output="json", format="text", json=False)
    assert resolve_output_format(args) == OutputFormat.json


def test_resolve_output_format_uses_format_when_no_output() -> None:
    args = Namespace(format="yaml")
    assert resolve_output_format(args) == OutputFormat.yaml


def test_resolve_output_format_supports_json_flag() -> None:
    args = Namespace(json=True)
    assert resolve_output_format(args) == OutputFormat.json


def test_dumps_json_preserves_unicode_and_trailing_newline() -> None:
    s = dumps_json({"msg": "привет"})
    assert "привет" in s
    assert s.endswith("\n")


def test_dumps_yaml_has_trailing_newline() -> None:
    s = dumps_yaml({"a": 1})
    assert "a:" in s
    assert s.endswith("\n")
