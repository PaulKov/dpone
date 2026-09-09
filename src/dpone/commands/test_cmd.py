"""Thin CLI facade for hermetic ``dpone.test.v1`` execution."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import stat
import sys
import tempfile
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from dpone.services.hermetic_test_service import HermeticTestService

_REPORT_SCHEMA = "dpone.test-suite-report.v1"
_REPORT_COMPLETE_LINE = "- report-complete: true"
_REPORT_DIGEST_PREFIX = "- report-sha256: sha256:"
_MAX_EXISTING_REPORT_BYTES = 20 * 1024 * 1024
_AUTHORING_INPUT_SUFFIXES = {".jsonl", ".sql", ".yaml", ".yml"}


class _UnsafeOutputError(ValueError):
    """Raised without carrying user-controlled path or file content."""


@dataclass(frozen=True, slots=True)
class _OutputPlan:
    path: Path
    existing_identity: tuple[int, int, int, int] | None
    existing_sha256: str | None


class _TestResultView(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def status(self) -> str: ...

    @property
    def process(self) -> Mapping[str, Any]: ...

    @property
    def temporary_target(self) -> Mapping[str, Any]: ...

    @property
    def expectations(self) -> Sequence[Mapping[str, Any]]: ...

    @property
    def errors(self) -> Sequence[Mapping[str, Any]]: ...


class _SuiteReportView(Protocol):
    @property
    def passed(self) -> bool: ...

    @property
    def counts(self) -> Mapping[str, int]: ...

    @property
    def tests(self) -> Sequence[_TestResultView]: ...

    @property
    def input_paths(self) -> tuple[str, ...]: ...

    @property
    def exit_code(self) -> int: ...

    def to_jsonable(self) -> dict[str, Any]: ...


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("test", help="Run credential-free dpone.test.v1 fixtures")
    parser.add_argument("target", help="Test file, pipeline id/path, or project root")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    parser.add_argument("--output", help="Atomically create a complete report or verify an identical one")
    return parser


def cmd_test(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    report = HermeticTestService(root=Path.cwd()).run(args.target)
    content = render_test_report(report, output_format=args.format)
    if args.output:
        try:
            plan = _plan_output(
                Path(args.output),
                root=Path.cwd(),
                input_paths=report.input_paths,
                content=content,
            )
            _write_atomic(plan, content)
        except _UnsafeOutputError:
            _write_stdout(
                "dpone test: FAIL\n"
                "- DPONE_TEST_OUTPUT_UNSAFE: report output would replace an input, symlink, or differing file\n"
            )
            return 4
        except OSError:
            _write_stdout(
                "dpone test: FAIL\n- DPONE_TEST_OUTPUT_WRITE_FAILED: report could not be written atomically\n"
            )
            return 5
        _write_stdout(_summary(report))
    else:
        _write_stdout(content)
    return report.exit_code


def render_test_report(report: _SuiteReportView, *, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(report.to_jsonable(), ensure_ascii=False, indent=2) + "\n"
    if output_format == "md":
        return _render_lines(report, markdown=True)
    return _render_lines(report, markdown=False)


def _render_lines(report: _SuiteReportView, *, markdown: bool) -> str:
    heading = "# dpone test" if markdown else f"dpone test: {'PASS' if report.passed else 'FAIL'}"
    if markdown:
        lines = [heading, "", f"- Schema: `{_REPORT_SCHEMA}`", f"- Status: **{'PASS' if report.passed else 'FAIL'}**"]
    else:
        lines = [heading, f"- schema: {_REPORT_SCHEMA}"]
    counts = report.counts
    lines.append(
        f"- tests: {counts['total']} ({counts['passed']} passed, {counts['failed']} failed, {counts['blocked']} blocked)"
    )
    for test in report.tests:
        lines.append(f"- {_render_inline(test.name)}: {_render_inline(test.status)}")
        strategy = test.process.get("strategy")
        if strategy:
            lines.append(f"- strategy: {_render_inline(strategy)}")
        lines.append(f"- rows: {test.temporary_target.get('rows', 0)}")
        lines.append("- coverage: hermetic contract")
        for expectation in test.expectations:
            lines.append(f"- {_render_inline(expectation.get('code'))}: {_render_inline(expectation.get('message'))}")
        for error in test.errors:
            lines.append(f"- {_render_inline(error.get('code'))}: {_render_inline(error.get('message'))}")
    if report.passed:
        lines.append("- journey: offline golden path complete")
    lines.append(_report_digest_line(lines))
    lines.append(_REPORT_COMPLETE_LINE)
    return "\n".join(lines) + "\n"


def _render_inline(value: object) -> str:
    """Keep user-controlled report values on one unambiguous text line."""

    return "".join(
        f"\\u{ord(character):04x}" if unicodedata.category(character) in {"Cc", "Zl", "Zp"} else character
        for character in str(value)
    )


def _report_digest_line(lines: Sequence[str]) -> str:
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    return _REPORT_DIGEST_PREFIX + hashlib.sha256(payload).hexdigest()


def _summary(report: _SuiteReportView) -> str:
    counts = report.counts
    return (
        f"dpone test: {'PASS' if report.passed else 'FAIL'}\n"
        f"- tests: {counts['total']} ({counts['passed']} passed, {counts['failed']} failed, "
        f"{counts['blocked']} blocked)\n"
    )


def _plan_output(
    path: Path,
    *,
    root: Path,
    input_paths: tuple[str, ...],
    content: str,
) -> _OutputPlan:
    raw_target = path if path.is_absolute() else root / path
    target = raw_target.resolve(strict=False)
    confined_inputs = {(root / item).resolve(strict=False) for item in input_paths}
    fixture_root = (root / "tests" / "fixtures").resolve(strict=False)
    if (
        target in confined_inputs
        or target == fixture_root
        or fixture_root in target.parents
        or target.suffix.lower() in _AUTHORING_INPUT_SUFFIXES
    ):
        raise _UnsafeOutputError
    try:
        metadata = raw_target.lstat()
    except FileNotFoundError:
        return _OutputPlan(path=target, existing_identity=None, existing_sha256=None)
    except OSError as exc:
        raise _UnsafeOutputError from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise _UnsafeOutputError
    identity = _file_identity(metadata)
    existing = _read_existing_output(raw_target, expected_identity=identity)
    if not _reports_equivalent(existing, content.encode("utf-8")):
        raise _UnsafeOutputError
    return _OutputPlan(
        path=target,
        existing_identity=identity,
        existing_sha256=hashlib.sha256(existing).hexdigest(),
    )


def _read_existing_output(path: Path, *, expected_identity: tuple[int, int, int, int]) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise _UnsafeOutputError from exc
    try:
        metadata = os.fstat(descriptor)
        if _file_identity(metadata) != expected_identity or metadata.st_size > _MAX_EXISTING_REPORT_BYTES:
            raise _UnsafeOutputError
        chunks: list[bytes] = []
        remaining = _MAX_EXISTING_REPORT_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
    finally:
        os.close(descriptor)
    content = b"".join(chunks)
    if len(content) > _MAX_EXISTING_REPORT_BYTES:
        raise _UnsafeOutputError
    return content


def _reports_equivalent(existing: bytes, desired: bytes) -> bool:
    if existing == desired:
        return True
    try:
        existing_payload = json.loads(existing.decode("utf-8"))
        desired_payload = json.loads(desired.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        return False
    return _semantic_report(existing_payload) == _semantic_report(desired_payload)


def _semantic_report(value: object) -> object:
    """Remove only the suite/test timing fields defined as volatile."""

    if not isinstance(value, Mapping):
        return value
    report = dict(value)
    report.pop("duration_ms", None)
    tests = report.get("tests")
    if not isinstance(tests, list):
        return report
    normalized_tests: list[object] = []
    for item in tests:
        if not isinstance(item, Mapping):
            normalized_tests.append(item)
            continue
        normalized = dict(item)
        normalized.pop("duration_ms", None)
        normalized_tests.append(normalized)
    report["tests"] = normalized_tests
    return report


def _write_atomic(plan: _OutputPlan, content: str) -> None:
    target = plan.path
    if plan.existing_identity is not None:
        existing = _read_existing_output(target, expected_identity=plan.existing_identity)
        if plan.existing_sha256 is None or hashlib.sha256(existing).hexdigest() != plan.existing_sha256:
            raise _UnsafeOutputError
        return

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = handle.name
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, target)
        except FileExistsError as exc:
            if not _existing_output_is_no_op(target, desired=content.encode("utf-8")):
                raise _UnsafeOutputError from exc
        Path(temporary).unlink()
        temporary = None
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def _existing_output_is_no_op(path: Path, *, desired: bytes) -> bool:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise _UnsafeOutputError from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise _UnsafeOutputError
    existing = _read_existing_output(path, expected_identity=_file_identity(metadata))
    return _reports_equivalent(existing, desired)


def _file_identity(metadata: os.stat_result) -> tuple[int, int, int, int]:
    return (metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns)


def _write_stdout(content: str) -> None:
    sys.stdout.write(content)


__all__ = ["cmd_test", "register_parser", "render_test_report"]
