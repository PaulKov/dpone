"""Public dbt runner contracts for bounded, secret-free captured output."""

from __future__ import annotations

import io
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from dpone.adapters.dbt_subprocess import SubprocessDbtCommandRunner


class _CompletedProcess:
    def __init__(self, payload: bytes, stream: str) -> None:
        self.stdout = io.BytesIO(payload if stream == "stdout" else b"")
        self.stderr = io.BytesIO(payload if stream == "stderr" else b"")

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return 23


def _capture(
    root: Path,
    *,
    payload: bytes,
    secrets: tuple[str, ...],
    limit: int,
    stream: str,
) -> tuple[str, bool]:
    result = SubprocessDbtCommandRunner(
        max_output_bytes=limit,
        popen_factory=lambda *args, **kwargs: _CompletedProcess(payload, stream),
        dbt_executable="synthetic",
    ).run(
        ("dbt", "build", "--target-path", str(root / "attempt" / "target")),
        cwd=root,
        timeout_seconds=5,
        redactions=secrets,
    )
    assert result.exit_code == 23
    text = getattr(result, stream)
    assert len(text.encode("utf-8")) <= limit
    assert "\ufffd" not in text or "\ufffd" in payload.decode("utf-8", errors="replace")
    other_stream = "stderr" if stream == "stdout" else "stdout"
    assert getattr(result, other_stream) == ""
    assert getattr(result, f"{other_stream}_truncated") is False
    return text, getattr(result, f"{stream}_truncated")


@pytest.mark.parametrize("stream", ("stdout", "stderr"))
@pytest.mark.parametrize("limit", (32, 64, 96))
def test_earlier_redaction_cannot_expose_a_secret_cut_by_retention(tmp_path: Path, stream: str, limit: int) -> None:
    secret = "SYNTHETIC_" + "s" * 54
    payload = (secret + "x" * 16 + secret).encode()

    text, truncated = _capture(tmp_path, payload=payload, secrets=(secret,), limit=limit, stream=stream)

    assert "SYNTHETIC" not in text
    assert "s" not in text
    assert truncated


@pytest.mark.parametrize("stream", ("stdout", "stderr"))
@pytest.mark.parametrize(
    ("payload", "secrets"),
    (
        ("SYNTHETIC_ALPHA_BETA", ("SYNTHETIC_ALPHA", "ALPHA_BETA")),
        ("SYNTHETIC_ALPHA_BETA", ("ALPHA_BETA", "SYNTHETIC_ALPHA")),
        ("ABABABA", ("ABA",)),
        ("synthetic-value", ("synthetic", "synthetic-value")),
        ("🔐αβ🔐", ("🔐αβ", "β🔐")),
    ),
)
def test_overlapping_occurrences_do_not_leave_secret_fragments(
    tmp_path: Path, stream: str, payload: str, secrets: tuple[str, ...]
) -> None:
    text, truncated = _capture(tmp_path, payload=payload.encode(), secrets=secrets, limit=64, stream=stream)

    assert text.replace("[REDACTED]", "") == ""
    assert not truncated


@pytest.mark.parametrize("stream", ("stdout", "stderr"))
@pytest.mark.parametrize("start", (65520, 65535, 65536))
def test_secret_crossing_read_or_output_boundary_is_fully_masked(tmp_path: Path, stream: str, start: int) -> None:
    secret = "SYNTHETIC_BOUNDARY_VALUE"
    payload = b"x" * start + secret.encode() + b" complete"
    text, truncated = _capture(tmp_path, payload=payload, secrets=(secret,), limit=65540, stream=stream)

    assert "SYNTHETIC" not in text
    assert text.startswith("x" * start)
    assert truncated


@pytest.mark.parametrize("stream", ("stdout", "stderr"))
def test_multibyte_secret_crossing_output_limit_does_not_emit_partial_codepoints(tmp_path: Path, stream: str) -> None:
    secret = "🔐" * 32
    text, truncated = _capture(
        tmp_path, payload=("x" * 30 + secret).encode(), secrets=(secret,), limit=64, stream=stream
    )

    assert text == "x" * 30 + "[REDACTED]"
    assert truncated


@pytest.mark.parametrize("stream", ("stdout", "stderr"))
@pytest.mark.parametrize("secret", ("🔐" * 32, "密" * 32))
def test_multibyte_secret_cut_by_retention_is_masked_after_earlier_shrinkage(
    tmp_path: Path, stream: str, secret: str
) -> None:
    text, truncated = _capture(
        tmp_path, payload=(secret + "x" * 16 + secret).encode(), secrets=(secret,), limit=64, stream=stream
    )

    assert text == "[REDACTED]" + "x" * 16 + "[REDACTED]"
    assert truncated


@pytest.mark.parametrize("stream", ("stdout", "stderr"))
@pytest.mark.parametrize(
    ("payload", "secrets", "limit", "expected", "truncated"),
    (
        (b"abcdefgh", (), 8, "abcdefgh", False),
        (b"abcdefghijkl", (), 8, "abcdefgh", True),
        (b"abc", (), 8, "abc", False),
        (b"", (), 8, "", False),
        (b"a", ("a",), 3, "[RE", True),
        (b"A\xffB", (), 16, "A\ufffdB", False),
        (b"A\xc3", (), 16, "A\ufffd", False),
        (b"A\xffB", ("A\ufffdB",), 16, "[REDACTED]", False),
        (b"a" + ("é" * 4).encode(), ("a",), 16, "[REDACTED]" + "é" * 3, True),
        (b"a" * 20 + b"x" * 11 + "é".encode(), ("a" * 20,), 32, "[REDACTED]" + "x" * 11 + "é", True),
        (b"a" * 20 + b"x" * 31 + "éé".encode(), ("a" * 20,), 32, "[REDACTED]" + "x" * 22, True),
        (b"x" * 63 + "é".encode(), (), 64, "x" * 63, True),
        (b"x" * 64 + b"SYNTHETIC_VALUE", ("SYNTHETIC_VALUE",), 64, "x" * 64, True),
        (b"x" * 49 + b"SYNTHETIC_VALUE", ("SYNTHETIC_VALUE",), 64, "x" * 49 + "[REDACTED]", False),
    ),
)
def test_output_limit_and_utf8_controls(
    tmp_path: Path,
    stream: str,
    payload: bytes,
    secrets: tuple[str, ...],
    limit: int,
    expected: str,
    truncated: bool,
) -> None:
    assert _capture(tmp_path, payload=payload, secrets=secrets, limit=limit, stream=stream) == (expected, truncated)


def test_large_synthetic_child_output_is_drained_from_both_pipes(tmp_path: Path) -> None:
    code = "import os; block=b'x'*65536\nfor _ in range(64):\n os.write(1,block); os.write(2,block)\n"

    def popen(args: tuple[str, ...], **kwargs: Any) -> subprocess.Popen[bytes]:
        del args
        return subprocess.Popen((sys.executable, "-c", code), **kwargs)

    result = SubprocessDbtCommandRunner(max_output_bytes=64, popen_factory=popen).run(
        ("dbt", "build", "--target-path", str(tmp_path / "attempt" / "target")),
        cwd=tmp_path,
        timeout_seconds=10,
        redactions=(),
    )

    assert result.exit_code == 0
    assert result.stdout == result.stderr == "x" * 64
    assert result.stdout_truncated and result.stderr_truncated


@pytest.mark.parametrize("stream", ("stdout", "stderr"))
@pytest.mark.parametrize("secret_size", (1, 64, 4096))
def test_maximum_retained_repetition_preserves_overlaps_with_long_secrets(
    tmp_path: Path, stream: str, secret_size: int
) -> None:
    text, truncated = _capture(
        tmp_path,
        payload=b"a" * (1024 * 1024 + 4097),
        secrets=("a" * secret_size,),
        limit=1024 * 1024,
        stream=stream,
    )

    assert text == "[REDACTED]"
    assert truncated
