"""Public retry arguments fail before context construction or side effects."""

from __future__ import annotations

import json

import pytest

from dpone.cli import main as cli_main


@pytest.mark.parametrize(
    "argv, expected_fragment",
    (
        (
            ["run", "missing.yml", "--retry-attempts", "-1", "--format", "json"],
            "non-negative integer",
        ),
        (
            ["run", "missing.yml", "--retry-backoff-seconds", "-0.1", "--format", "json"],
            "finite non-negative number",
        ),
        (
            ["run", "missing.yml", "--retry-backoff-seconds", "-inf", "--format", "json"],
            "finite non-negative number",
        ),
        (
            ["run", "missing.yml", "--retry-backoff-seconds", "-nan", "--format", "json"],
            "finite non-negative number",
        ),
        (
            ["run", "missing.yml", "--retry-backoff-seconds", "-1e2", "--format", "json"],
            "finite non-negative number",
        ),
        (
            [
                "orchestrate",
                "run",
                "--manifest",
                "missing.yml",
                "--retry-backoff-seconds",
                "nan",
                "--format",
                "json",
            ],
            "finite non-negative number",
        ),
    ),
)
def test_invalid_retry_arguments_are_structured_usage_errors_before_context(
    argv: list[str],
    expected_fragment: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli_main.AppContext,
        "from_env",
        staticmethod(lambda **_kwargs: pytest.fail("context must not be constructed")),
    )

    with pytest.raises(SystemExit, match="2"):
        cli_main.main(argv)

    captured = capsys.readouterr()
    payload = json.loads(captured.err)
    assert captured.out == ""
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_CLI_USAGE_INVALID"
    assert expected_fragment in payload["errors"][0]["message"]
