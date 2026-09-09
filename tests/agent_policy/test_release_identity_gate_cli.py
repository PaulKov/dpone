from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tests.agent_policy._release_identity_gate_helpers import (
    release_identity,
    repository,
    successful_git,
)


def test_cli_writes_credential_free_report_and_fails_closed(
    tmp_path: Path,
    monkeypatch: Any,
    capsys: Any,
) -> None:
    root = repository(tmp_path)
    output = tmp_path / "evidence" / "release-identity.json"
    monkeypatch.setattr(release_identity, "_run_command", successful_git)

    exit_code = release_identity.main(
        [
            "--root",
            str(root),
            "--tag",
            "v1.2.3",
            "--commit-sha",
            "invalid-token-like-value",
            "--output",
            str(output),
        ]
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    stdout = capsys.readouterr().out
    assert exit_code == 1
    assert payload["status"] == "FAIL"
    assert payload["blockers"][0]["code"] == "RELEASE_COMMIT_SHA_INVALID"
    assert "invalid-token-like-value" not in stdout
    assert "invalid-token-like-value" not in output.read_text(encoding="utf-8")
