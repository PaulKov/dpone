"""Run the authorized canonical metrics producer and retain reproducibility proof.

Usage: uv run python <this file> <checkout> <new-evidence-directory>.
The checkout must have clean tracked inputs and no untracked Python inputs.
Only the existing producer writes docs/quality-metrics.md; this script records
its commands and verifies that all other tracked bytes remain unchanged.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

DOCUMENT = "docs/quality-metrics.md"
START = b"<!-- DPONE_QUALITY_METRICS_START -->"
END = b"<!-- DPONE_QUALITY_METRICS_END -->"


def digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def refresh(root: Path, output: Path) -> None:
    root, output = root.resolve(), output.resolve()
    output.mkdir(parents=True, exist_ok=False)

    def git(*args: str) -> str:
        return subprocess.check_output(["git", *args], cwd=root, text=True).strip()

    def snapshot() -> dict[str, str]:
        return {name: digest((root / name).read_bytes()) for name in git("ls-files").splitlines()}

    def execute(name: str, *args: str) -> int:
        command = ["uv", "run", "dpone", "docs", "update-dev-metrics", *args]
        started = time.monotonic()
        with (output / f"{name}.log").open("x") as stream:
            result = subprocess.run(command, cwd=root, stdout=stream, stderr=subprocess.STDOUT, check=False)
        record["commands"].append(
            {
                "name": name,
                "argv": command,
                "exit_code": result.returncode,
                "duration_seconds": round(time.monotonic() - started, 3),
            }
        )
        return result.returncode

    record = {
        "schema_version": 1,
        "source_commit": git("rev-parse", "HEAD"),
        "source_tree": git("rev-parse", "HEAD^{tree}"),
        "checkout": str(root),
        "recorder_sha256": digest(Path(__file__).read_bytes()),
        "status": "FAIL",
        "commands": [],
    }
    try:
        if git("diff", "--name-only") or git("diff", "--cached", "--name-only"):
            raise ValueError("Tracked inputs must be committed before generation")
        if git("ls-files", "--others", "--exclude-standard", "--", "*.py"):
            raise ValueError("Untracked Python inputs must be committed before generation")
        before = snapshot()
        untracked_before = set(git("ls-files", "--others", "--exclude-standard").splitlines())
        inventory = {name: before[name] for name in git("ls-files", "*.py").splitlines()}
        inventory_bytes = (json.dumps(inventory, indent=2, sort_keys=True) + "\n").encode()
        (output / "tracked-python-inputs.json").write_bytes(inventory_bytes)
        record.update(input_inventory_sha256=digest(inventory_bytes), python_files=len(inventory))
        original = (root / DOCUMENT).read_bytes()
        if original.count(START) != 1 or original.count(END) != 1:
            raise ValueError("Expected exactly one generated block")
        if execute("before", "--check") not in (0, 2):
            raise ValueError("Canonical before check failed unexpectedly")
        if execute("generate"):
            raise ValueError("Canonical generation failed")
        generated = (root / DOCUMENT).read_bytes()
        changed = sorted(name for name, value in snapshot().items() if before.get(name) != value)
        record["changed_tracked_paths"] = changed
        if changed not in ([], [DOCUMENT]):
            raise ValueError("Producer changed an unauthorized tracked path")

        def surrounding(value: bytes) -> tuple[bytes, bytes]:
            return value.split(START)[0], value.split(END, 1)[1]

        record["surrounding_prose_unchanged"] = surrounding(original) == surrounding(generated)
        if not record["surrounding_prose_unchanged"]:
            raise ValueError("Producer changed prose outside generated markers")
        if execute("after", "--check") or execute("repeat"):
            raise ValueError("Freshness check or repeated generation failed")
        repeated = (root / DOCUMENT).read_bytes()
        record["byte_identical_repeat"] = generated == repeated
        record["document_sha256"] = digest(repeated)
        after = snapshot()
        unexpected = [
            name
            for name in set(git("ls-files", "--others", "--exclude-standard").splitlines()) - untracked_before
            if not (root / name).resolve().is_relative_to(output)
        ]
        record["unexpected_untracked_paths"] = sorted(unexpected)
        if (
            not record["byte_identical_repeat"]
            or any(value != after.get(name) for name, value in before.items() if name != DOCUMENT)
            or before.keys() != after.keys()
            or git("rev-parse", "HEAD") != record["source_commit"]
            or unexpected
        ):
            raise ValueError("Source identity or output changed during repeated generation")
        record["status"] = "PASS"
    except Exception as error:
        record["reason"] = str(error)
        raise
    finally:
        (output / "refresh.json").write_text(json.dumps(record, indent=2) + "\n")


if __name__ == "__main__":
    refresh(Path(sys.argv[1]), Path(sys.argv[2]))
