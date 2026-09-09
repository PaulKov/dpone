from __future__ import annotations

import argparse
import re
from pathlib import Path

SECTION_RE = re.compile(r"^##\s+(?P<version>[^\s]+)(?:\s+-\s+.*)?$", re.MULTILINE)


def extract_release_notes(changelog: str, version: str) -> str:
    """Return the changelog section for a release version."""

    matches = list(SECTION_RE.finditer(changelog))
    normalized = version.removeprefix("v")
    for index, match in enumerate(matches):
        if match.group("version") != normalized:
            continue
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(changelog)
        return changelog[start:end].strip() + "\n"
    raise ValueError(f"CHANGELOG.md does not contain release section for {normalized!r}.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Extract curated GitHub Release notes from CHANGELOG.md.")
    parser.add_argument("--version", required=True, help="Release version, for example 0.7.2 or v0.7.2.")
    parser.add_argument("--changelog", default="CHANGELOG.md", help="Path to CHANGELOG.md.")
    parser.add_argument("--output", required=True, help="Output markdown file path.")
    args = parser.parse_args(argv)

    changelog_path = Path(args.changelog)
    output_path = Path(args.output)
    notes = extract_release_notes(changelog_path.read_text(encoding="utf-8"), args.version)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(notes, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
