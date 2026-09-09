"""Portable manifest/support bundle generation with redaction."""

from __future__ import annotations

import json
import re
import shutil
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path

from dpone.ops.checksums import sha256_file

SECRET_LINE_PATTERN = re.compile(
    r"(?im)^(\s*(?:password|token|api[_-]?key|secret|client_secret|private_key|sasl_password)\s*[:=]\s*)(.+)$"
)


@dataclass(frozen=True, slots=True)
class ManifestBundleItem:
    name: str
    source_path: str
    copied_path: str
    sha256: str
    size_bytes: int
    redacted: bool
    passed: bool
    message: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ManifestBundleReport:
    bundle_id: str
    passed: bool
    blockers: tuple[str, ...]
    items: tuple[ManifestBundleItem, ...]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "bundle_id": self.bundle_id,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "output_dir": self.output_dir,
            "items": [item.to_dict() for item in self.items],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone ops manifest bundle",
            "",
            f"- Bundle ID: `{self.bundle_id}`",
            f"- Passed: `{self.passed}`",
            f"- Blockers: `{len(self.blockers)}`",
            "",
            "| item | status | redacted | sha256 | copied path | message |",
            "|---|---|---|---|---|---|",
        ]
        for item in self.items:
            status = "pass" if item.passed else "fail"
            lines.append(
                f"| `{item.name}` | {status} | `{item.redacted}` | `{item.sha256}` | "
                f"`{item.copied_path}` | {item.message} |"
            )
        if self.blockers:
            lines.extend(["", "Fix missing or unsafe bundle inputs before sharing:", ""])
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        return "\n".join(lines) + "\n"


class ManifestBundleService:
    """Creates portable, optionally redacted support/release bundles."""

    def build(
        self,
        *,
        output_dir: str | Path,
        bundle_id: str,
        files: Mapping[str, str | Path],
        redact: bool = True,
    ) -> ManifestBundleReport:
        directory = Path(output_dir)
        files_dir = directory / "files"
        files_dir.mkdir(parents=True, exist_ok=True)
        items = tuple(
            self._copy_item(name, Path(path), files_dir=files_dir, redact=redact) for name, path in files.items()
        )
        blockers = tuple(item.name for item in items if not item.passed)
        report = ManifestBundleReport(
            bundle_id=bundle_id,
            passed=not blockers,
            blockers=blockers,
            items=items,
            output_dir=str(directory),
        )
        (directory / "manifest_bundle.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "manifest_bundle.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _copy_item(self, name: str, source: Path, *, files_dir: Path, redact: bool) -> ManifestBundleItem:
        if not source.exists() or not source.is_file():
            return ManifestBundleItem(
                name=name,
                source_path=str(source),
                copied_path="",
                sha256="0" * 64,
                size_bytes=0,
                redacted=False,
                passed=False,
                message="source file missing",
            )
        destination = self._destination(files_dir=files_dir, name=name, source=source)
        if redact and self._is_text_file(source):
            text = source.read_text(encoding="utf-8")
            destination.write_text(self._redact_text(text), encoding="utf-8")
            redacted = text != destination.read_text(encoding="utf-8")
        else:
            shutil.copyfile(source, destination)
            redacted = False
        return ManifestBundleItem(
            name=name,
            source_path=str(source),
            copied_path=str(destination),
            sha256=sha256_file(destination),
            size_bytes=destination.stat().st_size,
            redacted=redacted,
            passed=True,
            message="copied",
        )

    @staticmethod
    def _destination(*, files_dir: Path, name: str, source: Path) -> Path:
        safe_name = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in name)
        suffix = source.suffix or ".txt"
        return files_dir / f"{safe_name}{suffix}"

    @staticmethod
    def _is_text_file(path: Path) -> bool:
        return path.suffix.lower() in {".json", ".yaml", ".yml", ".md", ".txt", ".log", ".toml", ".env"}

    @staticmethod
    def _redact_text(text: str) -> str:
        return SECRET_LINE_PATTERN.sub(r"\1[REDACTED]", text)
