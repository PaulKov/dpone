"""Fail closed when release archives contain repository-internal files.

The inspector reads ZIP and tar metadata only. It never extracts members,
executes package code, or includes member payload bytes in its JSON report.
"""

from __future__ import annotations

import argparse
import json
import sys
import tarfile
import zipfile
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

MAX_ARCHIVES = 16
MAX_ARCHIVE_MEMBERS = 10_000
MAX_REPORTED_FINDINGS = 20
MAX_MEMBER_NAME_LENGTH = 160
MAX_ARCHIVE_LABEL_LENGTH = 120
MAX_OUTPUT_BYTES = 1024 * 1024

_INTERNAL_DIRECTORIES = frozenset({".agents", ".codex", ".git"})


@dataclass(frozen=True)
class ForbiddenMember:
    """One bounded forbidden archive member observation."""

    member: str
    reason: str


@dataclass(frozen=True)
class Blocker:
    """A stable failure that does not expose parser exception details."""

    code: str
    message: str


@dataclass(frozen=True)
class ArchiveReport:
    """Metadata-only inspection result for one release archive."""

    archive: str
    format: str
    status: str
    member_count: int
    forbidden_members: tuple[ForbiddenMember, ...]
    omitted_forbidden_member_count: int
    blockers: tuple[Blocker, ...]

    def to_payload(self) -> dict[str, object]:
        """Return a deterministic JSON-compatible representation."""

        return {
            "archive": self.archive,
            "blockers": [asdict(item) for item in self.blockers],
            "forbidden_members": [asdict(item) for item in self.forbidden_members],
            "format": self.format,
            "member_count": self.member_count,
            "omitted_forbidden_member_count": self.omitted_forbidden_member_count,
            "status": self.status,
        }


@dataclass(frozen=True)
class GateReport:
    """Bounded aggregate result for one CLI invocation."""

    status: str
    archives: tuple[ArchiveReport, ...]
    blockers: tuple[Blocker, ...] = ()

    @property
    def decision(self) -> str:
        """Return the release decision associated with the status."""

        return "GO" if self.status == "PASS" else "NO-GO"

    def to_payload(self) -> dict[str, object]:
        """Return stable machine-readable release evidence."""

        failed = sum(item.status == "FAIL" for item in self.archives)
        return {
            "archives": [item.to_payload() for item in self.archives],
            "blockers": [asdict(item) for item in self.blockers],
            "decision": self.decision,
            "schema_version": 1,
            "status": self.status,
            "summary": {
                "archive_count": len(self.archives),
                "failed_archive_count": failed,
            },
        }


def _bounded(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return f"{value[: limit - 1]}…"


def _archive_label(path: Path) -> str:
    return _bounded(path.name or str(path), MAX_ARCHIVE_LABEL_LENGTH)


def _archive_format(path: Path) -> str | None:
    name = path.name.lower()
    if name.endswith(".whl"):
        return "wheel"
    if name.endswith(".tar.gz"):
        return "sdist"
    return None


def _forbidden_reason(member_name: str) -> str | None:
    components = tuple(part for part in member_name.replace("\\", "/").split("/") if part)
    if any(part in _INTERNAL_DIRECTORIES for part in components):
        return "INTERNAL_DIRECTORY"
    if "AGENTS.md" in components:
        return "INSTRUCTION_FILE"
    if ".gitignore" in components:
        return "VCS_FILE"
    if any(part == ".env" or part.startswith(".env.") for part in components):
        return "ENV_FILE"
    return None


def _scan_member_names(names: Iterable[str]) -> tuple[int, tuple[ForbiddenMember, ...], int, tuple[Blocker, ...]]:
    member_count = 0
    omitted_count = 0
    findings: list[ForbiddenMember] = []
    for raw_name in names:
        member_count += 1
        if member_count > MAX_ARCHIVE_MEMBERS:
            blocker = Blocker(
                "ARCHIVE_MEMBER_LIMIT_EXCEEDED",
                f"Archive exceeds the {MAX_ARCHIVE_MEMBERS} member inspection limit.",
            )
            return member_count, tuple(findings), omitted_count, (blocker,)
        normalized_name = raw_name.replace("\\", "/")
        reason = _forbidden_reason(normalized_name)
        if reason is None:
            continue
        if len(findings) < MAX_REPORTED_FINDINGS:
            findings.append(
                ForbiddenMember(
                    member=_bounded(normalized_name, MAX_MEMBER_NAME_LENGTH),
                    reason=reason,
                )
            )
        else:
            omitted_count += 1
    return member_count, tuple(findings), omitted_count, ()


def _malformed_report(path: Path, archive_format: str) -> ArchiveReport:
    return ArchiveReport(
        archive=_archive_label(path),
        format=archive_format,
        status="FAIL",
        member_count=0,
        forbidden_members=(),
        omitted_forbidden_member_count=0,
        blockers=(
            Blocker(
                "ARCHIVE_MALFORMED",
                "Archive metadata is unreadable or malformed.",
            ),
        ),
    )


def _inspect_wheel(path: Path) -> ArchiveReport:
    try:
        with zipfile.ZipFile(path, mode="r") as archive:
            scanned = _scan_member_names(info.filename for info in archive.infolist())
    except (OSError, ValueError, zipfile.BadZipFile, zipfile.LargeZipFile):
        return _malformed_report(path, "wheel")
    return _report_from_scan(path, "wheel", scanned)


def _inspect_sdist(path: Path) -> ArchiveReport:
    try:
        with tarfile.open(path, mode="r:gz") as archive:
            scanned = _scan_member_names(member.name for member in archive)
    except (EOFError, OSError, ValueError, tarfile.TarError):
        return _malformed_report(path, "sdist")
    return _report_from_scan(path, "sdist", scanned)


def _report_from_scan(
    path: Path,
    archive_format: str,
    scanned: tuple[int, tuple[ForbiddenMember, ...], int, tuple[Blocker, ...]],
) -> ArchiveReport:
    member_count, findings, omitted_count, blockers = scanned
    return ArchiveReport(
        archive=_archive_label(path),
        format=archive_format,
        status="FAIL" if findings or blockers else "PASS",
        member_count=member_count,
        forbidden_members=findings,
        omitted_forbidden_member_count=omitted_count,
        blockers=blockers,
    )


def inspect_archive(path: Path) -> ArchiveReport:
    """Inspect one wheel or sdist without accessing member payloads."""

    archive_format = _archive_format(path)
    if archive_format == "wheel":
        return _inspect_wheel(path)
    if archive_format == "sdist":
        return _inspect_sdist(path)
    return ArchiveReport(
        archive=_archive_label(path),
        format="unsupported",
        status="FAIL",
        member_count=0,
        forbidden_members=(),
        omitted_forbidden_member_count=0,
        blockers=(
            Blocker(
                "ARCHIVE_FORMAT_UNSUPPORTED",
                "Expected a .whl or .tar.gz release archive.",
            ),
        ),
    )


def inspect_archives(paths: Sequence[Path]) -> GateReport:
    """Inspect a bounded set of archives and aggregate a fail-closed result."""

    ordered = sorted((Path(path) for path in paths), key=lambda path: (_archive_label(path), str(path)))
    blockers: tuple[Blocker, ...] = ()
    if len(ordered) > MAX_ARCHIVES:
        ordered = ordered[:MAX_ARCHIVES]
        blockers = (
            Blocker(
                "ARCHIVE_COUNT_LIMIT_EXCEEDED",
                f"Invocation exceeds the {MAX_ARCHIVES} archive inspection limit.",
            ),
        )
    archives = tuple(inspect_archive(path) for path in ordered)
    status = "FAIL" if blockers or any(item.status == "FAIL" for item in archives) else "PASS"
    return GateReport(status=status, archives=archives, blockers=blockers)


def render_report(report: GateReport) -> str:
    """Serialize a report as compact, sorted and size-bounded JSON."""

    rendered = json.dumps(
        report.to_payload(),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    if len(rendered.encode("utf-8")) > MAX_OUTPUT_BYTES:
        raise AssertionError("package archive report exceeded its output bound")
    return f"{rendered}\n"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect wheel and sdist metadata for repository-internal members.",
    )
    parser.add_argument("archives", nargs="+", type=Path, help="Release .whl or .tar.gz archives.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the metadata-only package archive gate."""

    args = _parser().parse_args(argv)
    report = inspect_archives(args.archives)
    sys.stdout.write(render_report(report))
    return 0 if report.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
