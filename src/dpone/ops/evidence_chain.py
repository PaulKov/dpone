"""Tamper-evident chain for dpone release evidence artifacts."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.ops.checksums import sha256_file

_CHAIN_INDEX = "evidence_chain_index.json"
_CHAIN_JSON_SUFFIX = "__evidence_chain.json"
_CHAIN_MD_SUFFIX = "__evidence_chain.md"
_MAX_INDEX_BYTES = 4 * 1024 * 1024
_MAX_ARTIFACT_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class EvidenceChainEntry:
    release: str
    artifact_index_path: str
    artifact_index_hash: str
    previous_chain_hash: str | None
    chain_hash: str
    created_at: str
    entry_path: str
    markdown_path: str
    verified: bool = True

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        previous = self.previous_chain_hash or "genesis"
        return "\n".join(
            [
                "# dpone ops evidence chain entry",
                "",
                f"- Release: `{self.release}`",
                f"- Verified: `{self.verified}`",
                f"- Artifact index hash: `{self.artifact_index_hash}`",
                f"- Previous chain hash: `{previous}`",
                f"- Chain hash: `{self.chain_hash}`",
                f"- Created at: `{self.created_at}`",
                "",
                "## Runbook when verification fails",
                "",
                "1. Recompute the artifact index checksum.",
                "2. Compare the previous chain hash with the prior release entry.",
                "3. Regenerate the chain entry only after reviewing the artifact diff.",
                "4. Treat unexpected hash drift as a release-blocking audit event.",
                "",
            ]
        )


@dataclass(frozen=True, slots=True)
class EvidenceChainVerificationReport:
    verified: bool
    entry_count: int
    broken_entries: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "verified": self.verified,
            "entry_count": self.entry_count,
            "broken_entries": list(self.broken_entries),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone ops evidence chain verification",
            "",
            f"- Verified: `{self.verified}`",
            f"- Entry count: `{self.entry_count}`",
            "",
        ]
        if self.broken_entries:
            lines.extend(["## Broken entries", ""])
            lines.extend(f"- `{entry}`" for entry in self.broken_entries)
        return "\n".join(lines) + "\n"


class EvidenceChainService:
    """Appends and verifies release evidence as a linked checksum chain."""

    def append(
        self,
        *,
        chain_dir: str | Path,
        release: str,
        artifact_index_path: str | Path,
        previous_entry_path: str | Path | None = None,
    ) -> EvidenceChainEntry:
        directory = Path(chain_dir)
        directory.mkdir(parents=True, exist_ok=True)
        artifact_path = Path(artifact_index_path)
        existing_entries = self._load_entries_from_index(directory) or self._load_entries_from_files(directory)
        previous_hash = self._previous_hash(existing_entries, previous_entry_path)
        created_at = datetime.now(UTC).isoformat()
        artifact_hash = sha256_file(artifact_path)
        chain_hash = self._chain_hash(
            release=release,
            artifact_index_hash=artifact_hash,
            previous_chain_hash=previous_hash,
            created_at=created_at,
        )
        safe_release = self._safe_release(release)
        entry_path = directory / f"{safe_release}{_CHAIN_JSON_SUFFIX}"
        markdown_path = directory / f"{safe_release}{_CHAIN_MD_SUFFIX}"
        entry = EvidenceChainEntry(
            release=release,
            artifact_index_path=str(artifact_path),
            artifact_index_hash=artifact_hash,
            previous_chain_hash=previous_hash,
            chain_hash=chain_hash,
            created_at=created_at,
            entry_path=str(entry_path),
            markdown_path=str(markdown_path),
            verified=True,
        )
        entry_path.write_text(entry.to_json(), encoding="utf-8")
        markdown_path.write_text(entry.to_markdown(), encoding="utf-8")
        self._write_index(directory, (*existing_entries, entry))
        return entry

    def verify(
        self,
        *,
        chain_dir: str | Path,
        expected_release: str | None = None,
        required_artifact_path: str | Path | None = None,
    ) -> EvidenceChainVerificationReport:
        directory = Path(chain_dir)
        index_path = directory / _CHAIN_INDEX
        if not index_path.exists() and not tuple(directory.glob(f"*{_CHAIN_JSON_SUFFIX}")):
            return EvidenceChainVerificationReport(False, 0, ("chain.empty",))
        try:
            entries, index_broken = self._verified_index_entries(directory)
        except (KeyError, TypeError, ValueError, OSError, json.JSONDecodeError):
            return EvidenceChainVerificationReport(
                verified=False,
                entry_count=0,
                broken_entries=("chain.index_invalid",),
            )
        if not entries:
            return EvidenceChainVerificationReport(False, 0, ("chain.empty",))
        broken: list[str] = list(index_broken)
        previous_hash: str | None = None
        for entry in entries:
            if not self._entry_is_valid(entry=entry, expected_previous_hash=previous_hash):
                broken.append(entry.release)
            previous_hash = entry.chain_hash
        latest = entries[-1]
        if expected_release is not None and latest.release != expected_release:
            broken.append("chain.release_mismatch")
        if expected_release is not None and not self._artifact_index_is_bound(
            latest,
            expected_release=expected_release,
            required_artifact_path=required_artifact_path,
        ):
            broken.append("chain.artifact_index_unbound")
        unique_broken = tuple(dict.fromkeys(broken))
        return EvidenceChainVerificationReport(
            verified=not unique_broken,
            entry_count=len(entries),
            broken_entries=unique_broken,
        )

    def _verified_index_entries(
        self,
        directory: Path,
    ) -> tuple[tuple[EvidenceChainEntry, ...], tuple[str, ...]]:
        index_path = directory / _CHAIN_INDEX
        if not _safe_regular_file(index_path):
            raise ValueError("evidence chain index is missing or unsafe")
        payload = self._json_payload(index_path)
        raw_entries = payload.get("entries")
        if not isinstance(raw_entries, list):
            raise ValueError("evidence chain index entries are invalid")
        entries = tuple(self._entry_from_mapping(item) for item in raw_entries if isinstance(item, Mapping))
        if len(entries) != len(raw_entries):
            raise ValueError("evidence chain index contains malformed entries")
        if payload.get("entry_count") != len(entries):
            raise ValueError("evidence chain index count does not match entries")
        expected_latest = entries[-1].chain_hash if entries else None
        if payload.get("latest_chain_hash") != expected_latest:
            raise ValueError("evidence chain latest hash does not match entries")
        expected_files: set[Path] = set()
        actual_entries: list[EvidenceChainEntry] = []
        broken: list[str] = []
        root = directory.resolve(strict=True)
        for indexed in entries:
            entry_path = Path(indexed.entry_path).resolve(strict=True)
            if entry_path.parent != root or not _safe_regular_file(entry_path):
                raise ValueError("evidence chain entry escaped its chain directory")
            actual = self._entry_from_path(entry_path)
            if actual != indexed:
                broken.append(indexed.release)
            actual_entries.append(actual)
            expected_files.add(entry_path)
        actual_files = {
            path.resolve(strict=True) for path in directory.glob(f"*{_CHAIN_JSON_SUFFIX}") if _safe_regular_file(path)
        }
        if actual_files != expected_files:
            broken.append("chain.index_mismatch")
        return tuple(actual_entries), tuple(dict.fromkeys(broken))

    def _previous_hash(
        self,
        existing_entries: tuple[EvidenceChainEntry, ...],
        previous_entry_path: str | Path | None,
    ) -> str | None:
        if previous_entry_path is not None:
            return self._entry_from_path(Path(previous_entry_path)).chain_hash
        return existing_entries[-1].chain_hash if existing_entries else None

    def _load_entries_from_index(self, directory: Path) -> tuple[EvidenceChainEntry, ...]:
        index_path = directory / _CHAIN_INDEX
        if index_path.exists():
            payload = self._json_payload(index_path)
            entries = payload.get("entries", [])
            if isinstance(entries, list):
                return tuple(self._entry_from_mapping(item) for item in entries if isinstance(item, Mapping))
        return tuple()

    def _load_entries_from_files(self, directory: Path) -> tuple[EvidenceChainEntry, ...]:
        return tuple(self._entry_from_path(path) for path in sorted(directory.glob(f"*{_CHAIN_JSON_SUFFIX}")))

    def _write_index(self, directory: Path, entries: Iterable[EvidenceChainEntry]) -> None:
        payload = {
            "entry_count": 0,
            "latest_chain_hash": None,
            "entries": [],
        }
        normalized_entries = tuple(entries)
        if normalized_entries:
            payload["entry_count"] = len(normalized_entries)
            payload["latest_chain_hash"] = normalized_entries[-1].chain_hash
            payload["entries"] = [entry.to_dict() for entry in normalized_entries]
        (directory / _CHAIN_INDEX).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def _entry_is_valid(self, *, entry: EvidenceChainEntry, expected_previous_hash: str | None) -> bool:
        expected_chain_hash = self._chain_hash(
            release=entry.release,
            artifact_index_hash=entry.artifact_index_hash,
            previous_chain_hash=entry.previous_chain_hash,
            created_at=entry.created_at,
        )
        if expected_chain_hash != entry.chain_hash:
            return False
        if entry.previous_chain_hash != expected_previous_hash:
            return False
        artifact_path = Path(entry.artifact_index_path)
        if not entry.verified or not _safe_regular_file(artifact_path):
            return False
        if sha256_file(artifact_path) != entry.artifact_index_hash:
            return False
        return True

    def _artifact_index_is_bound(
        self,
        entry: EvidenceChainEntry,
        *,
        expected_release: str,
        required_artifact_path: str | Path | None,
    ) -> bool:
        artifact_index = Path(entry.artifact_index_path)
        try:
            payload = self._json_payload(artifact_index)
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
            return False
        items = payload.get("items")
        if payload.get("release") != expected_release or not isinstance(items, list) or not items:
            return False
        validated_items = _validated_artifact_items(items, index_path=artifact_index)
        if validated_items is None:
            return False
        if required_artifact_path is None:
            return True
        required = Path(required_artifact_path)
        if not _safe_bounded_artifact(required):
            return False
        try:
            required_resolved = required.resolve(strict=True)
        except OSError:
            return False
        return any(
            path == required_resolved and digest == sha256_file(required_resolved)
            for _, path, digest in validated_items
        )

    @staticmethod
    def _chain_hash(
        *,
        release: str,
        artifact_index_hash: str,
        previous_chain_hash: str | None,
        created_at: str,
    ) -> str:
        payload = {
            "artifact_index_hash": artifact_index_hash,
            "created_at": created_at,
            "previous_chain_hash": previous_chain_hash,
            "release": release,
        }
        canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _safe_release(release: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", release).strip("._-") or "release"

    @staticmethod
    def _json_payload(path: Path) -> Mapping[str, Any]:
        if not _safe_regular_file(path) or path.stat().st_size > _MAX_INDEX_BYTES:
            raise ValueError("evidence chain JSON is missing, unsafe, or too large")
        return json.loads(path.read_text(encoding="utf-8"))

    def _entry_from_path(self, path: Path) -> EvidenceChainEntry:
        return self._entry_from_mapping(self._json_payload(path))

    @staticmethod
    def _entry_from_mapping(payload: Mapping[str, Any]) -> EvidenceChainEntry:
        return EvidenceChainEntry(
            release=str(payload["release"]),
            artifact_index_path=str(payload["artifact_index_path"]),
            artifact_index_hash=str(payload["artifact_index_hash"]),
            previous_chain_hash=str(payload["previous_chain_hash"])
            if payload.get("previous_chain_hash") is not None
            else None,
            chain_hash=str(payload["chain_hash"]),
            created_at=str(payload["created_at"]),
            entry_path=str(payload["entry_path"]),
            markdown_path=str(payload["markdown_path"]),
            verified=bool(payload.get("verified", True)),
        )


def _safe_regular_file(path: Path) -> bool:
    try:
        return path.is_file() and not path.is_symlink()
    except OSError:
        return False


def _validated_artifact_items(
    items: list[object],
    *,
    index_path: Path,
) -> tuple[tuple[str, Path, str], ...] | None:
    validated: list[tuple[str, Path, str]] = []
    names: set[str] = set()
    paths: set[Path] = set()
    for item in items:
        if not isinstance(item, Mapping):
            return None
        name = item.get("name")
        raw_path = item.get("path")
        digest = item.get("sha256")
        if (
            not isinstance(name, str)
            or not name
            or name in names
            or not isinstance(raw_path, str)
            or not raw_path
            or not isinstance(digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        ):
            return None
        path = _resolve_artifact_path(index_path=index_path, raw_path=raw_path)
        if path is None or path in paths or sha256_file(path) != digest:
            return None
        names.add(name)
        paths.add(path)
        validated.append((name, path, digest))
    return tuple(validated)


def _resolve_artifact_path(*, index_path: Path, raw_path: str) -> Path | None:
    configured = Path(raw_path)
    if not configured.is_absolute() and (".." in configured.parts or "." in configured.parts):
        return None
    candidate = configured if configured.is_absolute() else index_path.parent / configured
    try:
        resolved = candidate.resolve(strict=True)
        if not configured.is_absolute():
            resolved.relative_to(index_path.parent.resolve(strict=True))
    except (OSError, RuntimeError, ValueError):
        return None
    return resolved if _safe_bounded_artifact(candidate) else None


def _safe_bounded_artifact(path: Path) -> bool:
    try:
        return _safe_regular_file(path) and path.stat().st_size <= _MAX_ARTIFACT_BYTES
    except OSError:
        return False
