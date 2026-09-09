"""Bounded source-tree copy for provider-produced dbt dev evidence."""

from __future__ import annotations

import stat
from pathlib import Path

from dpone.contracts.dbt_dev_evidence_campaign import (
    DBT_DEV_EVIDENCE_CAMPAIGN_OUTCOME_FILENAME,
    DBT_DEV_EVIDENCE_REQUEST_FILENAME,
    MAX_DBT_DEV_EVIDENCE_SOURCE_FILES,
)
from dpone.manifest.confined_files import ConfinedFileError, read_confined_file

_CATEGORIES = ("airflow", "dbt", "outcomes")
_CAMPAIGN_FILES = (
    DBT_DEV_EVIDENCE_CAMPAIGN_OUTCOME_FILENAME,
    DBT_DEV_EVIDENCE_REQUEST_FILENAME,
)
_MAX_SOURCE_FILE_BYTES = 16 * 1024 * 1024


class DbtDevEvidenceSourceError(ValueError):
    """Provider evidence source is incomplete, unsafe or ambiguous."""


def copy_dev_evidence_tree(
    source: Path,
    destination: Path,
    *,
    campaign_bound: bool,
) -> None:
    """Copy one exact category tree without following links."""

    try:
        source_metadata = source.lstat()
        root_entries = sorted(source.iterdir(), key=lambda item: item.name)
    except OSError as exc:
        raise DbtDevEvidenceSourceError("source dev evidence cannot be read") from exc
    if stat.S_ISLNK(source_metadata.st_mode) or not stat.S_ISDIR(source_metadata.st_mode):
        raise DbtDevEvidenceSourceError("source dev evidence root is invalid")
    expected_entries = sorted((*_CATEGORIES, *(_CAMPAIGN_FILES if campaign_bound else ())))
    if [item.name for item in root_entries] != expected_entries:
        raise DbtDevEvidenceSourceError("source dev evidence inventory is not exact")
    _copy_campaign_files(
        source,
        destination,
        campaign_bound=campaign_bound,
    )
    count = 0
    for category in _CATEGORIES:
        count = _copy_category(
            source,
            destination,
            category=category,
            count=count,
        )


def _copy_campaign_files(
    source: Path,
    destination: Path,
    *,
    campaign_bound: bool,
) -> None:
    for filename in _CAMPAIGN_FILES if campaign_bound else ():
        try:
            metadata = (source / filename).lstat()
        except OSError as exc:
            raise DbtDevEvidenceSourceError("source dev evidence campaign file is missing") from exc
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise DbtDevEvidenceSourceError("source dev evidence campaign file is invalid")
        try:
            (destination / filename).write_bytes(
                read_confined_file(
                    source,
                    filename,
                    max_bytes=1024 * 1024,
                )
            )
        except (ConfinedFileError, OSError) as exc:
            raise DbtDevEvidenceSourceError("source dev evidence campaign file cannot be copied") from exc


def _copy_category(
    source: Path,
    destination: Path,
    *,
    category: str,
    count: int,
) -> int:
    source_dir = source / category
    try:
        metadata = source_dir.lstat()
        files = sorted(source_dir.iterdir(), key=lambda item: item.name)
    except OSError as exc:
        raise DbtDevEvidenceSourceError("source dev evidence category cannot be read") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise DbtDevEvidenceSourceError("source dev evidence category is invalid")
    if not files:
        raise DbtDevEvidenceSourceError("source dev evidence category is empty")
    target_dir = destination / category
    target_dir.mkdir(mode=0o750)
    for path in files:
        file_metadata = path.lstat()
        if stat.S_ISLNK(file_metadata.st_mode) or not stat.S_ISREG(file_metadata.st_mode) or path.suffix != ".json":
            raise DbtDevEvidenceSourceError("source dev evidence file is invalid")
        relative = f"{category}/{path.name}"
        try:
            payload = read_confined_file(
                source,
                relative,
                max_bytes=_MAX_SOURCE_FILE_BYTES,
            )
            (destination / relative).write_bytes(payload)
        except (ConfinedFileError, OSError) as exc:
            raise DbtDevEvidenceSourceError("source dev evidence file cannot be copied") from exc
        count += 1
        if count > MAX_DBT_DEV_EVIDENCE_SOURCE_FILES:
            raise DbtDevEvidenceSourceError("source dev evidence has too many files")
    return count


__all__ = [
    "DbtDevEvidenceSourceError",
    "copy_dev_evidence_tree",
]
