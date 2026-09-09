"""Source-target reconciliation diff utilities."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class OpsDiffSample:
    kind: str
    key: dict[str, object]
    columns: tuple[str, ...]
    source: dict[str, object] | None
    target: dict[str, object] | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class OpsDiffReport:
    passed: bool
    source_count: int
    target_count: int
    missing_in_target_count: int
    extra_in_target_count: int
    mismatch_count: int
    duplicate_source_key_count: int
    duplicate_target_key_count: int
    samples: tuple[OpsDiffSample, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "source_count": self.source_count,
            "target_count": self.target_count,
            "missing_in_target_count": self.missing_in_target_count,
            "extra_in_target_count": self.extra_in_target_count,
            "mismatch_count": self.mismatch_count,
            "duplicate_source_key_count": self.duplicate_source_key_count,
            "duplicate_target_key_count": self.duplicate_target_key_count,
            "samples": [sample.to_dict() for sample in self.samples],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone ops reconciliation diff",
            "",
            f"- Passed: `{self.passed}`",
            f"- Source rows: `{self.source_count}`",
            f"- Target rows: `{self.target_count}`",
            f"- Missing in target: `{self.missing_in_target_count}`",
            f"- Extra in target: `{self.extra_in_target_count}`",
            f"- Mismatches: `{self.mismatch_count}`",
            f"- Duplicate source keys: `{self.duplicate_source_key_count}`",
            f"- Duplicate target keys: `{self.duplicate_target_key_count}`",
            "",
        ]
        if not self.passed:
            lines.extend(
                [
                    "Fix source-target drift before state commit.",
                    "",
                    "| kind | key | columns |",
                    "|---|---|---|",
                ]
            )
            for sample in self.samples:
                lines.append(
                    f"| `{sample.kind}` | `{json.dumps(sample.key, sort_keys=True)}` | `{', '.join(sample.columns)}` |"
                )
        else:
            lines.append("Source and target rows match for the configured keys and columns.")
        return "\n".join(lines) + "\n"


class OpsDiffService:
    """Compares bounded source and target row sets for operational reconciliation."""

    def compare(
        self,
        *,
        source_rows: Sequence[Mapping[str, Any]],
        target_rows: Sequence[Mapping[str, Any]],
        key_columns: Sequence[str],
        compare_columns: Sequence[str] | None = None,
        sample_limit: int = 20,
    ) -> OpsDiffReport:
        keys = tuple(str(column) for column in key_columns)
        if not keys:
            raise ValueError("At least one key column is required for ops diff.")
        limit = max(int(sample_limit), 0)
        source_index, duplicate_source = self._index(source_rows, keys)
        target_index, duplicate_target = self._index(target_rows, keys)
        columns = self._compare_columns(source_index, target_index, keys, compare_columns)

        source_keys = set(source_index)
        target_keys = set(target_index)
        missing = tuple(sorted(source_keys - target_keys, key=self._sort_key))
        extra = tuple(sorted(target_keys - source_keys, key=self._sort_key))
        mismatches = self._mismatches(source_index, target_index, source_keys & target_keys, columns)
        samples = self._samples(
            missing=missing,
            extra=extra,
            mismatches=mismatches,
            duplicate_source=duplicate_source,
            duplicate_target=duplicate_target,
            source_index=source_index,
            target_index=target_index,
            keys=keys,
            limit=limit,
        )
        return OpsDiffReport(
            passed=not missing and not extra and not mismatches and not duplicate_source and not duplicate_target,
            source_count=len(source_rows),
            target_count=len(target_rows),
            missing_in_target_count=len(missing),
            extra_in_target_count=len(extra),
            mismatch_count=len(mismatches),
            duplicate_source_key_count=len(duplicate_source),
            duplicate_target_key_count=len(duplicate_target),
            samples=samples,
        )

    def _index(
        self,
        rows: Sequence[Mapping[str, Any]],
        key_columns: tuple[str, ...],
    ) -> tuple[dict[tuple[object, ...], dict[str, object]], tuple[tuple[object, ...], ...]]:
        index: dict[tuple[object, ...], dict[str, object]] = {}
        duplicates: list[tuple[object, ...]] = []
        for row in rows:
            normalized = dict(row)
            key = tuple(normalized.get(column) for column in key_columns)
            if key in index:
                duplicates.append(key)
                continue
            index[key] = normalized
        return index, tuple(duplicates)

    def _compare_columns(
        self,
        source_index: Mapping[tuple[object, ...], Mapping[str, object]],
        target_index: Mapping[tuple[object, ...], Mapping[str, object]],
        key_columns: tuple[str, ...],
        compare_columns: Sequence[str] | None,
    ) -> tuple[str, ...]:
        if compare_columns is not None:
            return tuple(str(column) for column in compare_columns)
        columns = {
            column
            for row in [*source_index.values(), *target_index.values()]
            for column in row
            if column not in key_columns
        }
        return tuple(sorted(columns))

    def _mismatches(
        self,
        source_index: Mapping[tuple[object, ...], Mapping[str, object]],
        target_index: Mapping[tuple[object, ...], Mapping[str, object]],
        common_keys: set[tuple[object, ...]],
        compare_columns: tuple[str, ...],
    ) -> tuple[tuple[tuple[object, ...], tuple[str, ...]], ...]:
        mismatches: list[tuple[tuple[object, ...], tuple[str, ...]]] = []
        for key in sorted(common_keys, key=self._sort_key):
            changed = tuple(
                column for column in compare_columns if source_index[key].get(column) != target_index[key].get(column)
            )
            if changed:
                mismatches.append((key, changed))
        return tuple(mismatches)

    def _samples(
        self,
        *,
        missing: tuple[tuple[object, ...], ...],
        extra: tuple[tuple[object, ...], ...],
        mismatches: tuple[tuple[tuple[object, ...], tuple[str, ...]], ...],
        duplicate_source: tuple[tuple[object, ...], ...],
        duplicate_target: tuple[tuple[object, ...], ...],
        source_index: Mapping[tuple[object, ...], dict[str, object]],
        target_index: Mapping[tuple[object, ...], dict[str, object]],
        keys: tuple[str, ...],
        limit: int,
    ) -> tuple[OpsDiffSample, ...]:
        samples: list[OpsDiffSample] = []
        self._append_key_samples(samples, "duplicate_source_key", duplicate_source, keys, limit)
        self._append_key_samples(samples, "duplicate_target_key", duplicate_target, keys, limit)
        for key in missing:
            if len(samples) >= limit:
                break
            samples.append(
                OpsDiffSample("missing_in_target", self._key_dict(keys, key), tuple(), source_index[key], None)
            )
        for key in extra:
            if len(samples) >= limit:
                break
            samples.append(
                OpsDiffSample("extra_in_target", self._key_dict(keys, key), tuple(), None, target_index[key])
            )
        for key, columns in mismatches:
            if len(samples) >= limit:
                break
            samples.append(
                OpsDiffSample("mismatch", self._key_dict(keys, key), columns, source_index[key], target_index[key])
            )
        return tuple(samples)

    def _append_key_samples(
        self,
        samples: list[OpsDiffSample],
        kind: str,
        key_values: tuple[tuple[object, ...], ...],
        keys: tuple[str, ...],
        limit: int,
    ) -> None:
        for key in key_values:
            if len(samples) >= limit:
                return
            samples.append(OpsDiffSample(kind, self._key_dict(keys, key), tuple(keys), None, None))

    @staticmethod
    def _key_dict(key_columns: tuple[str, ...], key: tuple[object, ...]) -> dict[str, object]:
        return dict(zip(key_columns, key, strict=True))

    @staticmethod
    def _sort_key(key: tuple[object, ...]) -> str:
        return json.dumps(key, default=str, sort_keys=True)
