"""Child-table quality checks for nested normalization."""

from __future__ import annotations

import warnings
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from dpone.runtime.normalization.options import NestedChildQualityOptions

_ROW_ID = "__dpone__row_id"
_PARENT_ROW_ID = "__dpone__parent_row_id"
_ROOT_ROW_ID = "__dpone__root_row_id"


@dataclass(frozen=True, slots=True)
class ChildQualityResult:
    """Observed duplicate and hierarchy status for one child table."""

    status: str
    root_table: str
    child_table: str
    parent_rows: int
    child_rows: int
    duplicate_child_keys: int | None
    orphan_child_rows: int | None
    duplicate_child_key_policy: str = "fail"
    orphan_child_rows_policy: str = "fail"
    duplicate_child_key_status: str = "passed"
    orphan_child_rows_status: str = "passed"
    duplicate_row_ids: int | None = None
    missing_row_ids: int | None = None
    row_identity_status: str = "not_applicable"

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "root_table": self.root_table,
            "child_table": self.child_table,
            "parent_rows": self.parent_rows,
            "child_rows": self.child_rows,
            "duplicate_child_keys": self.duplicate_child_keys,
            "orphan_child_rows": self.orphan_child_rows,
            "duplicate_child_key_policy": self.duplicate_child_key_policy,
            "orphan_child_rows_policy": self.orphan_child_rows_policy,
            "duplicate_child_key_status": self.duplicate_child_key_status,
            "orphan_child_rows_status": self.orphan_child_rows_status,
            "duplicate_row_ids": self.duplicate_row_ids,
            "missing_row_ids": self.missing_row_ids,
            "row_identity_status": self.row_identity_status,
        }


class ChildQualityViolationError(ValueError):
    """Fail a nested package before snapshot or target staging."""

    code = "nested_child_quality_failed"

    def __init__(self, results: Sequence[ChildQualityResult]) -> None:
        self.results = tuple(results)
        failures: list[str] = []
        for result in results:
            if result.duplicate_child_key_status == "failed":
                failures.append(f"{result.child_table}:duplicate_child_key={result.duplicate_child_keys}")
            if result.orphan_child_rows_status == "failed":
                failures.append(f"{result.child_table}:orphan_child_rows={result.orphan_child_rows}")
            if result.row_identity_status == "failed":
                failures.append(
                    f"{result.child_table}:duplicate_row_id={result.duplicate_row_ids},"
                    f"missing_row_id={result.missing_row_ids}"
                )
        super().__init__(f"{self.code}: " + ", ".join(failures))


@dataclass(slots=True)
class _TableScan:
    row_count: int = 0
    duplicate_child_keys: int | None = None
    parent_refs: list[tuple[object, object]] | None = None
    duplicate_row_ids: int | None = None
    missing_row_ids: int | None = None


@dataclass(frozen=True, slots=True)
class _ChildQualityObservation:
    child_table: str
    child_rows: int
    duplicate_child_keys: int | None
    orphan_child_rows: int | None
    duplicate_row_ids: int | None
    missing_row_ids: int | None
    has_unique_key: bool
    hierarchy_applicable: bool


class ChildQualityService:
    """Validate generated child tables before state or target mutation."""

    def check_tables(
        self,
        *,
        root_table: str,
        root_rows: Sequence[Mapping[str, object]],
        child_table: str,
        child_rows: Sequence[Mapping[str, object]],
        parent_key: Sequence[str],
        child_unique_key: Sequence[str],
    ) -> ChildQualityResult:
        """Retain the public business-key check used by existing callers."""

        parent_keys = {_key(row, parent_key) for row in root_rows}
        seen_child_keys: set[tuple[object, ...]] = set()
        duplicate_child_keys = 0
        orphan_child_rows = 0
        for row in child_rows:
            child_key = _key(row, child_unique_key)
            if child_key in seen_child_keys:
                duplicate_child_keys += 1
            seen_child_keys.add(child_key)
            if _key(row, parent_key) not in parent_keys:
                orphan_child_rows += 1
        status = "passed" if duplicate_child_keys == 0 and orphan_child_rows == 0 else "failed"
        return ChildQualityResult(
            status=status,
            root_table=root_table,
            child_table=child_table,
            parent_rows=len(root_rows),
            child_rows=len(child_rows),
            duplicate_child_keys=duplicate_child_keys,
            orphan_child_rows=orphan_child_rows,
        )

    def evaluate_package(
        self,
        *,
        root_table: str,
        table_rows: Mapping[str, Iterable[Mapping[str, object]]],
        table_keys: Mapping[str, Sequence[str]],
        options: NestedChildQualityOptions,
        table_parents: Mapping[str, str] | None = None,
        ignored_hierarchy_tables: Sequence[str] = (),
    ) -> tuple[ChildQualityResult, ...]:
        """Evaluate one complete hierarchy with fail/warn/skip policy semantics."""

        row_owners: dict[str, list[tuple[str, object]]] = {}
        root_row_ids: set[str] = set()
        scans: dict[str, _TableScan] = {}
        ignored = set(ignored_hierarchy_tables)
        resolved_parents = table_parents or {}
        for table_name, rows in table_rows.items():
            unique_key = tuple(table_keys.get(table_name, ()))
            is_child_table = table_name != root_table
            scan = _TableScan(
                duplicate_child_keys=(0 if unique_key and options.duplicate_child_key != "skip" else None),
                parent_refs=[] if is_child_table and table_name not in ignored else None,
                duplicate_row_ids=0 if is_child_table and not unique_key else None,
                missing_row_ids=0 if is_child_table else None,
            )
            seen_keys: set[tuple[object, ...]] = set()
            seen_row_ids: set[str] = set()
            for row in rows:
                scan.row_count += 1
                row_id = row.get(_ROW_ID)
                if row_id is None:
                    if scan.missing_row_ids is not None:
                        scan.missing_row_ids += 1
                else:
                    normalized_row_id = str(row_id)
                    row_owners.setdefault(normalized_row_id, []).append((table_name, row.get(_ROOT_ROW_ID)))
                    if scan.duplicate_row_ids is not None:
                        if normalized_row_id in seen_row_ids:
                            scan.duplicate_row_ids += 1
                        seen_row_ids.add(normalized_row_id)
                    if table_name == root_table:
                        root_row_ids.add(normalized_row_id)
                if scan.parent_refs is not None:
                    scan.parent_refs.append((row.get(_PARENT_ROW_ID), row.get(_ROOT_ROW_ID)))
                if scan.duplicate_child_keys is not None:
                    key = _key(row, unique_key)
                    if key in seen_keys:
                        scan.duplicate_child_keys += 1
                    seen_keys.add(key)
            scans[table_name] = scan

        root_rows = scans.get(root_table, _TableScan()).row_count
        observations = tuple(
            self._observation_for_table(
                child_table=table_name,
                scan=scan,
                has_unique_key=bool(table_keys.get(table_name)),
                expected_parent_table=resolved_parents.get(table_name),
                row_owners=row_owners,
                root_row_ids=root_row_ids,
                options=options,
            )
            for table_name, scan in scans.items()
            if table_name != root_table
        )
        return self._evaluate_observations(
            root_table=root_table,
            root_rows=root_rows,
            observations=observations,
            options=options,
        )

    def _observation_for_table(
        self,
        *,
        child_table: str,
        scan: _TableScan,
        has_unique_key: bool,
        expected_parent_table: str | None,
        row_owners: Mapping[str, Sequence[tuple[str, object]]],
        root_row_ids: set[str],
        options: NestedChildQualityOptions,
    ) -> _ChildQualityObservation:
        orphan_rows = (
            None
            if scan.parent_refs is None or options.orphan_child_rows == "skip"
            else sum(
                1
                for parent_id, root_id in scan.parent_refs
                if _is_orphan(
                    parent_id=parent_id,
                    root_id=root_id,
                    expected_parent_table=expected_parent_table,
                    row_owners=row_owners,
                    root_row_ids=root_row_ids,
                )
            )
        )
        return _ChildQualityObservation(
            child_table=child_table,
            child_rows=scan.row_count,
            duplicate_child_keys=scan.duplicate_child_keys,
            orphan_child_rows=orphan_rows,
            duplicate_row_ids=scan.duplicate_row_ids,
            missing_row_ids=scan.missing_row_ids,
            has_unique_key=has_unique_key,
            hierarchy_applicable=scan.parent_refs is not None,
        )

    def _evaluate_observations(
        self,
        *,
        root_table: str,
        root_rows: int,
        observations: Sequence[_ChildQualityObservation],
        options: NestedChildQualityOptions,
    ) -> tuple[ChildQualityResult, ...]:
        results = tuple(
            self._result_from_observation(
                root_table=root_table,
                root_rows=root_rows,
                observation=observation,
                options=options,
            )
            for observation in observations
        )
        self._apply_policy(results)
        return results

    def _result_from_observation(
        self,
        *,
        root_table: str,
        root_rows: int,
        observation: _ChildQualityObservation,
        options: NestedChildQualityOptions,
    ) -> ChildQualityResult:
        duplicate_status = _policy_status(
            policy=options.duplicate_child_key,
            violations=observation.duplicate_child_keys,
            applicable=observation.has_unique_key,
        )
        orphan_status = _policy_status(
            policy=options.orphan_child_rows,
            violations=observation.orphan_child_rows,
            applicable=observation.hierarchy_applicable,
        )
        row_identity_status = (
            "not_applicable"
            if observation.duplicate_row_ids is None and observation.missing_row_ids is None
            else "failed"
            if observation.duplicate_row_ids or observation.missing_row_ids
            else "passed"
        )
        return ChildQualityResult(
            status=_overall_status(duplicate_status, orphan_status, row_identity_status),
            root_table=root_table,
            child_table=observation.child_table,
            parent_rows=root_rows,
            child_rows=observation.child_rows,
            duplicate_child_keys=observation.duplicate_child_keys,
            orphan_child_rows=observation.orphan_child_rows,
            duplicate_child_key_policy=options.duplicate_child_key,
            orphan_child_rows_policy=options.orphan_child_rows,
            duplicate_child_key_status=duplicate_status,
            orphan_child_rows_status=orphan_status,
            duplicate_row_ids=observation.duplicate_row_ids,
            missing_row_ids=observation.missing_row_ids,
            row_identity_status=row_identity_status,
        )

    def _apply_policy(self, results: Sequence[ChildQualityResult]) -> None:
        failed = [result for result in results if result.status == "failed"]
        if failed:
            raise ChildQualityViolationError(failed)
        for result in results:
            warnings_to_emit: list[str] = []
            if result.duplicate_child_key_status == "warning":
                warnings_to_emit.append(f"duplicate_child_key={result.duplicate_child_keys}")
            if result.orphan_child_rows_status == "warning":
                warnings_to_emit.append(f"orphan_child_rows={result.orphan_child_rows}")
            if warnings_to_emit:
                warnings.warn(
                    f"child-quality warning for `{result.child_table}`: {', '.join(warnings_to_emit)}",
                    UserWarning,
                    stacklevel=3,
                )


def _is_orphan(
    *,
    parent_id: object,
    root_id: object,
    expected_parent_table: str | None,
    row_owners: Mapping[str, Sequence[tuple[str, object]]],
    root_row_ids: set[str],
) -> bool:
    if parent_id is None or root_id is None or str(root_id) not in root_row_ids:
        return True
    owners = row_owners.get(str(parent_id), ())
    if expected_parent_table is not None:
        owners = tuple(owner for owner in owners if owner[0] == expected_parent_table)
    return not any(owner_root is not None and str(owner_root) == str(root_id) for _table, owner_root in owners)


def _policy_status(*, policy: str, violations: int | None, applicable: bool) -> str:
    if not applicable:
        return "not_applicable"
    if policy == "skip":
        return "skipped"
    if violations:
        return "failed" if policy == "fail" else "warning"
    return "passed" if policy == "fail" else "observed"


def _overall_status(*statuses: str) -> str:
    if "failed" in statuses:
        return "failed"
    if "warning" in statuses:
        return "warning"
    if "observed" in statuses:
        return "observed"
    if "skipped" in statuses:
        return "skipped" if all(status in {"skipped", "not_applicable"} for status in statuses) else "partial"
    if all(status == "not_applicable" for status in statuses):
        return "not_applicable"
    return "passed"


def _key(row: Mapping[str, object], columns: Sequence[str]) -> tuple[object, ...]:
    return tuple(row.get(column) for column in columns)


__all__ = [
    "ChildQualityResult",
    "ChildQualityService",
    "ChildQualityViolationError",
]
