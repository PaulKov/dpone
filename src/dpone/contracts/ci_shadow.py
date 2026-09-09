"""Closed contracts for the non-authoritative CI shadow producer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal

JobName = Literal[
    "static",
    "contracts",
    "docs",
    "python-3.11",
    "python-3.12",
    "packaging",
    "postgresql",
    "airflow",
    "runtime-wheel-smoke",
]
Selection = Literal["RUN", "N/A"]
Outcome = Literal["PASS", "FAIL", "N/A", "UNVERIFIED"]

JOB_ORDER: Final[tuple[JobName, ...]] = (
    "static",
    "contracts",
    "docs",
    "python-3.11",
    "python-3.12",
    "packaging",
    "postgresql",
    "airflow",
    "runtime-wheel-smoke",
)
FULL_ROUTE: Final[frozenset[JobName]] = frozenset(JOB_ORDER)
CORE_ROUTE: Final[frozenset[JobName]] = frozenset({"static", "contracts", "python-3.11", "python-3.12", "packaging"})
DOCS_ROUTE: Final[frozenset[JobName]] = frozenset({"static", "contracts", "docs"})
LOCK_ROUTE: Final[frozenset[JobName]] = CORE_ROUTE | frozenset({"runtime-wheel-smoke"})
POSTGRES_ROUTE: Final[frozenset[JobName]] = CORE_ROUTE | frozenset({"postgresql"})
AIRFLOW_ROUTE: Final[frozenset[JobName]] = CORE_ROUTE | frozenset({"airflow", "runtime-wheel-smoke"})


@dataclass(frozen=True)
class ShadowIdentity:
    """Immutable producer subject identity supplied by the PR event."""

    repository_id: int
    pr_number: int
    base_sha: str
    head_sha: str
    merge_sha: str


def jobs_for(*, changed_paths: tuple[str, ...], semantic_ambiguous: bool = False) -> frozenset[JobName]:
    """Choose a conservative closed route; uncertainty can never reduce work."""

    if semantic_ambiguous or not changed_paths:
        return FULL_ROUTE
    paths = set(changed_paths)
    if paths <= {"uv.lock"}:
        return LOCK_ROUTE
    if paths <= {"pyproject.toml"}:
        return FULL_ROUTE
    if any(path.startswith(".github/workflows/") for path in paths):
        return FULL_ROUTE
    if all(path.startswith("docs/") or path in {"mkdocs.yml", "README.md"} for path in paths):
        return DOCS_ROUTE
    if any(
        path.startswith(("packages/dpone-airflow-pack/", ".github/workflows/airflow-", "constraints/"))
        for path in paths
    ):
        return AIRFLOW_ROUTE
    if any("postgres" in path.lower() or "xmin" in path.lower() for path in paths):
        return POSTGRES_ROUTE
    if any(path.startswith(("src/", "tests/", "tools/", "packages/")) for path in paths):
        return CORE_ROUTE
    return FULL_ROUTE


def selected_jobs(selected: frozenset[JobName]) -> dict[str, str]:
    """Return the canonical complete selection projection."""

    return {job: "RUN" if job in selected else "N/A" for job in JOB_ORDER}
