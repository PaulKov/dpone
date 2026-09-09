"""Data models for the OSS code-quality benchmark."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectSpec:
    name: str
    slug: str
    kind: str
    repo_url: str
    branch: str
    commit: str
    path: Path


@dataclass(frozen=True)
class FileMetric:
    path: str
    lines: int
    sloc: int
    is_test: bool


@dataclass(frozen=True)
class LocSummary:
    files: int
    total_lines: int
    total_sloc: int
    max_lines: int
    max_sloc: int
    avg_lines: float
    median_lines: float
    p90_lines: float


@dataclass(frozen=True)
class CouplingMetrics:
    modules: int
    internal_edges: int
    avg_ce: float
    median_ce: float
    p90_ce: float
    p95_ce: float
    max_ce: int
    max_ce_module: str
    max_ca: int
    max_ca_module: str
    lcc_ratio: float
    avg_clustering: float
    cohesion_ratio: float
    cross_slice_ratio: float
    top_out: tuple[tuple[str, int], ...]
    top_in: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class QualitySignal:
    max_module_loc: int
    p90_module_loc: float
    avg_ce: float
    p90_ce: float
    max_ce: int
    avg_clustering: float
    cohesion_ratio: float
    interface_density: float


@dataclass(frozen=True)
class QualityScore:
    solid: float
    clean_oop: float
    evidence: tuple[str, ...]


@dataclass(frozen=True)
class ProjectMetrics:
    spec: ProjectSpec
    dirty: bool
    loc_with_tests: LocSummary
    loc_without_tests: LocSummary
    top_loc_with_tests: tuple[FileMetric, ...]
    top_sloc_with_tests: tuple[FileMetric, ...]
    top_loc_without_tests: tuple[FileMetric, ...]
    top_sloc_without_tests: tuple[FileMetric, ...]
    coupling: CouplingMetrics
    quality: QualityScore
    source_files_without_tests: int
    interface_like_files: int
