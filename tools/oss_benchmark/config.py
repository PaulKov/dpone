"""Static configuration for the OSS code-quality benchmark."""

from __future__ import annotations

from pathlib import Path

from tools.oss_benchmark.models import ProjectSpec

ROOT = Path(__file__).resolve().parents[2]
BENCHMARK_DATE = "2026-06-12"
SOURCE_SUFFIXES = {
    ".go",
    ".groovy",
    ".java",
    ".js",
    ".jsx",
    ".kt",
    ".kts",
    ".py",
    ".scala",
    ".ts",
    ".tsx",
}
IGNORED_DIRS = {
    ".cache",
    ".git",
    ".gradle",
    ".idea",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    "__pycache__",
    "build",
    "coverage",
    "dist",
    "generated",
    "node_modules",
    "out",
    "rendered",
    "site",
    "target",
    "test_artifacts",
    "tools",
    "vendor",
}
TEST_DIR_NAMES = {
    "__tests__",
    "integration-test",
    "integration-tests",
    "integration_tests",
    "it",
    "test",
    "test-fixtures",
    "testfixtures",
    "unit_tests",
    "tests",
}
LOGICAL_ANCHORS = (
    "airbyte-cdk",
    "airbyte-integrations",
    "airbyte-ci",
    "api",
    "cmd",
    "dlt",
    "dpone",
    "engine",
    "engine-ext",
    "plugins",
    "core",
    "web",
    "ui",
    "src",
)
DOC_PATH = Path("docs/benchmarks/oss-code-quality-benchmark-2026-06-12.md")
DATA_PATH = Path("docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json")
HISTORY_PATH = Path("docs/benchmarks/data/oss-code-quality-benchmark-history.json")
PROVENANCE_PATH = Path("docs/benchmarks/data/oss-benchmark-provenance.json")
ASSET_DIR = Path("docs/benchmarks/assets")
TRUST_CENTER_PATH = Path("docs/benchmarks/dpone-trust-center-snapshot-2026-06-12.md")
TRUST_CENTER_DATA_PATH = Path("docs/benchmarks/data/dpone-trust-center-snapshot-2026-06-12.json")
TRUST_CENTER_BADGE_PATH = Path("docs/benchmarks/assets/dpone-trust-center-badge.svg")
RELEASE_READINESS_PATH = Path("docs/benchmarks/oss-benchmark-release-readiness-2026-06-12.md")
RELEASE_READINESS_DATA_PATH = Path("docs/benchmarks/data/oss-benchmark-release-readiness-2026-06-12.json")
RELEASE_READINESS_SEAL_PATH = Path("docs/benchmarks/assets/oss-release-readiness-seal.svg")


def default_project_specs(workspace: Path) -> tuple[ProjectSpec, ...]:
    return (
        ProjectSpec(
            name="dpone",
            slug="dpone",
            kind="local-framework",
            repo_url="https://github.com/PaulKov/dpone",
            branch="local",
            commit="dynamic-at-refresh",
            path=ROOT,
        ),
        ProjectSpec(
            name="Airbyte",
            slug="airbyte",
            kind="oss-core",
            repo_url="https://github.com/airbytehq/airbyte.git",
            branch="master",
            commit="84c2d165ef11293b11e2321062e89996908d2f63",
            path=workspace / "airbyte",
        ),
        ProjectSpec(
            name="dlt",
            slug="dlt",
            kind="oss-core",
            repo_url="https://github.com/dlt-hub/dlt.git",
            branch="devel",
            commit="f5999614b613254961a9073310a5db6b13dc279d",
            path=workspace / "dlt",
        ),
        ProjectSpec(
            name="Pentaho Kettle",
            slug="pentaho-kettle",
            kind="oss-core",
            repo_url="https://github.com/pentaho/pentaho-kettle.git",
            branch="master",
            commit="89db5885a1e92e81eb01149cb3e536cc23482c39",
            path=workspace / "pentaho-kettle",
        ),
        ProjectSpec(
            name="Apache Hop",
            slug="apache-hop",
            kind="oss-core",
            repo_url="https://github.com/apache/hop.git",
            branch="main",
            commit="6aad589666c8458216b8685bb749d2b6dbd1b2f0",
            path=workspace / "apache-hop",
        ),
        ProjectSpec(
            name="Sling",
            slug="sling",
            kind="oss-core",
            repo_url="https://github.com/slingdata-io/sling-cli.git",
            branch="main",
            commit="6c4ca04c3328eb32da480780c9957bf17f80ffb5",
            path=workspace / "sling",
        ),
    )
