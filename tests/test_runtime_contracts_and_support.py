from __future__ import annotations

from pathlib import Path

from tools.oss_benchmark.architecture_taxonomy import build_architecture_taxonomy_matrix


def test_runtime_has_no_static_core_or_lib_imports() -> None:
    runtime_dir = Path("src/dpone/runtime")
    offenders: list[str] = []
    for path in runtime_dir.rglob("*.py"):
        text = path.read_text()
        for idx, line in enumerate(text.splitlines(), 1):
            line = line.strip()
            if line.startswith("from dpone.core") or line.startswith("import dpone.core"):
                offenders.append(f"{path}:{idx}:{line}")
            if line.startswith("from dpone.lib") or line.startswith("import dpone.lib"):
                offenders.append(f"{path}:{idx}:{line}")
    assert offenders == []


def test_runtime_modules_depend_on_narrow_artifact_ports() -> None:
    runtime_dir = Path("src/dpone/runtime")
    offenders: list[str] = []
    for path in runtime_dir.rglob("*.py"):
        if path.name == "artifacts.py":
            continue
        text = path.read_text()
        for idx, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("from dpone.runtime.artifacts import"):
                offenders.append(f"{path}:{idx}:{stripped}")
    assert offenders == []


def test_runtime_modules_depend_on_narrow_config_ports() -> None:
    runtime_dir = Path("src/dpone/runtime")
    offenders: list[str] = []
    for path in runtime_dir.rglob("*.py"):
        text = path.read_text()
        for idx, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("from dpone.config import") or stripped.startswith("import dpone.config"):
                offenders.append(f"{path}:{idx}:{stripped}")
    assert offenders == []


def test_runtime_sources_and_sinks_depend_on_connector_ports() -> None:
    matrix = build_architecture_taxonomy_matrix(
        [
            {
                "spec": {"slug": "dpone", "name": "dpone", "path": str(Path.cwd())},
                "coupling": {"cross_slice_ratio": 0.0, "cohesion_ratio": 1.0, "top_out": []},
            }
        ]
    )
    violations = matrix["summary"]["dpone"]["contract_conformance"]["violations"]
    offenders = [
        f"{item['path']}: {item['message']}"
        for item in violations
        if item["kind"] == "di_boundary" and item["slice"] in {"runtime.sinks", "runtime.sources"}
    ]
    assert offenders == []


def test_core_and_lib_shims_still_expose_canonical_symbols() -> None:
    from dpone.contracts.errors import ETLConfigurationError as ContractsConfigError
    from dpone.contracts.technical_columns import include_technical_columns as CanonicalInclude
    from dpone.core.artifacts import FileExportArtifact as CoreFileExportArtifact
    from dpone.core.errors import ETLConfigurationError as CoreConfigError
    from dpone.lib.technical_columns import include_technical_columns as ShimInclude
    from dpone.runtime.artifacts import FileExportArtifact as RuntimeFileExportArtifact

    assert CoreFileExportArtifact is RuntimeFileExportArtifact
    assert CoreConfigError is ContractsConfigError
    assert ShimInclude is CanonicalInclude
