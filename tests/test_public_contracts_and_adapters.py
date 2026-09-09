from __future__ import annotations

import logging
from importlib.metadata import version
from pathlib import Path

import pytest

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.fs_memory import InMemoryFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.logging import setup_logging
from dpone.app.settings import Settings


def test_local_filesystem_creates_parent_dirs_and_globs(tmp_path: Path) -> None:
    fs = LocalFileSystem()
    target = tmp_path / "nested" / "config.yaml"

    fs.write_text(target, "name: demo")

    assert fs.exists(target)
    assert fs.read_text(target) == "name: demo"
    assert fs.read_bytes(target) == b"name: demo"
    assert list(fs.glob(tmp_path, "nested/*.yaml")) == [target]


def test_in_memory_filesystem_supports_read_write_exists_and_missing_file() -> None:
    fs = InMemoryFileSystem()
    path = Path("manifest.yaml")

    assert not fs.exists(path)
    with pytest.raises(FileNotFoundError, match="manifest.yaml"):
        fs.read_text(path)

    fs.write_text(path, "kind: dpone.batch.v1")

    assert fs.exists(path)
    assert fs.read_text(path) == "kind: dpone.batch.v1"
    assert fs.read_bytes(path) == b"kind: dpone.batch.v1"


def test_in_memory_filesystem_documents_glob_as_unsupported() -> None:
    fs = InMemoryFileSystem()

    with pytest.raises(NotImplementedError, match="glob is not implemented"):
        list(fs.glob(Path("."), "*.yaml"))


def test_pyyaml_codec_round_trips_unicode_without_sorting_keys() -> None:
    codec = PyYamlCodec()

    dumped = codec.dump({"z": "последний", "a": [1, 2]})

    assert dumped.index("z:") < dumped.index("a:")
    assert "последний" in dumped
    assert codec.load(dumped) == {"z": "последний", "a": [1, 2]}


def test_settings_from_env_detects_repo_root_and_resolves_registry_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    nested = repo / "sub" / "dir"
    nested.mkdir(parents=True)
    (repo / "pyproject.toml").write_text("[project]\nname = 'demo'\n", encoding="utf-8")
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    monkeypatch.setenv("DPONE_PROJECT_DIR", str(project_dir))
    monkeypatch.setenv("DPONE_SOURCES_REGISTRY", "registry/sources.yaml,/abs/sources.yaml")

    settings = Settings.from_env(cwd=nested)

    assert settings.repo_root == repo
    assert settings.project_dir == project_dir
    assert settings.manifest_dir == project_dir / "etl-process-manifest"
    assert settings.sources_registry_paths == (project_dir / "registry" / "sources.yaml", Path("/abs/sources.yaml"))


def test_settings_from_env_falls_back_to_cwd_without_project_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DPONE_PROJECT_DIR", raising=False)
    monkeypatch.delenv("DPONE_SOURCES_REGISTRY", raising=False)

    settings = Settings.from_env(cwd=tmp_path)

    assert settings.repo_root == tmp_path
    assert settings.project_dir == tmp_path
    assert settings.manifest_dir == tmp_path / "etl-process-manifest"
    assert settings.sources_registry_paths == ()


def test_app_context_from_env_wires_default_adapters(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DPONE_PROJECT_DIR", str(tmp_path))
    logger = logging.getLogger("dpone-test")

    context = AppContext.from_env(logger=logger)

    assert context.logger is logger
    assert isinstance(context.fs, LocalFileSystem)
    assert isinstance(context.yaml, PyYamlCodec)
    assert context.settings.project_dir == tmp_path


def test_setup_logging_returns_dpone_logger_and_tolerates_invalid_level(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DPONE_LOG_LEVEL", "not-a-level")

    logger = setup_logging()

    assert logger.name == "dpone"


def test_root_package_lazy_exports_known_symbols_and_rejects_unknown_names() -> None:
    import dpone

    value = getattr(dpone, "LoadStrategy")

    assert value is dpone.LoadStrategy
    with pytest.raises(AttributeError, match="does-not-exist"):
        getattr(dpone, "does-not-exist")


def test_root_package_exposes_installed_distribution_version() -> None:
    import dpone

    assert dpone.__version__ == version("dpone")
    assert "__version__" in dpone.__all__


def test_cli_reference_source_builds_canonical_root_parser() -> None:
    from dpone.app.cli_reference_source import build_root_parser

    parser = build_root_parser()

    assert parser.prog == "dpone"
    assert "manifest" in parser.format_help()
