"""Public ETL process config model for DAG / manifest layers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.contracts.process_types import DependencyConfig, TransformConfig


from dataclasses import dataclass, field
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.config.load_strategy import LoadStrategy
from dpone.dag.errors import DagConfigurationError


def _symbol(path: str) -> Any:
    module_name, attr = path.split(":", 1)
    return getattr(import_module(module_name), attr)


def _split_manifest_ref(yaml_path: str | Path) -> tuple[Path, str | None]:
    return _symbol("dpone.dag.config_refs:split_manifest_ref")(yaml_path)


def _load_via_manifest_loader(path: Path, selector: str | None, *, metadata_only: bool) -> Any:
    return _symbol("dpone.dag.config_refs:load_via_manifest_loader")(path, selector, metadata_only=metadata_only)


@dataclass
class ETLProcessConfig:
    """Pure parsed ETL config.

    The DAG/manifest layers own only the *parsed* representation. Runtime
    objects (source/sink/logger/state storage) are attached via the runtime
    hydrator port when full execution is requested.
    """

    name: str
    load_config: LoadConfig
    transforms: list[TransformConfig] = field(default_factory=list)
    dependencies: list[DependencyConfig] = field(default_factory=list)
    options: dict[str, Any] = field(default_factory=dict)
    description: str | None = None
    unique_key: str | list[str] | None = None
    load_strategy: LoadStrategy = LoadStrategy.FULL_REFRESH
    task_group: str | None = None
    raw_config: dict[str, Any] = field(default_factory=dict, repr=False)
    source_obj: Any = None
    sink_obj: Any = None
    etl_logger: Any = None
    run_state_storage: Any | None = None
    xmin_handoff_state_storage: Any | None = None
    partition_checkpoint_store: Any | None = None
    load_identity_service: Any | None = None
    credential_resolution_receipts: tuple[dict[str, Any], ...] = ()
    postgres_mssql_correctness_activation: Any | None = None
    postgres_mssql_correctness_runtime: Any | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "load": self.load_config,
            "transforms": [tc.__dict__ for tc in self.transforms],
            "depends_on": [dc.__dict__ for dc in self.dependencies],
            "options": self.options,
        }

    @property
    def is_runtime_bound(self) -> bool:
        return self.source_obj is not None and self.sink_obj is not None

    def apply_runtime_bindings(self, bindings: Any) -> None:
        self.source_obj = bindings.source_obj
        self.sink_obj = bindings.sink_obj
        self.etl_logger = bindings.etl_logger
        self.run_state_storage = bindings.run_state_storage
        self.xmin_handoff_state_storage = getattr(bindings, "xmin_handoff_state_storage", None)
        self.partition_checkpoint_store = bindings.partition_checkpoint_store
        self.load_identity_service = getattr(bindings, "load_identity_service", None)
        self.postgres_mssql_correctness_activation = getattr(
            bindings,
            "postgres_mssql_correctness_activation",
            None,
        )
        self.postgres_mssql_correctness_runtime = getattr(bindings, "postgres_mssql_correctness_runtime", None)
        self.credential_resolution_receipts = tuple(
            dict(receipt) for receipt in bindings.credential_resolution_receipts
        )

    def ensure_runtime_bindings(self) -> None:
        if self.is_runtime_bound:
            return
        if not self.raw_config:
            raise DagConfigurationError(
                "Runtime bindings cannot be created because raw_config is missing. "
                "Load the config via ETLProcessConfig.from_dict()/from_yaml() or "
                "attach bindings explicitly."
            )
        bindings = _symbol("dpone.ports.runtime_hydrator:ensure_runtime_hydrator")().build(
            config=self.raw_config,
            load_config=self.load_config,
        )
        self.apply_runtime_bindings(bindings)

    @staticmethod
    def _split_manifest_ref(yaml_path: str | Path) -> tuple[Path, str | None]:
        return _split_manifest_ref(yaml_path)

    @classmethod
    def _load_via_manifest_loader(
        cls,
        path: Path,
        selector: str | None,
        *,
        metadata_only: bool,
    ) -> ETLProcessConfig:
        return _load_via_manifest_loader(path, selector, metadata_only=metadata_only)

    @classmethod
    def from_yaml(cls, yaml_path: str | Path) -> ETLProcessConfig:
        path, selector = cls._split_manifest_ref(yaml_path)
        if not path.exists():
            raise DagConfigurationError(f"YAML конфигурация не найдена: {path}")
        return cls._load_via_manifest_loader(path, selector, metadata_only=False)

    @classmethod
    def from_yaml_metadata(cls, yaml_path: str | Path) -> ETLProcessConfig:
        path, selector = cls._split_manifest_ref(yaml_path)
        if not path.exists():
            raise DagConfigurationError(f"YAML конфигурация не найдена: {path}")
        return cls._load_via_manifest_loader(path, selector, metadata_only=True)

    @classmethod
    def from_dict(
        cls,
        config: dict[str, Any],
        *,
        base_path: Path | None = None,
        metadata_only: bool = False,
        parse_tracer: Any | None = None,
    ) -> ETLProcessConfig:
        if str(config.get("kind") or "").strip() == "dpone.batch.v1":
            return cls._from_batch_dict(
                config,
                base_path=base_path,
                metadata_only=metadata_only,
                parse_tracer=parse_tracer,
            )
        parser = _symbol("dpone.dag.process_config_parser:ETLProcessConfigParser")()
        return parser.parse(
            config,
            base_path=base_path,
            metadata_only=metadata_only,
            parse_tracer=parse_tracer,
        )

    @classmethod
    def _from_batch_dict(
        cls,
        config: dict[str, Any],
        *,
        base_path: Path | None,
        metadata_only: bool,
        parse_tracer: Any | None,
    ) -> ETLProcessConfig:
        manifest_dir = base_path if base_path is not None else Path.cwd()
        manifest_path = manifest_dir / "__inline__.batch.yaml"

        apply_conventions = _symbol("dpone.manifest.conventions:apply_conventions")
        apply_registries = _symbol("dpone.manifest.registry:apply_registries")
        batch_compiler_cls = _symbol("dpone.manifest.batch_compiler_impl:BatchManifestCompiler")

        effective = apply_conventions(config, manifest_path=manifest_path)
        effective = apply_registries(effective, manifest_path=manifest_path)
        compiled = batch_compiler_cls().compile(effective, manifest_path=manifest_path)

        if len(compiled) != 1:
            selectors = ", ".join(proc.selector for proc in compiled)
            raise DagConfigurationError(
                "Batch manifest нельзя однозначно распарсить через ETLProcessConfig.from_dict(): "
                f"получено {len(compiled)} процессов ({selectors}). "
                "Используйте ETLProcessConfig.from_yaml('<path>.yaml#<selector>') "
                "или передайте уже скомпилированный процесс dict."
            )

        return cls.from_dict(
            compiled[0].raw_config,
            base_path=base_path,
            metadata_only=metadata_only,
            parse_tracer=parse_tracer,
        )

    @staticmethod
    def _build_load_config(config: dict[str, Any], *, parse_tracer: Any | None = None) -> LoadConfig:
        return _symbol("dpone.dag.load_config_builder:LoadConfigBuilder")().build(
            config,
            parse_tracer=parse_tracer,
        )


__all__ = ["ETLProcessConfig"]
