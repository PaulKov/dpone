from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

SNAPSHOT_ROOT_KEY = "airflowDevDponeSnapshot"
DEFAULT_TARGET_DIR = "/opt/airflow/.dpone-pkgs"
DEFAULT_INSTALL_MODE = "snapshot"


@dataclass(frozen=True)
class AirflowDevSnapshotOverride:
    enabled: bool
    install_mode: str
    package_spec: str
    target_dir: str

    def to_mapping(self) -> dict[str, Any]:
        return {
            SNAPSHOT_ROOT_KEY: {
                "enabled": self.enabled,
                "installMode": self.install_mode,
                "packageSpec": self.package_spec,
                "targetDir": self.target_dir,
            }
        }


@dataclass(frozen=True)
class SnapshotFileUpdateResult:
    path: Path
    changed: bool
    created: bool
    payload: dict[str, Any]


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ValueError(f"Expected YAML mapping in {path}, got {type(payload).__name__}")
    return payload


def render_airflow_dev_snapshot_override(
    package_spec: str,
    *,
    enabled: bool = True,
    install_mode: str = DEFAULT_INSTALL_MODE,
    target_dir: str = DEFAULT_TARGET_DIR,
) -> AirflowDevSnapshotOverride:
    if not package_spec or "==" not in package_spec:
        raise ValueError("package_spec must look like 'dpone==<version>'")
    if not install_mode:
        raise ValueError("install_mode must be non-empty")
    if not target_dir:
        raise ValueError("target_dir must be non-empty")
    return AirflowDevSnapshotOverride(
        enabled=enabled,
        install_mode=install_mode,
        package_spec=package_spec,
        target_dir=target_dir,
    )


def update_airflow_dev_snapshot_file(
    path: str | Path,
    package_spec: str,
    *,
    enabled: bool = True,
    install_mode: str = DEFAULT_INSTALL_MODE,
    target_dir: str = DEFAULT_TARGET_DIR,
) -> SnapshotFileUpdateResult:
    target = Path(path)
    existing = _load_yaml(target)
    created = not target.exists()

    override = render_airflow_dev_snapshot_override(
        package_spec,
        enabled=enabled,
        install_mode=install_mode,
        target_dir=target_dir,
    )

    updated = dict(existing)
    updated[SNAPSHOT_ROOT_KEY] = override.to_mapping()[SNAPSHOT_ROOT_KEY]
    changed = updated != existing

    if changed or created:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            yaml.safe_dump(updated, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )

    return SnapshotFileUpdateResult(path=target, changed=changed, created=created, payload=updated)
