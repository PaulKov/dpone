from __future__ import annotations

from pathlib import Path

import pytest

from dpone.manifest.models import LoadedManifest, ProcessSpec
from dpone.services.manifest.load_context import ManifestSelectionError, resolve_single_process


def _process(name: str) -> ProcessSpec:
    return ProcessSpec(
        name=name,
        selector=f"dbo.{name}",
        config_path=Path("pipelines/orders.yaml"),
        config=object(),
        raw_config={},
    )


def test_ambiguous_manifest_recommends_only_supported_selector_option() -> None:
    manifest = LoadedManifest(
        path=Path("pipelines/orders.yaml"),
        kind="dpone.batch.v1",
        raw={},
        processes=(_process("orders"), _process("order_items")),
    )

    with pytest.raises(ManifestSelectionError) as exc_info:
        resolve_single_process(manifest, selector=None)

    message = str(exc_info.value)
    assert "--selector <process-name-or-selector>" in message
    assert "--all" not in message
