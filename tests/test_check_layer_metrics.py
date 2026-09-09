from __future__ import annotations

import argparse
import dataclasses
import json
import logging
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.metrics.layer_metrics import compute_layer_coupling_stats
from dpone.metrics.layer_metrics_gate import (
    LayerMetricsThresholds,
    build_layer_metrics_snapshot,
    evaluate_layer_metrics_gate,
)
from dpone.services.docs.check_layer_metrics_service import CheckLayerMetricsService


def test_evaluate_layer_metrics_gate_detects_thresholds_and_regression() -> None:
    deps_out = {
        "dpone.commands.a": {"dpone.services.a", "dpone.commands.b"},
        "dpone.commands.b": {"dpone.services.a"},
        "dpone.services.a": {"dpone.ports.fs"},
        "dpone.ports.fs": set(),
    }
    stats = compute_layer_coupling_stats(deps_out, top_n=5, package_name="dpone")
    snapshot = build_layer_metrics_snapshot(stats, package_name="dpone")
    baseline = dataclasses.replace(
        snapshot,
        intra_layer_ratio=min(1.0, snapshot.intra_layer_ratio + 0.2),
        cross_layer_ratio=max(0.0, snapshot.cross_layer_ratio - 0.2),
        max_cross_layer_flow=max(0, snapshot.max_cross_layer_flow - 1),
    )
    report = evaluate_layer_metrics_gate(
        snapshot,
        thresholds=LayerMetricsThresholds(
            min_intra_layer_ratio=min(1.0, snapshot.intra_layer_ratio + 0.1),
            max_cross_layer_ratio=max(0.0, snapshot.cross_layer_ratio - 0.1),
            max_top_cross_layer_flow=max(0, snapshot.max_cross_layer_flow - 1),
            allowed_ratio_regression=0.0,
            allowed_flow_regression=0,
        ),
        baseline=baseline,
    )
    assert report.ok is False
    codes = {issue.code for issue in report.issues}
    assert "MIN_INTRA_LAYER_RATIO" in codes
    assert "MAX_CROSS_LAYER_RATIO" in codes
    assert "MAX_TOP_CROSS_LAYER_FLOW" in codes
    assert "INTRA_LAYER_RATIO_REGRESSION" in codes
    assert "CROSS_LAYER_RATIO_REGRESSION" in codes
    assert "TOP_CROSS_LAYER_FLOW_REGRESSION" in codes


def _ctx(repo_root: Path) -> AppContext:
    return AppContext(
        settings=Settings(
            repo_root=repo_root,
            project_dir=repo_root,
            manifest_dir=repo_root / "etl-process-manifest",
            sources_registry_paths=(),
        ),
        logger=logging.getLogger("test.layer_metrics"),
        fs=LocalFileSystem(),
        yaml=PyYamlCodec(),
    )


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_check_layer_metrics_service_write_and_check(tmp_path: Path) -> None:
    pkg = tmp_path / "src" / "dpone"
    _write(pkg / "__init__.py", "")
    _write(pkg / "commands" / "__init__.py", "")
    _write(pkg / "commands" / "foo.py", "from dpone.services.bar import run\n")
    _write(pkg / "services" / "__init__.py", "")
    _write(pkg / "services" / "bar.py", "from dpone.ports.fs import FileSystem\nrun = 1\n")
    _write(pkg / "ports" / "__init__.py", "")
    _write(pkg / "ports" / "fs.py", "class FileSystem: ...\n")

    svc = CheckLayerMetricsService(ctx=_ctx(tmp_path))
    baseline_rel = "docs/layer_metrics_baseline.json"

    code, payload = svc.run(
        argparse.Namespace(
            format="json",
            package="src/dpone",
            top=10,
            exclude_layer=["dpone.compat"],
            baseline=baseline_rel,
            no_baseline=False,
            write_baseline=True,
            min_intra_layer_ratio=0.0,
            max_cross_layer_ratio=1.0,
            max_top_cross_layer_flow=None,
            allowed_ratio_regression=0.0,
            allowed_flow_regression=0,
        )
    )
    assert code == 0
    assert isinstance(payload, dict)
    baseline_path = tmp_path / baseline_rel
    assert baseline_path.exists()

    code, payload = svc.run(
        argparse.Namespace(
            format="text",
            package="src/dpone",
            top=10,
            exclude_layer=["dpone.compat"],
            baseline=baseline_rel,
            no_baseline=False,
            write_baseline=False,
            min_intra_layer_ratio=0.0,
            max_cross_layer_ratio=1.0,
            max_top_cross_layer_flow=None,
            allowed_ratio_regression=0.0,
            allowed_flow_regression=0,
        )
    )
    assert code == 0
    assert isinstance(payload, str)
    assert "Status: OK" in payload

    code, payload = svc.run(
        argparse.Namespace(
            format="json",
            package="src/dpone",
            top=10,
            exclude_layer=["dpone.compat"],
            baseline=baseline_rel,
            no_baseline=False,
            write_baseline=False,
            min_intra_layer_ratio=0.9,
            max_cross_layer_ratio=0.1,
            max_top_cross_layer_flow=0,
            allowed_ratio_regression=0.0,
            allowed_flow_regression=0,
        )
    )
    assert code == 2
    assert isinstance(payload, dict)
    assert payload["ok"] is False
    assert payload["issue_count"] >= 1
    json.dumps(payload)
