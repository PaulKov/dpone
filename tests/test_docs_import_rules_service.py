from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.services.docs.check_import_rules_service import CheckImportRulesService

ROOT = Path(__file__).resolve().parents[1]


def _ctx() -> AppContext:
    return AppContext(
        settings=Settings.from_env(cwd=ROOT),
        logger=logging.getLogger("test.import_rules"),
        fs=LocalFileSystem(),
        yaml=PyYamlCodec(),
    )


def test_check_import_rules_service_text() -> None:
    svc = CheckImportRulesService(ctx=_ctx())
    code, payload = svc.run(argparse.Namespace(format="text", package="src/dpone"))
    assert code == 0
    assert isinstance(payload, str)
    assert "Import rules OK" in payload


def test_check_import_rules_service_json() -> None:
    svc = CheckImportRulesService(ctx=_ctx())
    code, payload = svc.run(argparse.Namespace(format="json", package="src/dpone"))
    assert code == 0
    assert isinstance(payload, dict)
    assert payload["ok"] is True
    assert payload["violation_count"] == 0
    json.dumps(payload)
