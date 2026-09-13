"""Trusted reconstruction must detach worker configuration and require signed inputs."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from dpone.runtime import bootstrap_preflight as module
from dpone.runtime.errors import RuntimeConfigurationError
from tests.test_composition_pack_execution_dispatcher import _ORDINARY_MANIFEST


def test_verified_rebuild_never_reuses_or_mutates_manifest_options(monkeypatch):
    manifest = deepcopy(_ORDINARY_MANIFEST)
    original = deepcopy(manifest)
    seen = []

    def preflight(**kwargs):
        seen.append(kwargs)
        kwargs["load_config"].options["independent_marker"] = True

    monkeypatch.setattr(module, "preflight_runtime_inputs", preflight)
    connections, context = SimpleNamespace(strict=True), object()
    result = module.prepare_verified_transfer_config(manifest, connections=connections, context=context)
    assert manifest == original
    assert result.options["independent_marker"] is True
    assert seen[0]["connections"] is connections and seen[0]["context"] is context
    assert seen[0]["config"] is not manifest


def test_unsigned_bindings_reject_before_configuration_reconstruction():
    with pytest.raises(RuntimeConfigurationError, match="verified_connections"):
        module.prepare_verified_transfer_config({}, connections=SimpleNamespace(strict=False), context=None)
