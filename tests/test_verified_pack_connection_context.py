"""Every verified execution receives the same pinned connection authority."""

from __future__ import annotations

from pathlib import Path

import pytest

from dpone.runtime.init_fetch_contract import InitFetchError, cache_relative_path
from dpone.runtime.verified_pack_launcher import RUNTIME_CONNECTION_CONTEXT_ENV, VerifiedPackLauncher
from tests.test_airflow_runtime_init_fetch_cli import _bundle, _publish_ready_from_init_view


@pytest.mark.parametrize("execution_kind", ["runtime", "pre_hook"])
@pytest.mark.parametrize("scope", [None, "process"])
def test_every_verified_child_receives_its_pinned_connection_context(
    tmp_path: Path, execution_kind: str, scope: str | None
) -> None:
    bundle = _bundle(
        execution_kind=execution_kind,
        execution_scope=scope,
        process_selector="dbo.orders" if scope else None,
        hook_execution="externalized" if scope else None,
        hook_name="pre_hook_refresh_orders" if execution_kind == "pre_hook" else None,
        hook_id="refresh_orders" if execution_kind == "pre_hook" else None,
    )
    _publish_ready_from_init_view(tmp_path, bundle)
    launcher = VerifiedPackLauncher(artifact_root=tmp_path / "artifacts", worktree_root=tmp_path / "worktree")

    command = launcher.prepare(bundle.plan, plan_sha256=bundle.plan_sha256, attestation_required=False)

    context = Path(command.env[RUNTIME_CONNECTION_CONTEXT_ENV])
    assert context.is_absolute()
    for descriptor in (bundle.plan.binding_set, bundle.plan.connection_registry, bundle.plan.credential_runtime):
        relative = cache_relative_path(descriptor.artifact_ref)
        assert context / relative.name == tmp_path / "artifacts" / "payload" / relative
        assert (context / relative.name).read_bytes() == bundle.objects[relative]
    assert command.publish_xcom is (execution_kind == "runtime")
    assert command.exit_code_policy == ("child" if execution_kind == "pre_hook" else "xcom_gate")
    if execution_kind == "pre_hook":
        assert set(command.env) == {RUNTIME_CONNECTION_CONTEXT_ENV}

    # A shared pointer never bypasses verification of the authority's bytes.
    binding = context / "binding-set.json"
    binding.write_bytes(binding.read_bytes() + b" ")
    with pytest.raises(InitFetchError) as error:
        launcher.prepare(bundle.plan, plan_sha256=bundle.plan_sha256, attestation_required=False)
    assert error.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"
