"""One scheduler context and canonical invocation policy for transfer execution.

Verification rebuilds state identity from the sealed manifest and real runtime
environment. It never accepts the executor's process label as its expectation.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from types import SimpleNamespace
from typing import Any

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.mssql_transaction_governance import InvocationIdentity
from dpone.contracts.run_context import RunContext
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.runtime.bootstrap_hydrator import apply_runtime_state_identity
from dpone.runtime.etl.mssql_transaction_identity import invocation_identity


def transfer_scheduler_context(
    *,
    run_id: str,
    workflow_id: str,
    task_id: str,
    map_index: int,
    run_identity: Any | None = None,
    airflow_attempt: Any | None = None,
) -> RunContext:
    """Preserve retry identity and isolate each mapped task partition."""
    task = task_id + (f"[{map_index}]" if map_index >= 0 else "")
    config = {"process": workflow_id, "pipeline_id": workflow_id, "task_id": task}
    context = RunContext(run_id=run_id, config=dict(config))
    if run_identity is not None:
        context.config["airflow_run_identity"] = run_identity.to_dict()
    if airflow_attempt is not None:
        context.config["airflow_attempt"] = airflow_attempt.to_dict()
    return context


def expected_transfer_invocation(
    attempt: Any,
    write: Any,
    *,
    verified_manifest: Mapping[str, Any] | None = None,
    runtime_environment: str | None = None,
) -> InvocationIdentity:
    """Apply the same normalization, state dimensions and identity as the runner.

    The optional form retains the historical five-argument verifier's default
    process expectation. Production supplies both authenticated inputs.
    """
    context = transfer_scheduler_context(
        run_id=attempt.dag_run_id,
        workflow_id=write.workflow_id,
        task_id=attempt.task_id,
        map_index=attempt.map_index,
    )
    config: Any
    if verified_manifest is None:
        config = SimpleNamespace(options={}, target_table=write.relation)
    else:
        if type(runtime_environment) is not str or not runtime_environment:
            raise CompositionAdmissionError("transfer_operation")
        original = deepcopy(dict(verified_manifest))
        config = LoadConfigBuilder().build(original)
        apply_runtime_state_identity(
            config=original,
            load_config=config,
            context=SimpleNamespace(environment=runtime_environment),
        )
    return invocation_identity(context, config, dag_id=write.workflow_id)
