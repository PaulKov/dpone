# SQL Server graph policy registry

The graph registry resolves an exact graph policy ID and SHA-256 pair before
selection-lock admission and manifest observation. Its only available registration
is the existing `dpone.dbt-sqlserver-selected-graph-policy.v1`. Unknown identities,
mixed ID/hash pairs and managed-policy requests fail closed.

Existing selection-lock and execution-pack schemas, serialized identities, graph
hashes and legacy model rules are unchanged. Existing callers need no migration.
An omitted registration selects the exact legacy policy; it does not select a
newest policy automatically.

## Platform Python usage

```python
from dpone.contracts.dbt_sqlserver_policy_registry import (
    dbt_sqlserver_graph_contract_sha256,
    evaluate_dbt_sqlserver_selected_graph,
    require_graph_registration,
)

registration = require_graph_registration(
    graph_policy_id=selection_lock.graph_policy_id,
    graph_policy_sha256=selection_lock.graph_policy_sha256,
)
report = evaluate_dbt_sqlserver_selected_graph(
    manifest,
    selection_lock.selected_graph_unique_ids,
    expected_logical_target=logical_target,
    registration=registration,
)
if report.passed:
    graph_hash = dbt_sqlserver_graph_contract_sha256(
        manifest,
        selection_lock.selected_graph_unique_ids,
        registration=registration,
    )
```

The existing `observe_dbt_selected_graph` boundary performs this shared selection
for source and runtime consumers. It retains target-mismatch error priority and
checks policy issues before deriving the graph hash. The registry invokes the
existing evaluator and real macro-authority hash implementation; it introduces no
alternative graph rules or generated macro baseline.

`DbtSqlserverGraphRegistration` is an immutable descriptive pair. Constructing it
does not register a policy or authorize execution. Both registry consumers resolve
the whole pair before evaluating a manifest. The record makes no claim about
upstream original authentication, live execution or resource capacity.

## Current boundary

Managed materialization admission is unavailable until its actual source and
complete generated macro authority exist. There is no stub registration or fallback
from `dpone_managed_table` to legacy rules. Publish-policy/authoring/intent root
selection and full package/control/qualification registration remain later work;
the graph registry does not infer them from normalized intent. It does not add a
CLI, launch permission, model enrollment or SQL operation.

If an artifact has an unsupported pair, use its producer with an actually supported
policy; do not edit the artifact hash or substitute the legacy ID to force admission.
