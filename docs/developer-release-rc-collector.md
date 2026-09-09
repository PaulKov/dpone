# Developer Release RC collector

`ReleaseRcCollectorService` prepares finalizer inputs from GitHub CLI JSON
exports and evidence references. It is a collection adapter, not a final
release policy engine.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.release_rc_collector` | Orchestrates PR JSON reads, evidence refs, collection blockers, finalizer command rendering, and artifact writes. |
| `dpone.ops.release_rc_collector_models` | Stable collector report, evidence ref, inputs, JSON, and Markdown contracts. |
| `dpone.ops.release_rc_payloads` | Shared merge-train, PR, and check-run payload normalization for both collector and finalizer. |
| `dpone.ops.release_rc_finalizer` | Reads collector-produced `merge_train.json`, validates evidence with policy, and writes the release go/no-go receipt. |
| `dpone.commands.ops_parsers_core` | Argument parsing only. |
| `dpone.services.ops.command_handlers_release` | Thin delegation to catalog services and output formatting. |

Do not call GitHub APIs from the collector service. The service receives paths
to JSON files that were produced by `gh pr view` or another external exporter.
This keeps OSS CI credential-free and lets tests use deterministic fixtures.

Do not add finalizer policy decisions to the collector. The collector may block
malformed collection inputs, such as a missing PR list or a broken branch chain,
but release readiness remains in `ReleaseRcFinalizerPolicy`.

## Extension rules

Add new GitHub payload variants in this order:

1. Extend `dpone.ops.release_rc_payloads`.
2. Add a service test that proves the variant normalizes into the stable
   `ReleaseRcPullRequest` or `ReleaseRcCheckRun` contract.
3. Update [Release RC collector](release-rc-collector.md) when the operator
   export command changes.
4. Keep CLI handlers free of normalization or policy logic.

## Stable contracts

Collector schema: `dpone.release_rc_collect.v1`.

Inputs schema: `dpone.release_rc_inputs.v1`.

Merge train schema: `dpone.release_rc_merge_train.v1`.

The generated `merge_train.json` must remain compatible with
`dpone ops release-rc-finalize`.

## Testing

Focused tests:

```bash
uv run pytest tests/test_release_rc_collector.py tests/test_cli_release_rc_collect_command.py -q
```

Docs contract:

```bash
uv run pytest tests/test_release_rc_collector_docs_contract.py -q
```

Full gates:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -q
uv run dpone docs check-docs
uv run mkdocs build --strict
```
