# Developer CDC poison quarantine and replay

CDC poison quarantine and replay is a runtime safety layer around bounded CDC
apply. It is deliberately split into small contracts so future source -> sink
CDC streams can reuse the same classifier, quarantine writer, replay executor,
and CLI facade without adding route-specific logic to the runtime loop.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.runtime.cdc.poison_models` | Stable poison record and quarantine report contracts. |
| `dpone.runtime.cdc.poison` | `CdcPoisonClassifier` and `FileCdcPoisonQuarantine`. |
| `dpone.runtime.cdc.runtime_orchestrator` | Coordinates read, poison classification, idempotency, sink apply, offset commit, and runtime report writing. |
| `dpone.runtime.cdc.replay_execution` | `CdcReplayExecutionService` and replay report writing. |
| `dpone.runtime.cdc.live_adapters` | `ClickHouseCdcSinkApplier` duplicate event hash filtering for live replay safety. |
| `dpone.ops.cdc.quarantine` | `CdcQuarantineInspectionService` for offline quarantine summaries. |
| `dpone.ops.cdc.replay_execute` | `CdcReplayExecutionOpsService` for local/live sink composition. |
| `dpone.services.ops.command_handlers_cdc` | Thin CLI delegation only. |

## Runtime contracts

`CdcPoisonClassifier` receives a `CdcRuntimeStream` and an iterable of
`CDCChange` objects. It returns clean changes plus normalized poison records.
The classifier owns generic event-quality rules only:

- unique key values must be present;
- operations must be supported by the current runtime apply contract;
- event identities must be unique inside the bounded batch.

`FileCdcPoisonQuarantine` writes `cdc_poison_quarantine.json` and
`cdc_poison_quarantine.md`. It must not apply sink changes or mutate offsets.

`CdcReplayExecutionService` loads replayable quarantine records, reconstructs a
bounded `CDCBatch`, and applies it through an injected `CdcSinkApplier`. Do not mutate CDC offsets from replay execution. The replay report always records
`committed=false`; normal runtime ticks own checkpoint advancement.

`ClickHouseCdcSinkApplier` keeps replay idempotent by checking existing
`dpone_cdc_event_hash` values for the same stream before insert. Duplicate
events are skipped and reported as `duplicate_events_skipped`.

## Policy behavior

`CdcRuntimePolicy.poison_mode` controls runtime behavior:

| Mode | Behavior |
| --- | --- |
| `fail_closed` | Write quarantine evidence, skip sink apply, do not commit offsets, and block the runtime report. |
| `quarantine_and_continue` | Write quarantine evidence, apply only clean changes, and commit the next offset only after durable sink success. |

Keep policy data in `CdcRuntimePolicy` or injected collaborators. Do not put route-specific poison rules in CdcRuntimeOrchestrator.

## Extension rules

For a new CDC source -> sink route:

1. Reuse `CDCChange`, `CDCBatch`, `CdcRuntimeStream`, `CdcSinkApplier`, and
   `CdcOffsetStore`.
2. Add route-specific reader or sink behavior in the adapter package, not the
   orchestrator.
3. Add optional poison policy extensions as small injected collaborators when
   the generic classifier is insufficient.
4. Add local tests for classifier behavior, quarantine report shape, replay
   execution, and CLI delegation.
5. Add a Docker-live gate when a vendor pair can run locally.
6. Update user docs, developer docs, architecture docs, CI docs, source-sink
   docs, and docs-contract tests in the same PR.

## Tests and quality gates

Focused local tests:

```bash
uv run pytest \
  tests/test_cdc_poison_quarantine.py \
  tests/test_clickhouse_cdc_sink_dedupe.py \
  tests/test_cdc_replay_execute.py \
  tests/test_cli_cdc_poison_replay_commands.py \
  tests/test_cdc_poison_quarantine_docs_contract.py -q
```

Full non-live gate:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -m "not integration_live"
uv run dpone docs update-cli-reference --check
uv run dpone docs update-dev-metrics --check
uv run dpone docs check-docs
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
HEAD_SHA="$(git rev-parse HEAD)"; BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"; if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json --base-ref "$BASE_SHA" --head-ref "$HEAD_SHA"
uv run dpone docs check-architecture-fitness
uv run mkdocs build --strict
uv build
```

Docker-live replay safety is opt-in with `DPONE_RUN_INTEGRATION=1` and must stay
outside the default OSS CI path unless the project explicitly changes that
policy.
