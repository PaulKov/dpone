# Configure durable quality replay

This tutorial is for pipeline authors and Airflow operators recovering a committed
ClickHouse full refresh without rereading its source or publishing it again. It
uses an explicit sink option and retains the configured quality gates. The scope
is one shard with internal replication, an immutable managed generation, and the
existing bounded full-refresh route. Other routes fail closed.

The examples and regression tests use synthetic names and dependencies. Live
certification is **UNVERIFIED**; offline validation does not establish database
capability. The platform owner must first provision the exact authority schema,
attest the shared Keeper service, and admit the replica inventory described in the
[reference](committed-replay-quality-reference.md#authority-preconditions).

## Configure and validate

Start from the complete [batch example](../examples/batch/durable-replay-full-refresh.batch.yaml)
or [flow example](../examples/flow/durable-replay-full-refresh.flow.yaml). Bind
`replay_source` and `replay_target` through the existing runtime connection
providers; follow the [credentials quickstart](getting-started/credentials-quickstart.md). Adapt their synthetic database, table and cluster names to the admitted
route. Keep credentials out of the manifest.

The selector is `sink.options.durable_quality_replay: true`. It is strictly boolean
and defaults to false. Batch defaults are inherited; an explicit process value
wins. The selector is also supported in flow fragments. It does not disable gates,
change their thresholds, or supply missing historical evidence.

Both examples retain a required row-count gate and request source, staged and
target acceptance observations. Acceptance null/distinct counts are observations;
they do not create implicit thresholds. Target observation uses one shared
60-second deadline, including replica metadata and the aggregate scan. The worker
is terminated and reaped with a separate bounded shutdown budget of at most five
seconds when observation expires. This is not a deadline for the whole ETL run.

From a development checkout, validate both complete examples and exercise the
synthetic command and Airflow execution boundaries:

```bash
uv run pytest tests/test_declarative_replay_cli.py tests/test_declarative_replay_airflow.py -q
```

These tests inject runtime dependencies and cannot certify KeeperMap, a live
ClickHouse replica, a Kubernetes executor, or a deployment's subprocess permissions.
The selector and connection references remain ordinary declarative data during
DAG parsing; credentials, clients and readers are composed at task execution.

## Execute and retry the same operation

After platform preparation, execute the batch example with an explicit identity:

```bash
dpone run examples/batch/durable-replay-full-refresh.batch.yaml --selector source.records --dag-id replay_demo --run-id demo_001 --format json
```

For the flow example, use `--selector refresh` and its flow path. Retry by repeating
exactly the same command with the same semantic configuration. A new `--run-id` is
a new operation. Without an explicit or scheduler identity, a manual invocation
receives a fresh UUID; rerunning that command is not recovery.

For Airflow, retain the same DAG run and task identity. A task retry changes the
attempt number, not the operation. A new DAG run creates a new operation. Use the
existing [Airflow pack/provider](airflow-pack-provider.md) execution path; no replay
CLI flag or scheduler-specific quality mode is needed. A quality failure must
fail the task before success-dependent assets or steps can run.

## Read the outcome

A successful replay includes `reconciliation_metrics.quality_replay` in the
processor result (under `result.details` in the normal CLI report). Inspect the
original `replayed_from` IDs, the immutable core digest, quality gates and requested
acceptance observations. Completion is authorized only after durable evidence,
current generation validation, a fresh local receipt, and verified guard release.
An already COMPLETE target capsule is validated without another aggregate scan.

Runtime quality failure exits 1. JSON mode writes one failure document to stdout
and a bounded safe diagnostic to stderr. Existing status, counters and error fields
remain. Optional `result.replay_details` separates `target_commit` (`proven`,
`unknown`, or `not_started`) from `governance` (`blocked` or `complete`) and carries
known original/current IDs and the safe error code. Missing metadata means no
claim was made. Zero counters do not establish commit truth.

For `INCOMPLETE`, a timeout, or a retained reader guard, follow the
[recovery runbook](committed-replay-quality-runbook.md). Preserve the operation
identity and evidence; never disable quality or clear authority to obtain success.

## Upgrade and operate

Quiesce writers before enabling target obligations. Source/staged-only v1 capsules
remain readable without conversion. Historical reports cannot be upgraded into
original proof. Do not downgrade to a binary unable to read an active v2 capsule,
or disable quality while TARGET_PENDING blocks a successor. Finish through a
compatible reader or preserve the fence for reviewed recovery.

Continue with the [reference](committed-replay-quality-reference.md),
[Python integration guide](committed-replay-quality.md), and
[ADR 0074](adr/0074-declarative-replay-target-completion.md) for capability, identity,
state transitions and worker isolation. Enabling this option does not authorize
consumer promotion or release publication.

## Optional live reader verification

`tests/integration/clickhouse_cluster/test_durable_quality_target_reader_live.py`
contains two opt-in real-row reader cases (nullable duplicate rows and an empty
table). Run only in an explicitly approved disposable cluster with
`DPONE_RUN_CLICKHOUSE_TARGET_READER=1`; the test module documents native connection
environment variables. Every advertised replica host and native port must be
reachable from the test process. Host-only Docker port mappings are insufficient.
The fixture creates and drops a unique database. Supply credentials through the
runtime environment, never through a committed manifest or test log.

These cases exercise the production bounded reader but do not establish strict
KeeperMap authority or end-to-end durable publication certification. They are
SKIP in ordinary source checks. Neither live reader behavior nor complete route
certification has been verified for this change.
