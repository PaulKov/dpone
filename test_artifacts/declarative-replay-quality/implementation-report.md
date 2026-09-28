# Declarative durable replay implementation evidence

Source implementation: `09428775b0a332dcc466dec14fee95603c0a27bb`.
Base: `8b0d38b08c349671cb739966aafa9322826a0c18` (0.85.0).
Scope: approved ADR 0074 and declarative replay specification; no consumer
promotion or release publication. This report is not live route certification.

## Behavior and compatibility

Strict default-off `sink.options.durable_quality_replay` is available in process,
batch, flow and flow-fragment schemas and both factory paths. Invalid values or
routes fail before hydration. Only the validated composition selector is excluded
from semantic identity. Airflow attempt number alone preserves operation identity;
manual CLI recovery requires the same explicit run/DAG/path identity.

Target capture uses an immutable v2 plan, bounded native observation and durable
PREPARED/TARGET_PENDING/COMPLETE/FAILED transitions. Receipt acceptance occurs
under the exact-generation guard and success requires verified release. Timeout,
cancellation, uncertain child lifecycle and authority ACK loss remain blocking.
COMPLETE recovery checks metadata without scanning rows, reading the source or
redispatching publication. V1 source/staged evidence retains its byte semantics.
Other load strategies retain their prior behavior and do not gain this guarantee.

The public constructor adds only optional reader injection. Existing callers keep
their defaults. Active v2 operations require compatible readers; historical proof
cannot be reconstructed from old reports or today's data.

## Checks

| Check | Status | Evidence |
|---|---|---|
| Focused integrated capsule/store/reader/CLI/Airflow | PASS, 151 tests | `consolidation-final.log` |
| Independent review and fixed regressions | PASS | `independent-review.md` |
| Architecture fitness, unchanged thresholds | PASS, 41 tests | `architecture-final.log` |
| Public metadata compatibility | PASS, 72 tests | `compat-signatures.log` |
| Finalization and target completion | PASS, 56 tests | `finalization-final.log` |
| Ruff and formatting | PASS | `ruff.log`, `format.log` |
| Full mypy | PASS, 1237 source files | `mypy.log` |
| Import rules, layer coupling, exact-head module-size ratchet | PASS | `imports.log`, `layers.log`, `module-size.log` |
| Docs links, English contracts, strict MkDocs | PASS | `docs.log`, `docs-language.log`, `mkdocs.log` |
| Generated references, developer metrics and compatibility registry | PASS | `generated.log`, `dev-metrics-check.log`, `compatibility.log` |
| Four package builds and archive metadata | PASS | `build.log`, `build-final-core.log`, `twine.log` |
| Fresh core wheel import/help/version without optional SDKs | PASS | `base-wheel-smoke.log` |
| Frozen-source full suite | PASS, 26485 passed / 577 skipped in 372.03 seconds | `full-suite-frozen.log` |
| Real-row native reader cases | SKIP, 2 cases; no approved environment | `live-skip.log` |
| Complete live durable route/Keeper/real scheduler | UNVERIFIED | No approved live run |

Raw logs are retained in this task's local `test_artifacts/declarative-replay-quality/`
directory. Selected review/report artifacts are committed; no generated PASS
receipt was created for an unobserved environment.

The first broad run produced 26461 passed, 577 skipped and 8 failed while source
integration was still in progress. Four failures were mixed old/new imported
modules; fresh ordered coverage passed 195 tests. The other failures exposed the
additive signature snapshot and architecture budgets. The signature was updated
without weakening compatibility checks, and cohesive consolidation resolved the
architecture failures without changing thresholds. The frozen rerun is authoritative.

## CLI and route audit

R1 scope covers existing `dpone run` selection, explicit identity, strict boolean
admission, exit codes, bounded stderr and single-document JSON stdout, text and
Markdown output, redaction, causal configuration errors and generic bookkeeping
failures. Help describes manual UUID identity. Fresh wheel help/import checks
exercise the absence of optional connector and Airflow SDKs. No new flags, output
file lifecycle or encoding policy were introduced; those contracts retain the
existing broad-suite coverage.

The R4 cell is a bounded single-shard internal replicated ClickHouse full refresh
with immutable plain ReplicatedMergeTree generations and strict authority.
Synthetic capability/identity/metric/retry/fence tests pass. Native reader tests
with actual nullable/duplicate rows and an empty table are explicitly opt-in and
were skipped. Their scope does not establish full publication authority or live
scheduler behavior. External replication, other engines/strategies and unapproved
live environments are outside this extension.

## Documentation, CJM and remaining limits

The configuration tutorial, complete batch/flow examples, reference, architecture,
compatibility and recovery runbook cover setup through stable-identity retry.
Generated CLI help is in sync. The changelog records default-off compatibility and
live certification limits. The module-size producer retired the old finalization
debt entry after reducing it below the warning threshold.

The reader requires POSIX subprocess lifecycle support and reachable advertised
native replica endpoints. The 60-second acceptance budget and at-most-five-second
local shutdown do not bound authority mutation or prove remote SELECT termination.
Unmanaged privileged writes remain outside the immutable-generation contract.

Implementation is independently approved and the frozen-source full suite passed.
Merge readiness still depends on exact-head CI; release/consumer deployment is not
authorized by this report. The initial CI preflight found stale generated developer
metrics. The normal producer refreshed them and its check passed; no threshold or
policy was changed.
