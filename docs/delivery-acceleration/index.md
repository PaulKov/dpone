# Bounded native delivery acceleration

Data engineers use this guide to understand preparation work in the opt-in
ClickHouse-to-MSSQL native route. Platform engineers can follow the
[operations guide](operations.md) to diagnose delivery phases and retain
reproducible evidence. Start with the
[native transport prerequisites](../mssql-native-transport.md#prepare-and-configure)
before composing a runtime.

The [approved design](../feature-design-data-delivery-acceleration-v1.md) preserves
the existing manifest, native bytes, journal and recovery formats. Its bounded
scope reduces repeated preparation work: reuse frame sizes, project metadata in
the prepared INSERT, and compute both prepared digests in one iterator. All four
raw verification boundaries and the independent prepared prepublication check
remain required. These structural improvements do not establish a measured speed
increase. Retained evidence for dpone 0.80.0 at commit
`6ae541d38ac223327d7edb23510859df91173bda` establishes scoped local Docker
correctness and controlled recovery: six non-binary profiles, 64 rows per
fidelity cell, full refresh and explicit UTC partition replacement, with
encoding/import policies 2/1 and 1/2. Production workload performance,
independent source DDL and target-writer governance, and hard-failure recovery
remain **UNVERIFIED**. These results do not certify a new deployment.

The maintainer retains the exact-source result in artifact bundle
`dpone-dda-stage02-20260913/live-C-summary.json`, SHA-256
`09e1f0293a433d27346d3f10bc200419e1f943833d40d6e95ee4ba6bddfb26c6`,
with its inventories and original route records. This local evidence is separate
from [source PR 48](https://github.com/PaulKov/dpone/pull/48) and is not included
in the package. Request the retained bundle to audit it; source CI or release
publication alone cannot substitute for the route evidence.

Stage 2 adds opt-in [independent encoding and import limits](concurrency.md).
Legacy settings retain their defaults and durable representation; extended
settings use versioned diagnostic run envelopes. Independent stage limits were released in
0.80.0. Exact-source local correctness and controlled-recovery evidence is
available; production acceptance remains UNVERIFIED.

## First success without database access

From a development checkout with the repository's `uv` environment, inspect the
existing example and exercise the synthetic composition:

```bash
uv run dpone plan examples/native/clickhouse-to-mssql-native.yaml --format md
uv run pytest tests/test_mssql_native_runtime.py tests/test_mssql_native_staged_prepare.py -q
```

The first command exits successfully and reports `status: composition_required`,
`live_preflight: not_run` and `certification_status: unverified`. It renders the
intended route and its requirements without running a delivery. The tests verify
preparation and lifecycle ordering without opening SQL Server or ClickHouse. Passing them establishes only the tested
hermetic behavior. To execute a route, the platform owner must supply the
[Python composition capabilities](../mssql-native-transport.md#compose-the-runtime),
including a `native_runtime_factory`, exclusion scopes, state and evidence
callbacks. A manifest alone cannot supply these capabilities.

## Follow the delivery

```mermaid
flowchart LR
    Source[One acquired source query] --> Frames[Frozen sized frames]
    Frames --> Workers[Bounded encoding and BCP]
    Workers --> Raw[Verified raw staging]
    Raw --> Prepared[Business and metadata INSERT]
    Prepared --> Digests[Business and full digests]
    Digests --> Quality[Quality gates]
    Quality --> Recheck[Independent prepublication checks]
    Recheck --> Commit[Target mutation and receipt commit]
    Commit --> Evidence[Durable evidence]
    Evidence --> State[Fenced checkpoint]
```

Before source EOF, failure requires complete re-extraction. Completed staging can
resume without opening the source. Publication intent requires checking the exact
target receipt first; an unknown commit outcome blocks replay. Diagnostic reports
cannot replace any of those authorities.

## Compatibility and limitations

Existing callers retain their Mapping and tuple APIs, resource defaults, wire
encoding and recovery artifacts. No manifest migration is required. The finalizer
still updates the authoritative target-clock load time inside its transaction.
Direct BCP ingestion retains its existing metadata projection behavior.

The separate SQL Server partition SWITCH component is unregistered. Public native
SWITCH requests remain rejected before I/O. Its activation requires a further
approved contract, aligned-stage ownership/provisioning and real-environment
certification. Hermetic component tests cannot authorize activation.

Use the [operations guide](operations.md) for measurement interpretation, failure
diagnosis and recovery. The [implementation task plan](../data-delivery-acceleration-tasks.md)
describes the immutable scope and ownership of this work.


## Choose the next step

- Follow the [next release stages](next-stages.md) for source sizing reuse and
  separately scoped future acceleration work.
- Tune [encoding and import concurrency](concurrency.md) and inspect its offline plan.
- Read [frame sizing](frames.md) and [preparation integrity](preparation.md) for
  algorithms, bounds and preserved verification boundaries.
- Add [phase observations](observations.md) using the composition instructions in
  the [operations guide](operations.md#connect-optional-observations).
- Generate retained experiment inputs with the
  [certification harness](certification.md#start-without-services), then follow
  the [comparison workflow](operations.md#create-and-compare-retained-reports).
- Review [isolated partition SWITCH](partition-switch.md) and
  [ADR 0062](../adr/0062-isolated-mssql-switch-activation.md) for activation limits.
