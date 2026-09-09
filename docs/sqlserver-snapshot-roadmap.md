# SQL Server physical design and safe snapshot roadmap

Purpose: help maintainers prioritize independent open-source improvements for
correct SQL Server transport, bounded full-refresh delivery and declarative
physical design. Intended readers are data engineers, platform engineers and
reviewers. This page is a planning overview; none of its proposed options is an
implementation approval.

- Status: RESEARCHED; owner approval is outstanding.
- Owner: maintainers; issue: none; target release: TBD.
- Last verified: 2026-09-09.
- Tested source commit: `6533c27fbf77c78a00b8013bcf72e2293e281e1f`.
- Scope: public repository source, public primary documentation and synthetic
  offline checks only. No live environment or performance evidence was used.
- Production code, runtime configuration, public schemas, pins and workflows are
  unchanged. No commit, issue, PR, merge or release is part of this task.

## Reading path and intended outcome

1. Review the findings and priorities below.
2. Resolve [BCP format correctness](feature-design-bcp-native-alignment-v1.md).
3. Review [resource safety and publication](feature-design-snapshot-resource-safety-v1.md).
4. Review [bounded physical-design admission](feature-design-sqlserver-physical-design-v1.md).
5. Approve a dedicated environment before executing the
   [research and benchmark protocol](sqlserver-snapshot-research.md).

Correctness comes before tuning: prevent binary misalignment and false bounded
execution, establish publication/recovery semantics, preserve actual catalog
facts, then admit a useful dbt columnstore subset. Native SQL Server already has
NONE/ROW/PAGE and a separate CCI selection. The existing dbt restriction is a
safety boundary, not proof that SQL Server or all dpone paths lack compression.

All source references below are repository-relative and refer to the tested
commit. PASS means the named offline observation/check passed, never that a live
route was certified. Confirmed bugs require a violated existing contract;
intentional limitations require new feature approval; hypotheses need further
proof; configuration issues should be resolved without weakening validators.

## Findings and priority

| Finding / classification | Evidence at tested commit | Impact | Priority and scope | Dependency | Acceptance criteria |
|---|---|---|---|---|---|
| BCP native UUID/decimal/numeric prefix — confirmed bug | `src/dpone/runtime/native_wire_mssql.py:153–164`; independent literal fixtures through Python and optimized readers | Wrong values and shifted adjacent fields; live publication incident not claimed | P0: canonical format-specific layout and defensive decoding | Exporter profile and artifact migration | Exact values, sentinels, EOF, stale-layout rejection and all-consumer parity |
| Dropped max_source_bytes — confirmed bug | `src/dpone/services/dbt_publish_model_compiler.py:121`, `:289`; `src/dpone/dag/load_config_builder.py:202–357`; direct synthetic reproduction | Advertised required size guard absent from runtime configuration | P0: reject unenforceable explicit policies, then implement approved metric | Metric/migration decision | Exact limit/profile preserved through all handoffs; breach prevents publication/state advancement |
| Generated strategy vs public schema — confirmed bug | `src/dpone/schema/etl-batch-manifest.schema.json:2426–2431`; strategy sub-schema reproduction | Compiled bounded strategy rejected by public schema; not a proven universal execution barrier | P0: fix coherently with enforcement, never schema-only admission | Same contract PR/integrator | Generated full-refresh manifest validates and reaches enforcing runtime or fails closed |
| Generic ClickHouse full-refresh atomicity — confirmed contract mismatch | `src/dpone/runtime/sinks/clickhouse_sink.py:207–220`; `src/dpone/runtime/sinks/clickhouse_staged_load.py:203`; `docs/load-strategies.md:48`; official RENAME semantics in research register | Multi-rename cannot establish promised atomic replacement; incident outcome UNVERIFIED | P0: engine-aware publication and unknown-commit recovery | Exact object journal/authority | Admitted engine sees old or new result; no blind replay; unsupported topology rejected |
| Lossy compression observation — confirmed representational defect | `src/dpone/runtime/sinks/mssql_physical_introspection.py:59–75`; synthetic ARCHIVE→NONE reproduction | Cannot faithfully inspect external archive or mixed partition layouts; unsafe migration outcome not proven | P1: unknown/unsupported drift must block | Focused reconciliation review | All actual partitions observed or explicitly unsupported; no false matching NONE |
| dbt as_columnstore false, empty indexes/hooks — intentional limitation | `src/dpone/contracts/dbt_sqlserver_graph_policy.py:160–171`, `:287–296` | Smaller dbt producer physical surface | P2: table-only ordinary CCI opt-in | P0 safety, verified macro authority | Exact admitted branch/layout; forbidden hooks and arbitrary macros still rejected |
| Native archive/NCCI/partition authoring absent — intentional limitation | `src/dpone/contracts/mssql_physical_design.py:16`, `:73–198` | Advanced storage cannot be declaratively selected through this contract | P3: independent extensions only | P1 observation, benchmark, new spec | Typed storage/index/partition semantics; no incompatible enum expansion |
| Generic cleanup ownership/crash recovery — unverified hypothesis | `src/dpone/runtime/sinks/clickhouse_operation_tables.py:28`; `src/dpone/runtime/sinks/clickhouse_sql_mixin.py:127` | Random names and named DROP alone do not prove durable ownership | P1: audit then exact inventory/journal tests | Publication authority | No foreign or active object deletion; crash reconciliation and safe cleanup retry |
| Stage retained after unknown commit — intentional safety behavior | `src/dpone/runtime/sinks/clickhouse_staged_load.py:100–108`; `src/dpone/runtime/sinks/strategies/mssql/mssql_staging_consumer.py:141–151` | Recovery requires retained evidence | Preserve in every PR | None | Separate commit-unknown from cleanup-pending; original outcome retained |
| Stage placement — configuration issue, not absent capability | `src/dpone/runtime/sinks/clickhouse_operation_tables.py:22`; `tests/test_clickhouse_managed_artifacts_schema.py:65` | Default stage database may surprise operators | P1/P2 diagnostics and dedicated synthetic test namespaces | Exact inventory | Explicit staging_schema honored; no shared-schema deletion |
| Budget docs incomplete — confirmed documentation gap | `docs/dbt-self-service-reference.md:403`; published policy schema v3 `:417`; no max_source_bytes reference in dbt docs | Users cannot infer metric or recovery | P0 alongside budget fix | Approved metric | First-success plus exceeded/unknown examples, exact units and limitations |
| Incomplete route plan command — confirmed documentation defect | `docs/source-sink/mssql-to-clickhouse.md:179`; command without path exits 2 | Copy/paste journey stops | P2 small independent docs correction | None | Example includes a tested synthetic manifest path |
| Expired physical approval example — configuration/documentation issue | `docs/physical-design.md:166–180` | Copying past expiry correctly blocks execution | P2 docs correction | None | Explain fresh bounded approval; frozen-time tutorial test; expiration guard preserved |
| Generic live route correctness/performance — unverified hypothesis | Existing focused tests and `docs/connector-certification.md:11–48` distinguish support from evidence | Offline tests do not prove real protocol, transaction or resource behavior | P1 certification plan, then P3 benchmarks | Explicitly approved isolated stand | Exact six-dimensional route, commit, versions and environment evidence |

The added BCP finding and its independent evidence are detailed in the
[BCP specification](feature-design-bcp-native-alignment-v1.md). Its correctness
priority precedes compression; no real dataset or alleged offending column is
identified by this research.

## Independent future PR sequence

Each PR needs its own approved contract and fresh review. Parallel analysis is
useful; parallel writers need separate worktrees and disjoint task contracts.
No PR is created by this planning task.

| Order | Proposed PR | Public contract / acceptance | Dependencies and rollback |
|---|---|---|---|
| 1 | BCP native format/decoder regression fix | Canonical format-specific prefix/layout, exact decoded UUID/decimal values and row alignment, defensive lengths and cache invalidation | Independently reviewable correctness fix; reject incompatible cached artifacts instead of reverting to wrong layout |
| 2 | Explicit full-refresh safety fail-closed bridge | No explicit budget can disappear or claim enforcement; old unbudgeted manifests remain NOT_CONFIGURED | Does not wait for tuning; rollback retains rejection of unsupported required controls |
| 3 | Versioned byte metric and end-to-end enforcement | Schema/compile/YAML/pack/runtime/evidence agree; N-1/N/N+1 and no-publish tests | Approve metric, serializer and migration first; shared semantic files owned by integrator |
| 4 | ClickHouse atomic publication and recovery | Admitted engine/topology, identity-bound journal, no double exchange, retained old generation | Can progress independently of metric implementation; requires exact lifecycle primitives |
| 5 | Catalog truth and bounded cleanup gaps | Explicit unsupported observation; exact owned-object cleanup only where audit proves missing protection | Separate introspection and cleanup PRs; reuse existing protections |
| 6 | dbt ordinary CCI table admission | Opt-in canonical mapping to pinned materialization, pre-publish proof, hooks closed | Safety and macro-authority review; no incremental expansion |
| 7 | dbt ROW/PAGE governed bridge | Same canonical semantics, verified intermediate DDL, no uncontrolled hooks | Separate macro/authority design approval |
| 8 | Synthetic benchmark harness and scoped certification | Exact values, recovery and measured resource/throughput/scan evidence | Harness can be developed early; live execution requires approved stand |
| 9 | Advanced physical tuning | Archive, NCCI, constraints, partitions, incremental migrations in separate specs | Only selected evidence-backed scope; no automatic winner/default |

Documentation corrections can run independently, but newly documented behavior
must land with its implementation. Avoid broader connector/runbook refactors;
create narrow metric/physical/recovery pages and link them from current hubs.

## Market comparison

Checked 2026-09-09. This compares relevant mechanisms, not whole products. Version
labels describe reviewed documentation, not a cross-product benchmark. Facts and
proposed adoption are deliberately separate.

| System / version | Relevant observed design and strength | Limitation of comparison | Adopt / reject | Primary source |
|---|---|---|---|---|
| dlt 1.30.0 docs | MSSQL staging-optimized replacement and full-loading docs explain transactional replacement and strategy-specific object/visibility behavior | No evidence here of the proposed aggregate serialized-byte contract or identical dependency preservation | Adopt explicit replace-mode and recovery documentation; reject transferring speed claims to dpone | [MSSQL destination](https://dlthub.com/docs/dlt-ecosystem/destinations/mssql), [full loading](https://dlthub.com/docs/general-usage/full-loading) |
| Microsoft SSIS, SQL Server docs 17.x/16.x | Separate buffer rows/bytes, spill observability, destination batch/commit controls | Buffer/commit size is not total snapshot budget or cross-system atomicity | Adopt distinct resource dimensions and observable tuning; reject conflating these limits | [Data flow performance](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/data-flow-performance-features?view=sql-server-ver17), [OLE DB destination](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/ole-db-destination?view=sql-server-ver16) |
| Apache Beam 2.76.0 current Javadoc | JdbcIO.Write exposes batching, buffering duration, retry settings and write-result ordering | Does not establish this MSSQL binary format or snapshot publication contract | Adopt explicit completion/retry boundaries; reject assuming JDBC batches provide whole-snapshot atomicity | [JdbcIO.Write](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/jdbc/JdbcIO.Write.html) |
| Informatica | N/A: broad managed/integration suite comparison not needed for the bounded binary/DDL contract | No capability-absence claim | Excluded from this scope | N/A |
| Airbyte | N/A: connector platform comparison does not resolve this pinned native format/DDL authority | No capability-absence claim | Excluded from this scope | N/A |
| Fivetran | N/A: managed service operation is not the proposed open-source execution authority | No capability-absence claim | Excluded from this scope | N/A |
| Pentaho | N/A: broader transformation tool comparison adds no necessary mechanism beyond focused bulk/buffer comparators | No capability-absence claim | Excluded from this scope | N/A |
| gusty | N/A: DAG construction, not storage or binary decoding | No capability-absence claim | Orchestration excluded | N/A |
| Astronomer Cosmos | N/A: dbt orchestration, not physical DDL or byte accounting | No capability-absence claim | Compact delivery integration boundary only | N/A |

Microsoft, pinned dbt adapter and ClickHouse facts, dates and edition caveats are
in the [primary source register](sqlserver-snapshot-research.md#primary-source-register-and-design-decisions).
No claim that dpone is faster or universally better is made. The measurable
correctness target is zero silent value changes, zero row-boundary errors and
zero publication/checkpoint advancement after rejected mandatory safety checks.

## Ownership, documentation and approval decisions

One future integrator owns shared public schemas, compatibility configuration,
registries, generated authority baselines, fixtures, changelog and navigation.
Feature writers own only approved narrow modules/tests; every other path is
read-only. Release workflows and unrelated workstreams are forbidden. Compact
multi-project dbt delivery / wire-v2 identity is a dependency and integration-test
boundary, never an implementation scope here.

The narrow specs describe input validation, normalization, authority, identity,
state transitions, retry/resume, concurrency, error handling, evidence and rollout.
They reuse canonical contracts/services and DI; they do not add policy to legacy
facades or introduce a universal physical-design manager.

Maintainer decisions still required:

- byte metric/serializer profile and migration for historical explicit budgets;
- supported server/edition/exporter/ClickHouse engine and topology matrix;
- artifact-format/layout compatibility and fail/re-export policy;
- dbt physical authoring envelope and pinned materialization verification point;
- journal backend, retained-generation horizon and mandatory capacity evidence;
- which advanced storage experiment, if any, merits a separate approved spec.

No approval is inferred from the RESEARCHED status. Stop before implementation.

## Verification record

The companion [verification record](sqlserver-snapshot-verification.md) lists
executed offline commands, observed failures/reproductions, documentation gates
and explicit live SKIP/UNVERIFIED boundaries. Future benchmark paths in the specs
are proposed outputs, not existing certification artifacts.

Review next: [BCP correctness](feature-design-bcp-native-alignment-v1.md), then
[resource/publication safety](feature-design-snapshot-resource-safety-v1.md).
