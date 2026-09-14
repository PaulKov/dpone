# Feature design: native delivery operational readiness

- Status: RESEARCHED; production implementation is not approved.
- Owner: DDA P07; programme integrator owns shared files and implementation selection.
- Issue: ClickHouse to MSSQL DDA industrial-readiness planning, P07.
- Target release: documentation correction only; future public capabilities require separate approval and release classification.
- Baseline: dpone 0.80.0, `6ae541d38ac223327d7edb23510859df91173bda`.
- Last verified: 2026-09-14; fetched `origin/master` matched this clean checkout at audit start.

This design is for data engineers preparing their first native delivery and
operators deciding whether an environment is ready. The released route has
scoped local live correctness evidence, but a deployment still needs application
services and workload-specific acceptance. The research result is ready for
review; production acceptance remains **UNVERIFIED**.

Start with the [native transport guide](../mssql-native-transport.md), use the
[concurrency reference](../delivery-acceleration/concurrency.md), then follow the
[operations runbook](../delivery-acceleration/operations.md). This specification
records proposed documentation corrections; it does not silently change those
shared pages or introduce a deployment adapter.

## Personas and customer journey

| Persona | Goal | Present obstacle | Success signal |
|---|---|---|---|
| Data engineer | Load a complete table or UTC window | A valid manifest plans successfully but cannot supply runtime services | Plan requirements and an explicitly supplied runtime agree |
| Platform owner | Provide durable execution and admission | Fencing, exclusion, evidence and checkpoint services are application responsibilities | Tested service ownership, permissions, capacity and recovery contract |
| Operator | Diagnose, recover and verify visibility | Receipt, journal and benchmark status are different authorities | One confirmed mutation, durable evidence, then checkpoint completion |
| Deployment approver | Accept a real workload | Synthetic rows and emulated SQL timing cannot establish the business SLA | Representative workload, resource and failure acceptance signed by named owners |

Discovery must distinguish the ordinary character-BCP route from opt-in native
delivery. Preparation covers Python 3.11/3.12, connector extras, native ClickHouse
driver, BCP/ODBC runtime, approved endpoints and a private durable spool. Platform
preflight must prove permissions and capacity; a successful `doctor` is not
source-governance or runtime-composition admission.

Configuration starts with the checked-in
[planning example](../../examples/native/clickhouse-to-mssql-native.yaml):

```bash
dpone plan examples/native/clickhouse-to-mssql-native.yaml --format md
```

A successful offline plan still reports `composition_required`,
`live_preflight: not_run`, and `certification_status: unverified`. These are
correct per-plan statuses and must remain unchanged even when another
environment has passed a live fixture. Supply an explicit UTC execution
`interval.interval_end`; the manifest cannot derive it from staging contents.

Execution requires constructor injection via
`DefaultProcessRunner(native_runtime_factory=...)`. The platform constructs
`NativeMssqlRuntime`, `MSSQLSink` and `compose_native_stage_context` with the
capabilities below. The first-success walkthrough must disclose this manual
integration step; the synthetic local factory is not a production deployment.

Observation starts from the invocation and exact receipt. Confirm target
visibility, then evidence completion and checkpoint state separately. Diagnose
the last durable boundary before retrying. Operate against the acceptance
template, preserve private recovery data, and upgrade only with compatible
bindings and report readers. Detailed rehearsals are in the P07 artifact package.

## Scope and public contract

This phase audits the released behavior, supplies external diagnostic artifacts,
drafts documentation edits, and proposes production acceptance criteria. It
does not change CLI, Python, manifest, state, wire, report, schema or checkpoint
contracts. There is no implementation approval, service activation or release
authority in this document. dbt, composition framework design, platform adapter
implementation, reverse routes, live execution and publisher changes are out of
scope. P01 exclusively runs live workloads; P02 owns failure recipes.

The existing Python capabilities remain:

| Capability | Required responsibility and admission |
|---|---|
| `store`, `target_id` | Durable fenced `WindowStore` and stable physical target identity |
| `bindings` | Restore the existing invocation and frozen identities using target-only services before opening the source |
| `source` | Owned `LoadPayload` context; source DDL exclusion lasts through cleanup |
| `preflight` | Local storage, target allocation/log headroom, permissions and service admission |
| `quality` | Configured checks against prepared staging before publication |
| `evidence` | Durable idempotent persistence bound to the exact commit receipt |
| `advance_state` | Fenced idempotent checkpoint CAS after durable evidence |
| Stage composition | Dedicated importer connectors, same-database independent preparation session, writer scopes, receipt verification and owned cleanup |

The ordinary runner raises `mssql_native.composition_required` without the
runtime factory. A no-op source guard and process-local callback state do not
satisfy production authorities. Import concurrency bounds import/verify tasks,
not total SQL sessions. The target writer-exclusion promise must include every
writer that could mutate business data, not only cooperative dpone workers.

No new public artifact schema is proposed in this phase. P07 JSON is an external,
non-authoritative planning record, never a v1/v2 live report or commit receipt.
It uses `PASS`, `FAIL`, `SKIP`, `N/A`, `UNVERIFIED`, and null with a reason for
missing observations. A `PASS` always identifies its subject and scope.

The offline audit reproduced a diagnostics gap: an authored
`encoding_parallelism: null` fails with exit 1 and
`mssql_native.invalid_limit:encoding_parallelism`, but stderr contains a Python
traceback rather than a concise user-facing remedy. Admission is correct; CLI
diagnostic usability is a separate **FAIL**. A future scoped fix should preserve
rejection and explain omission/fallback or strict integer 1–64. This research
does not change exception handling.

## Capability and evidence matrix

The retained stage-2 `live-C-summary.json` binds clean source
`6ae541d38ac223327d7edb23510859df91173bda`; its SHA-256 is
`09e1f0293a433d27346d3f10bc200419e1f943833d40d6e95ee4ba6bddfb26c6`.
It covers 24 cells: six profiles (`narrow`, `wide`, `unicode`, `decimal`, `null`,
`skewed`) × two strategies × E/I policies 2/1 and 1/2, with **64 rows per cell**.
Checks include exact typed multisets and duplicate multiplicity. The producer
labels metadata parity as a transaction-fixture check; do not relabel every
subcheck as an independent live measurement. Full-refresh outside-window checks
are `N/A`; partition replacement checks outside-window invariance.

| Capability | Implemented/admitted | Retained local evidence | Production status |
|---|---|---|---|
| Plain `MergeTree` in `Atomic`, nonzero UUID | Required; ordinary columns only | Exercised by synthetic factory | UNVERIFIED for intended source and privileges |
| `full_refresh` | Complete target replacement | PASS, 12 exact-C cells | UNVERIFIED at business scale |
| Explicit UTC `partition_replace` | Complete declared `[start,end)` replacement, including empty window | PASS, 12 exact-C cells plus recovery bundles | UNVERIFIED at business scale |
| NULL, duplicate, Unicode and Decimal fixture values | Supported within admitted scalar domains | PASS in named 64-row fixtures | UNVERIFIED for full business type/range distribution |
| Binary encoding | Encoder supports an admitted binary wire contract | Does not establish end-to-end String→varbinary | Authored binary-semantic route unsupported |
| `VARCHAR`/`CHAR` | Importer rejects, even with collation | Rejection is expected, not missing tuning | Unsupported |
| Source DDL exclusion | Mandatory injected guard through source cleanup | Cooperative file lock in exclusively owned synthetic fixture | UNVERIFIED against independent administrator DDL/access changes |
| Target writer exclusion | Required platform guarantee plus native stage/preparation locks | Cooperative fixture exercised | UNVERIFIED for external writers and administrative bypass |
| Completed-stage recovery and receipt-first recovery | Target-only restoration | Four recovery bundles; controlled restart after EOF/lost ACK | Actual SIGKILL, server/network/storage failures and RTO UNVERIFIED |
| Independent E/I limits | 1–64 with legacy fallback | Exact-C fidelity at 2/1 and 1/2 | Throughput and capacity acceptance UNVERIFIED |
| Native SWITCH | Isolated component; public activation rejected | Component scope only | Unsupported public mode |
| Target-MAX cursor, CDC, arbitrary query, views, other engines | Rejected or outside native contract | Not certified by the supported fixture | Unsupported |

Earlier H tuning at `36e90087bff89631db65a3a47702112803261c3a`
is a distinct source-component experiment, not released-C performance evidence.
SQL Server ran amd64 under ARM64 emulation. Local evidence cannot establish
production x86 performance, installed-release parity, governance or a visibility
SLA. P01/P02 completion manifests are required for later campaign conclusions;
their absence does not prevent completing this audit.

## Detailed operational algorithm

1. Bind the exact source, producer, dependency, schema, strategy, interval,
   configuration and environment identities. Reject mismatches; never repair a
   retained receipt by editing its checksum.
2. Resolve native capabilities and integer limits before row I/O. Admit only
   plain MergeTree/Atomic, the supported scalar mapping, complete refresh or an
   explicit UTC window, and the required application services.
3. Acquire the fenced owner. Restore invocation, physical target, query, source
   UUID, schema and wire identities through target-only bindings. Query ID is
   provenance for one query, not a reusable snapshot token.
4. Reconcile persisted publication state before source extraction. A publishing
   record requires the exact target commit receipt; missing/unreachable authority
   means unknown outcome, resource retention and no replay.
5. For a new extract, hold source schema exclusion, preflight local/SQL capacity,
   read the complete bounded query, encode immutable chunks and import through
   separately owned connectors. Retained-work capacity is
   `max(E, I) + max_pending`; logical payload bound is `(C + 1) * max_bytes`
   plus overhead. Neither is an RSS guarantee or exclusive SQL reservation.
6. Persist `stage_complete` only after EOF, contiguous verified receipts and
   completion metadata. Individual verified chunks do not authorize publication.
7. Under the independent same-database preparation scope, reverify raw content,
   construct and verify the prepared table, apply quality and recheck before
   finalization. Preserve all four raw verification boundaries and independent
   prepared prepublication verification.
8. Publish business mutation and target receipt in the existing transaction.
   Confirm a lost ACK by the exact receipt while the preparation lock remains.
   Unknown outcome retains resources. Known rollback is a terminal fixture
   outcome; do not promise general automatic rollback reconciliation. The
   conservative unknown-outcome behavior is intentional, not a confirmed defect:
   an exception or missing receipt alone cannot establish rollback.
9. After confirmed publication, persist idempotent evidence, CAS
   `evidence-complete`, advance the fenced checkpoint, CAS `succeeded`, then
   perform owned cleanup. A failed evidence/state callback resumes that boundary
   without reopening ClickHouse or publishing twice.
10. Inspect target visibility and full pipeline completion as separate durations.
    Evaluate only an acceptance profile with complete owner inputs and equivalent
    evidence; missing fields yield UNVERIFIED, never an implied production GO.

```text
admit -> fenced restore -> reconcile target outcome
  partial extraction -> settle writers -> reextract_required -> new complete query
  stage_complete -> reverify -> prepare -> quality -> publish transaction + receipt
  publishing -> exact receipt found -> published
  publishing -> outcome unknown -> retain; stop replay and destructive cleanup
  published -> durable evidence -> evidence-complete -> checkpoint -> succeeded
  succeeded -> retry owned cleanup
```

Cancellation, timeout, lease loss and unclassified vendor errors cannot fabricate
EOF. Up to two import retries reuse immutable bytes only for classified
`WindowTransientError` after old-writer settlement. Before EOF use complete
re-extraction; after EOF restore source-free. Empty windows still replace the
declared interval; outside rows and NULL window values remain unchanged. Schema
drift, ownership change or digest mismatch requires diagnosis, not dropping a
table or rewriting state. Unsupported nested data, non-finite floats, overflow
and lossy truncation fail admission/conversion rather than silently coercing.

## Architecture, alternatives and ADR

Reuse `NativeMssqlRuntime`, `NativeRuntimeBindings`, `WindowStore`, native chunk
and publication journals, the staged-load service, transaction finalizer,
existing observer and report readers. Dependencies stay injected at the
application composition root. Production never imports P07 diagnostics.

The research adds no production component, registry, module, import edge or
compatibility facade. Quality budgets remain those in
`docs/benchmarks/quality_budgets.yml`. No ADR is required for accurate documentation
or this external template. A new service capability, automatic rollback
resolution, public readiness command or diagnostic schema requires a separate
approved design; an authority/ordering change also requires an ADR. Naming an
adapter here does not implement it or claim its contract has been approved.

| Alternative | Benefit | Cost or risk | Decision |
|---|---|---|---|
| Keep blanket live-UNVERIFIED text | Conservative | Hides valid local evidence and conflicts with released state | Replace with scoped factual wording |
| Call 0.80.0 production-ready from local PASS | Simple marketing claim | No actual workload, SLA or governance proof | Reject |
| Build a second P07 platform adapter | Immediate demo | Duplicates parallel ownership and introduces unapproved authority | Reject; integration dependency only |
| External acceptance template and rehearsals | Reviewable without new runtime contracts | Deployment owners still supply inputs and execution | Adopt in research phase |

## Market comparison and measurable outcome

Current official sources were checked on 2026-09-14. These comparisons address
operator evidence and diagnosis, not vendor throughput.

| System/version | Observed design and useful pattern | Limitation for this task; decision |
|---|---|---|
| dlt 1.30.0 documentation | Load info records package/job outcomes; trace distinguishes extract, normalize and load | Different authority model; adopt clear phase/status explanation, not automatic equivalence to dpone receipt/checkpoint guarantees. [Official production guide](https://dlthub.com/docs/running-in-production/running) |
| SSIS, SQL Server v17 documentation | SSISDB exposes executions and operation messages with identities/status/timestamps | Different deployment/catalog model; adopt correlation of an operation to actionable diagnostics, not an SSISDB dependency. [Executions](https://learn.microsoft.com/en-us/sql/integration-services/system-views/catalog-executions-ssisdb-database?view=sql-server-ver17), [operation messages](https://learn.microsoft.com/en-us/sql/integration-services/system-views/catalog-operation-messages-ssisdb-database?view=sql-server-ver17) |
| Informatica, Pentaho | N/A for this bounded audit | No designer/runtime replacement is being proposed; no competitive claim |
| Airbyte, Fivetran | N/A for this bounded audit | Connector provisioning/managed sync design is outside this task |
| gusty, Astronomer Cosmos | N/A | DAG/dbt authoring is excluded |
| Apache Beam | N/A | Distributed execution semantics are not being redesigned |

The proposed measurable outcome is that a first-time operator correctly labels
plan-only, local-live, unsupported and production-unverified results and selects
the safe recovery boundary in every supplied scenario. Procedure: an independent
operator follows the corrected pages without source-code assistance; record
manual interventions, wrong success classifications and time to locate the
runbook. Target: zero false success or destructive recovery choices. This is a
future usability acceptance target, not a measured superiority claim; no human
usability study ran in P07.

## Production acceptance, security and operations

The external template requires workload/profile/type ranges and strategy,
row/encoded-byte distributions, arrival and late-data pattern, exact SQL/CH/BCP/
Python versions, hardware and emulation, network topology, target layout and
recovery model, concurrent readers/writers and admitted permissions. Owners must
supply visibility SLA and percentile/sample procedure, allowed blocking/log/RSS/
disk limits, RTO/RPO, failure envelope, alert destination/owner, runbook, recovery
authority, retention and upgrade/rollback criteria. Unfilled values are null
with `UNVERIFIED` and an owner-input reason, never invented service objectives.

Correctness requires exact typed content and duplicate multiplicity on admitted
fixtures, window invariance, one confirmed publication and checkpoint after
evidence. Performance requires representative scale/endurance and measured
resource maxima under the intended topology. Three diagnostic trials do not
prove p95 or an SLA. Alerts distinguish missing evidence after commit, checkpoint
lag, lease loss, capacity rejection, unknown outcome, digest/owner mismatch and
cleanup residue; thresholds and recipients remain deployment-owned.

Logs must exclude source values, SQL, credentials and connection strings.
Credential values never enter manifests or artifacts. Recovery spools/journals
contain sensitive data and need owner-controlled access, durable storage,
retention and recovery tests. Unknown outcome blocks destructive cleanup.

## Validation, documentation and rollout

| Layer | Required proof | P07 disposition |
|---|---|---|
| Offline UX | Help/version, native planning, invalid limits, harness SKIP and overwrite behavior | Captured external CLI audit; no live factory |
| Source/evidence audit | Exact source and retained hashes, implemented versus supported semantics | Read-only role review plus evidence inventory |
| Documentation | Change-aware selection, docs/reference checks, language contracts, strict rendered build | Required for this single-document diff |
| Live route/recovery | Exact-subject fixtures, receipt ordering, actual fault classes | Historical scoped evidence only; new work belongs to P01/P02 |
| Performance/security/deployment | Representative workload, exclusion, hardware, SLA and service ownership | UNVERIFIED pending owner inputs and approved execution |
| Compatibility | Legacy eight-field policy, v1/v2 readers and persisted invocation identity | Existing contract retained; no format migration in P07 |

Documentation correction drafts target the native transport overview, route
guide, DDA index, certification, local Docker and frame pages. Conditional
statements about an unexecuted environment, binary mapping, performance and
governance remain valid. Offline planner statuses must not be rewritten to PASS.
The integrator owns shared edits, navigation and changelog. A future guide should
link discovery → prerequisites → Python integration → first success → receipt
inspection → failure recovery → acceptance → upgrade without duplicating the
parallel composition documentation.

For upgrade, preserve old invocation/configuration/journal identities. Legacy
limits retain defaults; do not inject new policy into an in-flight invocation.
Use readers supporting extended v2 policies before authoring them. An old reader
rejects an unknown format; do not down-convert by dropping fields. Drain or
reconcile in-flight work with compatible services before rollback. The absence
of a migration is not evidence for arbitrary mixed-version recovery.

This research can be merged as documentation after review. Subsequent corrections
are a docs-only patch; platform service work, new public operational APIs and
authority changes require independent specifications. P07 neither chooses a new
version nor merges, tags or publishes. Revert a bad documentation correction;
never alter original evidence to match prose.

## Agent execution and approval

P07 owns only this specification and the external directory
`/Users/paulkov007/.codex/artifacts/dpone-dda-industrial-plan-20260914/P07/`.
Source, packages, existing tools/tests, shared docs, navigation, dependencies and
release files are read-only. The programme integrator is task
`01a08a6d-20f9-7900-9eef-ea42ecc0b124`. Four read-only roles independently examined
execution, architecture, evidence and UX; one P07 writer reconciles findings.
Future exact path proposals and P01/P02 dependency contracts are recorded in the
artifact package, not delegated as production authorization.

- [x] Problem, personas, journey, boundaries and compatibility are explicit.
- [x] Existing algorithm, recovery authority and alternatives are traced.
- [x] Evidence scope, market inputs and measurable proposed outcome are stated.
- [x] Test, documentation, acceptance, rollout and ownership plans are recorded.
- [ ] Maintainer approval for any future public implementation; intentionally pending.

The local artifact package contains `completion.md`, `completion.json`, the
capability matrix, documentation drafts, production acceptance template, operator
walkthrough, P01 request, dependency/path proposals and SHA-256 manifest. These
are research deliverables; production acceptance remains a separate decision.
