# DDA P03: verification CPU research

This researched design helps maintainers and platform engineers evaluate local
verification CPU work without weakening delivery integrity. Start with the
[DDA overview](../delivery-acceleration/index.md); operational prerequisites and
safe recovery remain in the [native transport guide](../mssql-native-transport.md).

- Status: RESEARCHED. Production implementation is not approved.
- Owner: DDA P03; programme integrator: task `01a08a6d-20f9-7900-9eef-ea42ecc0b124`.
- Baseline: released dpone 0.80.0, `6ae541d38ac223327d7edb23510859df91173bda`.
- Checkout: isolated P03 worktree, initially clean; fetched master matched baseline.
- Target release: no release for this research. A future selected, byte-identical
  implementation is patch scope; the integrator chooses its version.
- Last verified: 2026-09-14.

## Executive summary and decision

Repeated encoding and verification may consume CPU, but a repeated pass can be
an independent corruption detector. The current implementation already shares
the initial prepared SQL read and reuses source sizing reservations. Neither is
a new optimization opportunity.

The external experiment changes only the derived business projection from a
dictionary to an ordered tuple. Both canonical encoder invocations, digest
authorities and every independent read remain. Ten balanced measured pairs per
synthetic workload did not establish the proposed 10% CPU saving:

| Synthetic cell, 2,048 rows | Median candidate/baseline CPU | Exploratory 95% interval |
|---|---:|---:|
| Narrow | 1.111 | 0.988–1.175 |
| Wide Unicode and typed edges | 0.985 | 0.969–1.014 |

Decision: **NO-GO / no production change**. These local observations do not justify
adopting the candidate. A hash-bound, completed P01 route profile is also required
before selecting any optimization. At this research decision it was unavailable;
the P01 pilot was still running fidelity/recovery. Research completion does not
wait for future implementation approval or constitute production certification.

## Personas and customer journey

| Persona | Need | Success signal |
|---|---|---|
| Data engineer | Predictable confirmed visibility | Same content and duplicates, with measured cost |
| Operator | Explain CPU and recover safely | Diagnostic timing scope and exact retained receipt |
| Maintainer | Small defensible patch | Byte/error parity and evidence before selection |

1. Discover supported schemas and strategies through the native transport guide.
2. Prepare the existing platform services and injected `native_runtime_factory`;
   a planned manifest alone does not execute native delivery.
3. Configure the existing bounded limits; this proposal adds no settings.
4. Execute research with the external README's offline commands. Live runs go
   exclusively through P01's admitted queue and existing factory.
5. Observe literal-byte tests and non-authoritative microbenchmark results.
   A successful script exit establishes neither route fidelity nor CPU acceptance.
6. Diagnose missing profile scope, noisy measurements or unchanged dominant CPU;
   choose no-change rather than infer a gain from elapsed phase spans.
7. Recover failed delivery using the existing invocation, configuration and
   receipt rules. Never skip a verification stage to obtain a passing run.
8. Operate against the actual workload, resource envelope and SLA once supplied.
9. Upgrade only after the integrator approves, reviews and validates a selected
   implementation. No migration is proposed here.

## Scope and public contract

In scope: ClickHouse to MSSQL bounded native delivery, typed integrity trace,
external deterministic prototypes, proposed acceptance and future task ownership.
Excluded: live execution by P03, SQL/resource/network changes, shared codec edits,
Arrow, SWITCH, layout policies, changed-window authority, dbt and composition.

CLI, Python signatures/imports, manifest/schema, exceptions, defaults, output
formats, journal/receipt fields, artifact versions and checkpoint behavior remain
unchanged. No public benchmark option or bypass-validation flag is introduced.
Canonical report readers and their existing comparison thresholds remain intact.

External `p03-external-research-v1` JSON is explicitly non-authoritative. It uses
UTF-8, preserves all raw pair timings and null-with-reason for absent visibility.
The microbenchmark refuses an existing output directory, writes diagnostics as
they complete, and atomically renames a final result on success. A failed process
may leave partial diagnostics; absence of the final result is not a pass.

## Current execution and integrity authorities

The concrete local composition is `tools/native_delivery_local/session.py`, which
injects the source, importer, preparer, staged load service and `NativeMssqlRuntime`.
The runtime checks target-only recovery before opening a fresh source. Source
metadata and row shape are admitted, temporal values restored, text decoded
strictly, and exact sizing carried in a reservation into bounded IPC frames.
Workers validate values again while writing sealed, fsynced native files.

The ordinary successful path, without retries/recovery, has six SQL typed row
readbacks and eight native row encodings per business row. Count/key/catalog
queries, SQL metadata expressions, file reads and harness fidelity scans are
separate work and are not included in these counts.

| Boundary | Code under `src/dpone/runtime/` | Required authority / encodings |
|---|---|---|
| Source sizing | `sinks/mssql_native_source_values.py`, `mssql_native_sized_frames.py` | Exact reservation; sizing is not value certification |
| Native file creation | `mssql_native_chunks_files.py::encode_native_frame` | File SHA and typed sum; one encoding |
| Raw after BCP | `sinks/mssql_native_import.py::_verify_contents` | Schema/count/content versus file receipt; one encoding |
| Immediate raw inspection | `mssql_native_chunks.py::_import` | Owner/object/plan/content receipt; one encoding |
| Raw before preparation | `sinks/mssql_native_prepare.py::stage` | Contiguous retained receipts; one encoding |
| Initial prepared read | `sinks/mssql_native_prepared_digests.py::digest_prepared_rows` | Business versus raw; full digest retained; two encodings |
| Raw before publication | `sinks/mssql_native_prepare.py::reverify` | Fresh receipts and mutation detection; one encoding |
| Prepared before publication | `sinks/mssql_native_prepared_digests.py::digest_prepared_projection` | Fresh full read versus retained full digest; one encoding |

An ordinary file import additionally performs five complete retained-file SHA
read checks, apart from the write hash and BCP's own reader: scheduler verification,
artifact capture, initial integrity requirement, post-BCP integrity requirement
and consumed-evidence append. They bind file identity/scope as well as content;
their removal is outside this patch design.

The native multiset is exactly:

```text
row_hash = int.from_bytes(SHA256(native_row_bytes), "big")
total = sum(row_hash for every occurrence) modulo 2**256
digest = SHA256(json.dumps(["mssql-native-sha256-sum-v1", count, total]).encode()).hexdigest()
```

Standard JSON separators, count, field order, native prefixes and hash bytes are
part of the existing result. Reordering rows preserves the multiset; duplicates
retain multiplicity. It is probabilistic evidence, not exact equality proof.
Literal byte vectors and exact encoded-row comparisons complement digest parity
in the small research fixtures.

`UNION ALL` preserves duplicates during preparation. SQL canonical business hashes
and lineage IDs have different purposes and cannot replace native wire digests.
Business and full contracts can have different nullable prefixes: slicing full
bytes does not reproduce business bytes. Full verification alone receives the
finite metadata allowance; unbounded metadata placeholders must remain NULL.

## Detailed conditional patch algorithm

This algorithm is sufficiently specified for review, but is not selected or
authorized for production implementation.

1. Keep existing business encoder creation, allowance validation and full encoder
   creation in the same order, before row iteration.
2. Derive ordered business names and expected full-name set exactly as today.
3. For each mapping, validate its complete full column set and NULL-only metadata.
4. Read business values once in schema order into a tuple instead of a dictionary.
   Pass it to the existing encoder sequence branch, which validates arity.
5. Hash business native bytes with the existing modulo arithmetic. Independently
   encode the full mapping and hash it with the full contract.
6. Increment row count. Finish all access/encoding before advancing the iterator;
   never retain a row, a digest cache or references across readbacks.
7. On exhaustion, reject count mismatch and return unchanged `PreparedDigests`.
8. Leave the caller's business comparison, full digest persistence, ownership,
   transaction, mutation checks, errors and later fresh readback untouched.

```text
for row in the one initial prepared iterator:
    require exact full mapping shape
    require NULL-only unbounded metadata
    business_values = tuple(row[name] for name in ordered_business_names)
    accumulate hash(business_encoder.encode_row(business_values))
    accumulate hash(full_encoder.encode_row(row))
    count += 1
require count == expected_rows
return the same versioned business/full digests
```

### State, concurrency and failure semantics

```mermaid
flowchart LR
    A[Acquire and size source] --> B[Encode sealed files]
    B --> C[BCP and raw check]
    C --> D[Immediate raw inspect]
    D --> E[EOF and contiguous receipts]
    E --> F[Raw reverify and prepare]
    F --> G[One read, business and full encodings]
    G --> H[Quality and prepublication raw reverify]
    H --> I[Fresh full prepared read]
    I --> J[Intent, transaction, target receipt]
    J --> K[Durable evidence, checkpoint, cleanup]
```

`stage_complete` requires EOF, contiguous receipts and completion metadata.
Retries use retained immutable bytes, never an arbitrary ClickHouse query offset.
Recovery verifies appropriate raw/prepared authority without reopening the source
after its durable boundary. A confirmed target receipt resolves already-published
recovery; an unknown commit retains artifacts and requires exact receipt probing.
Evidence remains durable before checkpoint advancement. Cleanup is owner-scoped.

No scheduler, connection, lease, callback, writer exclusion, retry/backoff,
cancellation or transaction change is proposed. The helper remains synchronous
and one-shot; it does not retry exceptions or return a partial successful digest.

| Edge | Required behavior |
|---|---|
| Empty stream | Existing count-zero envelope; no fabricated nonempty result |
| NULL / empty / literal NULL | Distinct wire representation where specified |
| Duplicate / reordered input | Preserve multiplicity; order-independent digest |
| Unicode / binary | No normalization, replacement, truncation or implicit padding |
| Decimal / temporal | Preserve precision, seventh digit and offset; reject loss |
| Missing/extra columns, changed schema | Same shape/allowance errors before unsafe use |
| Reused driver mapping | Finish both encodings before next source pull |
| Multiple invalid fields | Preserve validation order and diagnostic class |
| Timeout, crash, partial write | Existing failure/recovery machinery; no new retry |
| Prepared mutation | Later independent raw/full readbacks still reject mismatch |
| Unsupported or nested types | Existing admission rejection; no capability expansion |

## Architecture and alternatives

The only potential production change is in
`src/dpone/runtime/sinks/mssql_native_prepared_digests.py`. It remains a pure runtime
helper; canonical encoders, wire models and allowance stay read-only dependencies.
The existing DI composition roots continue to supply SQL iterators and own state.
There is no new interface, registry, cache, dependency or compatibility shim.
Both narrow scalar rows and wide mixed rows use the same algorithm.

| Alternative | Tradeoff | Decision |
|---|---|---|
| Keep released behavior | No migration risk; current evidence remains applicable | Selected no-change |
| Derived business tuple | Small local allocation reduction hypothesis; tuple generator also costs CPU | Researched; gains not established |
| Cache schema names per encoder | Could reduce repeated sets; touches shared encoder for all callers | Deferred pending relevant profile and ownership |
| Share scalar encoding between business/full | Could avoid conversions; framing and error ordering become harder | Deferred; separate detailed proof needed |
| Remove reads/file checks or cache digests across boundaries | Less work but different mutation authority | Rejected from patch scope |
| Replace native digest with SQL hash, Arrow or backend | Different semantics/dependencies | Separate capability design |

No new ADR is normally required for the local byte-identical tuple change.
Removing/reordering an independent verification boundary, weakening checks or
changing digest authority requires an ADR and separate minor feature approval.
Existing bounded delivery and stage concurrency decisions remain authoritative;
see the [ADR index](../adr-index.md) and [preparation contract](../delivery-acceleration/preparation.md).

No new production module or import edge is proposed. Temporary tuple storage is
bounded by the existing current-row width; authored byte limits remain unchanged.
The single source of module/graph budgets remains
`docs/benchmarks/quality_budgets.yml`; implementation must not grow existing debt.

## Market comparison

Official primary documentation checked 2026-09-14. These are mechanism comparisons,
not vendor performance measurements. Limitations below describe relevance to
this research and do not assert that other products lack integrity features.

| System/version | Capability and observed design | Strength / limitation | Adopt or reject | Source |
|---|---|---|---|---|
| dlt 1.30.0 docs | Separate extract/normalize/load performance; process normalization and threaded loading | Stage-specific tuning; not dpone byte/recovery equivalence | Adopt stage attribution; reject importing parallel defaults or skipping verification | [Performance](https://dlthub.com/docs/reference/performance) |
| SSIS SQL Server v17 docs | BufferSizeTuning events and buffer experiments; avoid paging | Observable row/buffer cost; not native-digest parity | Adopt controlled measurements; reject treating verification columns as unused | [Data flow performance](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/data-flow-performance-features?view=sql-server-ver17) |
| Apache Beam current programming guide | Combine uses associative, commutative partial combining | Bounded accumulators; reduction algebra alone is not mutation authority | Adopt explicit reduction laws; reject changing the versioned digest/verification boundaries | [Combine](https://beam.apache.org/documentation/programming-guide/#combine) |
| Informatica | N/A for this measured local Python projection mechanism | No connector/throughput comparison attempted | No inference about product capability | N/A |
| Airbyte | N/A for this measured local Python projection mechanism | Replication/backend changes excluded | No comparison asserted | N/A |
| Fivetran | N/A for this measured local Python projection mechanism | Managed ELT throughput outside controlled local test | No comparison asserted | N/A |
| Pentaho | N/A for this measured local Python projection mechanism | No Java engine baseline in this experiment | No comparison asserted | N/A |
| gusty | N/A: DAG authoring | Different layer | Excluded | N/A |
| Astronomer Cosmos | N/A: dbt orchestration | Different layer; parallel project owns it | Excluded | N/A |

Inference: preserving local reduction/encoding semantics could reduce overhead
without a new backend, but actual benefit depends on the measured hot path.
The experiment above did not establish that benefit. No superiority claim is made.

## Proposed acceptance and uncertainty

The measurable axis is verification CPU for the identical admitted workload,
data/schema/configuration, hardware, dependency versions and publication policy.
Baseline is the actual current approved commit; candidate is an independently
reviewed implementation commit, not a relabeled result. P01 owns live runs.

Before selection, P01 must provide source/producer hashes, completed fidelity and
recovery evidence, profile attribution, observation overhead and environment
identity. Missing metrics remain null with a reason. Main-thread CPU profiles
exclude importer threads, child encoders, BCP and SQL CPU; default cProfile wall
times and observer `work_seconds` are not verification CPU.

For a selected workload, predeclare at least ten measured pairs after warmups,
balanced AB/BA ordering, identical GC/cache policy and no overlapping jobs. Retain
all pairs, including failures; no post-hoc outlier removal. Compute paired CPU
and confirmed-visibility ratios, their median and a fixed-seed 10,000-resample
paired percentile interval. Report dispersion and systematic uncertainty.

Suggested engineering target: at least 10% lower verification CPU and no greater
than 5% visibility regression. A conservative proposed GO requires the CPU ratio
interval upper bound at most 0.90 and visibility upper bound at most 1.05, alongside
all correctness gates. Wide intervals, missing metrics, mismatched subjects or
failed fidelity prevent performance PASS. Ten pairs are a minimum, not proof of
adequate precision, p95 or an SLA. A predeclared follow-up may use twenty new pairs;
never keep sampling until a desired result appears.

These targets are proposed, not approved promises. They are additional external
analysis and do not replace the existing canonical comparison rules in the
[measurement reference](../delivery-acceleration/observations.md).

## Tests, documentation, rollout and rollback

| Layer | Required evidence | Current scope |
|---|---|---|
| Unit | Literal hex/envelope vectors, Decimal context, temporal edges | External hermetic tests |
| Differential | Identical bytes and digest; two encodings/row; errors, one-shot/reused mapping | External hermetic tests |
| Negative | Same-count duplicate changes, metadata/source mutation, iterator failure | External hermetic tests |
| Integrated | Four raw plus two prepared checks and tamper at each boundary | Existing suites retained; run before any production patch acceptance |
| Recovery | Sealed files, exact receipt identity, unknown commit, evidence-before-checkpoint | Existing contract unchanged; P01/P02 live ownership |
| Live performance | Identical route/env, CPU scope, visibility, all paired raw data | UNVERIFIED at P03 decision |
| Docs | Selector, docs/generated checks, language contract, strict build, rendered CJM | Required for this specification |

Future red tests first establish frozen oracle bytes and both encoder calls;
then assert the business projection avoids a derived mapping while full mapping
validation and errors remain. Add same-count substitution, mixed framing,
multiple-invalid-field error precedence and reused-mapping regressions in
`tests/test_mssql_native_integrity_readbacks.py`. The tuple-shape assertion is the
narrow mechanism test; literal parity and retained-boundary tests are the safety
tests. Do not delete the existing two-encoder/read-count tests.

After a patch, run focused encoder, integrity, import, verification, delivery and
recovery suites, then the change selector and normal Python gates, independent
fresh-context review and accepted P01 paired route evidence. Package/release
checks belong to the integrator's actual release scope; this research publishes
nothing. Live binary source mapping and SQL temporal fidelity are not established
by encoder-only vectors.

The public document change is this explanation/design page. No CLI help, generated
reference, manifest, migration, navigation or common guide change is needed now.
If a patch is selected, the integrator updates changelog/preparation performance
claims only to measured scope. Existing route tutorial, reference and runbook
remain linked; stale broad release wording belongs to P07/integrator.

Rollback triggers include byte/error drift, mutation detection regression, changed
read counts, CPU failure or visibility regression. Revert the implementation commit
through the integrator; preserve durable bytes/receipts and exact invocation/limits.
Do not replay INSERT into populated prepared staging or clean up unknown outcomes.
No format migration, downgrade transform or feature flag is proposed.

## Agent execution and evidence handoff

| Role | Future owned paths | Read-only / forbidden | Dependency |
|---|---|---|---|
| Conditional patch writer | `src/dpone/runtime/sinks/mssql_native_prepared_digests.py`, `tests/test_mssql_native_integrity_readbacks.py` | Canonical codecs, other src/tests/tools/packages, shared files | Selected profile and APPROVED specification |
| P01 | Own campaign evidence and live queue | P03 production files | Resource admission and exact workload |
| Independent reviewer | Read-only final diff/evidence | All writes | Candidate commit and focused checks |
| Integrator | Shared semantic files and final reconciliation | Other agents' active paths | Reviews and scoped release checks |

The external campaign directory `dpone-dda-industrial-plan-20260914/P03/` contains
`README.md`, `request.json`, `recipe.md`, `implementation-contract.yml`, prototype
sources, measurement/test evidence and `completion.json`/`completion.md`. Completion
records source/prototype hashes and statuses; its implementation field means
research artifacts actually created. Production implementation is explicitly false.
External artifacts are local handoff evidence, not links required to build this site.

Next step: inspect the [measurement contract](../delivery-acceleration/observations.md)
and P01's completed hash-bound result; keep no-change unless a relevant bottleneck
and a reviewed alternative justify revisiting this design.

## Approval checklist

- [x] Problem, journey, exact algorithm and alternatives researched.
- [x] Public contracts, retained authority and compatibility explicit.
- [x] Current primary-source mechanisms and scoped measured results recorded.
- [x] Tests, ownership, docs, rollback and dependency contracts specified.
- [ ] P01 evidence supports selecting a candidate.
- [ ] Proposed engineering acceptance has been met on the real route.
- [ ] Maintainer changed status to APPROVED.
