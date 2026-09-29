# Feature design: validated MSSQL row stream for dbt publish

- Status: APPROVED
- Owner: dpone maintainers
- Issue: dbt publish cannot execute a strict row contract over an opaque BCP file
- Target release: TBD after implementation and certification
- Last verified: 2026-09-29
- Approval: user acting as maintainer, 2026-09-29; explicit approval of this specification

## Executive summary

The dbt publish compiler currently accepts only `source.options.native_transfer.mode: auto`.
For an MSSQL-to-ClickHouse full refresh, that selects an opaque BCP artifact. A
strict dbt column contract correctly fails because the file does not carry
evidence that each row was validated. The MSSQL runtime already supports a
bounded `StreamingRowsArtifact` and the runtime quality layer can validate its
rows. Expose that choice in the dbt publish policy without asserting that an
opaque file was prevalidated. This is a generic authoring capability, not a
customer-specific exception.

Success means a strict-contract model can select an explicit validated row
stream, pass policy compilation, fail closed on bad rows, and publish only a
fully validated candidate. No default changes are proposed.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
| --- | --- | --- | --- |
| dbt author | Publish a contract-bearing MSSQL model | `auto` chooses opaque BCP and runtime rejects the quality claim | One declarative option, compiler check and successful run |
| operator | Distinguish extraction from publication | Worker success can mask a failed outcome gate | Typed failure and zero published rows on validation error |
| platform maintainer | Certify the actual transport | A BCP-named certificate would be misleading for row streaming | Explicit route/evidence policy for row-stream mode |

The author discovers the option in the dbt integration reference, keeps the
existing source/sink connection aliases and strict quality policy, compiles
the profile, then runs the DAG. The operator reads the outcome receipt and
checks target parity. On schema drift or bad values, the run fails without
publishing the candidate; after correcting the source/model, a new governed
run is safe. The production journey uses the same policy through the normal
release and environment-binding process.

## Scope and non-goals

In scope: an explicit MSSQL dbt-publish source mode for validated rows;
compiler/schema acceptance; honest route certification; regression tests,
documentation and evidence. Out of scope: changing the default BCP path,
claiming prevalidation for file artifacts, skipping dbt tests, weakening
ClickHouse publication gates, and changing other connectors.

The existing `mssql_export_mode: streaming` runtime option is the approved
implementation, subject to verification of its memory bound,
batching, row count and cancellation behavior against the dbt publish path.
If it does not satisfy those properties, this spec must be revised rather than
shipping the option prematurely.

## Public contract

- **Manifest/schema:** add an optional, MSSQL-only
  `source.options.mssql_export_mode: streaming` to the current dbt publish
  policy. Omission retains `auto`/BCP behavior. Reject any other value and
  reject the option for non-MSSQL sources. The compiled manifest must carry
  exactly the selected mode; no hidden environment switch.
- **Certification:** a row stream must not claim the
  `native_bcp_to_clickhouse` transport. Use the distinct
  `mssql_validated_row_stream_to_clickhouse` variant and require current
  production-certified evidence for that exact route before a certified
  compile. A missing certificate fails closed. This choice does not itself
  assert that live route evidence already exists.
- **CLI/Python API:** the existing dbt publish check/compile entry points
  accept the new optional field and return the same output shapes and exit
  codes. No new command or public import is proposed.
- **Artifacts/evidence:** keep the existing outcome, quality and publication
  receipts. They must identify the effective source mode and row-validation
  result; no synthetic `contract_prevalidated` flag.
- **Compatibility:** existing policies compile and execute unchanged. A
  rollback removes the new option from authoring and restores the prior
  pinned dpone only when no in-flight run depends on it.

## Detailed algorithm

1. Parse the dbt profile with the closed schema. If the explicit option is
   present, require MSSQL source and the `streaming` literal.
2. Resolve a certified MSSQL-to-sink row-stream route and preserve the
   model's strict column contract, model identity and environment binding.
3. Compile the source mode into the manifest. The existing scheduler and
   attempt identity remain unchanged.
4. Extract MSSQL rows in bounded batches; for every row, apply the existing
   contract validator before the row is accepted for candidate ingestion.
5. Compare the reported inserted rows with a physical count of the completed
   staging table. Measure the source-byte budget over every fetched row,
   including rejected rows, before the
   contract transforms or filters it. Validate row count, required fields,
   types, source/sink schema and the
   existing full-refresh quality gates. Only then may the candidate be
   published through the existing transactional lifecycle.
6. On extraction, conversion, cancellation, timeout or quality failure,
   fail the outcome and leave the previous published generation intact.
   Existing authority reconciliation handles a crash after publication.

```text
profile = validate_closed_policy(input)
mode = profile.source.options.mssql_export_mode or DEFAULT
if mode == streaming:
    require(source.type == mssql)
    require(certified_route(source, sink, strategy, ROW_STREAM))
manifest = compile_with_exact_mode(profile, dbt_contract)
artifact = extract_bounded_rows(manifest)
for batch in artifact:
    validated = validate_every_row(batch, dbt_contract)
    stage(validated)
require(quality_and_replica_gates_pass())
publish_atomically()
emit_outcome_and_evidence()
```

There is no new state machine: planned → extracting → staged → validated →
committed → completed, with existing failed/reconciliation states. Empty input
follows existing full-refresh empty-source policy; missing/NULL/ill-typed
required values fail. Duplicate delivery is governed by existing attempt and
publication identity. A crash before commit leaves the old target; a crash
after commit reconciles rather than dispatching a second publication. Unsupported
capability fails at compile time. No implicit fallback from streaming to BCP is
allowed once the mode is selected.

## Architecture and tradeoffs

| Component | Role |
| --- | --- |
| dbt publish schema/registry | Validate the optional mode and connector constraint |
| dbt profile compiler | Forward exact source mode and resolve truthful certification |
| existing MSSQL strategy | Produce `StreamingRowsArtifact` in bounded batches |
| existing contract/quality layer | Validate rows and gate publication |

Dependency direction remains compiler → policy/capability contracts and runtime
strategy → existing source/sink ports. No new framework or cross-layer
callback is needed. Expected code change is small and must not increase module
size or import graph budgets. No ADR is needed if the approved design only
exposes existing row streaming; a new certification architecture would require
an ADR.

| Alternative | Benefit | Reason rejected |
| --- | --- | --- |
| Set `contract_prevalidated: true` | Minimal syntax | False evidence for opaque BCP; unsafe |
| Drop strict dbt contract | BCP remains fast | Allows bad rows and false acceptance |
| Separate one-off pipeline | Local workaround | Duplicates dbt publish orchestration and UX |
| Explicit validated row stream | Honest per-row evidence | Potentially slower; needs certification and memory/timeout measurement |

## Market comparison

Checked 2026-09-29 against official documentation. Facts below do not imply
equivalent implementation or performance.

| System/version | Relevant observation | Adopt/reject | Source |
| --- | --- | --- | --- |
| dlt 1.30 docs | Iterables are valid pipeline input; schema contracts can enforce column shape and types | Adopt explicit stream plus contract, not silent coercion | [pipeline](https://dlthub.com/docs/general-usage/pipeline), [schema contracts](https://dlthub.com/docs/general-usage/schema-contracts) |
| Microsoft SSIS SQL Server 2025 docs | Data-flow components can fail or redirect rows with conversion/truncation errors | Adopt fail-closed row error semantics; do not silently discard | [error handling](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/error-handling-in-data?view=sql-server-ver17) |
| Airbyte, Fivetran, Informatica, Pentaho, gusty, Cosmos, Beam | N/A for this narrow dbt-publish authoring-schema change; no comparative performance claim | No pattern inferred | — |

Measurable axis: strict-contract publish correctness. Scenario: one invalid
required MSSQL value in a full-refresh stream. Baseline: current auto/BCP
fails before publication because evidence is absent. Target: streaming mode
reports the violating column and publishes **zero** invalid candidate rows;
the old target identity and row count remain unchanged. Procedure: synthetic
contract integration test plus a separately approved live DEV run. Artifacts:
test result, route receipt, outcome and target-generation query. This does not
claim superior throughput.

## Security, tests and rollout

Connections remain aliases; no credentials enter manifests or evidence.
Source permissions stay read-only. Batch size, timeout and concurrency retain
existing limits. Logs must not print row values. Operators can diagnose a
contract failure from typed error and affected column without sensitive data.

| Layer | Required evidence |
| --- | --- |
| Unit | Schema accepts MSSQL streaming and rejects typo/non-MSSQL; compiler preserves exact mode |
| Contract | Existing policies unchanged; strict quality stays enabled; certification describes real transport |
| Integration | Valid rows load; bad type/NULL, empty result, cancellation and duplicate attempt fail/reconcile safely |
| Performance | Bounded-memory and elapsed-time comparison against BCP on synthetic batches |
| Live certification | Explicitly approved environment; source/target parity, target identity and outcome receipt |

Update dbt authoring reference, MSSQL-to-ClickHouse how-to, route-certification
reference, troubleshooting/runbook, generated schema examples and changelog.
Release in order: approved spec → implementation and fresh-context review →
CI and certification → tagged dpone release → synchronized consumer pin → DEV
acceptance → controlled PROD promotion. Rollback trigger: failed contract
evidence, unexpected memory growth, partial publication or wrong route claim.
No consumer run should be retried blindly after a publication-phase failure.

## Agent execution and approval

One integrator owns schema/compiler/shared docs; read-only reviewers inspect
runtime safety, test coverage and UX independently. Parallel writers, if any,
receive disjoint worktrees and explicit path contracts. No production code is
changed by this specification PR.

- [x] User problem and end-to-end journey described.
- [x] Algorithm, failure semantics and public compatibility described.
- [x] Architecture and alternatives described.
- [x] Relevant primary-source market comparison and measurable axis included.
- [x] Test, documentation, rollout and rollback plan included.
- [x] Maintainer confirms the row-stream route/certification decision.
- [x] Maintainer changes status to `APPROVED` before implementation.
