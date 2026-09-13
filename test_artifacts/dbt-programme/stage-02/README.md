# Stage 02: binary types and validated artifacts

Status: **B01/B02 implemented under the coordinator-granted isolated-repair
exception; remaining findings are deferred**. Operational contract copies and
transient logs remain untracked. No new feature specification was self-approved.
Assessed baseline: `46830976b214262c7772800523e832a5a6f6d78f` (0.79.2).
The scoped candidate changes three runtime files, two new tests and two narrow
documentation pages. Public signatures, manifests, wire framing and policies
remain compatible; no new formats or public artifact capabilities are added.
Only synthetic public-project fixtures and local recording connectors are used.
There is no live/database, corporate, credential, composition or DDA evidence.

## Current repairs and baseline limits

B01 normalizes the already-admitted nullable time spelling before using the
existing canonical text encoder. B02 adds an internal concrete-file consumption
boundary to retain ClickHouse dispatch, original source authority, before/after
receipt checks, count agreement and delayed per-attempt validation summaries.
Exact public-boundary tests cover changed evidence/bytes, insertion failure,
source retention, count mismatch and subsequent explicit readmission.

The candidate's expanded focused suite passed **746 tests, no skips**. Final
candidate validation and the still-open combined full gate are recorded in
[validation.md](validation.md). B03–B08 and the legacy narrowing policy below
remain outside this repair.

The initial focused baseline suite passed **715 tests, no failures/errors/skips**,
including the locally installed accelerator. The existing UUID/decimal/numeric
mandatory native-prefix fixes and exact decimal decoding remain intact. Final
checks for this artifact change are recorded separately when run; an unchanged
baseline suite does not prove that the newly reproduced defects are fixed.

The retained producers assert known **unfixed baseline** observations and are
expected to fail when run against the repaired source. Successful baseline execution
means the observations reproduced, including defective behavior. It is not a
correctness PASS or live route certification. Source-native fixtures are
independent synthetic bytes for the admitted dpone profile; they are not captured
BCP exporter/version evidence.

| ID | Reproduced observation | Scope and authority limit |
| --- | --- | --- |
| B01 | `time nullable` loses the seventh fractional digit in Python RowBinary and Python Native; accelerator output matches independent expected bytes. Three distinct sentinel rows include NULL. Python reports three rows and no failure despite unequal bytes. | Implicit-type codec defect. Normal MSSQL catalog metadata supplies explicit scale; its impact is not established. |
| B02 | A receipted raw `mssql-delimited` file stages two exact integer rows through public `ClickHouseSink.stage_payload`; its valid wrapper raises missing `ClickHouseSink.create` before additional rows are inserted. | Real sink staging/dispatch/cleanup code with constructor-injected recording connector; no method replacement or database I/O. |
| B03 | Temporal projection changes native `datetimeoffset(7)` schema to `timestamp`, retaining its file; actual transcoder rejects source-schema mismatch. | Lifecycle-component incompatibility; full database route remains unverified. |
| B04 | Per-column preserve-text, preserve-offset and fixed-timezone policies parse, but native mapping applies global UTC and omits requested companion columns. | Mapping-policy divergence; no final database value claim. |
| B05 | Partitioned wrapper admits an unreceipted child using only `prevalidated=True`; the equivalent single-file hint is correctly rejected. | Exported wrapper authority gap. No current production construction site found; default manifest bypass is not claimed. |
| B06 | Positional rename retains the old receipt/schema and fails `wire_identity_mismatch` before staging. | Safe failure. Immutable source identity and target projection require a contract decision. |
| B07 | `decimal(38)` defaults to Decimal(38,9) and rejects a valid 38-digit integer; explicit `decimal(38,0)` succeeds on every backend. | Exposed shorthand/API mismatch. Catalog extraction canonicalizes `(p,s)`. |
| B08 | Planning mapper calls date/datetime/datetime2 lossless while runtime mapping marks target-range limits. | Explainability/metadata divergence, not proof of corruption. |

### Source trace

All locations refer to the assessed baseline:

- Metadata: `src/dpone/runtime/connectors/mssql.py:297` and
  `mssql_sql.py:54` preserve catalog order, precision, scale and nullability.
- Native source: `runtime/sources/strategies/mssql/mssql_queryout_bcp.py`
  builds a `SourceNativeArtifact`; framing lives at
  `runtime/native_wire_mssql_framing.py:34`.
- DDL and encoder target schema both use the ClickHouse physical type resolver.
  Planning metadata also has a separate mapper under `type_system/source_sink`.
- B01: `src/dpone/runtime/clickhouse_binary_encoding.py:219` recognizes bare
  `time` or `time(...)` without normalizing the nullable suffix.
- B02: `src/dpone/runtime/sinks/clickhouse_payload_ingestion.py:63` recognizes
  concrete file classes; line 107 sends an unrecognized wrapper to materialize.
- B03: `src/dpone/runtime/support/temporal_fidelity.py:144` projects schema,
  while line 157 transforms only row artifacts. Normal payload preparation calls
  this projector at `runtime/etl/payload_lifecycle.py:67`.
- B04: `runtime/support/type_mapping/mssql_clickhouse.py:90` ignores column
  policy during target resolution; line 107 checks only global companion policy.
- B05/B06: `runtime/etl/contract_artifacts.py:334` / line 286 respectively.
- B07: `runtime/native_wire_mssql.py:350` versus
  `runtime/support/type_mapping/mssql_clickhouse.py:133`.
- B08: `type_system/source_sink/mssql_clickhouse.py:132` versus
  `runtime/support/type_mapping/mssql_clickhouse.py:166`.

## Preserved behavior and pending decisions

Single-file receipts bind exact bytes, size, source schema/order, wire format,
contract and exported row count. The wrapper rechecks them before materializing.
Native Nullable default bytes are accompanied by a null bitmap; they are not
business defaults. Business NULL policy and timezone choices remain authored.

Unbounded temporal narrowing is historical, explicitly tested behavior:
`datetime2(7)` at `00:00:01.0000001` becomes 1000 millisecond ticks in an explicit
DateTime64(3) target on every backend. Bounded encoders may reject loss. Changing
that behavior needs a compatibility decision; it is not an isolated regression.

Per-column temporal mapping, native schema projection, partition receipt
authority and source-to-target rename semantics (B03–B06) remain coordinator and
stage 01 contract decisions. See the [repair scope and deferred handoff](handoff-draft.md)
for the granted B01/B02 boundary and remaining approval requirements. Stage 01 and stage 03
reported no conflicting ownership. No compiler, publication, composition or DDA
integration is granted by this artifact.

## Verify the candidate

From the repaired repository root:

```bash
uv sync --locked --all-extras
.venv/bin/python -m pytest \
  tests/test_native_bcp_implicit_time_fidelity.py \
  tests/test_clickhouse_validated_file_contract.py \
  tests/test_streaming_contracts_and_evidence_pack.py -q
```

To replay the historical observations, use a separate checkout at the assessed
baseline, copy the retained producers into the same artifact path, and prepare
that checkout's own locked environment. Do not reset a working checkout or run
these baseline assertions as the repaired candidate's acceptance gate:

```bash
.venv/bin/python test_artifacts/dbt-programme/stage-02/reproduce_binary.py \
  --output-dir /tmp/dpone-stage-02-observations
.venv/bin/python test_artifacts/dbt-programme/stage-02/reproduce_wrappers.py \
  --output-dir /tmp/dpone-stage-02-observations
```

The output directory contains synthetic `.bcp` fixtures and JSON observations.
Retained [binary observations](evidence/binary-observations.json) include exact
expected/actual bytes, backend choice, counts and failure fields.
[Wrapper observations](evidence/wrapper-observations.json) retain the public DI
entrypoint, exact rows, receipt identity and failure/control results.
Replay on a changed source revision may deliberately fail a baseline assertion;
do not rewrite expected results to manufacture success.

### Superseded evidence

The initial exploratory B02 control replaced an ingestion instance's
`insert_file` method. That observation is **INADMISSIBLE for acceptance** and is
not copied into the executable producers. Its claimed raw-control/dispatch
comparison is superseded by the retained public `stage_payload` reproduction
using the real sink and constructor DI. No SDK/module/private method is patched
by either retained producer. Backend selection uses the registry's documented
`module_loader` constructor injection.

## Documentation and completion impact

The assigned fast-path page now explains source receipts, error recovery and the
unresolved partition authority gap. The route page clarifies implicit nullable
time precision. `docs/data-contract-runtime.md:204` still has obsolete boolean
guidance outside this path grant; the coordinator owns that follow-up.
`docs/temporal-fidelity.md:119` promises the per-column rules involved in B04.
The candidate does not imply those deferred contracts are fixed.

Candidate review and integration readiness are recorded separately from the
historical findings. Full combined regression and installed-runtime acceptance
remain coordinator-owned and **UNVERIFIED** until executed on the frozen combined
candidate. Live certification, exporter provenance, real target readback and
database retry/recovery remain **SKIP/UNVERIFIED**. Four read-only roles contributed
the initial audit; an independent fresh-context review must assess the exact
repair commit before integration handoff. No merge or release is authorized.
