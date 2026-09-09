# Feature design: SQL Server BCP native layout and decoder alignment

Purpose: plan a correctness fix for maintainers and connector authors, using only
public format rules and synthetic bytes. Return to the
[roadmap](sqlserver-snapshot-roadmap.md). The maintainer authorized the first correctness implementation stage.

- Status: APPROVED; owner: maintainers; issue: none; target release: 0.74.35.
- Last verified: 2026-09-09.
- Historical investigation baseline (before 0.74.35): `6533c27fbf77c78a00b8013bcf72e2293e281e1f`.
- At that historical baseline, source and optional accelerator declared 0.74.33 in their `pyproject.toml:7`.
  No matching local tag was present. Equivalence to the published distribution
  remains UNVERIFIED; no release artifact was downloaded or executed.

## Executive summary and evidence classification

The native layout builder assigns zero prefix bytes to nonnullable UUID,
decimal and numeric fields. SQL Server's documented native export rule requires
one. The reader then includes the length byte in the value and leaves the last
payload byte for the next field. Independent synthetic bytes reproduce wrong
values and row alignment in both Python and accelerated consumers.

Priority **P0 correctness**: a transport can yield incorrect values before a
later parsing error. This priority is based on value/alignment evidence, not any
real workload size or alleged offending column. This plan does not claim that
incorrect data was published by a complete live run.

| Classification | Evidence | Established result / limitation |
|---|---|---|
| Confirmed vendor format rule | [Microsoft prefix-length table](https://learn.microsoft.com/en-us/sql/relational-databases/import-export/specify-prefix-length-in-data-files-by-using-bcp-sql-server?view=sql-server-ver16), SQL Server 16.x view, checked 2026-09-09, updated 2026-07-20 | Native uniqueidentifier/decimal/numeric require prefix 1 for both nullable and NOT NULL; character UUID differs |
| Independently reproduced framework defect | `src/dpone/runtime/native_wire_mssql.py:153–164`; literal fixture experiment below | Prefix 0 corrupts exact UUID/decimal/numeric plus adjacent integer; prefix-only in-memory correction restores them |
| Confirmed additional consumer | `packages/dpone-native-accel/src/dpone_native_accel/_mssql_clickhouse_native.py:34–46`, `:154` | Optimized reader trusts the same layout and reproduces the shift |
| Confirmed test gap | `tests/test_native_bcp_decoder.py:145–155`, `:355–360` | Existing decimal/GUID fixture omits prefixes, agreeing with the incorrect layout |
| Unverified incident hypothesis | Negative length can follow misalignment; current readers trust signed lengths | The exact reported negative-length exception was not independently reproduced here; no real dataset inspected |
| Unverified live/exporter matrix | No approved server/exporter stand | Prefix rule is independently documented; full decimal payload and all exporter/version combinations still need captured synthetic exporter fixtures |
| Related evidence-quality gap | `src/dpone/runtime/sources/strategies/mssql/mssql_queryout_bcp.py:337` | bcp_version receives executable path, not observed version; fix provenance separately without persisting local paths |

## Personas and customer journey

A data engineer needs an accurate typed transfer; an operator needs a bounded,
safe failure instead of unexplained allocation/length errors; a maintainer needs
fixtures independent of the code under test. Discovery must show admitted native
formats and exporter versions. Planning validates metadata and format identity.
Execution verifies each field/row boundary, then checks target encoding before
publication. Failures report format/type/ordinal/offset and whether re-export is
required. Recovery rejects incompatible spools; upgrade never silently reuses
an old defective layout. The synthetic first-success tutorial belongs in the
native transport reference, with a link from the MSSQL-to-ClickHouse route.

## Root-cause flow and bounded scope

| Boundary | Source reference at baseline | Responsibility |
|---|---|---|
| Metadata | `src/dpone/runtime/connectors/mssql.py:297–310`; `src/dpone/runtime/connectors/mssql_sql.py:54`, `:75–77` | SQL type/precision/scale and nullable marker |
| Source selection | `src/dpone/runtime/sources/strategies/mssql/mssql_base.py:45`; `mssql_full.py:24–27`; `mssql_incremental.py:47` in the same directory | Selected schema and source query |
| Export mode | `src/dpone/runtime/sources/strategies/mssql/mssql_queryout_artifacts.py:324`; `mssql_queryout_bcp.py:92–96`, `:330–340` | Select typed native queryout and attach original schema |
| Contract | `src/dpone/runtime/sources/strategies/mssql/mssql_bcp_native_artifacts.py:26–37`; `src/dpone/runtime/native_wire_mssql.py:101–172` | Build physical layout and type_layout_hash |
| Python decoding | `src/dpone/runtime/native_wire_mssql.py:175–185`, `:208`, `:228`, `:334–338` | Read prefix/payload and decode typed values |
| Target encoding | `src/dpone/runtime/native_wire_transcoder.py:75`, `:203` | Python decoder into ClickHouse RowBinary or Native encoder |
| Optimized backend | `src/dpone/runtime/native_wire_transcoder.py:194`; `packages/dpone-native-accel/src/dpone_native_accel/_provider.py:98`; `_mssql_clickhouse_native.py:154`, `:241`, `:354` | Bundled backend reads layout and directly encodes target columns |

No other independent native reader was identified in this bounded trace; future
implementation must repeat the consumer search on its own commit. Native,
character (`-c`/`-w`), Unicode native and explicit format descriptors are distinct
contracts. Do not infer a physical prefix solely from fixed-width/nullability.
An explicit descriptor must be validated against its own file-storage type,
format version and exporter profile; it cannot be silently treated as default
native layout. Unsupported formats/types remain blocked.

## Independent offline reproduction

These literal bytes were not produced by the layout builder or the existing
fixture helper. The UUID prefix and value are independently specified; decimal
payload uses the 19-byte precision/scale/sign/magnitude representation understood
by the actual decoders. This establishes the prefix/alignment fault, not a live
proof of every decimal exporter representation.

| Case | Literal bytes before integer sentinel `04030201` | Expected value |
|---|---|---|
| UUID | `10 33221100554477668899aabbccddeeff` | `00112233-4455-6677-8899-aabbccddeeff` |
| decimal(10,2) | `13 0a0201 39300000000000000000000000000000` | `123.45` |
| numeric(10,2) | `13 0a0200 39300000000000000000000000000000` | `-123.45` |

| Case | Current Python value | Current integer sentinel | Bytes left |
|---|---|---:|---:|
| UUID | `11223310-5500-7744-6688-99aabbccddee` | 33752319 | 1 |
| decimal | `-0.0003160321` | 33752064 | 1 |
| numeric | `-0.0003160320` | 33752064 | 1 |

The expected sentinel is 16909060. An in-memory dataclass change to prefix 1,
without editing production code, yields the exact expected values, sentinel and
zero remaining bytes. The accelerated `_read_cell`→`_encode_value` boundary gives
expected corrected target bytes UUID `7766554433221100ffeeddccbbaa9988`, decimal
`3930000000000000`, numeric `c7cfffffffffffff`; the current layout does not.
Both Python ClickHouse encoders accept the wrong first row without raising.
Full iteration can subsequently fail on trailing bytes; no corrupt publication
is claimed.

Minimal runnable reproduction from the repository environment (private decoder
method is used only for isolated in-memory observation):

```python
from dataclasses import replace
from decimal import Decimal
from io import BytesIO

from dpone.runtime.native_wire_mssql import (
    MssqlBcpNativeDecoder,
    build_mssql_bcp_native_contract,
)

cases = (
    ("uniqueidentifier", "10 33221100554477668899aabbccddeeff",
     "00112233-4455-6677-8899-aabbccddeeff"),
    ("decimal(10,2)", "13 0a0201 39300000000000000000000000000000",
     Decimal("123.45")),
    ("numeric(10,2)", "13 0a0200 39300000000000000000000000000000",
     Decimal("-123.45")),
)
for sql_type, golden, expected in cases:
    contract = build_mssql_bcp_native_contract(
        schema=(("value", sql_type), ("sentinel", "int")),
        query="SELECT value, sentinel FROM sample.events",
    )
    wire = bytes.fromhex(golden + " 04030201")
    stream = BytesIO(wire)
    observed = MssqlBcpNativeDecoder(contract)._read_row(stream)
    assert contract.columns[0].prefix_width == 0
    assert observed["value"] != expected
    assert observed["sentinel"] != 16909060
    assert len(stream.read()) == 1
    corrected = replace(contract, columns=(
        replace(contract.columns[0], prefix_width=1), contract.columns[1],
    ))
    stream = BytesIO(wire)
    observed = MssqlBcpNativeDecoder(corrected)._read_row(stream)
    assert observed == {"value": expected, "sentinel": 16909060}
    assert stream.read() == b""
print("PASS: reproduced baseline defect and prefix-only positive controls")
```

The temporary replacement deliberately does not create a production-valid
artifact/hash. It is an experiment to isolate the faulty prefix; production must
recompute and validate complete layout identity.

## Proposed public contract and algorithm

Use one finite canonical format table keyed by exporter profile, file-storage
type, SQL type and nullability. Each entry defines prefix width and signed NULL
sentinel, allowed payload sizes, encoding/endianness and precision/scale checks.
Reuse current native wire models; extract pure format facts only where needed
by both runtime and optional acceleration package. Do not add a call-site UUID
exception in one reader while leaving another divergent table. Optional backend
capability/version negotiation must reject an unsupported layout contract.

1. Validate metadata, admitted source format and exporter/explicit descriptor.
   Reject empty/duplicate column layouts and unknown contract versions before
   streaming. Native model `src/dpone/runtime/native_wire_models.py:12–25`
   currently lacks a declared maximum payload length; add only the necessary
   validated limits through the canonical contract.
2. Build layout from the finite table. Required prefix remains present even for
   NOT NULL UUID/decimal/numeric. Bind exporter identity, layout version and hash
   to spool metadata before decoding.
3. Read the exact prefix, tolerating arbitrary stream chunk boundaries through
   bounded read-exact behavior. Only the admitted -1 sentinel means NULL; reject
   it for NOT NULL. Other negative lengths, invalid fixed lengths and lengths
   beyond the type/field cap fail before read/allocation.
4. Read exact payload; truncated prefix/payload and partial final row are errors.
   EOF is success only between complete rows. Validate decimal header/sign and
   declared precision/scale, UUID byte count/order, and temporal bounds before
   target encoding. Never silently skip, coerce, switch format or return success.
5. Decode or optimized-transcode with equivalent validation. Check exact typed
   values through an independent target decoder in tests; row counts alone are
   insufficient. Produce no successful transcode receipt until full EOF and
   expected row boundaries are established.
6. Pass sealed artifact/layout evidence into the existing staging/publication
   gate. Decoder failure cancels producer/consumer and prevents publication.
   Retain or clean only exact owned artifacts using the snapshot safety policy.

State: `FORMAT_VALIDATED → LAYOUT_BOUND → DECODING → EOF_VERIFIED → ENCODED`;
any malformed input leads to `FAILED_PREPUBLICATION`. Cancellation waits for
worker quiescence. Immutable source artifact may be replayed only with matching
validated layout/producer identity. Concurrent readers are independent; writers
must not mutate the spool during decoding. No new global clients or registry
lookups are introduced; parser/encoder and evidence dependencies remain injected.

Diagnostics include registered error classification, safe SQL type, ordinal,
byte offset, expected/observed length, format/layout/exporter version and recovery
action. Do not include data values, query contents, credentials or local user
paths. CLI/Python retain existing error/JSON/output conventions; no new CLI
command is needed. Layout/evidence version changes need explicit reader support,
not a success-shaped compatibility fallback.

## Regression and golden-fixture matrix

| Dimension | Required cases and acceptance |
|---|---|
| Every admitted SQL type | Enumerate current capability table; NOT NULL, nullable non-null, nullable NULL; exact value and full source consumption; unsupported types remain negative tests |
| UUID | Distinct byte groups, zero UUID, adjacent integer/string sentinels in both orders, multiple rows; mixed-field bytes_le source order and target UUID order independently checked |
| Decimal/numeric | Precision 1/9/10/19/20/28/29/38, scale 0/intermediate/precision, positive/negative/zero, sign/header validation, precision overflow and exact target Decimal width |
| Precision storage boundaries | Do not substitute SQL on-page 5/9/13/17-byte storage assumptions for BCP payload; freeze expected exporter format at every precision boundary |
| Fixed scalar families | Integer minima/maxima, bit values, float special-value policy, money signed-high/unsigned-low representation, datetime ranges |
| Temporal types | date/time/datetime2/datetimeoffset scale boundaries and 100 ns precision; prefix page alone does not prove modern temporal wire layout |
| String/binary | Empty versus NULL, Unicode encoding, zero bytes, declared maximum length and unsupported MAX/legacy types as appropriate |
| Framing | Multi-row mixed fields, sentinel before/after variable field, one-byte-at-a-time reads, prefix/payload split at every boundary, exact EOF and trailing partial row |
| Defensive | Illegal NULL; negative other than sentinel; impossible fixed length; oversized length; malformed decimal header/sign; truncation; empty/duplicate layout; wrong version/hash |
| Format authority | Native versus character versus explicit descriptors; unsupported exporter versions; no fallback by filename or failed decoding |
| All consumers | Python→RowBinary; Python→Native; accelerated→Native; exact target decode and byte/frame boundaries; optional backend absent/unsupported behavior |
| Failure integration | Decode failure after a valid first row must never publish partial stage or advance checkpoint; cancellation and retry preserve exact ownership |

Golden fixtures must be independent of the production layout helper. Store
small literal fixtures with source citations and explicitly labeled hand-built
provenance. Later add synthetic BCP exporter captures with exact server/BCP
version, platform, format descriptor, neutral schema/query and checksum. Never
replace a failing fixture with output generated by the faulty helper. A round
trip using the same encoder/decoder assumptions is supplementary, not an oracle.

## Compatibility, artifact identity and recovery

Current contract schema is `dpone.native_transfer.native_wire.v1`
(`src/dpone/runtime/native_wire_models.py:9`). Prefixes participate in
`type_layout_hash` (`native_wire_mssql.py:118–127`), so corrected layout changes
identity while query/schema identity may stay the same. Readers currently trust
supplied layouts; validation must reject stale zero-prefix layouts even when a
stored hash is internally consistent.

Old artifacts cannot be silently decoded with a newly guessed layout. Default
migration is fail with re-export required, rebuild metadata from the admitted
exporter profile, and produce a fresh spool/receipt. Reusing raw bytes would need
independent format provenance plus a new verified artifact identity; defer that
optimization. Missing provenance means re-export, not assume compatibility.
No persistent decode cache was identified; any cache keyed only by schema/query
must be audited and invalidated if present. Retries/resume require matching
layout, source artifact digest, exporter profile and reader capability. Compact
wire-v2 integration is a test boundary only; do not change its implementation.

Backend metadata at `packages/dpone-native-accel/src/dpone_native_accel/_provider.py:60–74`
advertises certification including these types. Reconcile advertised scope with
the new failing fixtures and current evidence; static certified metadata is not
route/live proof. Correct executable-version provenance separately from framing.

## Alternatives, rollout and required gates

| Alternative | Decision |
|---|---|
| Patch UUID in the Python reader only | Reject: decimal and accelerated consumers remain inconsistent |
| Infer prefix from fixed length and nullability | Reject: contradicts documented native type rule |
| Catch ValueError and skip/fallback | Reject: silent data loss or wrong interpretation |
| Canonical finite format table and shared conformance suite | Recommended narrow correction |
| Expand every BCP type and exporter now | Defer; keep unsupported combinations blocked |

This can be an isolated bug-fix PR under the existing correctness contract, but
this task authorizes planning only. Any new serialized contract/profile must be
approved before implementation. Roll out corrected layout plus stale-artifact
rejection and mandatory binary tests together. Rollback disables affected native
transport or fails closed; it must not restore known incorrect prefixes. Preserve
source rows and regenerate artifacts; never silently reinterpret old evidence.

Minimum proposed mandatory regression gate for changes to this transport:
`tests/test_native_bcp_decoder.py`, `test_native_bcp_decoder_contracts.py`,
`test_native_bcp_decoder_native_format.py`, `test_native_accel_provider.py`,
`test_native_acceleration_contracts.py`, `test_runtime_mssql_clickhouse_native_transfer.py`,
and `test_tools_mssql_clickhouse_bcp_native_type_certification.py` under `tests/`,
plus the new independent golden matrix. The gate must install the optional local
backend when certifying that path. Generic unit/schema/compile-green is not enough.
This proposal does not change CI/release policy in the planning task.

Live smoke, separately approved: export the synthetic matrix with admitted BCP
versions, pass captured native bytes through actual decoders and ClickHouse
encoders, insert into dedicated owned test tables, query exact UUID/Decimal and
sentinels, verify rows/EOF and cleanup, then inject truncation/cancellation before
publication. Manual/optional acceptance not executed is SKIP/UNVERIFIED, never
PASS. No downstream project pins, schedules or acceptance processes are in scope.

Architecture follows canonical contracts, DI and
[quality budgets](benchmarks/quality_budgets.yml). Future integrator owns shared
wire/evidence models and capability metadata. One writer owns layout/reader tests;
a separate worktree writer may own optional backend changes under a disjoint
contract. Public identity, shared fixtures and generated references are
integrator-only. An ADR is needed only if the fix introduces a new format/identity
contract, not merely corrects the documented prefix table.

Documentation: native format/type reference, golden provenance, decoder errors,
re-export migration, route evidence limitations and changelog. Market comparison
is N/A for the vendor byte rule; the [roadmap](sqlserver-snapshot-roadmap.md#market-comparison)
compares relevant surrounding transport/buffering mechanisms. Measurable target:
zero incorrect typed values and zero row-boundary mismatches across the admitted
matrix. No throughput advantage is claimed.

- [x] Vendor rule, independent defect and remaining hypotheses separated.
- [x] All identified Python/optimized consumers traced.
- [x] Golden, defensive, compatibility and live acceptance designed.
- [ ] Exporter profile/version matrix and serialized migration approved.
- [x] Narrow task contract assigned to the sole integrator; independent read-only review completed.
- [x] Maintainer authorizes implementation (scope below).


## Approved correction scope (2026-09-09)

Maintainer instruction to proceed authorizes the first roadmap correctness step.
Implement the existing native format contract: mandatory prefixes, faithful
Python/accelerated values, defensive frame validation, and rejection of stale
layouts before file access. Preserve the wire schema and valid old layouts;
re-export affected artifacts rather than rewriting metadata. No new exporter
formats, live environment, release actions, or dependency changes are authorized.
The standalone accelerator validates its external boundary without importing
core runtime; the core layout producer remains canonical, with conformance tests
across both consumers. Broader exporter provenance and temporal/code-page
certification remain explicitly UNVERIFIED follow-ups.


### Approved systemic type coverage extension

The maintainer additionally requested all-type encoding/decoding and regression
protection. Scope now includes float precision widths, decimal metadata defaults,
exact context-independent decimal/money values, provider numeric target widths,
exact decimal rescaling, source-domain validation, source/target identity, pre-epoch
fractional timestamps, binary FixedString policies, and independent
all-family vectors through each backend. Silent Decimal narrowing becomes an
explicit failure, including the shared binary encoder. No new exporter profile is
inferred: native character encoding and contemporary temporal exporter bytes remain
UNVERIFIED until captured on an approved stand. Offline profile conformance must
not be called complete live type certification. The implementation remains under
validation; approval of live checks or publication is not implied.

## Historical offline verification before Docker findings (2026-09-09)

This record covers the correction above on source baseline
`6533c27fbf77c78a00b8013bcf72e2293e281e1f`, with uncommitted implementation
changes. It does not certify that baseline commit, published packages, a live
route, or the separate snapshot resource/physical-design roadmap items.
The environment uses Python 3.12.11, locked optional test dependencies, and the
local accelerator distribution. No live credentials or environment were used.

The integrator implemented the changes; an independent read-only reviewer
approved after pre-epoch timestamps, FixedString policy parity and source-schema
binding were corrected. Existing acceleration fixtures now supply their actual
nullable source schema rather than contradictory metadata. The standalone
provider still accepts an omitted historical `schema` field, but validates it
when supplied. The wire schema is unchanged; stale incompatible layouts require
re-export as documented in [MSSQL recovery guidance](mssql.md).

| Check | Status | Observed result |
|---|---|---|
| Focused native framing, decoder, provider, encoder, runtime and certification-tool tests | PASS | 665 tests; includes 374 type-matrix cases with temporal scales 0–7 |
| Ruff check and format check | PASS | Repository-wide checks |
| Core mypy | PASS | 1,143 source files |
| Standalone provider mypy | PASS | Three changed provider modules, including read-only structural interfaces |
| Import rules and layer metrics | PASS | No import violations; cross-layer flow remains within baseline allowance |
| Module-size hard limits, advisory scan | PASS | Runtime and standalone provider scanned with canonical tooling; no hard-limit violations |
| Exact-commit module-size governance | UNVERIFIED | Tool rejects dirty inputs differing from checked-out HEAD; must run after integration on the exact commit |
| Documentation links, generated references, language contracts | PASS | 801 pages, 3,162 links, 3/3 generated references, 32 language tests |
| Strict documentation build | PASS | Strict MkDocs build completed |
| Core, accelerator, Airflow pack and provider builds; Twine | PASS | Eight local wheel/sdist artifacts checked; nothing published |
| Installed-wheel smoke | PASS | Separate environment uses built core/accelerator wheels and verifies exact UUID Native bytes with accelerated backend |
| Task contract | PASS | Zero errors or warnings |
| Full non-live suite | PASS | Full `pytest -m "not integration_live" -n auto --dist loadfile` run exited 0 with locked extras and virtual-environment executables on PATH; skipped tests are not certification passes |
| Live exporter/server acceptance | SKIP / UNVERIFIED | No approved environment; native code-page and temporal exporter provenance remain open |

Reproduce the focused gate using the eleven test files named by the task's
validation contract and type-matrix tests, with the local accelerator installed.
The new tests exercise independent source/target bytes, adjacent fields and
multiple rows; they do not derive expected framing from the production layout
builder. Tests additionally reject invalid lengths before payload reads, malformed
values, stale metadata and source/target identity mismatches. Partial decode
failure does not seal successful transcode evidence. Publication/checkpoint and
live cancellation acceptance still require the separately approved route gate.

The first broad attempt exposed unavailable optional dependencies and PATH
requirements, as well as outdated test expectations. Its failure was not counted
as a pass. Final verification uses dependencies from the existing lockfile;
project dependency declarations and lockfile are unchanged.


Completion: ready for integration review. The independent reviewer found no
remaining blocker in the corrected byte semantics. Merge/release readiness still
requires exact-commit governance and any applicable route certification; this
working-tree record is not a release GO. After the final broad run started, only read-only Protocol annotations,
additional temporal-scale test vectors and documentation were updated; the final
665-test focused run includes those vectors and passed. No production behavior
changed after that broad run started.


### Approved local Docker extension

The maintainer explicitly authorized local Docker validation with synthetic data.
The scope includes isolated SQL Server/ClickHouse containers, actual native BCP
exports, all implemented type families and scales, boundary/failure/replay cases,
and corrections discovered by these tests. No external environment or publication
is authorized. Record image/server/exporter identity and clean up owned resources.


## Docker-driven correction scope

Actual exporter bytes supersede the earlier synthetic physical-profile assumptions.
The implementation now records physical temporal scale 7, requires the corrected
provider capability revision 2, rejects ambiguous raw nonnullable char before
materialization/export, and enforces ClickHouse calendar bounds. These are
correctness fixes to the existing route. Logical source schema and serialized wire
schema version remain unchanged; incompatible retained layouts require re-export.
The current procedure and scope are in [local Docker validation](native-bcp-docker-validation.md).
The local matrix completed with **171 PASS, zero skips** against SQL Server
16.0.4265.3, BCP 18.6.0002.1 and ClickHouse 24.8.14.39. It exercised actual
exports and queried target values through all three encoders. Owned Docker
resources were removed successfully. The first receipt is at
`/tmp/dpone-native-docker-final/receipt.json`; its source digest binds the tested
working tree rather than claiming that HEAD contains the uncommitted changes.

A fresh-context review additionally closed a partial-suite false-PASS path in the
reusable runner: inherited pytest filters are cleared, nonpassing/deselected
outcomes are rejected, and cleanup failures fail the receipt. The final producer
rerun completed with **171 PASS in 333.95 seconds**, zero skipped/deselected/
expected-failure outcomes, unchanged source digest and cleanup PASS. Its receipt
is `/tmp/dpone-native-docker-verified/receipt.json`.

| Final working-tree check | Status | Evidence / scope |
|---|---|---|
| Focused codec, compatibility and runner tests | PASS | 714 passed; includes 18 runner rejection/cleanup tests |
| Full non-live suite | PASS | `/tmp/dpone-docker-broad-final.log`; integration-live excluded, skips are not live passes |
| Ruff, formatting, core and optional-provider typing | PASS | 5,685 formatted files; 1,143 core and 8 provider files typed |
| Import rules, layer metrics, advisory module budgets | PASS | `/tmp/dpone-corners-size-core.json`, `/tmp/dpone-corners-size-accel.json` |
| Four distribution builds and Twine | PASS | Local `dist/` artifacts; no publication |
| Isolated built-wheel smoke | PASS | UUID framing, byte order and accelerated Native output in a fresh environment |
| Exact-commit module-size gate | UNVERIFIED | Working-tree changes are not committed; advisory budgets do not replace the gate |
| Full publication/checkpoint/cancellation certification | UNVERIFIED | Outside this isolated codec test scope |

Production changes passed the broad suite. Subsequent changes only hardened the
runner and its focused tests, and updated documentation. Fresh-context review
approved runtime and evidence-producer correctness within this scope. This is
ready for review, not a merge/release GO; final commit-specific checks remain
required.
