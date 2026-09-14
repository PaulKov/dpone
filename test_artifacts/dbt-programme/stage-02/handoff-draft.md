# B01 repair scope and BLOCKED/DRAFT deferred contracts

Status: **B01 implemented; B02 excluded after blocking review; wider contracts DRAFT**.
Baseline: `46830976b214262c7772800523e832a5a6f6d78f`.
This is the concise impact/validation plan for an existing-contract repair,
not an approved new feature specification. The coordinator narrowed the grant
after independent review rejected the B02 candidate.

## B01: implicit nullable time fidelity

An admitted `time nullable` source is decoded at its default scale 7, but Python
target text classification misses the suffix and loses the 100ns remainder.
Explicit-scale forms and accelerated Native preserve it.

Retained implementation scope:

- `src/dpone/runtime/clickhouse_binary_encoding.py`: normalize the admitted
  nullable syntax for time classification, then use the existing time-text
  encoder and authored target/time policies.
- `tests/test_native_bcp_implicit_time_fidelity.py`: twelve public transcoder
  cases with independent expected bytes, distinct row sentinels, NULL, midnight,
  100ns and the final fractional tick. Compare implicit/case/whitespace spelling
  and explicit scale across RowBinary, Python Native and accelerated Native.
- `docs/source-sink/mssql-to-clickhouse.md`: clarify implicit time scale and
  precision without implying a new catalog-derived defect.

The algorithm only removes the nullable suffix before existing time
classification. Framing tables, UUID/decimal prefixes, wire hashes, acceleration,
timezone policy and explicit target narrowing remain unchanged. No public API,
manifest/schema or capability is added. Normal catalog metadata supplies explicit
scale; broader production impact has not been established.

Tests use `build_mssql_bcp_native_contract`, `SourceNativeArtifact` and
`NativeWireTranscoder.to_clickhouse_binary`. Backend selection uses the supported
`NativeAccelerationRegistry(module_loader=...)` constructor seam. Expected frames
do not call production encoders. No methods, modules or SDKs are replaced.

## B02: dispatch repair withdrawn

The baseline public `ClickHouseSink.stage_payload` can stage a raw two-row integer
file but rejects its genuine source-receipted wrapper at missing
`ClickHouseSink.create`. The attempted repair at
`69013d51dbdaff578767cee54ae49a2119cebc40` added an internal file callback with
before/after receipt checks, count agreement and delayed summaries.

Independent review found **P1 silent logical text changes**: the existing loader
does not decode the source `BulkTextCodec`, and its CSV parser removes literal
quotes. Genuine receipt and count agreement still allowed incorrect values and
a successful summary. See [the exact-commit review](rejected-b02-review.md) and
[generated public-DI observation](evidence/rejected-b02-codec-observation.json).
The candidate also failed the module-size ratchet. Numeric-only acceptance and
38/746 passing test selections did not certify logical text fidelity.

The coordinator therefore excluded B02 production behavior. Both runtime files
and the fast-path documentation are restored to the baseline, and the newly
added B02 acceptance module is removed. No marker-only or integer-only admission
exception is introduced. No new test locks in the incidental `AttributeError`:
the existing baseline public-DI producer already records rejection before any
inserted rows. B02 remains **BLOCKED/DRAFT**, not a supported capability.

A future implementation requires a reviewable **APPROVED** contract covering
NULL, empty/text values, delimiters, markers, quotes, binary values, and explicit
Python/client/HTTP transport behavior. Source receipt identity alone does not
establish that the consumer interprets the same logical values. Decoder changes,
new formats/capabilities and admission policy are outside this repair grant.

## Validation and remaining scope

Run focused native/type regressions and the required static, architecture and
documentation checks on the reduced B01 candidate. After commit, run the module
budget against full exact SHAs and obtain follow-up review from the independent
reviewer. [validation.md](validation.md) separates final B01 checks, rejected B02
evidence and baseline observations. The coordinator owns the mandatory combined
non-live suite and clean installed-runtime acceptance; both remain UNVERIFIED
until run on the frozen combined candidate.

B03–B08 remain deferred. Source schema/bytes must stay bound; NULL/timezone
choices remain authored; target names are not source receipt authority; partition
proof cannot come from a boolean. Shared changelog, schemas, factories, dependencies
and release artifacts remain coordinator-owned. No live, corporate, composition,
DDA, merge or publication activity is authorized by this stage.

## Integrator-owned changelog proposal

Preserve default-scale nullable MSSQL time text at 100ns precision across Python
binary encoders. The coordinator owns the shared changelog entry and combined gate.
