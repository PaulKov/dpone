# Independent architecture remediation reviews

The full effective contract is
[dda-06-architecture-remediation.yml](../planning-amendments/dda-06-architecture-remediation.yml),
with its [approved algorithm](../planning-amendments/dda-06-architecture-remediation.md).
Planning source `27a1252778ee5450be8736f79b1017413fc7aefb` was imported with
provenance as `ff581d57bb291c7f9ba2679ed0cc3668fe524432`. DDA-06 is the sole
writer for the exact transferred paths. Original feature specifications and the
task plan remain frozen and APPROVED; the coordinator confirmed that completion
status/evidence belongs in remediation reports and owned guides instead.

These paragraphs record independent review transcripts. Each approval concerns
one compatible correction, not final architecture, merge or release acceptance.
All reviewers were read-only and used no live services. Historical `final-*`
receipts and the original failed full run remain unchanged.

## Shared exact limits

`review_remediation_limits` (`dpone_architect`) **APPROVED**
`791f3ca581f961c6a58632b517e0a65c217f41b8` against `ff581d5`, with no findings.
Independent differential probes passed for 116 producer and 116 consumer cases,
including exact field/value errors, canonical schemas/model/helper identities,
configuration digest, detached dict/UserDict/MappingProxyType values and
idempotence. The small detached consumer allocation is bounded. No new class,
port, registry, I/O or public migration is needed.

Parent evidence: [expected missing-helper failure](remediation-limits-red.log),
then [142 successful test markers](remediation-limits-green.log). The exit-zero
result is recorded in the parent tool transcript; the quiet log does not itself
contain a final exit/status receipt. Final frozen checks supply exact-source
acceptance evidence separately. Independent probes were in memory and the
review conversation; temporary inputs were removed.

## Independent prepared digest

`review_remediation_digest` (`dpone_test_certifier`) **APPROVED** `60408b4`
against `791f3ca`, with no findings. It independently reran 45 tests, zero skips,
and verified both baseline source hashes and all 28 typed projection bytes,
envelopes and digests against the actual old preparer and current helper.
Success, eager callback failure, mid-iteration failure and encoding failure
preserve SQL, source finalization and exception identity. No retries or extra
encodings are introduced; the prepublication readback remains independent.

Parent JUnit: [36 baseline cases](remediation-digest-baseline-junit.xml),
[73 integration cases](remediation-digest-green-junit.xml), and
[45 final helper cases](remediation-digest-errors-junit.xml), all with zero
failures/errors/skips. The [frozen typed baseline](remediation-digest-goldens.json)
precedes the helper extraction. The review's first interpreter resolution escaped
the virtual environment and lacked pytest; the corrected independent invocation
passed. That harness issue required no repository change.

## Transaction catalog observations

`review_remediation_catalog` (`dpone_architect`) **APPROVED** `618f75a`
against `60408b4`, with no findings. It independently ran all 110 focused cases
and 26 baseline/candidate probes for malformed first/second observations, changed
sessions and query errors. Outcomes and SQL events matched. Focused lint/format,
canonical exports and signatures passed. Catalog construction remains I/O-free;
the executor retains all authority, lock, replan, verification and mutation
decisions. Canonical SQL, caller rollback, receipt-first/unknown-outcome recovery
and public admission remain unchanged.

Parent evidence: [expected missing-method failures](remediation-catalog-red.log)
and [110 passing cases](remediation-catalog-green-junit.xml). These are synthetic
transaction/control-flow proofs, not live SQL locking or rollback certification.

## Parameter annotations and reflection

`review_remediation_annotations` (`dpone_architect`) **APPROVED**
`5d8f77a446c59fb3b10780a9b2b6b2f497759f25` against `618f75a`, with no findings.
Independent AST comparison found no non-import production changes. It executed
all four compatibility tests directly and checked focused lint/format and diff
whitespace. Only the three approved parameter-only names moved. Raw annotations,
canonical-namespace resolution, ordinary runtime dataclass reflection and
canonical/pickle export identity remain intact under existing ADR 0058.

Parent JUnit: [four baseline cases](remediation-annotations-baseline-junit.xml)
and [66 passing cases](remediation-annotations-green-junit.xml), including
observation, metadata/parity and real spawned-worker integration. The reviewer
inspected those suites without claiming to rerun them. No user journey or
migration change is required; the developer guides describe the boundaries.

## Coordinator independent architecture review

The coordinator independently **APPROVED** `1281b83..5d8f77a`, with no findings.
The review checked exact limits, lazy digest supplier validation before I/O,
catalog authority and ordering, and the three annotation-only imports. It found
no dependency laundering or changes to budgets or PostgreSQL ownership.
Test execution in this architecture review was **SKIP**; it does not claim the
other reviewers' test runs as its own.

## Coordinator independent evidence and oracle review

The coordinator's separate evidence reviewer **APPROVED** the bounded
`1281b83..5d8f77a` scope, with independent probes against stable
`2f44960964be27f708873223c98c9dfdac17a9c9` and no P1/P2 findings.
It verified both old-source provenance hashes and all 28 byte/envelope/digest
chains, reproducing the 14 typed cases through the frozen old preparer.
Seven producer-limit cases retained exact errors, digest and detached inputs;
consumer invalid-limit classification stayed intact. Ten negative second
transaction observations retained errors and zero SWITCH mutations. All four
reflection and canonical pickle checks passed. JUnit counts were consistent,
and historical failed receipts/full analysis were unchanged. The reviewer made
no source, environment or artifact edits. This is an oracle and compatibility
approval; full-suite and hosted-CI acceptance remains separate.

The coordinator also independently inspected the actual unchanged-source
`2f44960` preflight receipts: architecture and layer gates **PASS**, with the
preferred clustering target still advisory debt. It confirmed that current
master `d5ad9aa` is an ancestor and dependency configuration, canonical budgets,
layer baseline, workflows and the separately owned PostgreSQL authority module
are unchanged from that base.

## Remaining acceptance work

Actual canonical graph, focused/broad checks, final documentation/metrics,
one frozen-source full non-live run and required hosted CI remain necessary.
The design simulation is not a PASS. Live SQL/BCP fidelity and measured
performance remain SKIP / UNVERIFIED without an approved environment.
