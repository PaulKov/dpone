# Independent review: rejected B01/B02 candidate

Reviewed commit: `69013d51dbdaff578767cee54ae49a2119cebc40`.
Baseline: `46830976b214262c7772800523e832a5a6f6d78f`.
Reviewer: fresh-context `dpone_architect`, `/root/final_fidelity_review`, not an implementer.
Verdict: **CHANGES REQUESTED**. Not ready for coordinator integration.

## P1: newly admitted files silently change logical text

`src/dpone/runtime/sinks/clickhouse_payload_ingestion.py:54` forwards the validated
wrapper's concrete file to a loader that does not decode `BulkTextCodec`.
The genuine source receipt proves decoded logical values, while the driver sees
encoded empty-string, TAB and LF markers. The public staging call reports four
accepted rows; no decoded staging configuration or decoding SQL exists.
The raw-loader gap predates the candidate, but this branch newly admits it through
the contract wrapper. Receipt, byte and count agreement do not prove value fidelity.

The reviewer independently used `ClickHouseSink.stage_payload`, a real receipt
producer and constructor DI. No patched methods or private entrypoints were used.
The reviewer also ran the focused native/wrapper/streaming selection: 38 passed.
B01 appears sound; B02 must preserve all logical codec semantics or reject an
unsupported wire/consumer combination before insertion. An integer-only exception
is inappropriate. Coverage must include empty strings, NULL, delimiter/marker
characters and quotes, with explicit Python/client/HTTP outcomes.

The remaining callback design, source ownership, count/receipt checks and cleanup
were found coherent. Full combined and installed-runtime checks remain UNVERIFIED;
live certification is SKIP and publication N/A.

## Author's separate follow-up evidence

`reproduce_rejected_b02_codec.py` and `evidence/rejected-b02-codec-observation.json`
reproduce eight rows on the same commit through public DI. Empty/TAB/LF/CR/marker
values remain encoded; a literal quoted string loses its quotes. NULL and Unicode
controls survive. Eight accepted rows and no decoded staging configuration are
reported. This evidence extends, but does not replace, the independent finding.

The exact-SHA module-size check also FAILS: `contract_artifacts.py` introduces
unbaselined warning debt at 367 SLOC against warn_sloc=350. The first invocation
using symbolic HEAD was a configuration error; this actual failure used both
full commit SHAs. No baseline adjustment is authorized.

## Disposition

The coordinator excluded B02 from integration. Both B02 production files and the
fast-path documentation were restored to the assessed baseline. The B02 numeric
acceptance tests were removed; their historical PASS counts do not establish
logical codec support. The final candidate retains only B01 production behavior.

Replay the retained codec producer only against the rejected `69013d5` source in
a separate checkout with its own locked environment. It intentionally observes
corrupt acceptance and is not a current-candidate acceptance test. The restored
wrapper fails before inserting data; do not re-enable its dispatch without an
APPROVED contract covering text/NULL/quotes/binary and Python/client/HTTP semantics.
