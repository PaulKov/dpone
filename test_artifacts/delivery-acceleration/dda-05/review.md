# DDA-05 review record

Planning dependency: `f3682940f8864563cde0e6b6ecee60f746b49020`, imported by ordinary
fast-forward merge after fetching `origin/master`. Production base:
`d5ad9aaecc900c24df421b160ed36b4cfc726e45`. Audit implementation paths after the
planning dependency, whose shared-file changes are not DDA-05 edits.

Initial red test: `uv run pytest tests/test_native_delivery_live_benchmark.py -q`
failed collection with `ModuleNotFoundError: tools.native_delivery_live_support`.
The first implementation passed 15 focused tests; successive coverage added
producer/reader, absence, identity, fault/recovery and maintenance checks.

First fresh-context read-only architecture review requested five changes:

1. Observe actual lost-ACK injection and exact receipt probing.
2. Require authoritative receipt and successful terminal state for recovery.
3. Recompute this producer's workload/configuration/environment digests in inspect.
4. Report observed target rows on failed trials.
5. Record SWITCH invocation ownership and publish fixture status after cleanup.

All five were corrected, with hermetic regression tests for findings 1–4 and
explicit finalization/owner recording for the isolated live SWITCH fixture.
The generic DDA-01 consumer retains opaque digest semantics; DDA-05's inspector
knows its own generator/preimages. No shared schema was changed.

A second fresh-context independent test/certification reviewer found four further
issues: unknown-outcome recovery had to preserve both target multisets; maintenance
had to reject duplicate publication; live pytest setup exceptions needed a redacted
boundary; and the independently located producer checkout needed drift checks.
All four were corrected with regression tests. The reviewer reran each independent
in-memory reproduction and approved the component for integration review with no
remaining significant findings. The local reader also requires a live successful
warmup before counting live timed samples.

An additional targeted reviewer reproduction caught mutable row mapping aliasing
in before-image snapshots. `capture_rows` now validates immutable typed scalars
and copies each mapping immediately. Regressions cover reused dictionaries,
in-place publication/recovery mutation, and legitimate full-refresh replacement.
The independent reviewer reran all reproductions and restored final approval.
No protocol or schema changed. Focused tests now include 70 passing cases.

DDA-01 independently consumed the retained v1 fixture and produced UNVERIFIED
with a null ratio; its evidence is retained by that owner. The producer/consumer
interop is a hermetic schema PASS, not live certification.

Focused tests pass. Broad non-live pytest is still running and its final summary
will be recorded in the completion report. This record does not claim merge readiness.
Live SQL, BCP, containers, performance and SWITCH execution: **SKIP/UNVERIFIED**;
no disposable environment was approved and DDA-06 supplies the real factory.

`contract-fixture.json` and its immutable artifacts were produced by
`generate_contract_fixture.py`; they intentionally record SKIP/hermetic and dirty
producer identity. They prove the frozen v1 local input shape, never live success.
