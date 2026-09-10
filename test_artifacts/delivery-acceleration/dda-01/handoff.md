# DDA-01 integration handoff

Code commit: `7fcdabd43ed8656f1de52415fa66d8b7db6befd4`.
Reviewed documentation commit: `3e57875` (same production code).
PR: https://github.com/PaulKov/dpone/pull/28.
Baseline: `d5ad9aaecc900c24df421b160ed36b4cfc726e45`.
Immutable planning dependency: `f3682940f8864563cde0e6b6ecee60f746b49020`.

Use the final PR branch tip to include subsequent evidence-only commits. If the
integrator already has the planning dependency, cherry-pick the implementation
commits with `-x` in order: `b8ae950`, `94bbd3c`, `7912df9`, `957c239`, `7fcdabd`, `3e57875`, then the
evidence commit. Preserve later master changes and do not force-push.

## Component and authority

- `contracts.native_delivery_observations`: immutable observation/metric models
  and local frozen diagnostic JSON contracts.
- `ports.native_delivery_observer`: one-method observer protocol.
- `runtime.native_delivery_observations`: bounded collection and injected-clock
  scopes; no SQL, clients, state, admission or business acceptance policy.
- `runtime.native_delivery_benchmark`: receipt/identity/comparison policy.
- `runtime.native_delivery_benchmark_artifacts`: retained bytes, safe paths,
  portable bundles and atomic output. This extra path was explicitly approved
  by DDA-06 and root in the retained supplemental full task contract.
- `tools/native_delivery_benchmark.py`: thin sanitized developer CLI.

Only the owned files and task-local artifacts changed after the planning import.
Original frozen planning documents/contracts were not edited. No shared wiring,
normalization, schemas, factories, registry, changelog or navigation edits occurred.

## Tested integration instructions

1. Add optional keyword-only observers at DDA-06 composition seams. Omission uses
   `NativeDeliveryRecorder(None, clock_domain=..., process_id=..., worker_id=...)`
   and performs no timing calls. Never use diagnostics as a business success signal.
2. Use `collector = BoundedNativeDeliveryObserver(max_observations=4096)` and
   `recorder = collector.recorder(clock_domain=..., process_id=..., worker_id=...)`.
   The factory explicitly connects `record_diagnostics`; `collector.snapshot()`
   then includes recorder failures and separately measured totals.
3. `recorder.phase(phase, reason=None, ordinal=None, attempt_id=None, rows=None,
   encoded_bytes=None, metrics=None)` is a context manager. Arguments after phase
   are keyword-only. Preserve every failed/cancelled attempt identifier.
4. A spawned worker returns `observation.to_dict()` plus `recorder.snapshot()`.
   The parent calls `NativeDeliveryObservation.from_dict(payload)`,
   `collector.record(observation)` and `collector.record_diagnostics(snapshot)`.
   Do not pickle observer/client/collector objects or change legacy journal keys.
5. For directly constructed recorders, inject
   `diagnostics=collector.record_diagnostics`, or explicitly merge final snapshots
   with `collector.snapshot(recorder_reports=[...])`. Bound worker cardinality;
   one recorder belongs to one worker, while the collector serializes updates.
6. Use `duration('delivery')` from source acquisition through confirmed commit plus
   successful independent visibility probe, and `duration('pipeline')` through
   evidence/checkpoint. The helper does not infer these boundaries. Incompatible
   clocks must remain in separate domains. Repeated/failed totals stay unavailable.
   The integrated runtime currently has no independent visibility probe, so its
   diagnostic totals remain unavailable; DDA-05 owns measured harness boundaries.
   A finalizer return must never be interpreted as independent visibility.
7. Accumulate source/adaptation next-call work in a bounded metric map per frame,
   attach it to `frame_build` with `reason='inclusive_frame_build'`, and describe
   provenance. Do not create per-row records, synthesize gap-spanning source
   intervals or label the entire source context as source_read.
8. Serialize the collector snapshot to the separate observations sidecar. Overflow
   or failed recorder channels yield UNVERIFIED. Never add count queries solely
   for telemetry. Missing resources use null/reason and truthful provenance.
9. Run DDA-05 producer and the consumer together. The retained DDA-05 SKIP fixture
   was consumed successfully: comparison is UNVERIFIED with no eligible ratio.
   See `producer-consumer-smoke.log` and `producer-consumer-comparison.json`.
   The actual DDA-05 hermetic factory also produced four timed samples and retained
   fidelity/recovery profiles accepted by this consumer. They remain hermetic,
   UNVERIFIED and ineligible for a live ratio. See `hermetic-producer-case/` and
   `hermetic-producer-consumer-smoke.log`.
10. Complete shared overview/runbook, navigation, architecture and changelog work
    in DDA-06. The feature guide is `docs/delivery-acceleration/observations.md`.

## Validation boundary

Focused suite: 70 PASS (27 observation and 43 benchmark cases). Targeted mypy (six new files), repository mypy, ruff,
formatting, import rules, module-size, docs/generated reference/language checks
and strict MkDocs have passed. Exact command evidence is retained in this folder.
Full regression and optional-dependency replay results are recorded in
`completion.md`; the original full-suite FAIL is retained without relabeling.

The isolated layer-metrics gate reports runtime-to-contracts flow 216, above its
214 tolerance (baseline 209). This is a real unresolved integration gate. DDA-06
and root were informed; only an approved genuine annotation-only dependency
cleanup in shared code can resolve it. No baseline, budget, facade or dependency
hiding was introduced here. Architecture fitness also reports average clustering
0.18170059431282887 against target 0.180 (planning source: 0.1813233335910147).
The architecture pytest gate also fails on cross-layer ratio
0.30024001745581497 > 0.300; all three results require integration resolution.
After installing locked optional dependencies, the replay of all 13 failed/error
files produced 685 PASS and this one architecture FAIL in 137.19 seconds.

Live SQL, containers and performance profiles were not run: SKIP, no approved
disposable environment. Benchmark acceptance targets are not measured results.
Existing public native SWITCH rejection and release/provider state are unchanged.
