# DDA-02 generated metrics handoff

Purpose: make the component PR's generated dashboard match its own reviewed
source while retaining DDA-06 as the single shared-file owner.

This is a bounded operation under DDA-06's existing producer-only authority for
`docs/quality-metrics.md`. It adds no owned path and transfers no shared-file
ownership to DDA-02. The effective full DDA-06 generated-metrics contract remains
in force, including all baseline, budget, producer and gate restrictions.

## Frozen inputs

DDA-02's reviewed component commit is
`61bcebc0c96b4db535bd4494de6f30cb15d11d18` (PR #26). Its owner confirmed that source,
tests and tracked Python inputs remain unchanged; the ongoing validation adds
only logs, JSON and Markdown. Recheck that assertion at execution and acceptance.
If a tracked Python input changes, discard the stale dashboard candidate and
regenerate it for the newly reviewed source commit.

## Execution and acceptance

1. DDA-06 creates its own isolated checkout from the exact component commit.
   Do not mutate DDA-02's active worktree or rewrite either branch's history.
2. Record source identity and the tracked Python input inventory, then run
   `uv run dpone docs update-dev-metrics --check` to preserve the actual before
   result. The existing generator and its options remain unchanged.
3. Run `uv run dpone docs update-dev-metrics` with the default output path.
   Run its `--check` mode again and verify byte-identical repeated generation.
   Confirm the only generated tracked-file change is `docs/quality-metrics.md`,
   preserving text outside the generated markers and all measured values.
4. Run the applicable documentation checks and obtain independent review of the
   exact generated diff and its source identity. DDA-06 records all evidence in
   its already-owned evidence directory. A docs-only handoff commit must contain
   only `docs/quality-metrics.md` and name its exact component source dependency.
5. DDA-02 may import that reviewed docs-only dependency through cherry-pick -x
   after confirming its tracked Python inputs and the dashboard's non-generated
   content are still compatible. Stop on a mismatch or conflict; do not manually
   resolve generated measurements. Rerun `update-dev-metrics --check` on the
   resulting PR head and retain provenance in the component handoff.
6. DDA-06 does not import this component dashboard snapshot into the combined
   integration tree. It regenerates the combined dashboard from all final
   reviewed tracked Python inputs and runs the existing final validation plan.

This operation does not authorize readiness package edits, test timeout changes,
live execution, public SWITCH activation, merge or release. The old doctor
timeout evidence remains retained alongside the successful unchanged-source
replays; a host-contention cause remains an inference. Dashboard freshness and
component CI do not supersede integrated architecture FAIL/HOLD.
