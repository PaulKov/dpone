# Integrated quality summary correction

Purpose: remove a stale manual claim that architecture checks pass while the
exact integrated gate receipts report failures. DDA-06 owns the correction on
the already assigned `docs/quality-metrics.md` path.

The full effective contract is `dda-06-quality-summary-scope.yml`. This is an
explicit exception to the previous outside-marker preservation rule, limited to
the three replacements below. The existing generator, generated marker contents,
all other manual text and all quality thresholds remain unchanged.

## Preconditions and exact replacement text

Record the exact source and completed canonical layer/architecture receipts.
Both must confirm the failures described below on the evaluated integrated
candidate. A generated dashboard heuristic is not sufficient evidence.

The integrator reported both failures at unchanged source
`b82e2098a642aff588f6a9c6b33d72292e25023a`, with receipts in
`test_artifacts/delivery-acceleration/dda-06/integrated-current-layers.json` and
`test_artifacts/delivery-acceleration/dda-06/integrated-current-architecture.json`.
Recheck their source identity before applying this correction.

Replace the obsolete YELLOW headline under `Current quality summary` with:

> 🔴 **RED - integrated architecture gates fail; merge readiness remains on hold.**

Replace only the following explanation paragraph, beginning `General import/layer
checks` and ending `the preferred clustering target remains unmet.`, with:

> The current integrated dependency and architecture gates report regressions.
> Generated dashboard labels do not establish acceptance or replace the complete
> exact-candidate CI suite. Regeneration updates observations, not thresholds;
> actual gate receipts determine readiness.

In the next paragraph, replace only `The narrow architecture margin still warrants
improvement.` with:

> Architecture coupling requires further improvement.

If the source or observed results no longer support this wording, stop this
correction and reconcile the narrative with reviewed evidence. Do not relabel a
gate or edit generated values to satisfy the document.

## Validation and handoff

Record the manual diff and identical SHA-256 of the generated marker block before
and after. Run `uv run dpone docs update-dev-metrics --check`, documentation and
language checks, and strict MkDocs on the final candidate. Obtain independent
review after the correction, then freeze the final source/document tree for the
remaining validation. A summary-only commit changes no Python inputs.

The DDA-02 component metrics operation retains its producer-only restriction.
This exception grants no component writer authority, source changes, new output
path, baseline/budget/threshold edits, live execution, merge or publication.
