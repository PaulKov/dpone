# DDA-01 fresh independent review and correction

User request: perform an independent subagent review and repeat it after every
improvement. Reviewer: fresh-context `/root/independent_review_v2`, read-only.
PR: https://github.com/PaulKov/dpone/pull/28.
Initial reviewed checkpoint: `006e0a1852a10e3f0a519b94edfc79f94c476d65`.
Final production source: `9b69be0c16e69e7b34352e0f1148d10c48e525f7`.
Documentation source: `9aa2a0ef9600644f1789b9de1b84b9ab9ba1d033`.
Environment: macOS 26.3.2 arm64, Python 3.12.11, existing locked dependencies.

## Findings, fixes and independent re-review

The new reviewer inspected the implementation without relying on earlier review
conclusions and found two reproducible P2 defects:

1. Exporting a resolved run/campaign symlink changed the directory against which
   relative evidence was interpreted. A valid input comparison could return PASS
   while exported envelope references could not be read again. Export now preserves
   logical envelope aliases and only resolves bytes for hashes. Input parent
   directories are normalized separately, including directory aliases.
2. Attached observations sidecars were validated only by their header and PASS
   label. Unknown nested observation versions, malformed recorder diagnostics or
   capacity-loss channels could be ignored. The consumer now reconstructs bounded
   typed observation/recorder records through the collector and compares the entire
   derived snapshot using canonical JSON. Malformed or inconsistent sidecars fail
   as input errors; valid failed/overflow diagnostics remain UNVERIFIED.

Twenty-one added regressions cover in-memory/same-tree/bundled run and campaign
aliases, parent-directory aliases, valid collector snapshots and invalid nested
records, versions, aggregates, capacity and status channels. The pre-fix run
reproduced 18 failures; the complete focused suite passes 91 cases after correction.

The reviewer independently reran all 91 cases and added combined campaign
folder+envelope alias probes and loss-only recorder snapshot roundtrips. No
remaining actionable findings. The reviewed five-file diff hash was
`93aae352c5a408021f075f145e1b6ee5fa281818ad6aa0cf4d6074421391e7bb`, verified unchanged
before committing `9aa2a0ef9600644f1789b9de1b84b9ab9ba1d033`.

The exact module-size gate then identified 352 SLOC, above the 350 warning ratchet.
The snapshot field declaration was extracted into a named immutable set. A second
independent re-review confirmed all eight keys and exact AST equivalence after
inlining the constant. Diff hash
`0b2cbb58be962084bb65e18ccb3aa503e230f607f0dcf989cbea1f9f24770ed7` was verified before
committing `9b69be0c16e69e7b34352e0f1148d10c48e525f7`. No budget/baseline was changed.

## Validation

Commands use `uv run --locked --no-sync`; focused tests use the two DDA-01 test
files. Per-command source/arguments/exits are retained where shown in the logs.

| Check | Status | Result | Evidence |
| --- | --- | --- | --- |
| New regression cases before fix | FAIL, expected red phase | 18 reproduced failures | [red](review-v2-red.log) |
| Focused observations + benchmark tests after each fix | PASS | 91 cases, also independently rerun | [initial fix](review-v2-focused.log), [final source](review-v2-focused-final.log) |
| Repository Ruff/format, mypy, six-file focused mypy, change-aware selector | PASS | Final source checked | [final checks](review-v2-final-checks.log) |
| Import rules | PASS | Correct dependency direction; benchmark deliberately reuses runtime collector | [imports](review-v2-imports.log) |
| Module-size at first fix | FAIL, corrected | 352 SLOC exceeded warning ratchet | [first gate](review-v2-module-size.log) |
| Module-size at final full SHA | PASS | No module-size issues | [final gate](review-v2-module-size-final.log) |
| Layer metrics | FAIL | runtime-to-contracts 216 exceeds 214 tolerance | [layers](review-v2-layers.log) |
| Architecture fitness command | FAIL, budget | CLI exit 0 with warning; clustering 0.18174658401113647 exceeds 0.180 | [architecture](review-v2-architecture.log) |
| Architecture pytest gate on final source | FAIL | Cross-layer ratio 0.30020726519035673 exceeds 0.300 | [test](review-v2-architecture-test.log) |
| Documentation link check | FAIL | Agreed DDA-06-owned overview index.md is absent in the isolated DDA-01 branch | [docs](review-v2-docs.log) |
| Generated references / language contracts / strict MkDocs | PASS | 32 language checks; strict build 14.25 seconds | [generated](review-v2-generated.log), [language](review-v2-language.log), [MkDocs](review-v2-mkdocs.log) |
| Actual retained DDA-05 producer fixtures consumed again | PASS | SKIP and timed hermetic fixtures remain UNVERIFIED, all ratios null | [interoperability](review-v2-interoperability.log), [hermetic comparison](review-v2-hermetic-comparison.json), [SKIP comparison](review-v2-skip-comparison.json) |
| Full regression on this final source | UNVERIFIED | DDA-06 owns the combined-source run after importing both reviewed fixes | [historical result](completion.md) |
| Owned-path audit | PASS | All 104 current paths remain within supplemental ownership | [path audit](path-audit.json) |
| Live SQL, performance and certification | SKIP / UNVERIFIED | No live environment was approved; synthetic live-shaped fixtures are consumer unit tests only | This report |
| Packaging / release / provider changes | N/A | Not part of this change | This report |

One module-size invocation incorrectly supplied `HEAD` instead of a full SHA;
its configuration failure remains in [invocation log](review-v2-module-size-invocation.log).
The corrected command used the full final source SHA and passed. Raw pytest
whitespace is retained unchanged; source/test/docs diff whitespace checks pass.

The first full broad run and optional-dependency replay documented in completion.md
remain historical FAIL results. They do not certify these later fixes. Current
focused, static, documentation and architectural checks are distinguished above;
combined-source regression and graph evidence must come from DDA-06's exact commit.

## Evidence correction and user journey

The previous completion/PR incorrectly grouped `docs-final.log` as PASS. That
retained log actually reports the same missing overview link as the current check.
The historical completion has been corrected to FAIL, separately from generated
references and MkDocs PASS. The dependency was already documented; the status
label was wrong. No raw evidence was rewritten to change its outcome.

The guide now explains logical envelope aliases, sidecar reconstruction, exit-2
input rejection and UNVERIFIED diagnostic loss. Existing delivery behavior,
manifest/default/journal/receipt contracts and public SWITCH rejection remain
unchanged. No migration is required for valid artifacts. Shared navigation,
overview and changelog are DDA-06-owned integration work.

## Handoff and readiness

After `006e0a1`, cherry-pick `-x` `9aa2a0e` then `9b69be0`, followed by the final
artifact-only commit containing this report. Both source improvements passed
independent re-review before handoff; the user-requested review rule remains in
force for subsequent changes.

Ready for scoped review/integration. Merge/release readiness is UNVERIFIED until
the combined-source graph, documentation and regression gates complete. No live
performance or publication authority follows from this review.
