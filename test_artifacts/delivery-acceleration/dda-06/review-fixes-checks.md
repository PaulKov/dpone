# Review-fix validation receipts

These are actual local producer receipts. Final replacement-head CI is retained
outside the source tree and linked from the replacement PR. Reproduction failures
and the rejected abbreviated-SHA command remain failures in this ledger.

| Check | Result | Seconds | Tested commit | Evidence |
|---|---|---:|---|---|
| `review-fixes-annotation-before` | **PASS** | 17.0 | `10215228c202` | [receipt](review-fixes-annotation-before.json) / [log](review-fixes-annotation-before.log) |
| `review-fixes-architecture-tests` | **PASS** | 189.415 | `b5b9f0eb3414` | [receipt](review-fixes-architecture-tests.json) / [log](review-fixes-architecture-tests.log) |
| `review-fixes-baseline-red` | **FAIL** | 6.619 | `3b510b62312f` | [receipt](review-fixes-baseline-red.json) / [log](review-fixes-baseline-red.log) |
| `review-fixes-check-selection` | **PASS** | 2.347 | `b5b9f0eb3414` | [receipt](review-fixes-check-selection.json) / [log](review-fixes-check-selection.log) |
| `review-fixes-docs-focused` | **PASS** | 23.439 | `425e06de2c16` | [receipt](review-fixes-docs-focused.json) / [log](review-fixes-docs-focused.log) |
| `review-fixes-docs-preliminary` | **PASS** | 16.269 | `425e06de2c16` | [receipt](review-fixes-docs-preliminary.json) / [log](review-fixes-docs-preliminary.log) |
| `review-fixes-final-docs` | **PASS** | 14.219 | `b5b9f0eb3414` | [receipt](review-fixes-final-docs.json) / [log](review-fixes-final-docs.log) |
| `review-fixes-fitness` | **PASS** | 87.143 | `ec7e2bf101c3` | [receipt](review-fixes-fitness.json) / [log](review-fixes-fitness.log) |
| `review-fixes-focused-green` | **PASS** | 126.27 | `f5705f83249d` | [receipt](review-fixes-focused-green.json) / [log](review-fixes-focused-green.log) |
| `review-fixes-format` | **PASS** | 2.69 | `ec7e2bf101c3` | [receipt](review-fixes-format.json) / [log](review-fixes-format.log) |
| `review-fixes-generated-references` | **PASS** | 11.052 | `b5b9f0eb3414` | [receipt](review-fixes-generated-references.json) / [log](review-fixes-generated-references.log) |
| `review-fixes-imports` | **PASS** | 52.513 | `ec7e2bf101c3` | [receipt](review-fixes-imports.json) / [log](review-fixes-imports.log) |
| `review-fixes-layer-green` | **PASS** | 47.95 | `ec7e2bf101c3` | [receipt](review-fixes-layer-green.json) / [log](review-fixes-layer-green.log) |
| `review-fixes-layer-red` | **FAIL** | 50.571 | `10215228c202` | [receipt](review-fixes-layer-red.json) / [log](review-fixes-layer-red.log) |
| `review-fixes-mkdocs` | **PASS** | 109.048 | `b5b9f0eb3414` | [receipt](review-fixes-mkdocs.json) / [log](review-fixes-mkdocs.log) |
| `review-fixes-module-size-corrected` | **PASS** | 20.437 | `ec7e2bf101c3` | [receipt](review-fixes-module-size-corrected.json) / [log](review-fixes-module-size-corrected.log) |
| `review-fixes-module-size` | **FAIL** | 13.075 | `ec7e2bf101c3` | [receipt](review-fixes-module-size.json) / [log](review-fixes-module-size.log) |
| `review-fixes-mypy` | **PASS** | 25.058 | `ec7e2bf101c3` | [receipt](review-fixes-mypy.json) / [log](review-fixes-mypy.log) |
| `review-fixes-ownership` | **PASS** | 105.386 | `425e06de2c16` | [receipt](review-fixes-ownership.json) / [log](review-fixes-ownership.log) |
| `review-fixes-provenance-green` | **PASS** | 22.128 | `425e06de2c16` | [receipt](review-fixes-provenance-green.json) / [log](review-fixes-provenance-green.log) |
| `review-fixes-provenance-red` | **FAIL** | 8.602 | `20f7e73a3f19` | [receipt](review-fixes-provenance-red.json) / [log](review-fixes-provenance-red.log) |
| `review-fixes-red` | **FAIL** | 5.658 | `14bebb61f46d` | [receipt](review-fixes-red.json) / [log](review-fixes-red.log) |
| `review-fixes-ruff` | **PASS** | 2.679 | `ec7e2bf101c3` | [receipt](review-fixes-ruff.json) / [log](review-fixes-ruff.log) |
| `review-fixes-supplement-focused` | **PASS** | 37.309 | `ec7e2bf101c3` | [receipt](review-fixes-supplement-focused.json) / [log](review-fixes-supplement-focused.log) |

The initial red run reproduced the live-entrypoint defect and exposed a macOS
venv fixture setup error. The corrected baseline red run reached the actual CLI
and reproduced both valid-input launch failures. The initial provenance run
lacked the new verification functions. The layer red run measured flow 215 above
the unchanged 214 ceiling. The first module-size invocation used an abbreviated
head SHA and was rejected before evaluating code; the corrected full-SHA run
passes. None of these results is relabelled.

The [canonical metrics producer](review-fixes-metrics/refresh.json) records the
tracked inventory, expected stale before-check, successful regeneration/freshness
and byte-identical repeat. Only the generated dashboard block changed. Independent
review is consolidated in [the review record](review-fixes-independent-review.md).

A duplicate unchanged local full suite is N/A under the approved final supplement.
Complete new-head CI populations on Python 3.11 and 3.12 remain required. Live
interoperability and measured performance remain UNVERIFIED without an approved
disposable environment; no such execution occurred here.
