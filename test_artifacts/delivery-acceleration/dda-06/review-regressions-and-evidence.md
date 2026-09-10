# Independent integration regression and evidence review

This is the integrator's record of the read-only `review_integrated_regressions`
subagent review, not an executable test receipt. The reviewer evaluated the
working changes over `550510f93940458b97b7f6cfc0abb61ba1fdaaa8` and executed the
scoped probes described below. Final frozen-source gates remain separate.

The first review found four gaps, all corrected and independently re-reviewed:

1. An empty trial list could satisfy the failure assertion. The integration test
   now requires exactly the warmup and three trial IDs, their warmup flags, and
   successful independent fidelity/recovery receipts before checking failures.
2. Export replay still had access to both original bundles. The test now requires
   the retained path to resolve under the export root and deletes both inputs
   before replaying the saved envelope.
3. `run_check.py` omitted package and evidence Python inputs. Its hash and dirty
   selections now both include `packages` and the recursive Git `*.py` pattern.
4. The ownership audit trusted cherry-pick provenance alone. It now requires the
   exact reviewed path set and actual patch identity. A known conflict may use
   the exact reviewed final file bytes; this accommodates the approved planning
   README resolution without granting additional paths or content.

A second adversarial audit found that Git's stable patch ID ignores semantic
whitespace inside Python strings. The recorder now preserves the raw patch and
uses `--verbatim`. Independent probes reject a foreign path, altered known file,
inserted whitespace inside an exception string and trailing string whitespace.
The exact DDA-01 import and reviewed planning README resolution remain accepted.

Reviewer-executed results:

- **PASS:** portable symlink export after both input directories are removed.
- **FAIL, expected RED:** two repeated-query/publication producer regressions
  still returned `UNVERIFIED` before the DDA-05 corrective checkpoint. Exact
  result: 2 failed, 1 passed, 16 deselected in 25.88 seconds. These failures are
  not final integration passes.
- **PASS:** no-write source-identity probes include a tracked measurement
  producer, untracked audit script and package paths; evidence-only Python byte
  changes alter the recorded source digest.
- **PASS:** actual ownership audit on the commit above reports 33 verified
  imported checkpoints and no violations.
- **PASS:** revised tests and evidence scripts approved, no remaining findings.
- **SKIP / UNVERIFIED:** broad suite, live SQL and performance certification were
  not executed by this reviewer.

The integrator retains independent raw RED/portable-export logs in this directory
and will rerun the complete focused set after the reviewed DDA-05 fix lands.
