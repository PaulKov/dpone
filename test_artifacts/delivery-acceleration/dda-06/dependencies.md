# Reviewed dependency provenance

All code imports below preserve the original commit in `cherry-pick -x` metadata.
GitHub forbids merge commits on the PR branch; the original attempted local merge
history remains retained as described in the integration plan.

| Task | Reviewed owner checkpoint | Integration commits | Review / validation scope at handoff |
|---|---|---|---|
| Planning | `f3682940f8864563cde0e6b6ecee60f746b49020` | Exact ancestor | Approved immutable algorithms and six YAML contracts |
| Planning registry | `85e598c` | `8826852` | Dispatch IDs and contract links only; no algorithm changes |
| DDA-02 / PR 26 | `3f37418f898bcd60f475ede8fbd89ca5ad24c7a4` | `a3c3a2e`, `f8ef4d8` | Two fresh reviews, no findings; focused/static/docs PASS; full non-live gate pending at checkpoint |
| DDA-03 / PR 27 | `a029878ac638c8ad7a2a59c28ac503fa860e7e9d` | `f9e0c37` | Fresh review and 1,200 legacy differential cases PASS; focused/static/docs PASS; layer flow FAIL 215 > 214, reviewed fix and full non-live gate pending |
| DDA-04 / PR 29 | `d8b09de644ab3bf72f9eee7b51cc8b8e38ca8a3c` | `4d3152c`, `b8999cc` | Two component reviews and fresh evidence-fix review; 103 focused + 4 producer cases PASS; static/type/docs/module PASS; layer and architecture-fitness FAIL, full non-live pending |

DDA-03 follow-up `ad5c117f89c7df33f2bdc2e1aa42d671ca8e0f1b` was independently
reviewed and imported with provenance. It removes only an annotation-only eager
import from the new private sized-frame module; existing tuple-adapter exports
remain. Owner focused/type/import/module/layer gates PASS (layer flow 214); the
full suite is being rerun with locked optional dependencies installed.

ADR allocation was checked against current master and filenames in all registered
worktrees. Master ends at 0058, while parallel composition work already uses
0059, 0060 and 0061. The first unused number after that sequence is 0062; DDA-06
uses it for the isolated SWITCH activation boundary. No parallel ADR was edited.

Checkpoint acceptance permits integration work. It does not promote a pending
owner gate into PASS. Final owner evidence and any subsequent fixes must be
reconciled before claiming integrated readiness. Live SQL is SKIP and performance
UNVERIFIED for every checkpoint without approved environment evidence.

Additional reviewed imports, all through ordinary `cherry-pick -x`:

| Task | Owner checkpoints | Integrated commits | Scoped evidence |
|---|---|---|---|
| DDA-01 / PR 28 | `b8ae950`, `94bbd3c`, `7912df9`, `957c239`, `7fcdabd` | `1887352`, `db0077f`, `4ed4004`, `fe789e6`, `8a237fe` | Independent reviews approved; latest 70 focused PASS. Strict numeric identity and actual producer provenance fixes included. Owner full suite pending; failures observed, not promoted to PASS. |
| DDA-05 / PR 30 | `d406dbf`, `5cc2296`, `518cc16`, `f09af01` | `62558a6`, `bd3bc67`, `5bd0b67`, `b77266a` | Independent review approved latest immutable typed snapshot fix; 70 focused PASS. Owner full suite at earlier 518cc16 pending. |
| Annotation/staging supplement | `4edfb91fb160acb3e1a7505d0e1393e4bf8daeff` | `9be1a4c` | Planning owner approved six annotation edges, conditional sink annotation reserve and cohesive staging checks extraction. |

DDA-02's initial full run completed with 20,537 PASS / 815 SKIP / 28 FAIL /
2 collection errors. Its owner reports missing declared optional dependencies and
is replaying failures after installing them; benchmark/probe failures remain
under investigation. This is an owner checkpoint FAIL, not a dpone integration
PASS. DDA-06 installed its own locked all-extras environment before final checks.

After six annotation edges were removed and observation wiring was added,
`after-six-annotations-layers.log` records the actual working-tree check: runtime
flow 216 > 214 (FAIL). This triggers the explicitly authorized reserve in
`sinks/mssql.py`: `MssqlCatalogColumn` still appears only in a postponed method
return annotation and is moved under TYPE_CHECKING. No sink behavior changes.
A final clean-commit check must establish the remaining result; the forecast is
215, still above the unchanged limit. No unowned PostgreSQL cleanup is authorized.

The native encoder opt-in selects a separate worker entrypoint with the original
argument tuple. The default submitted-task serialization and admission boundary
are preserved; no diagnostic flag enlarges that tuple. Legacy journal records
remain unchanged. The new sidecar is bounded separately.

Reviewed owner follow-ups received after the implementation handoff:

- DDA-01 documentation `3e57875` imported as `a4db8b2`: integrated observation
  composition, explicitly missing independent visibility authority, and preserved
  native cancellation exception classification.
- DDA-02 documentation `ad9d96b`: reviewer-approved current structural behavior,
  historical component recipe and immutable integration test/evidence links.
- DDA-04 evidence-only `039fe201c9c24c9f95f1b2af55794c384b2299c5`: final report,
  immutable gate receipts and scoped ownership audit. Source/docs/tests remain
  identical to the already imported reviewed component checkpoint.

DDA-01 replay after locked all-extras installation resolved dependency/collection
failures: 685 PASS / 1 FAIL across the failed-file set; the remaining architecture
cross-layer-ratio failure remains documented. DDA-02's replay retained one
pre-existing doctor PYTHONTRACEMALLOC subprocess timeout, including an isolated
reproduction; its owned helper tests remain PASS. No unowned timeout or baseline
was modified by DDA-06.

All component guide follow-ups are now integrated and independently re-reviewed:
DDA-03 `4851bd3` (local `81a0203`) describes the final original-task byte check and
417-case integrated receipt; DDA-05 `5761738` (local `cec722c`) leaves factory and
visibility authority with the approved application. DDA-01 final artifact commit
`006e0a1`, DDA-02 `61bcebc` and DDA-05 `38f6603` were imported with provenance.
DDA-03's final evidence `76ee6da` was imported as `4dff1ce`. Fresh additional
DDA-01/DDA-05 owner reviews requested by the user found defects; their fixes
must be integrated before the final validation freeze.

DDA-03 and DDA-05 full runs with installed extras each retained two failures in
unchanged Airflow cache permission tests. Their independently controlled probes
identified a macOS temporary-root group mismatch. DDA-06 created a unique root
under the platform user temp directory; its real chmod probe confirms effective
GID 20 and retained mode 02777. The final full suite uses that root, preserving
all shared permission tests and production checks.

The next independent owner review cycle produced these reviewed follow-ups:

- Planning `642874b` (local `0a23d9b`) records the approved isolated DDA-05
  recovery regression module and its focused command. The full effective
  supplement remains in DDA-06's supplemental-contracts directory.
- DDA-04 `e0fbad8` (local `1243bf3`) documents the fixture-owner grant of
  `SELECT ON OBJECT::sys.sql_expression_dependencies`. No production or public
  admission behavior changed. Independent follow-up review reported no findings.
- DDA-01 `9aa2a0e` and `9b69be0` (local `d7bd090`, `6b2d78e`) preserve logical
  envelope aliases when exporting relative proof bundles, reconstruct and
  validate diagnostic sidecars, and keep the immutable snapshot field contract
  below the module warning budget. Each revision received independent review;
  the owner reports 91 focused cases PASS. Its standalone documentation link
  check remains FAIL because the DDA-06 overview is absent in that checkout.
  Earlier owner completion prose incorrectly grouped that retained failed log
  with PASS checks; combined documentation validation remains mandatory.
- DDA-01 artifact-only `f7ae00c` (local `9547b42`) retains the independently
  audited correction, exact source checks and original failed logs. The
  production checkpoint remains `9b69be0`.
- Planning `c1a88ad` (local `550510f`) authorizes a bounded sole-integrator
  metrics handoff for the frozen DDA-02 component source `61bcebc`. Its generated
  component dashboard is not imported into this combined branch.
