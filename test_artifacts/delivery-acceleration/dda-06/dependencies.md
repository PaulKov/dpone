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
