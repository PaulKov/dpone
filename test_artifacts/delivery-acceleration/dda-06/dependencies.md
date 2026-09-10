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

DDA-03 follow-up `ad5c117f89c7df33f2bdc2e1aa42d671ca8e0f1b` was independently
reviewed and imported with provenance. It removes only an annotation-only eager
import from the new private sized-frame module; existing tuple-adapter exports
remain. Owner focused/type/import/module/layer gates PASS (layer flow 214); the
full suite is being rerun with locked optional dependencies installed.

Checkpoint acceptance permits integration work. It does not promote a pending
owner gate into PASS. Final owner evidence and any subsequent fixes must be
reconciled before claiming integrated readiness. Live SQL is SKIP and performance
UNVERIFIED for every checkpoint without approved environment evidence.
