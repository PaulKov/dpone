# Historical DDA evidence retention

The [generated location index](index.json) identifies 35 historical raw test-output
copies removed from the current checkout after the source hygiene check. Original
files remain byte-for-byte available at Git commit
`6a69861832ffc3dea86fb05d0dd882645399f21a` and in the maintainer's verified external
archive of the complete original DDA evidence subtree. The index records original
Git blob IDs, SHA-256 values, sizes, modes, the archive/inventory hashes and the
retained maintenance producer's hash.

The index is **N/A for execution and release certification**. It does not replace
JUnit, a test population, a shard receipt or any historical audit. Original test
names, hashes, failures, skips and producer identities were not rewritten. Existing
historical reports and relative links must be interpreted in their original Git
context. In particular, keep the original population and all 16 corresponding
shard receipts together when reviewing that earlier CI run.

## Retrieve original context

From a clone containing the original commit, extract into a new directory outside
the checkout:

```bash
dda_originals="$(mktemp -d)"
git archive 6a69861832ffc3dea86fb05d0dd882645399f21a test_artifacts/delivery-acceleration | tar -x -C "$dda_originals"
```

Review reports and their relative evidence links inside that complete snapshot.
Compare any relocated file against its SHA-256 and Git blob ID in `index.json`.
The immutable Git objects provide an independent recovery path; the external
archive is an additional verified copy. Raw environment metadata and test values
belong in the recovered historical context, not back in the current public tree.

## Preservation procedure and recovery

The one-time producer and its synthetic tests are retained with the maintainer's
release audit, outside this repository. The producer accepts only the exact
original failed hygiene ZIP and frozen source commit. It retains every original
DDA Git blob and mode, then rereads and verifies complete archive membership and
bytes before removing the 35 duplicate checkout copies. It changes no private
policy, test assertion, original audit, Git history or publication authority.

The public index alone is not a completed-operation receipt. An interrupted
removal without a successful final execution record is **UNVERIFIED**. All original
Git objects and the verified archive remain recoverable. Inspect the partial diff,
restore the selected paths from the frozen snapshot if a clean retry is needed,
and preserve the interrupted attempt. Retry only from a clean original checkout
with a new external retention directory; never overwrite a prior archive or
reclassify an incomplete attempt as successful.

Release readiness still requires fresh normal source checks, merge identity,
packaging and both source/archive hygiene reports on the eventual release commit.
This archival index grants no release or live-certification approval.
