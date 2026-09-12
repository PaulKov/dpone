# Historical Agent Receipt Retention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove six duplicate historical receipt files from the current tree
while preserving byte-exact recovery, append-only provenance, and fail-closed
privacy semantics.

**Architecture:** A repository test fixes the public retention-record contract.
A separately reviewed private one-shot producer verifies the immutable Git
source, unchanged download manifest, and complete external archive before it
atomically emits the public record. The integrator stages the generated record
and six deletions only after producer verification; the generic scanner and
existing retention index remain unchanged.

**Tech Stack:** Python 3.12, pytest, Git object inspection, strict JSON, SHA-256,
the existing public-clean gate, and offline Docker.

## Global Constraints

- Base parent is `a54281906040f7e8d21e52071820c8b5f9d9608d`.
- Source authority is commit `6a69861832ffc3dea86fb05d0dd882645399f21a`
  and DDA tree `ea2153662106bbd58b27e3ca963425796145fdc9`.
- The six source files, `download.json`, `review-fixes-ownership.log`, and
  `hygiene-retention/index.json` are immutable inputs.
- The existing retention index SHA-256 remains
  `7cc4a46fb0fd7c930fef93ab63923be5d261eddf20f145d3a25095136bbb5e2e`.
- No LF projection, generic NUL exception, history rewrite, scanner relaxation,
  or current-execution claim is allowed.
- Public status is exactly `N/A`; reachable-history status remains
  `UNVERIFIED`.
- Candidate, history, and worktree scans are separate evidence.
- No production package, workflow, dependency, CLI, route, state, checkpoint,
  provider, or live behavior changes.
- No commit, push, pull request, release, or provider activation occurs before
  the existing public-clean commit protocol authorizes it.

---

### Task 1: Fix the public retention-record contract with RED tests

**Files:**
- Create:
  `tests/agent_policy/test_public_clean_historical_agent_receipt_retention.py`
- Read:
  `test_artifacts/delivery-acceleration/hygiene-retention/index.json`
- Read:
  `test_artifacts/delivery-acceleration/dda-06/remediation-ci-7c25ce9/download.json`

**Interfaces:**
- Consumes: repository root and immutable public authority files.
- Produces: assertions for
  `dpone.public-clean-historical-receipt-retention.v1`.

- [ ] **Step 1: Add exact authority constants and a strict JSON reader**

Use `json.loads(..., object_pairs_hook=...)` to reject duplicate keys and
`parse_constant` to reject non-finite values. Define the six exact paths and
their mode, size, Git blob, and SHA-256 values:

```python
EXPECTED_FILES = (
    ("agent_audit_manifest.json", "100644", 67354,
     "6aed59df5f6d337c488cd657065c80685873211e",
     "5586482c52d7d07dd1cc09ca3007604ec5f1cfb60d0911b784ced27b220190bd"),
    ("agent_pr_receipt.json", "100644", 154640,
     "cdd0e5072428eecd95de5037eb2489103bb1c775",
     "897e685b74f8fbf70f0ced6089fc007b1973570c935157625070e150df96d1de"),
    ("pr-body.md", "100644", 2734,
     "ef733464442aec062bea482a4bbb11c08eab05a8",
     "2738d3f28fdf9cb9dba0ad03951425863bcc4f604c86b5f9653f77b6908aa906"),
    ("pr-changed-paths.txt", "100644", 51237,
     "f6ad629e0f69a76dc0c8a63c7e5e6f742a6542a6",
     "446c054414bc56e3de6f66c4c50c595cdf5fea71e6476834a669472c1fec0506"),
    ("pr-head-sha.txt", "100644", 41,
     "4a0264a42075cc0d564a62e1f02bad8647dc1622",
     "d25c7b29af7ab46b91fd967f0687396a222ba547f487977ea7bc441246b0b7aa"),
    ("pr-receipt-exit-code.txt", "100644", 2,
     "573541ac9702dd3969c9bc859d2b91ec1f7e6e56",
     "9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa"),
)
```

- [ ] **Step 2: Add the public-record and current-tree tests**

Assert exact top-level keys, exact schema/status/source identities, sorted
six-file records, unchanged download-manifest digest
`e9ea9e06d09bf9a9d742a8b519d42430bb022b9066c927aa99fa5aacda5f801d`,
external archive/inventory digests and 796-file count copied from the immutable
index, a 64-lowercase-hex producer digest, explicit limitations, and absence of
absolute/private locators. Assert all six old current-tree paths are absent and
the existing index bytes retain their fixed digest.

- [ ] **Step 3: Run RED and retain the failure**

Run:

```bash
uv run pytest \
  tests/agent_policy/test_public_clean_historical_agent_receipt_retention.py -q
```

Expected: `FAIL` because the append-only record does not exist and the six
current-tree copies still exist. A collection error or unrelated failure is not
valid RED evidence.

- [ ] **Step 4: Review test scope**

Run Ruff on the new test and verify no production file, old authority record, or
source evidence changed. Do not stage the test until the RED result is recorded
outside the repository.

### Task 2: Generate and integrate the append-only retention operation

**Files:**
- Create:
  `test_artifacts/delivery-acceleration/hygiene-retention/agent-pr-receipt-retention-v1.json`
- Delete the six files under:
  `test_artifacts/delivery-acceleration/dda-06/remediation-ci-7c25ce9/agent-pr-receipt/`
- Private producer: `$PRIVATE_RETENTION_DIR/produce.py`
- Private evidence: `$PRIVATE_RETENTION_DIR/`

**Interfaces:**
- Consumes: fixed parent/index, source commit/tree, six regular files,
  `download.json`, immutable retention index, and complete external archive.
- Produces: one atomic JSON record and a private execution receipt; it never
  edits the repository.

- [ ] **Step 1: Write the private producer and its private unit tests**

The producer must use descriptor-relative no-follow reads; strict JSON with
duplicate/non-finite rejection; bounded file sizes/counts; exact Git
blob/mode/size/SHA checks; exact archive membership; before/after identity and
index checks; exclusive temporary output; `fsync`; final readback; and atomic
no-overwrite publication. It constructs the closed public shape from verified
values, never from caller-supplied output fields:

```python
record = {
    "schema": "dpone.public-clean-historical-receipt-retention.v1",
    "status": "N/A",
    "scope": (
        "Historical location and integrity only; no execution, privacy, "
        "release, or live certification"
    ),
    "source_commit": "6a69861832ffc3dea86fb05d0dd882645399f21a",
    "source_tree": "ea2153662106bbd58b27e3ca963425796145fdc9",
    "download_manifest": {
        "path": DOWNLOAD_PATH,
        "git_blob": "0a0b569abf11e8d526047450a34b8f5fbb628215",
        "sha256": "e9ea9e06d09bf9a9d742a8b519d42430bb022b9066c927aa99fa5aacda5f801d",
        "size_bytes": 1431,
    },
    "original_retention": {
        "archive_sha256": "b4c1d695b5aae8a61d23755142514adb0527d81b69397a253a79e76005028832",
        "inventory_sha256": "42264395c79af082c108261b566ff7f1a6e42df8f7d263c271c2dcac4f2f497b",
        "files": 796,
    },
    "producer_sha256": hashlib.sha256(producer_bytes).hexdigest(),
    "retained_files": verified_records,
    "retrieval": (
        "Recover all six files together from the pinned source commit or "
        "verified complete external archive."
    ),
    "limitations": [
        "Reachable-history privacy scan remains UNVERIFIED.",
        "This record is not execution or release evidence.",
    ],
}
```

- [ ] **Step 2: Prove producer failures before success**

Run private tests for source/download mismatch, missing/extra archive member,
mode/blob/hash drift, duplicate/non-finite JSON, symlink ancestor/leaf,
concurrent mutation, short/failed write, existing output, cleanup failure,
private locator leakage, prior-index mutation, and false `PASS` status. Preserve
only redacted counts and hashes in the handoff.

- [ ] **Step 3: Obtain independent producer review**

A fresh reviewer checks the private producer, exact inputs, archive relation,
atomicity, redaction, and generated public bytes. Fix all Critical and Important
findings and repeat review.

- [ ] **Step 4: Execute the producer once**

Execute against the frozen candidate and new private output directory. Require a
complete private execution receipt before copying only the generated public JSON
into the candidate.

- [ ] **Step 5: Integrate exact generated changes**

Stage the generated JSON and delete exactly the six bound current-tree files.
Verify `download.json`, `review-fixes-ownership.log`, and the existing retention
index retain their pre-operation hashes. Do not use `git add -A`.

- [ ] **Step 6: Run GREEN**

Run the Task 1 test. Expected: all tests `PASS`. Run it again in the prepared
offline Linux image with the repository mounted read-only.

### Task 3: Document retrieval, status, and recovery

**Files:**
- Modify: `docs/public-clean-migration.md`
- Modify: `docs/data-delivery-acceleration-tasks.md`
- Modify:
  `test_artifacts/delivery-acceleration/hygiene-retention/README.md`

**Interfaces:**
- Consumes: approved amendment and generated record.
- Produces: maintainer retrieval, interpretation, failure, and recovery guidance.

- [ ] **Step 1: Update the retention README**

Explain the second append-only record, six-file all-or-nothing retrieval,
original NUL framing, `N/A` semantics, immutable old index, and recovery from the
pinned commit or complete archive.

- [ ] **Step 2: Update operator and DDA guidance**

Explain that current-tree candidate scanning can proceed after retention, while
reachable-history scanning remains `UNVERIFIED`. State that no LF projection,
current receipt, release evidence, or active-consumer change exists.

- [ ] **Step 3: Validate documentation**

Run:

```bash
uv run dpone docs check-docs
uv run pytest tests/test_docs_language_contracts.py -q
uv run mkdocs build --strict
```

Expected: all `PASS`.

### Task 4: Revalidate the exact integrated candidate

**Files:**
- Private immutable evidence under `$PRIVATE_RETENTION_DIR/`
- Existing full-regression harness under the approved private validation root.

**Interfaces:**
- Consumes: exact staged candidate after Tasks 1-3.
- Produces: separate candidate/history/worktree receipts and full regression
evidence.

- [ ] **Step 1: Run focused authority regressions**

Run the new retention test plus path-evidence, source-archive, merge-receipt,
workflow-governance, scanner binary/text, candidate, history, and worktree tests.
Run the same focused set in offline Linux Docker.

- [ ] **Step 2: Freeze fresh scan inputs**

Create new metadata, review, and receipt filenames bound to current HEAD, exact
index tree, policy identity, and scanner identity. Never reuse the pre-fast-
forward or pre-retention receipts.

- [ ] **Step 3: Run independent scans**

Run candidate, reachable-history, and worktree modes separately. Candidate may
advance only after every finding receives exact review. History must remain
`UNVERIFIED` for the preserved NUL Git blob. Generated or ignored worktree
content is not silently excluded.

- [ ] **Step 4: Run the full local Docker regression**

Freeze the exact source snapshot and image identity, verify installed source
hashes, then run `pytest -m "not integration_live" -n auto --dist loadfile`.
Classify every failure and skip; no interrupted or partial run is `PASS`.

- [ ] **Step 5: Run the change-aware gate**

Run every command selected by:

```bash
uv run python tools/agent_policy/select_checks.py --base-ref origin/master
```

Include package, docs, architecture, security, and module-size checks selected
for the complete migration diff.

- [ ] **Step 6: Obtain final independent reviews**

Review privacy/evidence integrity, silent-data risk, compatibility, tests, docs,
and the complete branch diff. Fix all Critical and Important findings and rerun
affected checks. Keep live/vendor `UNVERIFIED`.

- [ ] **Step 7: Stop before commit**

Prepare candidate metadata and exact review inputs, but do not commit, push,
open a pull request, release, or activate providers until the public-clean
candidate protocol and maintainer authority explicitly permit that next step.
