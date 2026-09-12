# Public-clean migration operator guide

Purpose: help maintainers review migration preparation without disclosing source
identities. This is a preparation checkpoint, not migration completion or route
certification. See the [approved gate specification](feature-design-public-clean-migration-v1.md)
and [agent workflow](agent-development.md).

## Authority and privacy

The specification and task contract are APPROVED. Run current validation and
independent review against the exact implementation before relying on it.
Unknown private facts still require human review. Earlier reconnaissance scripts
remain historical observations, not substitutes for the gate.

Keep source identities, policy values and detailed findings in a private external
ledger. Never copy those values into public artifacts, test fixtures, commit
messages or exception output. Task-specific migration progress belongs in that
ledger, not in this operator reference.

## Inventory interpretation

The private inventory includes refs, reflogs, unreachable commits, registered
worktrees, dirty entries and the named corrective draft. Every discovered commit
has an explicit provisional or rejected disposition. Literal commit-reference
closure expands the initial path/subject selection; symbolic prose and unknown
external checkouts still need review.

Complete changed-path postimage comparison includes every source parent,
non-route paths, deletions and modes. Matching postimages are evidence of content
presence only. Empty deltas are counted separately. Neither a matching file nor
a historical approval transfers exact public authority automatically.

Rejected experimental assets and measurements remain historical lineage. They
cannot become current test, measurement or activation authority. The uncommitted
draft is inventoried using separate staged-blob and working-file digests.

### Retained NUL-framed path evidence

One historical agent receipt stores its Git changed-path list with NUL
delimiters. The generic scanner continues to reject NUL-framed content. The
current tree therefore retains only an
[append-only location and integrity record](https://github.com/PaulKov/dpone/blob/master/test_artifacts/delivery-acceleration/hygiene-retention/agent-pr-receipt-retention-v1.json);
the six original files remain byte-exact in the pinned Git commit and verified
complete external archive.

This operation creates no LF projection, current execution receipt, privacy
exception, or release evidence. Retrieve and verify all six files together when
historical inspection is required; never normalize the changed-path list or mix
files from different snapshots. The current candidate scan can proceed after
the duplicate checkout copies are removed. Reachable-history scanning remains
`UNVERIFIED` because the immutable NUL-framed Git blob is intentionally
preserved. Candidate, history, and worktree results are not interchangeable.

## Review and recovery

1. Confirm the approved specification and scoped task contract.
2. Run the focused gate tests and independent implementation review.
3. Scan the public reachable baseline, source metadata/full deltas, index,
   worktree and embedded artifact strings. Resolve every finding privately.
4. Classify all provisional source deltas and confirm complete coverage. A
   malformed source pin stays unresolved until authoritative evidence identifies
   it; never repair a pin by truncation or guessing.
5. Prepare each needed sanitized logical patch. Bind review to exact candidate
   content and metadata, commit locally and verify the resulting commit.
6. Establish the public-clean provider-attestation authority and exact nine-path
   corrective RED contract. Recompute every digest in the new lineage.
7. Obtain fresh architecture, test, docs and release GO on the exact RED-assets
   commit before creating a measurement amendment.

For BLOCKED findings, inspect the private item identified by the neutral ordinal,
sanitize it and repeat the scan. For UNABLE_TO_CERTIFY input, resolve acquisition,
format or resource limits; absence of a finding is not a PASS. A changed index,
policy, message or identity invalidates earlier review. See the commands and
formats below. Gate commands do not create commits or advance refs. The explicitly
identified local commit helper later in this guide creates a commit and advances HEAD.

## Prepare a private policy

Use the repository's supported Python environment (`uv sync`) on a POSIX system
with `O_NOFOLLOW`, directory descriptors and signal timers. Keep the policy,
metadata, review and receipts outside every Git checkout. Their containing
directory must be owned by the current user with mode `0700`; input files use
`0600`. Symlinked ancestors and repository-local output are rejected.

The following JSON illustrates the closed policy schema using synthetic values.
It is not a real migration policy: replace it with the maintainer's private,
reviewed protected terms, public identities and exact host allowances. Never
check a real policy into Git or copy its contents into command output.

```json
{
  "schema": "dpone.public-clean-policy.v1",
  "protected_terms": ["ExampleSecretMarker"],
  "protected_ticket_prefixes": ["SYNTH"],
  "allowed_hosts": ["example.org"],
  "allowed_identities": ["reviewer@example.org"],
  "reviewers": ["reviewer@example.org"]
}
```

All fields are required. Lists reject duplicates and malformed values; protected
terms, allowed hosts, identities and reviewers cannot be empty. Protected terms
use NFKC, case folding and removal of Unicode punctuation, separators and format
characters. Ticket prefixes require a numeric ticket suffix. Host allowances are
exact, with no wildcard or blanket path exceptions. Generic rules also flag
credential shapes, private IPv4/IPv6, loopback/link-local addresses, unapproved
hosts/emails, local paths and concrete database/cloud/application identifiers.
Bare-host heuristics recognize documented DNS suffixes; URL and explicit DNS
contexts are also inspected. This cannot identify every unknown company name.

Overlapping host matchers report one finding at the same character offset in
the same input. Each distinct occurrence still requires its own review. An
allowlisted shorter match does not suppress a longer unknown URL host; duplicate
or ambiguous entries supplied in a review remain invalid.

Set `PRIVATE_DIR` to that private directory and `PUBLIC_ROOT` to the destination
checkout. The commands below run from the destination repository root. Static help is available with `--help` or
`candidate --help`, returning JSON without echoing supplied paths. Each
receipt filename must be new; an existing file is never overwritten.

## Discover and inspect inputs

```bash
uv run python tools/agent_policy/public_clean_gate.py history \
  --root "$PUBLIC_ROOT" --policy "$PRIVATE_DIR/policy.json" \
  --receipt "$PRIVATE_DIR/history.json"

uv run python tools/agent_policy/public_clean_gate.py worktree \
  --root "$PUBLIC_ROOT" --policy "$PRIVATE_DIR/policy.json" \
  --receipt "$PRIVATE_DIR/worktree.json"

uv run python tools/agent_policy/public_clean_gate.py source-delta \
  --root "$SOURCE_ROOT" --commit-sha "$SOURCE_COMMIT" \
  --policy "$PRIVATE_DIR/policy.json" --receipt "$PRIVATE_DIR/source-delta.json"
```

The source root and commit variables stay local. Git acquisition disables
optional locks, replacement objects, lazy promisor fetching and all transports.
A missing source object fails closed; the operator must resolve source
availability independently, without letting the scan fetch or repair it.

History covers frozen reachable refs, detached HEAD, annotated-tag chains, raw
metadata, paths and blobs. Reflogs, unreachable commits and other worktree heads
remain inventory-ledger obligations. Source-delta covers every parent, complete
before/after changed blobs and full binary patches. A root commit compares with
the empty tree. Worktree covers the full index, raw index identity, ignored files,
and working bytes; only actual Git administration is excluded. A second inventory
checks for changes. Generated environments and build outputs can therefore block
worktree scanning even when they are not staged; record and resolve their scope
explicitly, never silently treat that scan as passed.

## Review an exact candidate

Prepare `metadata.json` privately with these exact fields:

| Field | Value |
|---|---|
| `message` | Exact UTF-8 commit message, including its final newline |
| `author_name`, `committer_name` | Reviewed public display names without controls or angle brackets |
| `author_email`, `committer_email` | Exact identities allowed by the policy |
| `author_date`, `committer_date` | Raw Git timestamp and timezone, such as `1700000000 +0000` |
| `parent` | Full current destination HEAD identity |

Stage only reviewed, sanitized paths. Then scan the complete index tree:

```bash
uv run python tools/agent_policy/public_clean_gate.py candidate \
  --root "$PUBLIC_ROOT" --policy "$PRIVATE_DIR/policy.json" \
  --metadata "$PRIVATE_DIR/metadata.json" --receipt "$PRIVATE_DIR/candidate.json"
```

A lexical scan with no findings returns exit `2`, `REVIEW_REQUIRED`. Its private
receipt contains the exact tree, parent, index identity, metadata digest, dates,
policy digest, scanner identity, counts and findings. Raw proposed messages and
identities are not copied into receipts. Public stdout contains only status,
counts and neutral finding ordinals/codes; policy hashes and private locators
remain private. Do not publish the complete receipt.

The reviewer reads every proposed content and metadata input, including unknown
proper names. **Only after that review**, this command writes their private
approval record with the exact nested binding object. Set `PUBLIC_REVIEWER` to
an identity permitted by the private policy; the command prints no input values.

```bash
uv run python - <<'PYCODE'
try:
    import os
    from pathlib import Path
    from tools.agent_policy.public_clean_receipts import read_json, write_receipt

    private = Path(os.environ["PRIVATE_DIR"])
    root = Path(os.environ["PUBLIC_ROOT"])
    candidate = read_json(private / "candidate.json", root)
    review = {
        "schema": "dpone.public-clean-review.v1",
        "binding": {key: candidate["binding"][key]
                    for key in ("tree", "parent", "metadata_digest")},
        "policy_digest": candidate["policy_digest"],
        "scanner_identity": candidate["scanner_identity"],
        "reviewer": os.environ["PUBLIC_REVIEWER"],
        "approved": True,
        "exceptions": [],
    }
    write_receipt(private / "review.json", root, review)
except Exception:
    raise SystemExit("Private migration helper failed; inspect inputs locally.") from None
PYCODE
```

For one individually reviewed generic finding, set `REVIEW_ITEM` to its neutral
ordinal and insert the following inside the `try` block before `write_receipt`
in the program above, preserving the block indentation:

```python
finding = next(item for item in candidate["findings"]
               if item["item"] == int(os.environ["REVIEW_ITEM"]))
exception = {key: finding[key]
             for key in ("label", "content_digest", "code", "offset")}
exception.update(justification="Reviewed synthetic documentation example.",
                 synthetic=True)
review["exceptions"].append(exception)
```

Use the actual justification for the selected occurrence before writing the
review. Do not loop over findings to grant blanket approval. An exception covers
one occurrence, not every occurrence of a rule or path. Protected terms, protected
ticket keys and compiled artifacts cannot be waived. Credential-shaped false
positives require explicit synthetic classification; actual credentials must be
removed. The record is not cryptographic protection against a malicious local
operator.

```bash
uv run python tools/agent_policy/public_clean_gate.py candidate \
  --root "$PUBLIC_ROOT" --policy "$PRIVATE_DIR/policy.json" \
  --metadata "$PRIVATE_DIR/metadata.json" --review "$PRIVATE_DIR/review.json" \
  --receipt "$PRIVATE_DIR/reviewed-candidate.json"
```

Only the unchanged, completely reviewed candidate can return `PASS`. Ordinary
`git commit` may change dates, clean up the message or add signing headers. The
following local procedure preserves the reviewed bytes, verifies the new commit
object, and only then advances HEAD with a compare-and-swap. It never pushes.
If verification fails, HEAD remains at the reviewed parent; retain the local
receipt and object for diagnosis, revalidate inputs and retry with new filenames.

```bash
uv run python - <<'PYCODE'
try:
    import os
    import subprocess
    import sys
    from pathlib import Path
    from tools.agent_policy.public_clean_candidate import parse_metadata
    from tools.agent_policy.public_clean_receipts import digest, read_json

    root = Path(os.environ["PUBLIC_ROOT"])
    private = Path(os.environ["PRIVATE_DIR"])
    metadata = parse_metadata(read_json(private / "metadata.json", root))
    preflight = subprocess.run([
        sys.executable, "tools/agent_policy/public_clean_gate.py", "candidate",
        "--root", str(root), "--policy", str(private / "policy.json"),
        "--metadata", str(private / "metadata.json"), "--review", str(private / "review.json"),
        "--receipt", str(private / "commit-preflight.json")], cwd=root, timeout=910)
    if preflight.returncode:
        raise SystemExit("Candidate revalidation failed; no commit created.")
    candidate = read_json(private / "commit-preflight.json", root)
    binding = candidate["binding"]
    env = {key: value for key, value in os.environ.items()
           if not key.startswith("GIT_")}
    for role in ("author", "committer"):
        for field in ("name", "email", "date"):
            env[f"GIT_{role}_{field}".upper()] = metadata[f"{role}_{field}"]
    env.update(GIT_OPTIONAL_LOCKS="0", GIT_NO_REPLACE_OBJECTS="1",
               GIT_NO_LAZY_FETCH="1", GIT_ALLOW_PROTOCOL="")

    def git(*arguments, data=None):
        return subprocess.run(
            ["git", "-c", "commit.gpgsign=false", *arguments], cwd=root,
            env=env, input=data, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, check=True, timeout=30).stdout.decode().strip()

    if (candidate["status"] != "PASS" or not candidate["complete"]
            or digest(metadata) != binding["metadata_digest"]
            or git("rev-parse", "HEAD") != binding["parent"]
            or git("write-tree") != binding["tree"]):
        raise SystemExit("Revalidate the exact candidate before committing.")
    commit = git("commit-tree", binding["tree"], "-p", binding["parent"],
                 data=metadata["message"].encode("utf-8"))
    result = subprocess.run([
        sys.executable, "tools/agent_policy/public_clean_gate.py", "commit",
        "--root", str(root), "--commit-sha", commit,
        "--policy", str(private / "policy.json"),
        "--candidate-receipt", str(private / "commit-preflight.json"),
        "--receipt", str(private / "verified-commit.json")], cwd=root, timeout=910)
    if result.returncode:
        raise SystemExit("Verification failed; HEAD was not advanced.")
    git("update-ref", "HEAD", commit, binding["parent"])
except Exception:
    raise SystemExit("Private migration helper failed; inspect inputs locally.") from None
PYCODE
```

Commit verification re-scans tree and metadata, compares their exact bindings,
findings and counts, and applies the original review. Unsupported signatures,
encoding headers, extra headers and merge commits cannot use this single-parent
candidate format. Discovery receipts cannot be substituted for candidate PASS.

## Results, limits and recovery

| Result | Exit | Operator action |
|---|---:|---|
| `SCAN_COMPLETE` | 0 | Discovery acquired its full scope; this is not commit authorization |
| `PASS` | 0 | Exact candidate review or actual commit verification succeeded |
| `REVIEW_REQUIRED` | 2 | Review all content and metadata, then supply the matching record |
| `BLOCKED` | 2 | Resolve every finding privately and scan a new candidate |
| `UNABLE_TO_CERTIFY` | 3 | Resolve unsupported, unreadable, changed or oversized input; no success receipt |

Serialized credential-shaped locators or review prose cause `RECEIPT_SENSITIVE`
(exit 3) and suppress receipt publication, even for synthetic source findings.
Inspect original inputs locally, remove credential-shaped locators/prose and rerun
with fresh filenames. Never echo matched text; this failure has no private receipt
from which to retrieve a finding ordinal.

The gate accepts strict UTF-8 text without NUL bytes and conservative ZIP, gzip
and USTAR containers. It scans raw container bytes, names, headers, comments and
members without extraction or execution. ZIP64, data descriptors, unknown ZIP
extras, encrypted entries, GNU/PAX tar extensions, links, unsafe paths, nested
archives and appended data are unsupported. A gzip wrapper around USTAR is
supported; concatenated gzip streams are rejected. Compiled Python inputs have
raw/printable strings inspected but always block migration.

Existing hygiene byte/member/ratio budgets remain authoritative. Additional
limits include 100,000 findings per complete scan and exceptions per review,
10,000 entries per policy list and protected-ticket positions per input,
100,000 history refs/commits/tags and working entries, one MiB policy/metadata,
and 64 MiB per review/receipt input and serialized receipt output.
Tree scans reuse a bounded `git cat-file --batch` process. Each object must match
its requested identity, type, size and content hash before inspection. Clean
process shutdown is required before a receipt can be issued; malformed or
truncated responses fail acquisition without retrying the damaged stream.

Git operations have a 30-second deadline; the complete CLI, including bootstrap,
has a 15-minute deadline. Partial results never become PASS. Inputs within these limits use one complete
execution; larger input fails closed without a capacity override. A review that fits
the input limit can still produce an oversized receipt and fail publication.
Run complete candidate, reachable-history and worktree scans separately: evidence
for one mode does not clear another. Scanner upgrades invalidate old candidate
receipts and reviews; preserve historical records and rerun with fresh filenames
and matching review. Increased capacity clears no findings automatically.

Receipt publication uses a private pinned directory and an exclusive atomic link.
Data is synchronized before publication. Before that point, a failure cannot
publish a success receipt. After publication, a process crash may leave a complete
receipt even when no stdout response reached the caller. Directory durability and
temporary cleanup after publication are best-effort; recovery inspects the exact
existing receipt instead of overwriting it. Never infer commit success from the
presence of a temporary file.

## Local regression checks in Docker

Build a disposable synthetic test runtime from an empty build context. The image
contains Python, Git and pytest; it contains no repository or private policy files.

```bash
docker build -t dpone-public-clean-tests:local - <<'DOCKERFILE'
FROM python:3.12.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pytest==9.0.3
ENV PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=/workspace
WORKDIR /workspace
DOCKERFILE
docker run --rm --network none --read-only --memory 2g \
  --tmpfs /tmp:rw,exec,size=1g \
  --mount "type=bind,source=$PWD,target=/workspace,readonly" \
  dpone-public-clean-tests:local python -m pytest --noconftest -c /dev/null \
  -p no:cacheprovider tests/test_public_clean_gate.py \
  tests/test_public_clean_gate_failures.py tests/test_public_clean_capacity.py -q
```

The Docker memory limit is a Linux container constraint; this run does not replace
the separate macOS arm64/Python 3.12.11 measured RSS and elapsed-time acceptance.

These local tests cover synthetic Git, review identity, archive/failure handling,
capacity boundaries and replay. They neither certify a live database route nor
replace complete privacy scans or the repository-wide non-live regression suite.
Retain image identity, Python/Git/pytest versions, exit status and test counts in
the private validation evidence. A failed or unavailable container run is not PASS.

## Route and user-journey impact

The [PostgreSQL to MSSQL route](source-sink/postgres-to-mssql.md) is unchanged.
Runtime behavior, CLI, manifest, compatibility, state and checkpoint contracts
are unchanged by this preparation. The maintainer journey now distinguishes
inventory, privacy review, historical lineage, public authority, RED diagnostics
and retained measurement evidence. Live/vendor checks remain UNVERIFIED and
activation remains blocked. Push, PR and release require separate authorization.

## Reviewing non-credential syntax

Use `NON_CREDENTIAL_SYNTAX` only for a precise credential-shaped span that a
fixed validator proves to be a permission declaration, a value-free Python type
annotation, or a JSON Schema field-name declaration. Keep `synthetic: false`:
real program metadata is not synthetic. Supply the paired `context_evidence`
object produced for the exact source and scanner identity; a flag or a prose
justification alone is insufficient.

The reviewer still approves each exact occurrence and all candidate content.
Actual values, defaults, runtime mappings, string imitations, ambiguous parser
contexts and protected terms cannot use this classification. Unsupported roles
remain unresolved. Verification reads the recorded Git tree even if a working
file has changed. Changing source, context, checker or parser invalidates the
proof. Re-run the complete candidate scan after upgrading this gate; do not edit
old receipts or assume old scanner identities remain accepted.

The initial fixed validators intentionally support a narrow subset:

- Plain workflow permission enums in the root or job `permissions` mapping of a
  structurally parsed `.github/workflows/*.yml` or `.yaml` file; aliases,
  duplicate keys and interpolation are rejected.
- Direct unshadowed builtin Python type annotations whose complete matched span
  contains no default, assigned value or expression. Unsupported annotations
  remain unresolved rather than being evaluated.
- Field-name declarations in formally metavalidated JSON Schema 2020-12,
  traversed only through that dialect's schema-valued keywords. Other dialects
  and nested dialect switches are unsupported. Formal schema validation proves
  this declaration's role; it does not certify application use of the schema.

The source cap is 256 KiB with bounded parser nodes/events and nesting. Evidence
includes the actual metavalidator's packaged vocabulary resources as well as
parser implementation files. No external reference is fetched to classify a
finding. Derived permission reports and archive member contexts need separate
consumer proof and are not admitted by these initial validators.
