# Native original object storage

For connector and platform authors, `NativeOriginalStore` connects native subjects
and document kinds to the [bounded versioned artifact provider](versioned-artifact-store.md).
It implements the native writer and reader ports without constructing SDK clients,
selecting credentials or creating a SQL binding. It returns an actual provider
`ArtifactObjectRef` after independent metadata and exact-byte verification.

Authenticated SQL bindings and application composition still require implementation
and qualification. The adapter's tests include the real shared S3 implementation
with an injected synthetic client; they are not live provider or complete-route
certification.

## Supply authenticated inputs

```python
from dpone.adapters.native_original_store import NativeOriginalStore
from dpone.ports.versioned_artifact_store import VersionedArtifactIoBudget

originals = NativeOriginalStore(
    provider=bounded_provider,
    authority=storage_authority,
    authority_ref=storage_authority_ref,
    budget=VersionedArtifactIoBudget(
        max_bytes=1048576,
        chunk_bytes=65536,
        deadline_monotonic=monotonic_clock() + 30.0,
    ),
)
object_ref = originals.publish(
    subject=subject,
    kind="generation_stored_file_v1",
    payload=canonical_payload,
)
```

The variables come from authenticated application composition. The constructor
checks that the supplied authority reference identifies the exact canonical
[storage authority document](native-original-storage.md), then snapshots the
validated authority and budget. Digest agreement does not authenticate them.
The logical authority locator is separate from the physical object key.

One instance retains one attempt's absolute deadline. Constructing a fresh budget
for each request would incorrectly extend that allowance. The bounded provider
must enforce it and additionally use finite SDK connection/read/retry settings.
The native adapter itself does not own another clock or timeout mechanism.

## Persistent key and representation

Original bytes are stored directly, without another envelope. They must be a
canonical native JSON object within the primitive codec limits. The exact v1 key is:

```text
{artifact_prefix}/native-originals/v1/{subject_sha256_hex}/{kind}/{payload_sha256_hex}
```

Both digest segments contain 64 lowercase hexadecimal characters without the
`sha256:` prefix. The subject digest covers its complete canonical subject encoding;
the payload digest covers the exact stored bytes. Changing subject, kind or payload
changes the key. Reads recompute the whole expected key; a matching prefix alone is
insufficient. This grammar is persistent compatibility behavior: no implicit key
normalization, relocation or alternative spelling is accepted.

Supported kinds are `generation_storage_root_v1`, `generation_stored_file_v1` and
`generation_seal_resolution_v1`. Their identity does not grant execution authority.
The key must also fit the existing native provider-coordinate string limit; an
otherwise valid authority prefix can leave insufficient room for its derived key.
That condition fails before object I/O.

## Publish, verify and reconcile

Publication validates types, canonical bytes, limits, subject and kind before I/O.
It issues one conditional create with the authenticated writer scope and exact
retention timestamp. A normal acknowledgement must contain a well-formed actual
provider reference. The adapter independently reads metadata, compares all six
reference coordinates, reads the exact version and compares the complete bytes
before returning that reference.

A conditional-create conflict or unavailable write acknowledgement permits only
read-only reconciliation at the same deterministic key. A retained matching object
can be independently verified and returned. Absence, conflicting metadata,
unavailable readback or corrupt bytes remain failure; no second PUT, replacement
version, deletion or rollback is attempted. Retained orphan objects remain available
for authorized investigation. The adapter never fabricates an `OriginalRef` or
creates a control binding.

The [publication service](native-original-publication.md) uses this writer/reader
alongside a separately authenticated binding port. It independently resolves the
complete binding and reads back again before returning the logical original
reference. Neither layer grants source/target dispatch authority.

## Read an exact reference

```python
payload = originals.read(
    object_ref,
    expected_subject=subject,
    expected_kind="generation_stored_file_v1",
    max_bytes=1048576,
)
```

The adapter snapshots and validates the reference before provider calls. It rejects
an incorrect key, writer scope, retention or oversized reference. Independent HEAD
must match key, version, size, digest, encryption scope and retention exactly. The
exact-version read must produce matching complete bytes and canonical native JSON.
Read performs no mutation.

The effective byte allowance is the minimum of the reader allowance, stored attempt
budget, authenticated authority limit and native JSON limit. Chunk size is clamped
to that allowance while retaining the same absolute deadline. A larger read request
cannot expand the constructor budget.

Invalid caller values raise validation errors before provider I/O. Failed provider
proof raises `NativeOriginalStoreError`, a subclass of `ArtifactStoreUnavailable`;
underlying provider failures may propagate directly. An error is not permission to
retry a mutation or release retained state.

## Shared contract validation

`contracts.native_originals.native_original_object_payload` validates and copies the
six provider coordinates into primitive values without a binding or authentication
claim. `require_native_original_kind` validates the closed kind vocabulary. Binding
encoding and native adapters reuse these rules; callers need not manufacture a
binding or a fictitious authority just to validate an object reference.
