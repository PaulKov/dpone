# Bounded versioned S3 artifacts

Connector and platform authors can use `VersionedS3ArtifactStore` to create and
read immutable artifacts with an explicit byte budget and absolute deadline.
The adapter shares conditional writes, exact-version metadata checks, KMS,
retention and version-history verification with the existing semantic-refresh
S3 adapter. It does not select credentials or authenticate a deployment policy.

The [native original-store adapter](native-original-store.md) implements subject-bound
object publication. Authenticated SQL bindings and application composition still
require implementation and qualification. This
primitive does not by itself complete the [native publication journey](native-original-publication.md).
Unit fault fixtures are not live S3 certification.

## Construct and bound an operation

The application supplies an already configured `S3VersionedObjectClient`, protected
`S3VersionedArtifactPolicy`, a timezone-aware wall clock and a monotonic clock.
The existing canonical `S3ArtifactStorePolicy` and native storage authority expose
the structural policy properties; no semantic-refresh operation needs to be
fabricated for native storage.

```python
from dpone.adapters.versioned_artifact_s3 import VersionedS3ArtifactStore
from dpone.ports.versioned_artifact_store import VersionedArtifactIoBudget

store = VersionedS3ArtifactStore(
    client=client,
    bucket=policy.bucket_or_container_authority_id,
    operation_prefix=policy.artifact_prefix,
    policy=policy,
    clock=wall_clock,
    monotonic_clock=monotonic_clock,
)
budget = VersionedArtifactIoBudget(
    max_bytes=1048576,
    chunk_bytes=65536,
    deadline_monotonic=monotonic_clock() + 30.0,
)
content = store.read_version(key=object_ref.key, version=object_ref.version, budget=budget)
```

These variables are supplied by authenticated application composition, not looked
up from environment defaults. `max_bytes` and `chunk_bytes` must be exact positive
integers, with `chunk_bytes <= max_bytes`. The deadline is a finite float in the
injected monotonic clock's domain. Pass the same budget through all operations
belonging to one attempt; creating a fresh deadline per request would extend the
approved allowance.

Configure finite SDK connection/read timeouts and retry limits separately. The
adapter checks its deadline before and after SDK calls and body reads, but cannot
interrupt an already blocking SDK call or `close()`. These cooperative checks do
not prove a finite wall-clock limit for an arbitrary injected client.

## Proof before success

`create`, `head` and `read_version` require a budget on the bounded API.

- `create` checks the protected prefix, content digest, byte limit, retention,
  enabled bucket versioning and Object Lock before one conditional PUT. It requires
  the returned version, matching exact-version HEAD and unambiguous history.
- `head` returns the sole retained version or absence. It rejects an object beyond
  the operation byte limit and verifies its protected writer/retention coordinates.
- `read_version` requests the exact version. The GET response must contain that
  `VersionId` and a valid declared content length. Positive bounded reads consume
  the complete body and confirm EOF, including a one-byte probe at the exact cap.
  Excess bytes, invalid chunks, framing/read errors or size/digest disagreement
  deny success. The acquired body is closed even when identity validation fails.
  Exact-version HEAD and complete sole-version history then verify the retained
  provider identity and protected metadata.

No method retries a mutation, invents a version, deletes uncertain objects or
converts a lost acknowledgement into success. `ArtifactCreateConflict` identifies
a conditional-create conflict; `ArtifactStoreUnavailable` means the required
proof was unavailable or failed, including deadline, body and metadata failures.
Caller input/budget validation errors occur before provider calls. A failed write
acknowledgement may leave a retained object; reconcile the same immutable identity
through an authorized higher-level workflow before deciding what to do next.

A body-close exception also denies success. During another failure, cleanup may
become the primary exception while retaining the original in exception context.
This is not permission to retry or release retained evidence.

## Legacy compatibility

`S3CreateOnlyArtifactStore` keeps its historical import path, constructor and method
signatures and delegates to the shared operations. `retention_datetime` remains
importable from its old module. The old policy protocol name resolves to the same
shared protocol object and preserves its historical pickle identity.

The legacy read path still tolerates an omitted GET `VersionId` and uses its
historical unbudgeted body read. It rejects an explicitly different version and
closes the acquired body. These compatibility behaviors do not satisfy the new
bounded port; native composition must use `VersionedS3ArtifactStore` explicitly.

See [storage authority](native-original-storage.md) for policy metadata and
[testing](testing/index.md) for the distinction between contract tests, live
qualification and release evidence.
