# Publish and verify a native original

For connector authors and platform engineers, `publish_bound_native_original`
implements the ordered publication boundary using injected original-store and
binding capabilities. It returns a reference only after independent binding
resolution and exact-version byte comparison. It does not construct providers,
select credentials or grant source/target execution authority.

The [bounded S3 primitive](versioned-artifact-store.md) is available. Native
[subject-to-object adaptation](native-original-store.md) is implemented. [SQL bindings](native-originals-mssql.md) implement explicit protected installation
and independent transactional readback. Automated authenticated application
composition and full-route qualification remain pending. Existing unbounded legacy readers cannot simply be
wrapped to satisfy the new bounded reader port. The current application tests exercise
injected fault fixtures and are not live provider certification.

See [bindings](native-original-bindings.md), [storage policy](native-original-storage.md)
and [ADR 0065](adr/0065-trusted-isolated-native-generation-execution.md) for the
underlying identity and recovery contracts.

## Required capabilities

`ports.native_originals` defines three separate capabilities:

- `NativeOriginalWriterPort.publish` returns the actual provider `ArtifactObjectRef`
  after its own exact-version verification or reports uncertainty.
- `NativeOriginalBindingPort.bind` creates an immutable complete tuple, or proves
  its exact existing value. `resolve` independently authenticates that binding.
- `NativeOriginalReaderPort.read` verifies expected kind/subject and the exact
  provider version while enforcing `max_bytes` during reading. Concrete providers
  also require finite connection, read and retry settings.

Application composition binds those capabilities and the authenticated subject and
storage-policy reference. `dpone.ports.native_originals.BoundNativeOriginalPublisher` describes the resulting
callable with explicit `kind`, `locator`, `payload` and `max_bytes`. Consumer-local
wrappers may later bind those coordinates; this service does not choose them.

## Operation order

1. Validate exact types, positive byte limit, canonical bounded JSON, subject,
   storage reference and logical locator before any provider call.
2. Publish once through the injected writer and retain its actual object version.
3. Construct and snapshot the entire binding, including both hashes, encryption
   and retention coordinates. No version is fabricated.
4. Bind once and verify that its returned locator/digest equal the request.
5. Independently resolve and compare the complete canonical binding snapshot.
6. Read the verified exact version with the byte limit and compare all payload
   bytes. Return the requested `OriginalRef` only after this comparison succeeds.

The service snapshots request/binding identity before crossing capability boundaries
so an accidental mutation of a passed object cannot change the expected proof.

```python
from dpone.services.native_original_publication import publish_bound_native_original

reference = publish_bound_native_original(
    writer=writer,
    reader=reader,
    bindings=bindings,
    subject=subject,
    kind="generation_stored_file_v1",
    storage_authority=storage_authority,
    locator="generation/control-original",
    payload=canonical_payload,
    max_bytes=1048576,
)
```

The variables above are already authenticated, configured capabilities and values
from application composition; this is an integration example, not a standalone
credential/setup tutorial. The function does not write a local output file.

## Failure and recovery

Preflight errors leave providers untouched. A binding/reference or readback mismatch
raises `NativeOriginalPublicationError`; underlying contract and provider exceptions
retain their failure semantics. No error is converted into a success response.

The helper performs no automatic write retry, rollback or orphan deletion. Concrete
writers and binders must independently reconcile a lost acknowledgement against the
same immutable identity before returning; if they cannot, they report uncertainty.
The helper cannot manufacture a missing object reference or grant another dispatch.
Retained bytes and bindings remain available for authorized investigation.

An explicit replay can succeed only when the concrete providers prove the same
immutable object and binding. Conflicting content cannot overwrite an existing
locator. Tests in `tests/test_native_original_publication.py` cover call order,
coordinate substitution, exact replay, ambiguous failures and bounded readback.
The [testing guide](testing/index.md) distinguishes these unit checks from live
route qualification and release evidence.

## Application ownership

The application service owns publication, binding and independent readback.
Composition binds its authenticated providers and subject into the callback
contract in `dpone.ports.native_originals`; runtime consumers receive that callback
through injection. This keeps application orchestration out of the execution
runtime and prevents runtime imports of application services. The callable
signature and persistent original representations are unchanged.
