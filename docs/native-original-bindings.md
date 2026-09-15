# Native original bindings

For connector authors and platform engineers, `NativeOriginalBinding` records the
complete identity of one native original and its stored object version. It is a
pure value and canonical codec. It does not read an object, authenticate a policy,
reserve capacity or permit source/target execution.

See the [subject reference](native-original-subjects.md) for workspace identity and
[ADR 0065](adr/0065-trusted-isolated-native-generation-execution.md) for the execution
boundary. The complete native route remains under implementation.

## Complete binding identity

All six fields are required and nonnullable. Equality includes every nested field.

| Field | Meaning |
| --- | --- |
| `subject` | Complete GENERATION, DELIVERY or PLATFORM subject, including workspace authority |
| `kind` | Closed original kind listed below |
| `storage_authority` | Existing `OriginalRef` identifying the separately authenticated store policy |
| `object_ref` | Existing six-field `ArtifactObjectRef`, including exact provider version |
| `payload_sha256` | Canonical SHA-256 of the payload representation selected by its producer |
| `locator` | Canonical relative logical locator, using `OriginalRef` validation |

The first binding slice admits `generation_storage_root_v1`,
`generation_stored_file_v1` and `generation_seal_resolution_v1`. Other producer
kinds are added with their contracts; unknown strings fail. There is no generic
kind-to-subject inference: a registered storage root can belong to its separate
PLATFORM subject. The authenticating consumer checks its required kind and subject.

The provider reference preserves `key`, `version`, `size_bytes`, `sha256`,
`encryption_scope` and `retention_until`. Keys are nonempty bounded strings without
`..` path segments; they are not normalized as filesystem locators. Protected
prefix and endpoint ownership are checked by the store consumer. Versions are
nonempty opaque strings, retained exactly, with no fabricated or latest fallback.
Zero-byte objects are representable; a producer can separately require nonempty
content. Size rejects booleans, negative integers and native JSON integer overflow.
Retention timestamps must be timezone-aware ISO values and retain their original
spelling and offset. Expiration and retention-policy admission require runtime
context and are not decided by this timeless value.

Payload and object hashes are distinct coordinates. The generic binding does not
assume their representations are identical. Publication/readback must check the
actual bytes and complete expected tuple; matching one hash or locator is not
sufficient authority.

## Encode and decode

Given a validated `subject` and actual provider `object_ref`:

```python
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import (
    NativeOriginalBinding,
    decode_native_original_binding,
    encode_native_original_binding,
)

binding = NativeOriginalBinding(
    subject=subject,
    kind="generation_stored_file_v1",
    storage_authority=OriginalRef(store_policy_locator, store_policy_sha256),
    object_ref=object_ref,
    payload_sha256=payload_sha256,
    locator="generation/export",
)
encoded = encode_native_original_binding(binding)
assert decode_native_original_binding(encoded) == binding
```

The supplied variables are values from the authenticated producer/store workflow,
not credentials. The codec returns bytes and performs no file write. Wire schema
`dpone.native-original-binding.v1` includes the six fields and a required `schema`
tag. Nested references use their exact field sets. Duplicate keys, missing or extra
fields, nulls, noncanonical bytes and invalid nested values raise
`NativeOriginalBindingError` without coercion. Encoding revalidates frozen values.
The [native JSON limits](native-contract-primitives.md) apply to the whole document.

On failure, correct the malformed input at its producer. Do not substitute a new
provider version, normalize object identity, or infer permission to retry a write.
Successful decoding establishes syntax only; consumers still authenticate originals
and apply the recorded publication/recovery policy.

Existing `ArtifactObjectRef` construction and existing subject wire formats remain
unchanged. These restrictions apply at the additive native binding boundary.
Contract tests are in `tests/test_native_original_bindings.py`; provider/live route
qualification is a separate requirement described in the
[testing guide](testing/index.md).

The [storage policy reference](native-original-storage.md) describes the closed
policy metadata to which `storage_authority` refers.
