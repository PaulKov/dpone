# Native original subjects

This reference is for connector and evidence authors who need to identify which
generation, delivery attempt or platform policy owns an original. It extends
[native contract primitives](native-contract-primitives.md) with closed typed
subjects and canonical codecs. It does not create or authenticate stored evidence.

## Local example

With dpone installed, the following synthetic example needs no database:

```python
from uuid import UUID

from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.contracts.native_originals import (
    NativeGenerationOriginalSubject,
    decode_native_original_subject,
    encode_native_original_subject,
)

digest = "sha256:" + "a" * 64
# Synthetic descriptor identities demonstrate the wire shape only.
authority = DbtWorkspaceRuntimeAuthority.build(
    environment="dev",
    release_id=digest,
    deployment_id=digest,
    release_sha256=digest,
    deployment_sha256=digest,
    binding_set_sha256=digest,
    connection_registry_sha256=digest,
    credential_runtime_sha256=digest,
)
subject = NativeGenerationOriginalSubject(
    authority=authority,
    generation_id=UUID("12345678-1234-5678-1234-567812345678"),
)
payload = encode_native_original_subject(subject)
assert decode_native_original_subject(payload) == subject
print(subject.scope)
```

The output is `GENERATION`. Encoding writes no file and resolves no credentials.
In a real consumer, the authority must come from the authenticated active workspace;
constructing a matching fingerprint from arbitrary inputs does not authorize them.

## Closed variants

Every wire document contains `schema: "dpone.native-original-subject.v1"`, its
fixed `scope`, and the complete existing workspace runtime `authority`.

| Python value | Scope | Additional required fields |
|---|---|---|
| `NativeGenerationOriginalSubject` | `GENERATION` | `generation_id`: UUID |
| `NativeDeliveryOriginalSubject` | `DELIVERY` | `operation_id`, `attempt_id`: canonical SHA-256 digests |
| `NativePlatformOriginalSubject` | `PLATFORM` | `platform_policy_sha256`: canonical SHA-256 digest |

`NativeOriginalSubject` is the union of those three immutable values. Direct
constructors take the authority and the variant's additional fields; schema and
scope are fixed class attributes, not caller-configurable constructor arguments.
Use the codec instead of generic dataclass serialization to include them on the wire.
Every field is required and nonnullable. Fields from another variant are rejected.

The nested authority contains exactly `environment`, `release_id`, `deployment_id`,
`release_sha256`, `deployment_sha256`, `binding_set_sha256`,
`connection_registry_sha256`, `credential_runtime_sha256`, and
`authority_subject_sha256`. All are strings. The native codec preserves the existing
`DbtWorkspaceRuntimeAuthority` class and verifies its descriptor fingerprint; it
neither recomputes a replacement for a mismatched input nor accepts extra fields.

## Canonical identity

`encode_native_original_subject(value)` emits bounded canonical UTF-8 JSON.
`decode_native_original_subject(payload)` accepts only exact canonical object bytes,
then validates the discriminator, complete field sets, domain types and authority
fingerprint. UUID strings must use lowercase hyphenated canonical form. Digests use
`sha256:` followed by 64 lowercase hexadecimal characters.

Duplicate decoded keys, whitespace differences, alternate Unicode escapes, extra
fields and malformed or over-budget documents are rejected. All primitive limits
from the [native JSON reference](native-contract-primitives.md#json-limits) apply.
Changing a generation ID, attempt ID, operation ID, policy hash or authority
coordinate changes the complete subject identity. Compare the whole canonical
subject, not one convenient field.

For `PLATFORM`, the caller must first authenticate the exact full selected canonical
v4 policy bytes and hash those bytes. A reconstructed subsection is not the same
policy identity. Never place a policy's own digest inside its hash preimage.
Platform subjects describe authorized snapshots; physical resource ownership and
capacity accounting must remain shared across snapshots. Retained originals keep
their original subject and cannot be relabeled under a newer deployment.

## Failure and recovery

Invalid subjects raise `NativeOriginalSubjectError`, a `ValueError`. Decode failure
has no external side effects and never advances state or permits publication.
Correct the producing contract or reject/quarantine its original according to the
owning domain's policy. Do not delete unknown fields, accept stale fingerprints,
coerce UUIDs or rewrite stored bytes to manufacture successful validation.

Canonical identity is only one boundary. Consumers must still resolve the exact
stored object/version, authenticate subject and kind, validate payload digests,
and check active ownership before acting. These codecs do not implement an original
store, source reservation, target claim or checkpoint transition.

## Compatibility and validation

Existing workspace authority fingerprints and older evidence readers are unchanged.
The new subject codec is explicit; it is not registered as a permissive fallback
for older document types. No analyst configuration or database route is enabled by
this contract slice. The broader [dbt user workflow](dbt.md) remains the entry point.

Run `uv run pytest tests/test_native_original_subjects.py` for variant, nested
fingerprint, canonicality, malformed input and immutability cases. These are
unit/contract checks. See the [testing overview](testing/overview.md) for the
separate requirements on actual delivery and replica qualification.
