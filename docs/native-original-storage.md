# Native original storage policy

For platform engineers and connector authors, `NativeOriginalStorageAuthority`
describes the initial native S3 storage policy. It is a closed, immutable contract
in `dpone.contracts.native_originals`. Its name does not mean that parsing it grants
access: enrollment, original authentication, credentials and live capability checks
remain separate requirements.

A [binding](native-original-bindings.md) references the authenticated policy original
through `storage_authority: OriginalRef`; it does not embed or recreate that authority.
See [ADR 0065](adr/0065-trusted-isolated-native-generation-execution.md) for the
execution boundary and [native JSON primitives](native-contract-primitives.md) for
encoding limits.

## Required policy fields

Wire schema `dpone.native-original-storage-authority.v1` requires `schema` and all
18 fields below, with no nullable or extra fields.

| Fields | Structural rule |
| --- | --- |
| `provider` | Exactly `s3` |
| `provider_profile` | Existing `s3_create_only_versioned_kms_object_lock_v1` or `MINIO_LOCAL_UNVERIFIED` |
| `endpoint_authority_id`, `bucket_or_container_authority_id`, `writer_scope`, `retention_policy_id` | Exact nonempty strings, preserving spelling |
| `kms_key_authority_id` | Existing KMS key ARN rule for the selected profile |
| `capability_evidence_sha256`, `encryption_policy_sha256`, `retention_policy_sha256` | Canonical lowercase SHA-256 digests |
| `artifact_prefix` | Nonempty, no leading/trailing slash or parent traversal segments; existing S3 policy rule |
| `retention_days`, `max_artifact_bytes` | Exact positive integers; booleans rejected; native JSON bounds apply |
| `retention_issued_at`, `retention_until` | Existing UTC `Z` timestamp grammar; interval at least `retention_days`, original spelling preserved |
| `conditional_create_authorized`, `require_object_lock` | Exact boolean `true` |
| `object_lock_mode` | Exactly `COMPLIANCE` |

`MINIO_LOCAL_UNVERIFIED` remains explicitly unverified. Accepting that description
does not turn local observations into production qualification. The initial native
profile rejects false capability flags and GOVERNANCE mode, even though the legacy
S3 policy can represent those descriptions for other callers.

There is no current-time expiration check in this timeless DTO. Consumers compare
current time and protected enrollment before use. Invalid or overflowing date
intervals fail as `NativeOriginalStorageAuthorityError`.

## Canonical interchange

`encode_native_original_storage_authority(value)` returns bytes;
`decode_native_original_storage_authority(payload)` restores the exact type.
Neither function reads files, performs network requests or selects credentials.
They reject unknown/missing fields, nulls, duplicate keys, noncanonical JSON,
unsupported policy fields and invalid frozen values before returning a result.

To correct a validation error, fix the policy at its protected authoring source and
revalidate it. Do not alter retained policy bytes, relabel another deployment's
original or treat successful parsing as permission to retry a write. Actual store
resolution still verifies the full protected tuple and provider capabilities.

## Shared validation and compatibility

The existing `S3ArtifactStorePolicy` validation lives in
`dpone.contracts.s3_artifact_store_policy`, a module without provider imports.
The existing resolver module reexports the same class, keeping its constructor,
defaults, validation behavior and historical pickle module identifier. Native
validation delegates to that policy after applying native exact-type and capability
rules; it does not inherit permissive legacy defaults or duplicate ARN/retention
validation. Existing semantic-refresh behavior is unchanged.

Tests in `tests/test_native_original_storage_authority.py` cover the native schema
and legacy import/pickle compatibility. Existing S3 tests and architecture checks
remain required. Complete provider qualification and the installed native journey
are separate, unfinished work; see the [testing guide](testing/index.md).
