# `DPONE_ARTIFACT_ATTESTATION_REQUIRED`

This production blocker means dpone could not prove that the approved build
authority produced the exact Airflow deployment selected for cache activation
or runtime extraction.

## Safety behavior

- cache materialization stops before installing candidate release/deployment
  directories;
- activation does not replace `current`;
- KPO init-fetch stops before extracting executable bytes or starting source
  I/O;
- no fallback to checksum-only, unsigned, `optional`, local, or mutable
  artifacts occurs.

The last-known-good scheduler cache is preserved.

## Diagnose

1. Confirm the deployment requests `trust_tier=production`,
   `verify.attestations=required_for_prod`, and pins a v2 trust-policy SHA-256.
2. Confirm `/etc/dpone/artifact-trust/policy.json` and every allowlisted public
   key are mounted read-only.
3. Hash the public key and compare it to `trusted_public_keys[].sha256`.
4. Confirm the policy allows the exact environment, logical registry ref,
   endpoint-bound registry scope, source project, and protected ref.
5. Confirm the immutable package contains statement, Sigstore bundle, and
   `_SUCCESS` at the exact deployment prefix.
6. Run `cosign version --json`; the version must satisfy the policy range.
7. Compare signed subject IDs and root digests with the local exact projection.
8. Check `revoked_attestation_ids` and `revoked_public_key_ids`.

Do not print the private key, signing password, connection payload, or bundle
contents into logs.

## Recover

If the package is missing, repeat the protected CI sequence:

```text
exact publish -> prepare -> cosign sign-blob -> verify/publish attestation
```

Then retry desired-state reconcile. If the package is invalid, create a new
release/deployment from the protected source rather than overwriting immutable
objects.

For an urgent rollback, select a previously signed, non-revoked exact
deployment through desired-state CAS. Do not disable attestation enforcement.

See [Airflow artifact trust and attestation](../airflow-artifact-trust.md) for
the complete configuration, key rotation, and evidence contracts.
