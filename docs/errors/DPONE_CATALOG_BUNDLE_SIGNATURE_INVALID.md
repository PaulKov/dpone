# DPONE_CATALOG_BUNDLE_SIGNATURE_INVALID

The detached signature, certificate identity, OIDC issuer, publisher, kind, or
environment does not satisfy the local trust policy. The bundle is not promoted.

## Fix

Confirm that protected CI signed the exact `catalog-bundle.json` bytes and that
the reviewed trust policy names that workflow identity and issuer exactly. Do
not weaken the policy or replace immutable bundle bytes in place. Build a new
bundle when content changed. See
[signed catalog bundles](../signed-catalog-bundles.md#verify-before-promotion).
