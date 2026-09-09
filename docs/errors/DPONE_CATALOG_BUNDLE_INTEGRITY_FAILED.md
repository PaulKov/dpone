# DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED

The local catalog bundle does not match its content-addressed manifest, trusted
root pin, inventory, size, or completion contract. Verification stops before
cosign and promotion.

## Fix

Do not edit the immutable bundle. Delete only the corrupt local materialization,
fetch the exact bundle ID again, and rerun
`dpone supply-chain catalog-bundle-verify`. If source content intentionally
changed, build and externally sign a new bundle ID. See
[signed catalog bundles](../signed-catalog-bundles.md#recovery).
