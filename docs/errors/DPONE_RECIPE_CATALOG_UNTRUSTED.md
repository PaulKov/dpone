# DPONE_RECIPE_CATALOG_UNTRUSTED

The catalog's declared id is absent from the project's
`trusted_catalog_ids`. dpone treats this as a safety violation and exits `4`.

## Fix

Do not bypass the check. Ask the platform owner to verify the catalog ownership
and content, then add the exact id to the project allowlist through normal code
review. Recipe or credential data is not loaded from an untrusted catalog.
