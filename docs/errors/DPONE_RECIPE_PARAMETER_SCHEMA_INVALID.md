# DPONE_RECIPE_PARAMETER_SCHEMA_INVALID

The recipe parameter schema, profile values, or effective scalar value violates
the restricted declarative parameter contract.

## Fix

Use a closed draft-2020-12 object with supported scalar types and bounded
defaults/enums. Every profile, lock, and override name must be declared, and
profile values must match their declared type. Run `dpone recipe validate` after
the platform artifact is corrected.
