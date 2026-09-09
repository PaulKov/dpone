# DPONE_RECIPE_OVERRIDE_FORBIDDEN

The pipeline attempted to override an undeclared, non-allowlisted, or
profile-locked parameter.

## Fix

Run `dpone recipe show <exact-recipe-ref>` and remove the value from the answers
file unless it is listed as overridable. Locked operational policy can only
change through a reviewed new profile/recipe version.
