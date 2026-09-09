# DPONE_RECIPE_PARAMETER_REQUIRED

A required recipe parameter has no recipe default, selected-profile value, or
allowed pipeline answer.

## Fix

Inspect the safe parameter list:

```bash
dpone recipe show <exact-recipe-ref>
```

Add the missing non-secret scalar to a project-confined answers file or ask the
platform owner to publish an appropriate profile.
