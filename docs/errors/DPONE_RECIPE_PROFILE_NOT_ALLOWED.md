# DPONE_RECIPE_PROFILE_NOT_ALLOWED

The selected exact profile ref is not among the profile pins owned by the
selected recipe.

## Fix

```bash
dpone recipe show <exact-recipe-ref>
```

Choose one listed profile. A pipeline cannot add an unreviewed profile pin to a
platform-owned recipe.
