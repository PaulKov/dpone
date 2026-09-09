# DPONE_RECIPE_EXPANSION_LIMIT_EXCEEDED

The pinned closure or expanded process graph exceeds a hard safety budget: 8
MiB total, 32 components, 100 processes, depth 32, or 10,000 nodes.

## Fix

Split the design into independently runnable workloads/recipes. Do not raise
global limits to accommodate an accidental monolith. Republish bounded
artifacts and rerun `dpone recipe validate`.
