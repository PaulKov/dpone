# dpone pull-request review

Review the diff as a fresh-context owner. Read active `AGENTS.md` files and only
the standards relevant to changed paths. Inspect implementation, tests,
documentation, schemas, generated outputs, and artifacts; do not trust the PR
description as evidence.

Prioritize:

1. silent loss, duplication, corruption, false success, or false certification;
2. identity, ordering, checkpoint/state, evidence, retry, replay, and recovery;
3. public CLI/API/manifest/schema/artifact and compatibility regressions;
4. architecture/import/DI/capability boundary violations;
5. missing negative, boundary, compatibility, integration, or live proof;
6. misleading CLI UX, documentation, examples, runbooks, or CJM gaps;
7. secret leakage, unsafe workflow permissions, dependency, or supply-chain risk.

Lead with concrete findings ordered by severity. Include file/symbol, failure
scenario, user/operator impact, and a suggested validation or fix. Avoid
style-only comments unless they expose a real maintainability or correctness
risk. State explicitly when no blocking finding is present and list residual
uncertainty or checks you could not verify.
