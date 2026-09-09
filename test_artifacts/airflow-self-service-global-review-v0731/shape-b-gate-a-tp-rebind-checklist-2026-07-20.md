# Gate A checklist — Trusted Publisher rebind (2026-07-20)

**GO:** accepted as `Gate A` (Trusted Publisher rebind).
**Mode:** rehearsal — **no PyPI upload**, no draft publish, no policy cutover.
**UI rebind completed:** 2026-07-20 (operator session via PyPI Trusted Publishers UI).

## Target binding (all four projects)

| Field | Value |
|---|---|
| Owner | `PaulKov` |
| Repository | `dpone-release-controller` |
| Workflow | `release-controller.yml` |
| Environment | `pypi` |

## Project URLs

1. https://pypi.org/manage/project/dpone/settings/publishing/
2. https://pypi.org/manage/project/dpone-native-accel/settings/publishing/
3. https://pypi.org/manage/project/dpone-airflow-pack/settings/publishing/
4. https://pypi.org/manage/project/apache-airflow-providers-dpone/settings/publishing/

## Operator steps (per project)

1. Open Publishing settings (login required).
2. Under GitHub: **Add** publisher with the table above (do not upload).
3. Confirm the new publisher row is listed.
4. Only after controller row is present: remove/disable legacy
   `PaulKov` / `dpone` / `release.yml` if it still exists.
5. Screenshot or note: project name + publisher rows after change.

## Exit criteria

- [x] `dpone` has controller TP
- [x] `dpone-native-accel` has controller TP
- [x] `dpone-airflow-pack` has controller TP
- [x] `apache-airflow-providers-dpone` has controller TP
- [x] Legacy candidate-only binding removed or no longer sole publisher
- [x] No upload performed
- [x] Post-rebind controller `mode=live` observe run recorded below

## Evidence (fill after UI)

| Project | Controller TP present | Legacy removed/disabled | Notes |
|---|---|---|---|
| dpone | yes | yes (`PaulKov/dpone` + `release.yml` removed) | sole publisher = controller |
| dpone-native-accel | yes | yes (`PaulKov/dpone` + `release.yml` removed) | sole publisher = controller |
| dpone-airflow-pack | yes | n/a (no prior publishers) | sole publisher = controller |
| apache-airflow-providers-dpone | yes | n/a (no prior publishers) | sole publisher = controller |

Post-observe controller run: `29735049575` **PASS**
URL: https://github.com/PaulKov/dpone-release-controller/actions/runs/29735049575
(`mode=live`; UI rebind complete; observe jobs still report `rebind_attempted=false` by design — rebind was UI-side)

## Security follow-up (operator)

Chat-exposed PyPI password, API token, and recovery codes must be rotated after Gate A:
revoke token, change password, regenerate recovery codes, confirm authenticator still works.
