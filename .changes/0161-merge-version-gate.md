# repo_merge.sh: fail closed on unbumped shipping merges

## Changes
- `.devin/scripts/repo_merge.sh`: added a pre-merge version gate - a PR whose diff touches shipping paths (`src/`, `pyproject.toml`, `uv.lock`) without bumping `[project].version` is blocked BEFORE `gh pr merge` with `failure_class=version_not_bumped` (developer ruling 2026-11-12, issue #161). Exemptions: non-shipping-only diffs and PRs labeled `no-release`. The post-merge release path is unchanged for the bumped case; a `no_version_change` result there can now only come from an exempted merge.
- Added `labels` to the `gh pr view` field set; `VER_AT` helper moved above the merge call for reuse; `gh pr diff` failure fails closed as `upstream_error`; PR head version read from a dedicated `refs/merge-gate/pr-N` ref instead of FETCH_HEAD.

## Spec impact
No spec anchors affected (tooling policy change).
