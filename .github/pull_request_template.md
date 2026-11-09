## Summary

<!-- what this PR does, in one or two sentences -->

## Change entry

- [ ] `.changes/NNNN-slug.md` added (see `.changes/README.md`; public-safe content)
- [ ] Spec impact section filled - `adds:`/`modifies:`/`removes:` anchors resolve in `docs/specs/` (or `-`), `tools/check_spec_anchors.sh` clean
- [ ] ADR required: **yes/no** - required for `type: feat`, `breaking: true`, or cross-module refactor (`docs/adr/`); for fix/docs/chore the `.changes` file is the micro-ADR
- ADR link (if yes):

## Test plan

- [ ] `pytest` green
- [ ] `uvx ruff@0.17.3 check src/` + `format --check` clean
- [ ] `.devin/scripts/verify_constraints.sh` ALL PASS
- [ ] G/W/T tests added for behavior changes

Closes #
