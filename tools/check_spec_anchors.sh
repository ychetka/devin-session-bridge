#!/usr/bin/env bash
# tools/check_spec_anchors.sh - validate Spec impact anchors.
#
# Every .changes/<slug>.md must carry a "## Spec impact" section after
# its frontmatter, listing requirement anchors it touches:
#
#   ## Spec impact
#   - adds:     docs/specs/<domain>.md#<requirement-anchor>
#   - modifies: docs/specs/<domain>.md#<requirement-anchor>
#   - removes:  docs/specs/<domain>.md#<requirement-anchor>
#
# or a bare "-" / "- none" when the change touches no documented
# behavior. Post-change invariant: adds/modifies anchors MUST exist
# in the referenced spec file, removes anchors MUST NOT exist (the
# PR applies its own spec delta - the check reads the working tree,
# which already carries the delta).
#
# Anchors are kebab-case slugs of "### Requirement: <name>" headings:
# lowercase, non-alphanumeric runs collapse to single hyphens, edges
# stripped - the same rule GitHub applies to markdown headings.
#
# Usage: tools/check_spec_anchors.sh   (exit 0 = clean, 1 = violations)
set -euo pipefail

TOP=$(git rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
CHANGES="$TOP/.changes"

[ -d "$CHANGES" ] || { echo "no .changes/ directory - nothing to check"; exit 0; }

CHANGES_DIR="$CHANGES" TOP_DIR="$TOP" python3 - <<'PYEOF'
import os, re, sys

changes_dir = os.environ["CHANGES_DIR"]
top = os.environ["TOP_DIR"]

ANCHOR_OPS = ("adds", "modifies", "removes")
NOOP = {"-", "- none", "none", "- n/a", "n/a"}

def slugify(title):
    """GitHub-style heading anchor: lowercase, non-alnum -> '-'."""
    s = re.sub(r"[^a-z0-9]+", "-", title.lower())
    return s.strip("-")

def requirement_anchors(path):
    """All requirement anchors present in a spec file."""
    anchors = set()
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                m = re.match(r"^###\s+Requirement:\s*(.+?)\s*$", line)
                if m:
                    anchors.add(slugify(m.group(1)))
    except FileNotFoundError:
        return None
    return anchors

def parse_impact(path):
    """Extract Spec impact entries from a fragment, or None."""
    lines = open(path, encoding="utf-8").read().splitlines()
    # frontmatter occupies [0 .. second '---']; the section lives after
    end = next(
        (i for i, l in enumerate(lines[1:], 1) if l.strip() == "---"),
        None,
    ) if lines and lines[0].strip() == "---" else None
    body = lines[end + 1:] if end is not None else lines
    start = next(
        (i for i, l in enumerate(body) if re.match(r"^##\s+Spec impact\s*$", l)),
        None,
    )
    if start is None:
        return None
    entries = []
    for line in body[start + 1:]:
        if re.match(r"^##\s", line):
            break
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        m = re.match(r"^-\s*(\w+):\s*(\S+)\s*$", s)
        if m:
            entries.append((m.group(1).lower(), m.group(2)))
        elif s in NOOP:
            continue
        else:
            entries.append(("malformed", s))
    return entries

files = sorted(
    f for f in os.listdir(changes_dir)
    if f.endswith(".md") and not f.startswith("_") and f != "README.md"
)
if not files:
    print("no pending .changes entries - nothing to check")
    sys.exit(0)

problems = []
anchor_cache = {}
for name in files:
    entries = parse_impact(os.path.join(changes_dir, name))
    if entries is None:
        problems.append(f"{name}: missing '## Spec impact' section")
        continue
    for op, ref in entries:
        if op == "malformed":
            problems.append(f"{name}: unparsable Spec impact line: {ref!r}")
            continue
        if op not in ANCHOR_OPS:
            problems.append(f"{name}: unknown Spec impact op '{op}' (use adds/modifies/removes or '-')")
            continue
        spec_rel, _, anchor = ref.partition("#")
        if not anchor or not spec_rel.endswith(".md") or "/" not in spec_rel:
            problems.append(f"{name}: bad anchor ref '{ref}' - expected docs/specs/<domain>.md#<anchor>")
            continue
        if not spec_rel.startswith("docs/specs/"):
            problems.append(f"{name}: Spec impact must target docs/specs/, got '{spec_rel}'")
            continue
        spec_abs = os.path.join(top, spec_rel)
        if spec_rel not in anchor_cache:
            anchor_cache[spec_rel] = requirement_anchors(spec_abs)
        anchors = anchor_cache[spec_rel]
        if anchors is None:
            problems.append(f"{name}: '{spec_rel}' does not exist (needed by {op}: {anchor})")
            continue
        if op in ("adds", "modifies") and anchor not in anchors:
            problems.append(
                f"{name}: {op} anchor '#{anchor}' not found in {spec_rel}"
            )
        if op == "removes" and anchor in anchors:
            problems.append(
                f"{name}: removes anchor '#{anchor}' still present in {spec_rel}"
            )

for p in problems:
    print(f"FAIL: {p}", file=sys.stderr)
if problems:
    sys.exit(1)
print(f"spec anchors OK ({len(files)} entries)")
PYEOF
