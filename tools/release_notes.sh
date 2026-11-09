#!/usr/bin/env bash
# tools/release_notes.sh - aggregate .changes/*.md into release notes.
#
# Usage:
#   tools/release_notes.sh                      print both sections
#   tools/release_notes.sh --version X.Y.Z      print with a
#                                               "## [X.Y.Z] - <date>" header
#   tools/release_notes.sh --version X.Y.Z --apply
#                                               insert the section into
#                                               CHANGELOG.md (after the
#                                               title) and delete the
#                                               consumed .changes files
#
# Zero dependencies beyond POSIX bash + python3 (repo standard).
# .changes/ holds only unreleased entries by convention, so "since the
# last tag" is simply "all files present". See .changes/README.md.
set -euo pipefail

TOP=$(git rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
CHANGES="$TOP/.changes"
CHANGELOG="$TOP/CHANGELOG.md"

VERSION=""
APPLY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --version) VERSION="${2:?--version needs a version}"; shift 2 ;;
    --apply)   APPLY=1; shift ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
if [ "$APPLY" = 1 ] && [ -z "$VERSION" ]; then
  echo "--apply requires --version" >&2; exit 2
fi
[ -d "$CHANGES" ] || { echo "no .changes/ directory" >&2; exit 1; }

GENERATED=$(CHANGES_DIR="$CHANGES" VERSION="$VERSION" python3 - <<'PYEOF'
import os, re, sys, datetime

changes_dir = os.environ["CHANGES_DIR"]
version = os.environ.get("VERSION", "")

FIELDS = ("type", "scope", "user_visible", "breaking",
          "user_summary", "dev_summary", "why", "refs")
TYPES = ("feat", "fix", "refactor", "docs", "chore")

def parse(path):
    text = open(path, encoding="utf-8").read()
    m = re.match(r"^---\s*\n(.*?)\n?---\s*\n?", text, re.S)
    if not m:
        raise SystemExit(f"{path}: missing --- frontmatter")
    fm, last = {}, None
    for line in m.group(1).splitlines():
        s = line.strip()
        if s.startswith("#") or not s:
            continue
        km = re.match(r"^([A-Za-z_][\w]*)\s*:(.*)$", line)
        if km and not line[0].isspace():
            last = km.group(1)
            fm[last] = km.group(2).strip().strip('"').strip("'")
        elif last:
            fm[last] = (fm[last] + " " + s).strip()
    missing = [f for f in FIELDS if f not in fm]
    if missing:
        raise SystemExit(f"{path}: missing fields: {', '.join(missing)}")
    if fm["type"] not in TYPES:
        raise SystemExit(f"{path}: bad type {fm['type']!r}")
    for b in ("user_visible", "breaking"):
        if fm[b] not in ("true", "false"):
            raise SystemExit(f"{path}: {b} must be true|false")
    if fm["user_visible"] == "true" and fm["user_summary"] in ("", "-"):
        raise SystemExit(f"{path}: user_visible entry needs user_summary")
    fm["_file"] = os.path.basename(path)
    return fm

files = sorted(
    f for f in os.listdir(changes_dir)
    if f.endswith(".md") and not f.startswith("_") and f != "README.md"
)
entries = [parse(os.path.join(changes_dir, f)) for f in files]
if not entries:
    raise SystemExit(".changes/ has no pending entries")

out = []
if version:
    out.append(f"## [{version}] - {datetime.date.today().isoformat()}")
    out.append("")

# --- User-facing: only user_visible entries, grouped by type -------
visible = [e for e in entries if e["user_visible"] == "true"]
out.append("### User-facing")
out.append("")
if not visible:
    out.append("_No user-visible changes._")
    out.append("")
for t in TYPES:
    group = [e for e in visible if e["type"] == t]
    if not group:
        continue
    flag = " **(breaking)**" if any(e["breaking"] == "true" for e in group) else ""
    out.append(f"#### {t}{flag}")
    for e in group:
        refs = f" ({e['refs']})" if e["refs"] not in ("", "-") else ""
        brk = " **[breaking]**" if e["breaking"] == "true" and not flag else ""
        out.append(f"- {e['user_summary']} `{e['scope']}`{brk}{refs}")
    out.append("")

# --- Codebase: every entry, grouped by scope ------------------------
out.append("### Codebase")
out.append("")
scopes = {}
for e in entries:
    scopes.setdefault(e["scope"], []).append(e)
for scope in sorted(scopes):
    out.append(f"#### {scope}")
    for e in scopes[scope]:
        refs = f" ({e['refs']})" if e["refs"] not in ("", "-") else ""
        out.append(f"- [{e['type']}] {e['dev_summary']} Why: {e['why']}{refs}")
    out.append("")

sys.stdout.write("\n".join(out).rstrip() + "\n")
PYEOF
) || exit 1

if [ "$APPLY" = 0 ]; then
  printf '%s\n' "$GENERATED"
  exit 0
fi

# --apply: fold into CHANGELOG.md after the title, drop consumed files
[ -f "$CHANGELOG" ] || { printf '# Changelog\n' > "$CHANGELOG"; }
GENERATED="$GENERATED" python3 - "$CHANGELOG" <<'PYEOF'
import os, sys, re
path = sys.argv[1]
text = open(path, encoding="utf-8").read()
new = os.environ["GENERATED"]
m = re.match(r"^(# [^\n]+\n)(.*)$", text, re.S)
if not m:
    raise SystemExit(f"{path}: no '# ' title line")
open(path, "w", encoding="utf-8").write(m.group(1) + "\n" + new.rstrip() + "\n" + m.group(2))
PYEOF

for f in "$CHANGES"/*.md; do
  base=$(basename "$f")
  case "$base" in _*|README.md) ;; *) rm -f "$f" ;; esac
done
echo "{\"status\":\"applied\",\"version\":\"$VERSION\",\"changelog\":\"$CHANGELOG\"}"
