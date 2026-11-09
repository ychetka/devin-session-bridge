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
if [ -n "$VERSION" ] && ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "--version must be X.Y.Z, got '$VERSION'" >&2; exit 2
fi
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
    name = os.path.basename(path)
    lines = open(path, encoding="utf-8").read().splitlines()
    if not lines or lines[0].strip() != "---":
        raise SystemExit(f"{name}: missing --- frontmatter")
    end = next(
        (i for i, l in enumerate(lines[1:], 1) if l.strip() == "---"),
        None,
    )
    if end is None:
        raise SystemExit(f"{name}: unterminated --- frontmatter")
    fm, last = {}, None
    for line in lines[1:end]:
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
        raise SystemExit(f"{name}: missing fields: {', '.join(missing)}")
    if fm["type"] not in TYPES:
        raise SystemExit(f"{name}: bad type {fm['type']!r}")
    for b in ("user_visible", "breaking"):
        if fm[b] not in ("true", "false"):
            raise SystemExit(f"{name}: {b} must be true|false")
    if fm["user_visible"] == "true" and fm["user_summary"] in ("", "-"):
        raise SystemExit(f"{name}: user_visible entry needs user_summary")
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
    out.append(f"#### {t}")
    for e in group:
        refs = f" ({e['refs']})" if e["refs"] not in ("", "-") else ""
        brk = " **[breaking]**" if e["breaking"] == "true" else ""
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

# --apply: fold into CHANGELOG.md above the first release section
# (the title and any preamble stay on top), then drop consumed files.
# The rewrite goes through a temp file + os.replace so a crash cannot
# leave a truncated CHANGELOG.
[ -f "$CHANGELOG" ] || { printf '# Changelog\n' > "$CHANGELOG"; }
GENERATED="$GENERATED" python3 - "$CHANGELOG" <<'PYEOF'
import os, sys, re, tempfile
path = sys.argv[1]
text = open(path, encoding="utf-8").read()
new = os.environ["GENERATED"].rstrip() + "\n"
m = re.search(r"(?m)^## ", text)
if m:
    out = text[: m.start()] + new + "\n" + text[m.start():]
else:
    out = text.rstrip() + "\n\n" + new
fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
with os.fdopen(fd, "w", encoding="utf-8") as f:
    f.write(out)
os.replace(tmp, path)
PYEOF

for f in "$CHANGES"/*.md; do
  base=$(basename "$f")
  case "$base" in _*|README.md) ;; *) rm -f "$f" ;; esac
done
echo "{\"status\":\"applied\",\"version\":\"$VERSION\",\"changelog\":\"$CHANGELOG\"}"
