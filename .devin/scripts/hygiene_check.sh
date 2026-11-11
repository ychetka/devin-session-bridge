#!/usr/bin/env bash
# hygiene_check.sh - deterministic gate for outbound text.
# stdin: text to check. --diff: stdin is plain text followed by a raw
# git diff (filenames parsed for path exemptions). --diff-base <ref>:
# check commit subjects + added lines of the diff vs <ref>.
# Exit 0 = clean. Exit 1 = violations JSON.
set -uo pipefail

# .devin may sit outside the git toplevel - resolve it from the
# script's own location so callers running from $TOP still find config.
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PJ="$DEVIN_DIR/project.json"
LANG="en"
CYR_PATHS="[]"
if [ -f "$PJ" ]; then
  LANG=$(python3 -c "import json;print(json.load(open('$PJ')).get('text_language','en'))" 2>/dev/null || echo en)
  CYR_PATHS=$(python3 -c "import json;print(json.dumps(json.load(open('$PJ')).get('cyrillic_paths',[])))" 2>/dev/null || echo '[]')
fi

MODE="stdin"
if [ "${1:-}" = "--diff-base" ]; then
  MODE="diff"
  BASE="${2:?--diff-base needs a ref}"
  TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo '{"violations":[{"class":"infra","line":0,"hint":"not a git repo - fail closed"}]}'; exit 1; }
  INPUT=$( { git -C "$TOP" log --format=%s "${BASE}..HEAD" 2>/dev/null; printf '\n@@SUBJECTS_END@@\n'; git -C "$TOP" diff "${BASE}..HEAD" -- ':(exclude)*.lock'; } )
elif [ "${1:-}" = "--diff" ]; then
  MODE="diff"
  INPUT=$(cat)
else
  INPUT=$(cat)
fi

INPUT_FILE=$(mktemp)
trap 'rm -f "$INPUT_FILE"' EXIT
printf '%s' "$INPUT" > "$INPUT_FILE"
HYGIENE_INPUT_FILE="$INPUT_FILE" HYGIENE_LANG="$LANG" HYGIENE_MODE="$MODE" \
HYGIENE_CYR_PATHS="$CYR_PATHS" python3 - <<'PYEOF'
import json, os, re, sys
lang = os.environ.get("HYGIENE_LANG", "en")
mode = os.environ.get("HYGIENE_MODE", "stdin")
with open(os.environ["HYGIENE_INPUT_FILE"], encoding="utf-8",
          errors="replace") as fh:
    text = fh.read()
cyr_paths = json.loads(os.environ.get("HYGIENE_CYR_PATHS", "[]"))
violations = []

def hit(cls, pattern, line_no, hint, path=None):
    v = {"class": cls, "line": line_no,
         "pattern": pattern, "hint": hint}
    if path is not None:
        v["file"] = path
    violations.append(v)

rules = [
    (r"generated\s+with", "attribution",
     "tool attribution footer - remove it"),
    (r"co-""authored-""by", "attribution",
     "attribution trailer - remove it"),
    (r"signed-""off-""by:.*(devin|claude|copilot|bot|ai\b)", "attribution",
     "tool sign-off - remove it"),
    (r"\b(devin|claude|copilot|chatgpt|gpt-\d|gemini|swe-\d)[\w.-]*\s*<", "attribution",
     "tool name in author/trailer line"),
    (r"-----BEGIN [A-Z ]*PRIVATE KEY-----", "secret",
     "private key material"),
    (r"(ghp_|gho_|ghu_|ghs_|ghr_|github_pat_)[A-Za-z0-9_]{20,}", "secret",
     "GitHub token pattern"),
    (r"(s""k-|xox[baprs]-|AKIA[0-9A-Z]{16})", "secret",
     "API key pattern"),
    (r"(?i)(password|passwd|secret|api[_-]?key)\s*[:=]\s*['\"]?[^\s'\"]{8,}", "secret",
     "credential assignment"),
]

# In diff mode each checked line carries the file it was added to;
# plain text before the first `diff --git` (subjects/messages) has no path.
def items():
    # In --diff-base mode INPUT is subjects + sentinel + raw diff. Subjects
    # are attacker-controlled text (commit messages) and must never feed the
    # diff state machine - a subject like "diff --git a/x b/x" could desync
    # path tracking and smuggle exempted cyrillic. Split first.
    body = text
    if mode == "diff" and "\n@@SUBJECTS_END@@\n" in text:
        subjects, body = text.split("\n@@SUBJECTS_END@@\n", 1)
        for line in subjects.splitlines():
            yield line, None  # subjects scanned with no path context
    path = None
    in_diff = mode != "diff"
    header = False
    for line in body.splitlines():
        if mode == "diff" and line.startswith("diff --git"):
            in_diff = True
            header = True
            path = ""
            continue
        if mode == "diff" and not in_diff:
            # Commit-message preamble before the first file diff: scanned
            # as plain text, never allowed to steer the path state.
            yield line, None
            continue
        if mode == "diff" and line.startswith("@@"):
            header = False
            continue
        if mode == "diff" and header and line.startswith("+++"):
            if line.startswith("+++ b/"):
                path = line[6:]
            else:
                path = line[4:]
            continue
        if mode == "diff" and line.startswith("+"):
            yield line, path
            continue
        if mode == "diff":
            continue
        yield line, None

for i, (line, path) in enumerate(items(), 1):
    for pattern, cls, hint in rules:
        if re.search(pattern, line, re.IGNORECASE):
            hit(cls, pattern, i, hint, path)
    exempt = path is not None and any(
        path.startswith(p) for p in cyr_paths)
    if lang == "en" and not exempt and re.search(
            "[\\u0410-\\u044f\\u0401\\u0451]", line):
        hit("language", "[cyrillic]", i,
            "non-English text in en-language repository", path)

if violations:
    print(json.dumps({"violations": violations}, indent=2))
    sys.exit(1)
sys.exit(0)
PYEOF
