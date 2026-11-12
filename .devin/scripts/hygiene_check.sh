#!/usr/bin/env bash
# hygiene_check.sh - deterministic gate for outbound text.
# stdin: text to check (no path exemptions - messages/subjects are
# scanned whole). --diff: stdin is a raw git diff (filenames parsed
# for path exemptions). --diff-base <ref>: check commit subjects +
# added lines of the diff vs <ref> in two separate passes - subjects
# and diffs are never mixed into one stream, so no framing/sentinel
# trick can smuggle content under an exempt path.
# Exit 0 = clean. Exit 1 = violations JSON.
set -uo pipefail

# .devin may sit outside the git toplevel - resolve it from the
# script's own location so callers running from $TOP still find config.
# project.json is read inside the python heredoc (path passed via env)
# so a quote in $PJ can never break an inline -c string and silently
# degrade to defaults - malformed JSON fails closed instead.
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PJ="$DEVIN_DIR/project.json"

run_check() { # $1 = file to scan, $2 = mode (stdin|diff)
  HYGIENE_INPUT_FILE="$1" HYGIENE_MODE="$2" \
  HYGIENE_PJ="$PJ" python3 - <<'PYEOF'
import json, os, re, sys
mode = os.environ.get("HYGIENE_MODE", "stdin")
with open(os.environ["HYGIENE_INPUT_FILE"], encoding="utf-8",
          errors="replace") as fh:
    text = fh.read()
# project.json drives language + cyrillic-path exemptions. Absent file
# -> defaults (en, no extra paths). Present but malformed - bad JSON,
# non-object root, or a wrong value type - -> fail closed: emit an
# infra violation instead of silently defaulting.
lang = "en"
# Fleet convention: .devin/agents/ profiles are intentionally Russian
# in every repo, so that path is exempt even without project.json.
cyr_paths = {".devin/agents/"}
pj = os.environ.get("HYGIENE_PJ", "")
if pj and os.path.exists(pj):
    try:
        with open(pj, encoding="utf-8") as fh:
            cfg = json.load(fh)
        if not isinstance(cfg, dict):
            raise ValueError("project.json is not a JSON object")
        # Value types are validated before use - a wrong type is
        # malformed config and must fail closed, never silently
        # default: "text_language":null would disable the cyrillic
        # gate, and "cyrillic_paths":"s" would coerce to {"s"} and
        # exempt every path starting with "s".
        tl = cfg.get("text_language", "en")
        if not isinstance(tl, str):
            raise ValueError(
                "project.json text_language must be a string")
        cp = cfg.get("cyrillic_paths", [])
        if not isinstance(cp, list) or not all(
                isinstance(p, str) for p in cp):
            raise ValueError(
                "project.json cyrillic_paths must be a list of strings")
        lang = tl
        cyr_paths |= set(cp)
    except (OSError, ValueError) as exc:
        print(json.dumps({"violations": [{
            "class": "infra", "line": 0,
            "hint": "project.json malformed - fail closed: %s" % exc}]},
            indent=2))
        sys.exit(1)
cyr_paths = list(cyr_paths)
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
    # The s" "k- split keeps the literal "sk-" out of this file's own
    # source (the gate would flag itself). BOTH halves must be raw
    # strings: a non-raw literal would compile \b to a backspace and
    # silently kill the alternatives (issue #88 review finding).
    # \b anchors + length floors: "task-terminal" no longer trips the
    # sk- rule, and xox/AKIA cannot match mid-word noise.
    (r"(\bs" r"k-[A-Za-z0-9_-]{16,}|\bxox[baprs]-[A-Za-z0-9-]{10,}|\bAKIA[0-9A-Z]{16})", "secret",
     "API key pattern"),
    (r"(?i)(password|passwd|secret|api[_-]?key)\s*[:=]\s*['\"]?[^\s'\"]{8,}", "secret",
     "credential assignment"),
]

# In diff mode each checked line carries the file it was added to;
# any non-diff content is scanned as plain text with no path context.
# Subjects and diffs are NEVER mixed into one stream - callers scan
# them separately, so no sentinel/framing line can be forged by
# input content.
def items():
    path = None
    in_diff = mode != "diff"
    header = False
    for line in text.splitlines():
        if mode == "diff" and line.startswith("diff --git"):
            in_diff = True
            header = True
            path = ""
            yield line, None  # filenames are outbound text too
            continue
        if mode == "diff" and not in_diff:
            yield line, None
            continue
        if mode == "diff" and line.startswith("@@"):
            header = False
            continue
        if mode == "diff" and header and line.startswith("+++"):
            # git quotes paths with special chars: +++ "b/f o o"
            raw = line[4:].strip()
            if raw.startswith('"') and raw.endswith('"'):
                raw = raw[1:-1]
            if raw.startswith("b/"):
                path = raw[2:]
            elif raw == "/dev/null":
                path = None
            else:
                path = raw
            yield line, None  # scan the path itself with no exemption
            continue
        if mode == "diff" and line.startswith("+"):
            yield line, path
            continue
        if mode == "diff":
            continue
        yield line, None

# Lockfiles are skipped entirely: generated content can contain
# token-shaped strings and non-English identifiers, and reviewing them
# line-by-line has no signal. --diff-base's ':(exclude)*.lock' pathspec
# remains a generation-time perf filter; this python-side check is the
# semantic one for both modes (--diff used by ship_commit has no
# generation-side filter, and lockfiles that do not end in .lock like
# package-lock.json are caught here too).
LOCKFILE_NAMES = {"package-lock.json", "pnpm-lock.yaml", "yarn.lock",
                  "go.sum", "poetry.lock", "composer.lock"}

for i, (line, path) in enumerate(items(), 1):
    if path:
        base = os.path.basename(path)
        if base.endswith(".lock") or base in LOCKFILE_NAMES:
            continue
    for pattern, cls, hint in rules:
        # Canon rule-source files under .devin/ legitimately carry
        # attribution-pattern examples (they define the detection
        # rules) - exempt them from the attribution class only.
        # Secret patterns still scan everywhere: a real key in a
        # .devin file must keep tripping the gate.
        if (cls == "attribution" and path is not None
                and path.startswith(".devin/")):
            continue
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
}

fail=0
if [ "${1:-}" = "--diff-base" ]; then
  BASE="${2:?--diff-base needs a ref}"
  TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo '{"violations":[{"class":"infra","line":0,"hint":"not a git repo - fail closed"}]}'; exit 1; }
  # An unresolvable ref must not silently scan nothing - fail closed.
  git -C "$TOP" rev-parse --verify --quiet "${BASE}^{commit}" >/dev/null 2>&1 \
    || { echo '{"violations":[{"class":"infra","line":0,"hint":"base ref does not resolve - fail closed"}]}'; exit 1; }
  # Subjects and the diff are checked in SEPARATE passes (issue #88):
  # a subject can forge a fake "diff --git"/"+++ <exempt>" header and
  # smuggle content under an exempt path if both share one stream.
  # Stdin mode gives subjects no path context (no exemptions).
  F1=$(mktemp); F2=$(mktemp)
  trap 'rm -f "$F1" "$F2"' EXIT
  git -C "$TOP" log --format=%s "${BASE}..HEAD" > "$F1" 2>/dev/null
  git -C "$TOP" diff "${BASE}..HEAD" -- ':(exclude)*.lock' > "$F2"
  run_check "$F1" stdin || fail=1
  run_check "$F2" diff || fail=1
elif [ "${1:-}" = "--diff" ]; then
  F1=$(mktemp); trap 'rm -f "$F1"' EXIT
  cat > "$F1"
  run_check "$F1" diff || fail=1
else
  F1=$(mktemp); trap 'rm -f "$F1"' EXIT
  cat > "$F1"
  run_check "$F1" stdin || fail=1
fi
exit $fail
