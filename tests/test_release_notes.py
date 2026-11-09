"""G/W/T tests for tools/release_notes.sh (issue #96, directive 444).

The script is exercised end-to-end inside a scratch git repo per test
- it resolves its root via `git rev-parse --show-toplevel`, so the
fixture builds a minimal repo in tmp_path and runs the real file.
"""

import os
import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "tools" / "release_notes.sh"

ENTRY = """---
type: {type}
scope: {scope}
user_visible: {visible}
breaking: {breaking}
user_summary: {user}
dev_summary: {dev}
why: {why}
refs: {refs}
---
"""


def _scratch(tmp_path):
    """Minimal git repo: .changes/, CHANGELOG.md, copied script."""
    (tmp_path / "tools").mkdir(parents=True)
    (tmp_path / ".changes").mkdir()
    shutil.copy(SCRIPT, tmp_path / "tools" / "release_notes.sh")
    os.chmod(tmp_path / "tools" / "release_notes.sh", 0o755)
    (tmp_path / "CHANGELOG.md").write_text(
        "# Changelog\n\nPreamble line.\n\n## [0.5.0] - 2026-11-01\n\nprior\n"
    )
    for cmd in (
        ["git", "init", "-q"],
        ["git", "config", "user.email", "t@t"],
        ["git", "config", "user.name", "t"],
    ):
        subprocess.run(cmd, cwd=tmp_path, check=True)
    return tmp_path


def _entry(tmp_path, name, **kw):
    kw.setdefault("type", "fix")
    kw.setdefault("scope", "cli")
    kw.setdefault("visible", "false")
    kw.setdefault("breaking", "false")
    kw.setdefault("user", "-")
    kw.setdefault("dev", "dev summary")
    kw.setdefault("why", "because")
    kw.setdefault("refs", "issue #1")
    (tmp_path / ".changes" / name).write_text(ENTRY.format(**kw))


def _run(repo, *args):
    return subprocess.run(
        ["bash", "tools/release_notes.sh", *args],
        cwd=repo,
        capture_output=True,
        text=True,
    )


def test_gwt1_groups_sections_by_type_and_scope(tmp_path):
    """Given entries of mixed types/visibility, When the script runs,
    Then User-facing holds only user_visible entries grouped by type
    and Codebase holds everything grouped by scope."""
    repo = _scratch(tmp_path)
    _entry(
        repo,
        "0001-a.md",
        type="feat",
        scope="cli",
        visible="true",
        breaking="true",
        user="New frobnicate command.",
    )
    _entry(
        repo, "0002-b.md", type="fix", scope="daemon", visible="true", user="Fixed wake detection."
    )
    _entry(repo, "0003-c.md", type="chore", scope="cli", dev="Internal tidy.")

    res = _run(repo, "--version", "0.5.2")

    assert res.returncode == 0, res.stderr
    out = res.stdout
    assert out.startswith("## [0.5.2] - ")
    uf, _, cb = out.partition("### Codebase")
    assert "#### feat **(breaking)**" in uf and "frobnicate" in uf
    assert "#### fix" in uf and "wake detection" in uf
    assert "Internal tidy" not in uf
    assert "#### cli" in cb and "#### daemon" in cb
    assert "[feat]" in cb and "[chore] Internal tidy" in cb


def test_gwt2_apply_folds_into_changelog_and_cleans(tmp_path):
    """Given pending entries, When --apply runs, Then the generated
    section lands above the first release section (title and
    preamble stay on top) and consumed entry files are deleted while
    _template/README survive."""
    repo = _scratch(tmp_path)
    _entry(repo, "0001-a.md")
    (repo / ".changes" / "_template.md").write_text("tpl")
    (repo / ".changes" / "README.md").write_text("readme")

    res = _run(repo, "--version", "0.5.1", "--apply")

    assert res.returncode == 0, res.stderr
    log = (repo / "CHANGELOG.md").read_text()
    assert "## [0.5.1]" in log and "dev summary" in log
    # order: title < preamble < new section < prior release
    assert (
        log.index("# Changelog")
        < log.index("Preamble line.")
        < log.index("## [0.5.1]")
        < log.index("## [0.5.0]")
    )
    left = sorted(p.name for p in (repo / ".changes").iterdir())
    assert left == ["README.md", "_template.md"]


def test_gwt3_missing_fields_fail_loud(tmp_path):
    """Given an entry without required fields, When the script runs,
    Then it exits non-zero naming the file - no silent skips."""
    repo = _scratch(tmp_path)
    (repo / ".changes" / "0001-bad.md").write_text("---\ntype: fix\n---\n")

    res = _run(repo)

    assert res.returncode != 0
    assert "missing fields" in res.stderr and "0001-bad.md" in res.stderr


def test_gwt4_empty_dir_fails(tmp_path):
    """Given no pending entries, When the script runs, Then it fails
    instead of emitting an empty release section."""
    repo = _scratch(tmp_path)

    res = _run(repo)

    assert res.returncode != 0
    assert "no pending entries" in res.stderr


def test_gwt5_apply_requires_version(tmp_path):
    """Given --apply without --version, When the script runs, Then it
    refuses - a CHANGELOG section without a version header is
    malformed by definition."""
    repo = _scratch(tmp_path)
    _entry(repo, "0001-a.md")

    res = _run(repo, "--apply")

    assert res.returncode == 2
    assert "--apply requires --version" in res.stderr


def test_gwt6_folded_continuation_lines(tmp_path):
    """Given a field whose value wraps onto indented continuation
    lines (the dogfood entry itself uses this), When the script
    runs, Then the folded parts join the value, not become keys."""
    repo = _scratch(tmp_path)
    (repo / ".changes" / "0001-a.md").write_text(
        "---\n"
        "type: fix\n"
        "scope: cli\n"
        "user_visible: false\n"
        "breaking: false\n"
        'user_summary: "-"\n'
        "dev_summary: first part\n"
        "  second part\n"
        "why: motive\n"
        "refs: issue #9\n"
        "---\n"
    )

    res = _run(repo)

    assert res.returncode == 0, res.stderr
    assert "first part second part" in res.stdout


def test_gwt7_mid_value_fence_does_not_truncate(tmp_path):
    """Given a value containing '---' mid-line, When the script
    parses it, Then only a whole-line fence terminates frontmatter -
    the value is not silently truncated."""
    repo = _scratch(tmp_path)
    (repo / ".changes" / "0001-a.md").write_text(
        "---\n"
        "type: fix\n"
        "scope: cli\n"
        "user_visible: false\n"
        "breaking: false\n"
        'user_summary: "-"\n'
        "dev_summary: keeps tail\n"
        "why: refs issue #5 --- see ADR-0001\n"
        "refs: task #7\n"
        "---\n"
    )

    res = _run(repo)

    assert res.returncode == 0, res.stderr
    assert "see ADR-0001" in res.stdout


def test_gwt8_validation_branches(tmp_path):
    """Given entries violating each field rule, When the script
    runs, Then each fails loud: bad type, bad bool, missing
    user_summary on a visible entry."""
    repo = _scratch(tmp_path)
    _entry(repo, "0001-badtype.md", type="banana")
    assert _run(repo).returncode != 0 and "bad type" in _run(repo).stderr
    (repo / ".changes" / "0001-badtype.md").unlink()

    _entry(repo, "0002-badbool.md", visible="maybe")
    res = _run(repo)
    assert res.returncode != 0 and "true|false" in res.stderr
    (repo / ".changes" / "0002-badbool.md").unlink()

    _entry(repo, "0003-nosummary.md", visible="true", user="-")
    res = _run(repo)
    assert res.returncode != 0 and "user_summary" in res.stderr
