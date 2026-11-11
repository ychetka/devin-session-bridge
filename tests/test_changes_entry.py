"""G/W/T tests for tools/check_changes_entry.sh (task 261, issue #135).

Same scratch-repo approach as test_spec_anchors.py: the --base mode is
exercised against a real two-branch git repo in tmp_path. The --pr gh
mode is a thin adapter over the same verdict logic and needs GitHub,
so it is not covered here.
"""

import os
import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "tools" / "check_changes_entry.sh"


def _git(repo, *args):
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


def _scratch(tmp_path):
    """A repo on main with one base commit, branched to `feature` -
    tests then commit their scenario on the feature branch and diff
    against main."""
    (tmp_path / "tools").mkdir(parents=True)
    (tmp_path / ".changes").mkdir()
    shutil.copy(SCRIPT, tmp_path / "tools" / "check_changes_entry.sh")
    os.chmod(tmp_path / "tools" / "check_changes_entry.sh", 0o755)
    repo = tmp_path
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    (repo / "app.py").write_text("x = 1\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "base")
    _git(repo, "switch", "-qc", "feature")
    return repo


def _commit(repo, path, content="y = 2\n"):
    target = repo / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "change")


def _run(repo, env=None):
    e = dict(os.environ)
    e.pop("PR_TITLE", None)
    e.pop("PR_BODY", None)
    e.pop("PR_LABELS", None)
    if env:
        e.update(env)
    return subprocess.run(
        ["bash", "tools/check_changes_entry.sh", "--base", "main"],
        cwd=repo,
        capture_output=True,
        text=True,
        env=e,
    )


def test_gwt1_added_fragment_passes(tmp_path):
    """Given a branch adding a .changes fragment, When checked against
    base, Then it passes."""
    repo = _scratch(tmp_path)
    _commit(repo, ".changes/0135-gate.md")
    out = _run(repo)
    assert out.returncode == 0, out.stderr
    assert "fragment" in out.stdout


def test_gwt2_code_change_without_record_fails(tmp_path):
    """Given a branch that changes a source file with no fragment, no
    changelog and no marker, When checked, Then it fails naming the
    required record."""
    repo = _scratch(tmp_path)
    _commit(repo, "app.py")
    out = _run(repo)
    assert out.returncode == 1
    assert "missing_changes_entry" in out.stdout
    assert ".changes" in out.stderr


def test_gwt3_changelog_touch_passes(tmp_path):
    """Given a release-prep branch that folds CHANGELOG.md instead of
    carrying a pending fragment, When checked, Then it passes."""
    repo = _scratch(tmp_path)
    (repo / "CHANGELOG.md").write_text("# Changelog\n")
    _commit(repo, "CHANGELOG.md")
    out = _run(repo)
    assert out.returncode == 0, out.stderr
    assert "CHANGELOG" in out.stdout


def test_gwt4_marker_in_body_exempts(tmp_path):
    """Given a docs-only branch whose PR body carries the
    no-changes-entry marker, When checked with PR_BODY set, Then the
    documented opt-out applies."""
    repo = _scratch(tmp_path)
    _commit(repo, "docs/guide.md")
    out = _run(repo, env={"PR_BODY": "Docs sweep.\n\nno-changes-entry"})
    assert out.returncode == 0, out.stderr
    assert "marker" in out.stdout


def test_gwt5_marker_in_label_exempts(tmp_path):
    """Given the marker arriving as a PR label name, When checked with
    PR_LABELS set, Then it still exempts."""
    repo = _scratch(tmp_path)
    _commit(repo, "app.py")
    out = _run(repo, env={"PR_LABELS": "docs no-changes-entry"})
    assert out.returncode == 0, out.stderr


def test_gwt6_template_and_readme_do_not_count(tmp_path):
    """Given only _template.md / README.md touched under .changes,
    When checked, Then the real-fragment rule still fails the PR."""
    repo = _scratch(tmp_path)
    _commit(repo, ".changes/_template.md")
    out = _run(repo)
    assert out.returncode == 1
    _commit(repo, ".changes/README.md")
    assert _run(repo).returncode == 1


def test_gwt7_modified_fragment_counts(tmp_path):
    """Given a pending fragment carried over from base and edited on
    the branch, When checked, Then it passes - the record is present
    in the diff either way."""
    repo = _scratch(tmp_path)
    (repo / ".changes" / "0001-old.md").write_text("old\n")
    _git(repo, "switch", "main")
    (repo / ".changes" / "0001-old.md").write_text("old\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "base fragment")
    _git(repo, "switch", "feature")
    _commit(repo, ".changes/0001-old.md", "old updated\n")
    assert _run(repo).returncode == 0


def test_gwt8_empty_diff_passes(tmp_path):
    """Given an empty diff (no file changes), When checked, Then the
    record requirement is vacuous - it passes."""
    repo = _scratch(tmp_path)
    out = _run(repo)
    assert out.returncode == 0, out.stderr
    assert "no file changes" in out.stdout
