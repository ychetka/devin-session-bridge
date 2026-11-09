"""G/W/T tests for tools/check_spec_anchors.sh (task 226, issue #107).

Same scratch-repo approach as test_release_notes.py: the script
resolves its root via `git rev-parse --show-toplevel`, so each test
builds a minimal repo in tmp_path and runs the real file.
"""

import os
import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "tools" / "check_spec_anchors.sh"

FRAGMENT = """---
type: fix
scope: cli
user_visible: false
breaking: false
user_summary: "-"
dev_summary: dev
why: because
refs: issue #1
---

## Spec impact
{impact}
"""

SPEC = """# domain

### Requirement: {req}
The bridge SHALL behave.

#### Scenario: ok
- **WHEN** x
- **THEN** y
"""


def _scratch(tmp_path):
    (tmp_path / "tools").mkdir(parents=True)
    (tmp_path / ".changes").mkdir()
    (tmp_path / "docs" / "specs").mkdir(parents=True)
    shutil.copy(SCRIPT, tmp_path / "tools" / "check_spec_anchors.sh")
    os.chmod(tmp_path / "tools" / "check_spec_anchors.sh", 0o755)
    for cmd in (
        ["git", "init", "-q"],
        ["git", "config", "user.email", "t@t"],
        ["git", "config", "user.name", "t"],
    ):
        subprocess.run(cmd, cwd=tmp_path, check=True)
    (tmp_path / "docs" / "specs" / "domain.md").write_text(
        SPEC.format(req="park on explicit limit")
    )
    return tmp_path


def _frag(repo, name="0001-a.md", impact="-"):
    (repo / ".changes" / name).write_text(FRAGMENT.format(impact=impact))


def _run(repo):
    return subprocess.run(
        ["bash", "tools/check_spec_anchors.sh"],
        cwd=repo,
        capture_output=True,
        text=True,
    )


def test_gwt1_bare_dash_passes(tmp_path):
    """Given a fragment with no spec delta, When checked, Then it
    passes."""
    repo = _scratch(tmp_path)
    _frag(repo)
    out = _run(repo)
    assert out.returncode == 0, out.stderr


def test_gwt2_adds_anchor_must_exist(tmp_path):
    """Given an adds anchor that resolves, When checked, Then pass;
    Given one that does not, Then fail with the anchor named."""
    repo = _scratch(tmp_path)
    _frag(
        repo,
        impact="- adds: docs/specs/domain.md#park-on-explicit-limit",
    )
    assert _run(repo).returncode == 0
    _frag(repo, impact="- adds: docs/specs/domain.md#no-such-req")
    out = _run(repo)
    assert out.returncode == 1
    assert "no-such-req" in out.stderr


def test_gwt3_removes_anchor_must_be_gone(tmp_path):
    """Given a removes anchor still present in the spec, When checked,
    Then fail - the PR must delete what it declares removed."""
    repo = _scratch(tmp_path)
    _frag(
        repo,
        impact="- removes: docs/specs/domain.md#park-on-explicit-limit",
    )
    out = _run(repo)
    assert out.returncode == 1
    assert "still present" in out.stderr
    # delete the requirement heading -> the remove now resolves
    (repo / "docs" / "specs" / "domain.md").write_text("# domain\n")
    assert _run(repo).returncode == 0


def test_gwt4_missing_section_is_error(tmp_path):
    """Given a fragment without '## Spec impact', When checked, Then
    fail - the section is required, not optional."""
    repo = _scratch(tmp_path)
    (repo / ".changes" / "0001-a.md").write_text(
        FRAGMENT.format(impact="-").split("## Spec impact")[0]
    )
    out = _run(repo)
    assert out.returncode == 1
    assert "Spec impact" in out.stderr


def test_gwt5_bad_refs_and_ops_rejected(tmp_path):
    """Given malformed lines, unknown ops and non-spec paths, When
    checked, Then each is reported."""
    repo = _scratch(tmp_path)
    _frag(repo, impact="- tweaks: docs/specs/domain.md#x")
    out = _run(repo)
    assert out.returncode == 1
    assert "unknown Spec impact op" in out.stderr
    _frag(repo, impact="- adds: docs/OPERATOR.md#park")
    out = _run(repo)
    assert out.returncode == 1
    assert "docs/specs/" in out.stderr
    _frag(repo, impact="- adds: docs/specs/ghost.md#x")
    out = _run(repo)
    assert out.returncode == 1
    assert "does not exist" in out.stderr


def test_gwt6_kebab_slug_matches_github_rules(tmp_path):
    """Given a requirement name with punctuation and mixed case, When
    slugified, Then GitHub's rule applies: punctuation drops, spaces
    become hyphens, hyphen runs keep - 'A/B: C--d!' -> 'ab-c--d'."""
    repo = _scratch(tmp_path)
    (repo / "docs" / "specs" / "domain.md").write_text(
        SPEC.format(req="A/B: C--d!")
    )
    _frag(repo, impact="- modifies: docs/specs/domain.md#ab-c--d")
    assert _run(repo).returncode == 0


def test_gwt7_empty_changes_dir_passes(tmp_path):
    """Given no pending fragments, When checked, Then pass silently
    (verify_constraints skips calling, but the tool itself is safe)."""
    repo = _scratch(tmp_path)
    out = _run(repo)
    assert out.returncode == 0


def test_gwt8_path_traversal_rejected(tmp_path):
    """Given a Spec impact ref that escapes docs/specs/ via '..', When
    checked, Then rejected - normpath must still land inside the
    registry."""
    repo = _scratch(tmp_path)
    _frag(repo, impact="- modifies: docs/specs/../OPERATOR.md#x")
    out = _run(repo)
    assert out.returncode == 1
    assert "docs/specs/" in out.stderr


def test_gwt9_empty_section_is_error(tmp_path):
    """Given a '## Spec impact' heading with no entries, When checked,
    Then fail - the section must carry '-' at minimum."""
    repo = _scratch(tmp_path)
    (repo / ".changes" / "0001-a.md").write_text(
        FRAGMENT.format(impact="").rstrip() + "\n"
    )
    out = _run(repo)
    assert out.returncode == 1
    assert "empty" in out.stderr


def test_gwt10_removes_tolerates_deleted_file(tmp_path):
    """Given a removes anchor whose whole spec file was deleted in the
    same PR, When checked, Then pass - the anchor is gone with it."""
    repo = _scratch(tmp_path)
    _frag(
        repo,
        impact="- removes: docs/specs/domain.md#park-on-explicit-limit",
    )
    (repo / "docs" / "specs" / "domain.md").unlink()
    out = _run(repo)
    assert out.returncode == 0
    # but adds on a missing file still fails
    _frag(repo, impact="- adds: docs/specs/domain.md#x")
    assert _run(repo).returncode == 1
