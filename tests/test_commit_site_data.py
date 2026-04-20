import json
import subprocess
from pathlib import Path

import pytest
from publish import commit_site_data


def run_git(repo_root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout.strip()


def init_repo(tmp_path: Path) -> tuple[Path, Path]:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    run_git(repo_root, "init", "-b", "main")
    run_git(repo_root, "config", "user.name", "Test User")
    run_git(repo_root, "config", "user.email", "test@example.com")

    site_data_dir = repo_root / "evidence" / "sources" / "site_data" / "current"
    site_data_dir.mkdir(parents=True)
    (site_data_dir / "build_meta.json").write_text(
        json.dumps(
            {
                "build_id": "build-initial",
                "generated_at": "2026-04-18T15:00:00Z",
                "snapshot_date": "2026-04-01",
                "source_run_date": "2026-04-18",
                "dbt_test_passed": True,
                "datasets_dir": str(site_data_dir),
                "dataset_version": 1,
            }
        ),
        encoding="utf-8",
    )
    (site_data_dir / "manifest.json").write_text(
        json.dumps({"files": [], "snapshot_date": "2026-04-01", "dataset_version": 1}),
        encoding="utf-8",
    )
    (site_data_dir / "site_kpis.csv").write_text("snapshot_date\n2026-04-01\n", encoding="utf-8")
    run_git(repo_root, "add", ".")
    run_git(repo_root, "commit", "-m", "initial")
    return repo_root, site_data_dir


def test_publish_site_data_update_rejects_unrelated_dirty_state(tmp_path):
    repo_root, site_data_dir = init_repo(tmp_path)
    (repo_root / "README.md").write_text("dirty", encoding="utf-8")

    with pytest.raises(commit_site_data.SiteDataGitError):
        commit_site_data.publish_site_data_update(
            repo_root=repo_root,
            site_data_path=site_data_dir,
            target_branch="site-data-updates",
            remote="origin",
            push=False,
        )


def test_publish_site_data_update_rejects_build_output_changes(tmp_path):
    repo_root, site_data_dir = init_repo(tmp_path)
    build_dir = repo_root / "evidence" / "build"
    build_dir.mkdir(parents=True)
    (build_dir / "index.html").write_text("build", encoding="utf-8")

    with pytest.raises(commit_site_data.SiteDataGitError):
        commit_site_data.publish_site_data_update(
            repo_root=repo_root,
            site_data_path=site_data_dir,
            target_branch="site-data-updates",
            remote="origin",
            push=False,
        )


def test_publish_site_data_update_returns_no_changes_when_site_data_is_unchanged(tmp_path):
    repo_root, site_data_dir = init_repo(tmp_path)

    result = commit_site_data.publish_site_data_update(
        repo_root=repo_root,
        site_data_path=site_data_dir,
        target_branch="site-data-updates",
        remote="origin",
        push=False,
    )

    assert result["git_commit_status"] == "no_changes"


def test_publish_site_data_update_commits_only_site_data_into_target_branch(tmp_path):
    repo_root, site_data_dir = init_repo(tmp_path)
    run_git(repo_root, "remote", "add", "origin", "https://example.com/repo.git")

    (site_data_dir / "build_meta.json").write_text(
        json.dumps(
            {
                "build_id": "build-next",
                "generated_at": "2026-04-19T15:00:00Z",
                "snapshot_date": "2026-05-01",
                "source_run_date": "2026-05-01",
                "dbt_test_passed": True,
                "datasets_dir": str(site_data_dir),
                "dataset_version": 1,
            }
        ),
        encoding="utf-8",
    )
    (site_data_dir / "manifest.json").write_text(
        json.dumps(
            {
                "files": ["site_kpis.csv"],
                "snapshot_date": "2026-05-01",
                "dataset_version": 1,
            }
        ),
        encoding="utf-8",
    )
    (site_data_dir / "site_kpis.csv").write_text("snapshot_date\n2026-05-01\n", encoding="utf-8")

    result = commit_site_data.publish_site_data_update(
        repo_root=repo_root,
        site_data_path=site_data_dir,
        target_branch="site-data-updates",
        remote="origin",
        push=False,
    )

    assert result["git_commit_status"] == "committed_local_ref_only"
    assert result["commit_message"] == "chore(site-data): update published datasets for 2026-05-01"

    current_head = run_git(repo_root, "rev-parse", "HEAD")
    target_head = run_git(repo_root, "rev-parse", "site-data-updates")
    assert current_head != target_head

    changed_files = set(
        filter(
            None,
            run_git(
                repo_root,
                "diff-tree",
                "--no-commit-id",
                "--name-only",
                "-r",
                "site-data-updates",
            ).splitlines(),
        )
    )
    assert changed_files == {
        "evidence/sources/site_data/current/build_meta.json",
        "evidence/sources/site_data/current/manifest.json",
        "evidence/sources/site_data/current/site_kpis.csv",
    }
