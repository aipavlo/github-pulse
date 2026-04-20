import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SITE_DATA_CURRENT_DIR = PROJECT_ROOT / "evidence" / "sources" / "site_data" / "current"
DEFAULT_TARGET_BRANCH = "site-data-updates"
DEFAULT_REMOTE = "origin"
FORBIDDEN_PATH_PREFIXES = (
    "evidence/build/",
    "evidence/.evidence/",
    "evidence/sources/site_data/_tmp/",
    "evidence/node_modules/",
    "evidence/.npm/",
    "evidence/.cache/",
    "node_modules/",
    ".npm/",
)


class SiteDataGitError(RuntimeError):
    pass


def run_git(
    args: list[str],
    repo_root: Path,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=repo_root,
        env=env,
        input=input_text,
        text=True,
        capture_output=True,
        check=True,
    )


def normalize_paths(output: str) -> set[str]:
    return {line.strip() for line in output.splitlines() if line.strip()}


def collect_repo_changes(repo_root: Path, scope: str | None = None) -> set[str]:
    scoped_args = ["--", scope] if scope else ["--", "."]
    changed_paths = set()
    changed_paths |= normalize_paths(
        run_git(["diff", "--name-only", *scoped_args], repo_root).stdout
    )
    changed_paths |= normalize_paths(
        run_git(["diff", "--cached", "--name-only", *scoped_args], repo_root).stdout
    )

    ls_files_args = ["ls-files", "--others", "--exclude-standard"]
    if scope:
        ls_files_args.extend(["--", scope])
    changed_paths |= normalize_paths(run_git(ls_files_args, repo_root).stdout)
    return changed_paths


def assert_clean_outside_site_data(repo_root: Path, site_data_path: Path) -> None:
    allowed_prefix = site_data_path.relative_to(repo_root).as_posix()
    dirty_paths = collect_repo_changes(repo_root)
    forbidden_paths = sorted(
        path
        for path in dirty_paths
        if any(
            path == prefix.rstrip("/") or path.startswith(prefix)
            for prefix in FORBIDDEN_PATH_PREFIXES
        )
    )
    if forbidden_paths:
        raise SiteDataGitError(
            "Refusing to create a site-data commit because build output or cache paths changed: "
            + ", ".join(forbidden_paths)
        )

    disallowed_paths = sorted(
        path
        for path in dirty_paths
        if path != allowed_prefix and not path.startswith(f"{allowed_prefix}/")
    )
    if disallowed_paths:
        raise SiteDataGitError(
            "Refusing to create a site-data commit because the worktree has unrelated changes: "
            + ", ".join(disallowed_paths)
        )


def build_commit_message(site_data_path: Path) -> str:
    build_meta_path = site_data_path / "build_meta.json"
    if not build_meta_path.exists():
        raise SiteDataGitError(f"Missing required metadata file: {build_meta_path}")

    build_meta = json.loads(build_meta_path.read_text(encoding="utf-8"))
    snapshot_date = build_meta.get("snapshot_date")
    if not snapshot_date:
        raise SiteDataGitError("build_meta.json must contain snapshot_date")

    return f"chore(site-data): update published datasets for {snapshot_date}"


def ensure_remote_exists(repo_root: Path, remote: str) -> None:
    remotes = normalize_paths(run_git(["remote"], repo_root).stdout)
    if remote not in remotes:
        raise SiteDataGitError(f"Git remote '{remote}' does not exist")


def build_commit_with_temp_index(
    repo_root: Path,
    site_data_path: Path,
    commit_message: str,
) -> str | None:
    relative_site_data_path = site_data_path.relative_to(repo_root).as_posix()

    with tempfile.NamedTemporaryFile(prefix="site-data-index-", delete=True) as temp_index_file:
        env = os.environ.copy()
        env["GIT_INDEX_FILE"] = temp_index_file.name

        run_git(["read-tree", "HEAD"], repo_root, env=env)
        run_git(["add", "-A", "--", relative_site_data_path], repo_root, env=env)

        staged_changes = normalize_paths(
            run_git(
                ["diff-index", "--cached", "--name-only", "HEAD", "--", relative_site_data_path],
                repo_root,
                env=env,
            ).stdout
        )
        if not staged_changes:
            return None

        tree_sha = run_git(["write-tree"], repo_root, env=env).stdout.strip()
        commit_sha = run_git(
            ["commit-tree", tree_sha, "-p", "HEAD", "-F", "-"],
            repo_root,
            env=env,
            input_text=commit_message,
        ).stdout.strip()
        return commit_sha


def update_local_target_branch(repo_root: Path, target_branch: str, commit_sha: str) -> None:
    run_git(["update-ref", f"refs/heads/{target_branch}", commit_sha], repo_root)


def push_commit_to_remote(
    repo_root: Path,
    remote: str,
    target_branch: str,
    commit_sha: str,
) -> None:
    run_git(
        [
            "push",
            "--force-with-lease",
            remote,
            f"{commit_sha}:refs/heads/{target_branch}",
        ],
        repo_root,
    )


def publish_site_data_update(
    repo_root: Path,
    site_data_path: Path,
    target_branch: str = DEFAULT_TARGET_BRANCH,
    remote: str = DEFAULT_REMOTE,
    push: bool = True,
) -> dict[str, str]:
    if not site_data_path.exists():
        raise SiteDataGitError(f"Site data directory does not exist: {site_data_path}")

    assert_clean_outside_site_data(repo_root, site_data_path)

    commit_message = build_commit_message(site_data_path)
    commit_sha = build_commit_with_temp_index(repo_root, site_data_path, commit_message)
    if commit_sha is None:
        return {
            "git_commit_status": "no_changes",
            "target_branch": target_branch,
            "remote": remote,
            "commit_message": commit_message,
        }

    update_local_target_branch(repo_root, target_branch, commit_sha)

    if push:
        ensure_remote_exists(repo_root, remote)
        push_commit_to_remote(repo_root, remote, target_branch, commit_sha)
        git_commit_status = "pushed"
    else:
        git_commit_status = "committed_local_ref_only"

    return {
        "git_commit_status": git_commit_status,
        "target_branch": target_branch,
        "remote": remote,
        "commit_sha": commit_sha,
        "commit_message": commit_message,
        "manual_push_command": (
            f"git push --force-with-lease {remote} "
            f"{commit_sha}:refs/heads/{target_branch}"
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=str(PROJECT_ROOT))
    parser.add_argument("--site-data-path", default=str(SITE_DATA_CURRENT_DIR))
    parser.add_argument("--target-branch", default=DEFAULT_TARGET_BRANCH)
    parser.add_argument("--remote", default=DEFAULT_REMOTE)
    parser.add_argument("--no-push", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = publish_site_data_update(
            repo_root=Path(args.repo_root),
            site_data_path=Path(args.site_data_path),
            target_branch=args.target_branch,
            remote=args.remote,
            push=not args.no_push,
        )
    except (SiteDataGitError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        print(f"[error] {exc}")
        return 1

    print(json.dumps(result, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
