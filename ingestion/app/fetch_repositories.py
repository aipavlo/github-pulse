import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="data/repositories_urls.csv")
    parser.add_argument("--raw-root", default="data/raw/repositories")
    parser.add_argument("--repo", default="")
    parser.add_argument("--run-date", default=datetime.now(timezone.utc).strftime("%Y-%m-01"))
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def normalize_run_date(value):
    return value[:8] + "01"


def parse_repo_url(url):
    owner, repo = urlparse(url.strip()).path.strip("/").split("/")[:2]
    return owner, repo


def get_repositories(csv_path, repo_arg):
    if repo_arg:
        return [tuple(repo_arg.strip().split("/", 1))]

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [
            parse_repo_url(row["url"])
            for row in reader
            if row.get("url", "").strip()
        ]


def build_session():
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "open-source-de-ecosystem-radar",
        }
    )
    if token:
        session.headers["Authorization"] = f"Bearer {token}"

    return session


def snapshot_path(raw_root, run_date, owner, repo):
    return Path(raw_root) / f"extract_date={run_date}" / f"owner={owner}" / f"repo={repo}"


def build_snapshot_payload(owner, repo, run_date, fetched_at, payload):
    return {
        "_meta": {
            "entity": "repository",
            "owner": owner,
            "repo": repo,
            "full_name": f"{owner}/{repo}",
            "run_date": run_date,
            "fetched_at": fetched_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        },
        "data": payload,
    }


def main():
    args = parse_args()
    run_date = normalize_run_date(args.run_date)
    session = build_session()
    repositories = get_repositories(args.csv, args.repo)
    saved = 0
    skipped = 0

    for owner, repo in repositories:
        repo_dir = snapshot_path(args.raw_root, run_date, owner, repo)

        if not args.force and any(repo_dir.glob("*.json")):
            print(f"[skip] {owner}/{repo}")
            skipped += 1
            continue

        response = session.get(f"https://api.github.com/repos/{owner}/{repo}", timeout=60)
        response.raise_for_status()
        payload = response.json()

        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y%m%dT%H%M%SZ")

        repo_dir.mkdir(parents=True, exist_ok=True)
        file_path = repo_dir / f"{owner}__{repo}__{timestamp}.json"
        result = build_snapshot_payload(owner, repo, run_date, now, payload)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
            f.write("\n")

        print(f"[saved] {owner}/{repo} -> {file_path}")
        saved += 1

    print()
    print(f"total={len(repositories)}")
    print(f"saved={saved}")
    print(f"skipped={skipped}")


if __name__ == "__main__":
    main()
