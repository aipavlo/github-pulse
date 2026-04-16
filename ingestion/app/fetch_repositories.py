import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.exceptions import HTTPError, RequestException


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
    parsed = urlparse(url.strip())
    parts = [part for part in parsed.path.strip("/").split("/") if part]

    if parsed.netloc not in {"github.com", "www.github.com"} or len(parts) < 2:
        raise ValueError(f"Invalid GitHub repository URL: {url}")

    if len(parts) != 2:
        raise ValueError(f"Malformed GitHub repository URL: {url}")

    owner, repo = parts[:2]

    if any(marker in repo for marker in ("http://", "https://", "github.com")):
        raise ValueError(f"Malformed GitHub repository URL: {url}")

    return owner, repo


def get_repositories(csv_path, repo_arg):
    if repo_arg:
        return [tuple(repo_arg.strip().split("/", 1))]

    repositories = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("url", "").strip()
            if not url:
                continue

            try:
                repositories.append(parse_repo_url(url))
            except ValueError as exc:
                print(f"[skip] {exc}")

    return repositories


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
    failed = 0

    for owner, repo in repositories:
        repo_dir = snapshot_path(args.raw_root, run_date, owner, repo)

        if not args.force and any(repo_dir.glob("*.json")):
            print(f"[skip] {owner}/{repo}")
            skipped += 1
            continue

        try:
            response = session.get(f"https://api.github.com/repos/{owner}/{repo}", timeout=60)
            response.raise_for_status()
        except HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else "unknown"
            print(f"[skip] {owner}/{repo} -> GitHub API returned {status_code}")
            skipped += 1
            failed += 1
            continue
        except RequestException as exc:
            print(f"[skip] {owner}/{repo} -> request failed: {exc}")
            skipped += 1
            failed += 1
            continue

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
    print(f"failed={failed}")


if __name__ == "__main__":
    main()
