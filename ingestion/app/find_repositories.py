import csv
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

OUTPUT_FILE = Path("data/repositories_urls.csv")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
SEARCH_URL = "https://api.github.com/search/repositories"

MIN_STARS = 10000
DAYS_BACK = 365
PER_PAGE = 10
MAX_PAGES = 1

HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2026-03-10",
    "User-Agent": "de-repo-url-registry",
}

if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN}"


def build_queries():
    since = (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).date().isoformat()
    return [
        # f"topic:data-engineering archived:false fork:false stars:>={MIN_STARS} pushed:>={since}",
        # f'"data engineering" in:readme archived:false fork:false pushed:>={since}',
        f'"data warehouse" in:readme archived:false fork:false pushed:>={since}',
    ]


def ensure_output_file():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not OUTPUT_FILE.exists():
        with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(["url"])


def read_existing_urls():
    ensure_output_file()
    existing_urls = set()

    with OUTPUT_FILE.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if row and row[0].strip():
                existing_urls.add(row[0].strip())

    return existing_urls


def search_urls(query):
    found_urls = []
    for page in range(1, MAX_PAGES + 1):
        response = requests.get(
            SEARCH_URL,
            headers=HEADERS,
            params={
                "q": query,
                "sort": "updated",
                "order": "desc",
                "per_page": PER_PAGE,
                "page": page,
            },
            timeout=60,
        )
        response.raise_for_status()

        items = response.json().get("items", [])
        if not items:
            break

        for item in items:
            url = item.get("html_url")
            if url:
                found_urls.append(url)

        if len(items) < PER_PAGE:
            break

    return found_urls


def append_new_urls(urls):
    existing_urls = read_existing_urls()
    added = 0

    with OUTPUT_FILE.open("a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)

        for url in urls:
            if url not in existing_urls:
                writer.writerow([url])
                existing_urls.add(url)
                added += 1

    return added


def collect_urls():
    collected_urls = []
    seen_urls = set()

    for query in build_queries():
        print(f"search: {query}")
        for url in search_urls(query):
            if url in seen_urls:
                continue

            seen_urls.add(url)
            collected_urls.append(url)

    return collected_urls


def main():
    all_urls = collect_urls()

    added = append_new_urls(all_urls)

    print(f"File: {OUTPUT_FILE}")
    print(f"Found: {len(all_urls)}")
    print(f"Add new: {added}")
    print(f"Existed: {len(all_urls) - added}")


if __name__ == "__main__":
    main()
