import csv
from pathlib import Path

import requests

from ingestion.app import fetch_repositories
from ingestion.app.github_api import GITHUB_API_VERSION, GITHUB_USER_AGENT

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class DummyResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(
                f"{self.status_code} error",
                response=self,
            )

    def json(self):
        return self._payload


class DummySession:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, timeout):
        self.calls.append((url, timeout))
        owner_repo = url.rsplit("/repos/", 1)[1]
        return self.responses[owner_repo]


def test_fetch_session_uses_shared_github_headers(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "token-123")

    session = fetch_repositories.build_session()

    assert session.headers["Accept"] == "application/vnd.github+json"
    assert session.headers["X-GitHub-Api-Version"] == GITHUB_API_VERSION
    assert session.headers["User-Agent"] == GITHUB_USER_AGENT
    assert session.headers["Authorization"] == "Bearer token-123"


def test_parse_repo_url_rejects_concatenated_urls():
    url = (
        "https://github.com/irppanpilihanrambe/data-pipeline-etl"
        "https://github.com/khalilzaghla/khalilzaghla"
    )

    try:
        fetch_repositories.parse_repo_url(url)
    except ValueError as exc:
        assert "Malformed GitHub repository URL" in str(exc)
    else:
        raise AssertionError("Expected malformed URL to be rejected")


def test_get_repositories_skips_invalid_rows(tmp_path, capsys):
    csv_path = tmp_path / "repositories_urls.csv"
    csv_path.write_text(
        "\n".join(
            [
                "url",
                "https://github.com/example/project-one",
                "https://github.com/example/project-twohttps://github.com/other/project",
                "https://github.com/org/project-three",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    repositories = fetch_repositories.get_repositories(str(csv_path), "")

    assert repositories == [
        ("example", "project-one"),
        ("org", "project-three"),
    ]

    output = capsys.readouterr().out
    assert "Malformed GitHub repository URL" in output


def test_repository_catalog_contains_only_valid_github_urls():
    csv_path = PROJECT_ROOT / "data" / "repositories_urls.csv"
    invalid_rows = []
    parsed_repositories = []

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for line_number, row in enumerate(reader, start=2):
            url = row.get("url", "").strip()
            if not url:
                continue

            try:
                parsed_repositories.append(fetch_repositories.parse_repo_url(url))
            except ValueError as exc:
                invalid_rows.append(f"line {line_number}: {exc}")

    assert parsed_repositories
    assert invalid_rows == []


def test_main_skips_not_found_and_continues(tmp_path, monkeypatch, capsys):
    csv_path = tmp_path / "repositories_urls.csv"
    csv_path.write_text(
        "\n".join(
            [
                "url",
                "https://github.com/good/repo",
                "https://github.com/missing/repo",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    raw_root = tmp_path / "raw"
    session = DummySession(
        {
            "good/repo": DummyResponse(payload={"id": 1, "full_name": "good/repo"}),
            "missing/repo": DummyResponse(status_code=404),
        }
    )

    monkeypatch.setattr(fetch_repositories, "build_session", lambda: session)
    monkeypatch.setattr(
        fetch_repositories,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "csv": str(csv_path),
                "raw_root": str(raw_root),
                "repo": "",
                "run_date": "2026-04-01",
                "force": False,
            },
        )(),
    )

    fetch_repositories.main()

    output = capsys.readouterr().out
    assert "[saved] good/repo" in output
    assert "[skip] missing/repo -> GitHub API returned 404" in output
    assert "saved=1" in output
    assert "skipped=1" in output
    assert "failed=1" in output

    saved_files = list(Path(raw_root).rglob("*.json"))
    assert len(saved_files) == 1
    assert "good__repo__" in saved_files[0].name
