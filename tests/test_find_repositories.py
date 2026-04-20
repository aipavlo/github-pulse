import requests

from ingestion.app import find_repositories
from ingestion.app.github_api import GITHUB_API_VERSION, GITHUB_USER_AGENT


class DummySearchResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {"items": []}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(
                f"{self.status_code} error",
                response=self,
            )

    def json(self):
        return self._payload


class DummySearchSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append((url, params, timeout))
        return self.responses.pop(0)


def test_find_and_fetch_sessions_use_same_github_headers(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "token-123")

    session = find_repositories.build_session()

    assert session.headers["Accept"] == "application/vnd.github+json"
    assert session.headers["X-GitHub-Api-Version"] == GITHUB_API_VERSION
    assert session.headers["User-Agent"] == GITHUB_USER_AGENT
    assert session.headers["Authorization"] == "Bearer token-123"


def test_search_urls_returns_html_urls_from_github_search():
    session = DummySearchSession(
        [
            DummySearchResponse(
                payload={
                    "items": [
                        {"html_url": "https://github.com/example/one"},
                        {"html_url": "https://github.com/example/two"},
                        {"ignored": "missing-url"},
                    ]
                }
            )
        ]
    )

    urls = find_repositories.search_urls("topic:data", session=session)

    assert urls == [
        "https://github.com/example/one",
        "https://github.com/example/two",
    ]
    assert session.calls == [
        (
            find_repositories.SEARCH_URL,
            {
                "q": "topic:data",
                "sort": "updated",
                "order": "desc",
                "per_page": find_repositories.PER_PAGE,
                "page": 1,
            },
            60,
        )
    ]


def test_search_urls_logs_http_failure_and_returns_partial_results(capsys):
    session = DummySearchSession([DummySearchResponse(status_code=403)])

    urls = find_repositories.search_urls("topic:data", session=session)

    output = capsys.readouterr().out
    assert urls == []
    assert "GitHub API returned 403" in output


def test_search_urls_logs_request_failure_and_returns_partial_results(capsys):
    class FailingSession:
        def get(self, url, params, timeout):
            raise requests.exceptions.Timeout("slow")

    urls = find_repositories.search_urls("topic:data", session=FailingSession())

    output = capsys.readouterr().out
    assert urls == []
    assert "request failed: slow" in output
