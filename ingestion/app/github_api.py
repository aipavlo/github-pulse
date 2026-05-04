import os
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

GITHUB_API_VERSION = "2022-11-28"
GITHUB_ACCEPT_HEADER = "application/vnd.github+json"
GITHUB_USER_AGENT = "open-source-de-ecosystem-radar"
GITHUB_RETRY_TOTAL = 4
GITHUB_RETRY_BACKOFF_FACTOR = 1.0


def build_github_headers(token=None):
    resolved_token = os.environ.get("GITHUB_TOKEN", "").strip() if token is None else token.strip()
    headers = {
        "Accept": GITHUB_ACCEPT_HEADER,
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
        "User-Agent": GITHUB_USER_AGENT,
    }
    if resolved_token:
        headers["Authorization"] = f"Bearer {resolved_token}"

    return headers


def build_github_session(token=None):
    session = requests.Session()
    session.headers.update(build_github_headers(token=token))
    retry = Retry(
        total=GITHUB_RETRY_TOTAL,
        read=GITHUB_RETRY_TOTAL,
        connect=GITHUB_RETRY_TOTAL,
        backoff_factor=GITHUB_RETRY_BACKOFF_FACTOR,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def describe_github_rate_limit(response):
    if response is None:
        return None

    status_code = response.status_code
    retry_after = response.headers.get("Retry-After")
    remaining = response.headers.get("X-RateLimit-Remaining")
    reset_at = response.headers.get("X-RateLimit-Reset")
    is_rate_limited = status_code == 429 or retry_after is not None or remaining == "0"

    if not is_rate_limited:
        return None

    details = [f"GitHub API rate limit reached (status {status_code})"]
    if retry_after is not None:
        details.append(f"retry after {retry_after}s")
    elif reset_at:
        try:
            reset_dt = datetime.fromtimestamp(int(reset_at), tz=timezone.utc)
            details.append(f"reset at {reset_dt.strftime('%Y-%m-%d %H:%M:%SZ')}")
        except ValueError:
            details.append(f"reset at {reset_at}")

    return ", ".join(details)
