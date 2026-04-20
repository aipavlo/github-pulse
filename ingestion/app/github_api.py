import os

import requests

GITHUB_API_VERSION = "2022-11-28"
GITHUB_ACCEPT_HEADER = "application/vnd.github+json"
GITHUB_USER_AGENT = "open-source-de-ecosystem-radar"


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
    return session
