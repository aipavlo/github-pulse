import csv
import json
import pathlib

from publish import export_site_data

ALLOWED_CSV_FIELDS = {
    "snapshot_date",
    "snapshot_month",
    "generated_at",
    "popularity_rank",
    "repo_full_name",
    "owner_login",
    "repo_name",
    "repo_url",
    "owner_group",
    "language",
    "primary_language",
    "topic",
    "description",
    "is_archived",
    "is_fork",
    "repo_count",
    "owner_count",
    "language_count",
    "topic_count",
    "total_stars",
    "avg_stars",
    "median_stars",
    "total_forks",
    "avg_forks",
    "total_watchers",
    "total_open_issues",
    "active_repo_count",
    "archived_repo_count",
    "stale_repo_count",
    "repo_uniq_count",
    "stargazers_count",
    "forks_count",
    "watchers_count",
    "open_issues_count",
    "days_since_push",
    "repo_popularity_score",
}

ALLOWED_BUILD_META_FIELDS = {
    "build_id",
    "generated_at",
    "snapshot_date",
    "source_run_date",
    "dbt_test_passed",
    "datasets_dir",
    "dataset_version",
    "git_commit",
    "git_branch",
    "exporter_version",
}

ALLOWED_MANIFEST_ROOT_FIELDS = {
    "dataset_version",
    "generated_at",
    "snapshot_date",
    "files",
}

ALLOWED_MANIFEST_FILE_FIELDS = {
    "path",
    "sha256",
    "size_bytes",
    "row_count",
    "content_type",
}

FORBIDDEN_PATTERNS = {
    "node_id",
    "owner_id",
    "organization_id",
    "homepage",
    "email",
    "GITHUB_TOKEN",
    "DWH_PASSWORD",
    "CLICKHOUSE_PASSWORD",
    "ghp_",
    "github_pat_",
    "AKIA",
    "-----BEGIN",
    "debug",
}


class DummyClient:
    def __init__(self, dataset_rows):
        self.dataset_rows = dataset_rows
        self.database = "dezc_dwh"

    def query_json(self, query):
        table_name = query.split("FROM ", 1)[1].split("\n", 1)[0].split(".", 1)[1]
        return self.dataset_rows[table_name]


def make_dataset_rows():
    return {
        "publish_site_kpis": [
            {
                "snapshot_date": "2026-04-01",
                "generated_at": "2026-04-18T15:00:00Z",
                "repo_count": 3,
                "owner_count": 2,
                "language_count": 2,
                "topic_count": 2,
                "total_stars": 100,
                "total_forks": 50,
                "active_repo_count": 2,
                "archived_repo_count": 1,
            }
        ],
        "publish_repo_top": [
            {
                "snapshot_date": "2026-04-01",
                "generated_at": "2026-04-18T15:00:00Z",
                "popularity_rank": 1,
                "repo_full_name": "org/repo",
                "owner_login": "org",
                "repo_name": "repo",
                "repo_url": "https://github.com/org/repo",
                "description": "Repository description",
                "primary_language": "Python",
                "stargazers_count": 100,
                "forks_count": 50,
                "watchers_count": 25,
                "days_since_push": 3,
                "repo_popularity_score": 5.1234,
                "is_archived": 0,
                "is_fork": 1,
            }
        ],
        "publish_repo_trend_monthly": [
            {
                "snapshot_month": "2026-04-01",
                "repo_full_name": "org/repo",
                "owner_login": "org",
                "repo_name": "repo",
                "primary_language": "Python",
                "stargazers_count": 100,
                "forks_count": 50,
                "watchers_count": 25,
                "open_issues_count": 10,
                "repo_popularity_score": 5.1234,
            }
        ],
        "publish_owner_summary": [
            {
                "snapshot_date": "2026-04-01",
                "owner_group": "org",
                "repo_count": 3,
                "total_stars": 100,
                "avg_stars": 33.3333,
                "total_forks": 50,
                "total_watchers": 25,
                "total_open_issues": 10,
                "archived_repo_count": 1,
                "active_repo_count": 2,
                "language_count": 2,
            }
        ],
        "publish_language_summary": [
            {
                "snapshot_date": "2026-04-01",
                "language": "Python",
                "repo_count": 2,
                "total_stars": 80,
                "avg_stars": 40.0,
                "median_stars": 40.0,
                "total_forks": 30,
                "avg_forks": 15.0,
                "total_watchers": 20,
                "total_open_issues": 8,
                "archived_repo_count": 1,
                "stale_repo_count": 0,
            }
        ],
        "publish_topic_summary": [
            {
                "snapshot_date": "2026-04-01",
                "topic": "analytics",
                "repo_count": 2,
                "repo_uniq_count": 2,
                "total_stars": 80,
                "avg_stars": 40.0,
                "median_stars": 40.0,
                "total_forks": 30,
                "total_watchers": 20,
                "archived_repo_count": 1,
            }
        ],
    }


def export_fixture(tmp_path: pathlib.Path) -> pathlib.Path:
    client = DummyClient(make_dataset_rows())
    export_site_data.export_datasets(
        client=client,
        output_root=tmp_path,
        build_id="safety-build",
        run_date="2026-04-18",
        dry_run=True,
        only_dataset=None,
        fail_on_empty=False,
    )
    return tmp_path / "_tmp" / "safety-build"


def test_exported_csv_headers_use_only_whitelisted_public_fields(tmp_path):
    export_dir = export_fixture(tmp_path)

    for csv_path in sorted(export_dir.glob("*.csv")):
        with csv_path.open(encoding="utf-8", newline="") as csv_file:
            header = next(csv.reader(csv_file))
        assert set(header).issubset(ALLOWED_CSV_FIELDS), csv_path.name


def test_exported_metadata_contains_only_allowed_fields(tmp_path):
    export_dir = export_fixture(tmp_path)

    build_meta = json.loads((export_dir / "build_meta.json").read_text(encoding="utf-8"))
    manifest = json.loads((export_dir / "manifest.json").read_text(encoding="utf-8"))

    assert set(build_meta).issubset(ALLOWED_BUILD_META_FIELDS)
    assert set(manifest).issubset(ALLOWED_MANIFEST_ROOT_FIELDS)
    for file_entry in manifest["files"]:
        assert set(file_entry).issubset(ALLOWED_MANIFEST_FILE_FIELDS)


def test_exported_artifacts_do_not_contain_forbidden_patterns(tmp_path):
    export_dir = export_fixture(tmp_path)

    for file_path in sorted(export_dir.iterdir()):
        content = file_path.read_text(encoding="utf-8")
        lower_content = content.lower()
        for pattern in FORBIDDEN_PATTERNS:
            assert pattern.lower() not in lower_content, (
                f"{file_path.name}: found forbidden pattern {pattern}"
            )


def test_build_meta_does_not_expose_env_or_debug_fields(tmp_path):
    export_dir = export_fixture(tmp_path)
    build_meta = json.loads((export_dir / "build_meta.json").read_text(encoding="utf-8"))

    assert "debug" not in build_meta
    assert "env" not in build_meta
    assert "token" not in build_meta
    assert "password" not in build_meta
    assert "secret" not in build_meta
    assert build_meta["datasets_dir"].endswith("current") is False
    assert "token" not in build_meta["datasets_dir"].lower()
    assert "password" not in build_meta["datasets_dir"].lower()
    assert "secret" not in build_meta["datasets_dir"].lower()
