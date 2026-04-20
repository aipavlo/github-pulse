import csv
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from publish import export_site_data


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


def read_csv_rows(csv_path: Path):
    with csv_path.open(encoding="utf-8", newline="") as csv_file:
        return list(csv.reader(csv_file))


def test_export_datasets_dry_run_writes_expected_files(tmp_path):
    client = DummyClient(make_dataset_rows())

    result = export_site_data.export_datasets(
        client=client,
        output_root=tmp_path,
        build_id="build123",
        run_date="2026-04-18",
        dry_run=True,
        only_dataset=None,
        fail_on_empty=False,
    )

    export_dir = tmp_path / "_tmp" / "build123"
    assert result["dry_run"] is True
    assert result["no_op"] is False
    assert export_dir.exists()
    assert (export_dir / "build_meta.json").exists()
    assert (export_dir / "manifest.json").exists()

    repo_top_rows = read_csv_rows(export_dir / "repo_top.csv")
    assert repo_top_rows[0] == [
        "snapshot_date",
        "generated_at",
        "popularity_rank",
        "repo_full_name",
        "owner_login",
        "repo_name",
        "repo_url",
        "description",
        "primary_language",
        "stargazers_count",
        "forks_count",
        "watchers_count",
        "days_since_push",
        "repo_popularity_score",
        "is_archived",
        "is_fork",
    ]
    assert repo_top_rows[1][-2:] == ["false", "true"]

    build_meta = json.loads((export_dir / "build_meta.json").read_text(encoding="utf-8"))
    assert build_meta["build_id"] == "build123"
    assert build_meta["dbt_test_passed"] is True
    assert build_meta["snapshot_date"] == "2026-04-01"
    assert build_meta["source_run_date"] == "2026-04-18"
    assert build_meta["dataset_version"] == export_site_data.DATASET_VERSION
    assert build_meta["exporter_version"] == "1"

    manifest = json.loads((export_dir / "manifest.json").read_text(encoding="utf-8"))
    assert {entry["path"] for entry in manifest["files"]} == {
        "site_kpis.csv",
        "repo_top.csv",
        "repo_trend_monthly.csv",
        "owner_summary.csv",
        "language_summary.csv",
        "topic_summary.csv",
        "build_meta.json",
        "manifest.json",
    }
    repo_top_manifest = next(
        entry for entry in manifest["files"] if entry["path"] == "repo_top.csv"
    )
    assert repo_top_manifest["row_count"] == 1
    assert repo_top_manifest["content_type"] == "text/csv"
    build_meta_manifest = next(
        entry for entry in manifest["files"] if entry["path"] == "build_meta.json"
    )
    assert build_meta_manifest["content_type"] == "application/json"


def test_export_datasets_rejects_invalid_schema_shape(tmp_path):
    dataset_rows = make_dataset_rows()
    dataset_rows["publish_repo_top"] = [
        {
            "snapshot_date": "2026-04-01",
            "generated_at": "2026-04-18T15:00:00Z",
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
            "popularity_rank": 1,
        }
    ]
    client = DummyClient(dataset_rows)

    with pytest.raises(export_site_data.ExportContractError, match="expected columns"):
        export_site_data.export_datasets(
            client=client,
            output_root=tmp_path,
            build_id="bad-shape",
            run_date="2026-04-18",
            dry_run=True,
            only_dataset=None,
            fail_on_empty=False,
        )


def test_export_datasets_rejects_only_without_dry_run(tmp_path):
    client = DummyClient(make_dataset_rows())

    with pytest.raises(export_site_data.ExportContractError):
        export_site_data.export_datasets(
            client=client,
            output_root=tmp_path,
            build_id="build123",
            run_date="2026-04-18",
            dry_run=False,
            only_dataset="repo_top",
            fail_on_empty=False,
        )


def test_export_datasets_fail_on_empty(tmp_path):
    dataset_rows = make_dataset_rows()
    dataset_rows["publish_topic_summary"] = []
    client = DummyClient(dataset_rows)

    with pytest.raises(export_site_data.ExportContractError):
        export_site_data.export_datasets(
            client=client,
            output_root=tmp_path,
            build_id="build123",
            run_date="2026-04-18",
            dry_run=True,
            only_dataset=None,
            fail_on_empty=True,
        )


def test_export_datasets_rejects_sharp_dataset_growth(tmp_path):
    current_dir = tmp_path / "current"
    current_dir.mkdir(parents=True)
    (current_dir / "manifest.json").write_text(
        json.dumps(
            {
                "dataset_version": 1,
                "generated_at": "2026-04-18T15:00:00Z",
                "snapshot_date": "2026-04-01",
                "files": [
                    {
                        "path": "repo_top.csv",
                        "sha256": "abc",
                        "size_bytes": 1,
                        "row_count": 1,
                        "content_type": "text/csv",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    dataset_rows = make_dataset_rows()
    dataset_rows["publish_repo_top"] = []
    for index in range(1505):
        dataset_rows["publish_repo_top"].append(
            {
                "snapshot_date": "2026-04-01",
                "generated_at": "2026-04-18T15:00:00Z",
                "popularity_rank": index + 1,
                "repo_full_name": f"org/repo-{index}",
                "owner_login": "org",
                "repo_name": f"repo-{index}",
                "repo_url": f"https://github.com/org/repo-{index}",
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
        )
    client = DummyClient(dataset_rows)

    with pytest.raises(export_site_data.ExportContractError):
        export_site_data.export_datasets(
            client=client,
            output_root=tmp_path,
            build_id="build-growth",
            run_date="2026-04-18",
            dry_run=True,
            only_dataset=None,
            fail_on_empty=False,
        )


def test_replace_current_dir_removes_orphan_files(tmp_path):
    current_dir = tmp_path / "current"
    current_dir.mkdir()
    (current_dir / "orphan.txt").write_text("orphan", encoding="utf-8")

    tmp_export_dir = tmp_path / "_tmp" / "build123"
    tmp_export_dir.mkdir(parents=True)
    (tmp_export_dir / "site_kpis.csv").write_text("snapshot_date\n2026-04-01\n", encoding="utf-8")

    export_site_data.replace_current_dir(tmp_export_dir, current_dir)

    assert (current_dir / "site_kpis.csv").exists()
    assert not (current_dir / "orphan.txt").exists()


def test_export_datasets_non_dry_run_replaces_current_and_cleans_tmp(tmp_path):
    client = DummyClient(make_dataset_rows())
    tmp_root = tmp_path / "_tmp"
    tmp_root.mkdir(parents=True)
    stale_tmp = tmp_root / "old-build"
    stale_tmp.mkdir()
    (stale_tmp / "stale.txt").write_text("stale", encoding="utf-8")

    current_dir = tmp_path / "current"
    current_dir.mkdir()
    (current_dir / "orphan.txt").write_text("orphan", encoding="utf-8")

    result = export_site_data.export_datasets(
        client=client,
        output_root=tmp_path,
        build_id="publish-build",
        run_date="2026-04-18",
        dry_run=False,
        only_dataset=None,
        fail_on_empty=False,
    )

    assert result["output_dir"] == str(current_dir)
    assert (current_dir / "site_kpis.csv").exists()
    assert not (current_dir / "orphan.txt").exists()
    assert not stale_tmp.exists()
    assert not (tmp_root / "publish-build").exists()


def test_export_datasets_sets_no_op_when_csv_payload_is_unchanged(tmp_path):
    client = DummyClient(make_dataset_rows())

    export_site_data.export_datasets(
        client=client,
        output_root=tmp_path,
        build_id="baseline-build",
        run_date="2026-04-18",
        dry_run=False,
        only_dataset=None,
        fail_on_empty=False,
    )

    second_result = export_site_data.export_datasets(
        client=client,
        output_root=tmp_path,
        build_id="repeat-build",
        run_date="2026-04-19",
        dry_run=True,
        only_dataset=None,
        fail_on_empty=False,
    )

    assert second_result["no_op"] is True


def test_main_returns_non_zero_on_export_contract_error(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(
        export_site_data,
        "parse_args",
        lambda: SimpleNamespace(
            output_root=str(tmp_path),
            run_date="2026-04-18",
            build_id="build123",
            dry_run=True,
            only=None,
            fail_on_empty=False,
        ),
    )
    monkeypatch.setattr(export_site_data, "build_client_from_env", lambda: object())

    def raise_contract_error(**_kwargs):
        raise export_site_data.ExportContractError("boom")

    monkeypatch.setattr(export_site_data, "export_datasets", raise_contract_error)

    assert export_site_data.main() == 1
    assert "[error] boom" in capsys.readouterr().out
