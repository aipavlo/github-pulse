import importlib
import json
import sys
import types
from datetime import date

import pytest


def _prefect_task(*decorator_args, **decorator_kwargs):
    def decorator(func):
        func.fn = func
        return func

    is_direct_decorator_call = (
        decorator_args
        and callable(decorator_args[0])
        and len(decorator_args) == 1
        and not decorator_kwargs
    )
    if is_direct_decorator_call:
        return decorator(decorator_args[0])
    return decorator


def _prefect_flow(*decorator_args, **decorator_kwargs):
    def decorator(func):
        return func

    is_direct_decorator_call = (
        decorator_args
        and callable(decorator_args[0])
        and len(decorator_args) == 1
        and not decorator_kwargs
    )
    if is_direct_decorator_call:
        return decorator(decorator_args[0])
    return decorator


sys.modules.setdefault(
    "prefect",
    types.SimpleNamespace(flow=_prefect_flow, task=_prefect_task),
)

prefect_flow = importlib.import_module("orchestration.prefect_flow")


def test_prefect_flow_has_no_git_publish_task():
    assert not hasattr(prefect_flow, "commit_site_data")
    assert "commit_site_data_module" not in vars(prefect_flow)


def test_parse_args_supports_publish_export_flags(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prefect_flow.py",
            "--run-date",
            "2026-04-01",
            "--skip-find",
            "--skip-publish-export",
            "--fail-on-empty",
        ],
    )

    args = prefect_flow.parse_args()

    assert args.run_date == "2026-04-01"
    assert args.skip_find is True
    assert args.skip_publish_export is True
    assert args.fail_on_empty is True


def test_prepare_site_export_dir_creates_context(monkeypatch, tmp_path):
    monkeypatch.setattr(prefect_flow, "SITE_DATA_ROOT", tmp_path)

    context = prefect_flow.prepare_site_export_dir.fn()

    assert context["output_root"] == str(tmp_path)
    assert context["current_dir"] == str(tmp_path / "current")
    assert context["tmp_export_dir"].startswith(str(tmp_path / "_tmp"))
    assert (tmp_path / "_tmp").exists()


def test_default_run_date_uses_first_day_of_current_month(monkeypatch):
    class FixedDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 4, 18)

    monkeypatch.setattr(prefect_flow, "date", FixedDate)

    assert prefect_flow.default_run_date() == "2026-04-01"


def test_validate_site_export_logs_manifest(tmp_path, capsys):
    export_dir = tmp_path / "_tmp" / "build123"
    export_dir.mkdir(parents=True)
    (export_dir / "build_meta.json").write_text(
        json.dumps(
            {
                "build_id": "build123",
                "generated_at": "2026-04-18T15:00:00Z",
                "snapshot_date": "2026-04-01",
                "source_run_date": "2026-04-18",
                "dbt_test_passed": True,
                "datasets_dir": str(tmp_path / "current"),
                "dataset_version": 1,
            }
        ),
        encoding="utf-8",
    )
    (export_dir / "manifest.json").write_text(
        json.dumps(
            {
                "dataset_version": 1,
                "generated_at": "2026-04-18T15:00:00Z",
                "snapshot_date": "2026-04-01",
                "files": [
                    {
                        "path": "repo_top.csv",
                        "sha256": "abc",
                        "size_bytes": 123,
                        "row_count": 7,
                        "content_type": "text/csv",
                    },
                    {
                        "path": "build_meta.json",
                        "sha256": "def",
                        "size_bytes": 55,
                        "content_type": "application/json",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    result = prefect_flow.validate_site_export.fn(
        {
            "output_dir": str(export_dir),
            "current_dir": str(tmp_path / "current"),
            "output_root": str(tmp_path),
        }
    )

    output = capsys.readouterr().out
    assert "site export file: repo_top.csv size_bytes=123 row_count=7" in output
    assert "site export file: build_meta.json size_bytes=55 row_count=n/a" in output
    assert result["build_meta"]["dbt_test_passed"] is True


def test_replace_site_data_atomically_promotes_tmp_export(monkeypatch, tmp_path):
    output_root = tmp_path
    current_dir = output_root / "current"
    current_dir.mkdir()
    (current_dir / "old.txt").write_text("old", encoding="utf-8")

    tmp_export_dir = output_root / "_tmp" / "build123"
    tmp_export_dir.mkdir(parents=True)
    (tmp_export_dir / "new.txt").write_text("new", encoding="utf-8")

    result = prefect_flow.replace_site_data_atomically.fn(
        {
            "output_dir": str(tmp_export_dir),
            "current_dir": str(current_dir),
            "output_root": str(output_root),
        }
    )

    assert (current_dir / "new.txt").exists()
    assert not (current_dir / "old.txt").exists()
    assert result["output_dir"] == str(current_dir)


def test_flow_skips_publish_export_when_flag_enabled(monkeypatch):
    calls = []

    monkeypatch.setattr(prefect_flow, "find_repositories", lambda: calls.append("find"))
    monkeypatch.setattr(
        prefect_flow,
        "fetch_repositories",
        lambda run_date: calls.append(("fetch", run_date)),
    )
    monkeypatch.setattr(prefect_flow, "dbt_deps", lambda: calls.append("dbt_deps"))
    monkeypatch.setattr(prefect_flow, "dbt_run", lambda: calls.append("dbt_run"))
    monkeypatch.setattr(prefect_flow, "dbt_test", lambda: calls.append("dbt_test"))
    monkeypatch.setattr(prefect_flow, "prepare_site_export_dir", lambda: calls.append("prepare"))
    monkeypatch.setattr(
        prefect_flow,
        "export_site_data",
        lambda *args, **kwargs: calls.append("export"),
    )

    prefect_flow.github_repository_radar_flow(
        run_date="2026-04-01",
        refresh_urls=True,
        skip_publish_export=True,
        fail_on_empty=False,
    )

    assert calls == [
        "find",
        ("fetch", "2026-04-01"),
        "dbt_deps",
        "dbt_run",
        "dbt_test",
    ]


def test_flow_does_not_start_export_chain_when_dbt_test_fails(monkeypatch):
    calls = []

    monkeypatch.setattr(prefect_flow, "find_repositories", lambda: calls.append("find"))
    monkeypatch.setattr(
        prefect_flow,
        "fetch_repositories",
        lambda run_date: calls.append(("fetch", run_date)),
    )
    monkeypatch.setattr(prefect_flow, "dbt_deps", lambda: calls.append("dbt_deps"))
    monkeypatch.setattr(prefect_flow, "dbt_run", lambda: calls.append("dbt_run"))

    def fail_dbt_test():
        calls.append("dbt_test")
        raise RuntimeError("dbt test failed")

    monkeypatch.setattr(prefect_flow, "dbt_test", fail_dbt_test)
    monkeypatch.setattr(prefect_flow, "prepare_site_export_dir", lambda: calls.append("prepare"))
    monkeypatch.setattr(
        prefect_flow,
        "export_site_data",
        lambda *_args, **_kwargs: calls.append("export"),
    )

    with pytest.raises(RuntimeError, match="dbt test failed"):
        prefect_flow.github_repository_radar_flow(
            run_date="2026-04-01",
            refresh_urls=False,
            skip_publish_export=False,
            fail_on_empty=False,
        )

    assert calls == [
        ("fetch", "2026-04-01"),
        "dbt_deps",
        "dbt_run",
        "dbt_test",
    ]


def test_flow_runs_export_chain_after_dbt_test(monkeypatch):
    calls = []

    monkeypatch.setattr(prefect_flow, "find_repositories", lambda: calls.append("find"))
    monkeypatch.setattr(
        prefect_flow,
        "fetch_repositories",
        lambda run_date: calls.append(("fetch", run_date)),
    )
    monkeypatch.setattr(prefect_flow, "dbt_deps", lambda: calls.append("dbt_deps"))
    monkeypatch.setattr(prefect_flow, "dbt_run", lambda: calls.append("dbt_run"))
    monkeypatch.setattr(prefect_flow, "dbt_test", lambda: calls.append("dbt_test"))
    monkeypatch.setattr(
        prefect_flow,
        "prepare_site_export_dir",
        lambda: calls.append("prepare")
        or {
            "build_id": "build123",
            "output_root": "/tmp",
            "current_dir": "/tmp/current",
        },
    )
    monkeypatch.setattr(
        prefect_flow,
        "export_site_data",
        lambda export_context, run_date, fail_on_empty: calls.append(
            ("export", export_context["build_id"], run_date, fail_on_empty)
        )
        or {
            "output_dir": "/tmp/_tmp/build123",
            "current_dir": "/tmp/current",
            "output_root": "/tmp",
        },
    )
    monkeypatch.setattr(
        prefect_flow,
        "validate_site_export",
        lambda export_result: calls.append(("validate", export_result["output_dir"]))
        or export_result,
    )
    monkeypatch.setattr(
        prefect_flow,
        "replace_site_data_atomically",
        lambda validated_export: calls.append(("replace", validated_export["output_dir"]))
        or validated_export,
    )
    prefect_flow.github_repository_radar_flow(
        run_date="2026-04-01",
        refresh_urls=False,
        skip_publish_export=False,
        fail_on_empty=True,
    )

    assert calls == [
        ("fetch", "2026-04-01"),
        "dbt_deps",
        "dbt_run",
        "dbt_test",
        "prepare",
        ("export", "build123", "2026-04-01", True),
        ("validate", "/tmp/_tmp/build123"),
        ("replace", "/tmp/_tmp/build123"),
    ]


def test_flow_does_not_call_git_publish_step(monkeypatch):
    calls = []

    monkeypatch.setattr(
        prefect_flow,
        "fetch_repositories",
        lambda run_date: calls.append(("fetch", run_date)),
    )
    monkeypatch.setattr(prefect_flow, "dbt_deps", lambda: calls.append("dbt_deps"))
    monkeypatch.setattr(prefect_flow, "dbt_run", lambda: calls.append("dbt_run"))
    monkeypatch.setattr(prefect_flow, "dbt_test", lambda: calls.append("dbt_test"))
    monkeypatch.setattr(
        prefect_flow,
        "prepare_site_export_dir",
        lambda: {"build_id": "build123", "output_root": "/tmp", "current_dir": "/tmp/current"},
    )
    monkeypatch.setattr(
        prefect_flow,
        "export_site_data",
        lambda *_args, **_kwargs: {
            "output_dir": "/tmp/_tmp/build123",
            "current_dir": "/tmp/current",
            "output_root": "/tmp",
        },
    )
    monkeypatch.setattr(prefect_flow, "validate_site_export", lambda export_result: export_result)
    monkeypatch.setattr(
        prefect_flow,
        "replace_site_data_atomically",
        lambda validated_export: calls.append(("replace", validated_export["output_dir"]))
        or validated_export,
    )

    prefect_flow.github_repository_radar_flow(
        run_date="2026-04-01",
        refresh_urls=False,
        skip_publish_export=False,
        fail_on_empty=False,
    )

    assert calls == [
        ("fetch", "2026-04-01"),
        "dbt_deps",
        "dbt_run",
        "dbt_test",
        ("replace", "/tmp/_tmp/build123"),
    ]


def test_full_flow_smoke_with_mock_commands(monkeypatch, tmp_path):
    commands = []

    def fake_run_command(command, cwd):
        commands.append((tuple(command), str(cwd)))

    monkeypatch.setattr(prefect_flow, "run_command", fake_run_command)
    monkeypatch.setattr(prefect_flow, "SITE_DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        prefect_flow.export_site_data_module,
        "build_client_from_env",
        lambda: object(),
    )

    def fake_export_datasets(**kwargs):
        output_root = kwargs["output_root"]
        build_id = kwargs["build_id"]
        export_dir = output_root / "_tmp" / build_id
        export_dir.mkdir(parents=True, exist_ok=False)
        (export_dir / "build_meta.json").write_text(
            json.dumps(
                {
                    "build_id": build_id,
                    "generated_at": "2026-04-18T15:00:00Z",
                    "snapshot_date": "2026-04-01",
                    "source_run_date": kwargs["run_date"],
                    "dbt_test_passed": True,
                    "datasets_dir": str(output_root / "current"),
                    "dataset_version": 1,
                }
            ),
            encoding="utf-8",
        )
        (export_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "dataset_version": 1,
                    "generated_at": "2026-04-18T15:00:00Z",
                    "snapshot_date": "2026-04-01",
                    "files": [
                        {
                            "path": "site_kpis.csv",
                            "sha256": "abc",
                            "size_bytes": 12,
                            "row_count": 1,
                            "content_type": "text/csv",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        (export_dir / "site_kpis.csv").write_text("snapshot_date\n2026-04-01\n", encoding="utf-8")
        return {
            "build_id": build_id,
            "snapshot_date": "2026-04-01",
            "output_dir": str(export_dir),
            "files": [],
            "dry_run": True,
            "no_op": False,
        }

    monkeypatch.setattr(
        prefect_flow.export_site_data_module,
        "export_datasets",
        fake_export_datasets,
    )
    result = prefect_flow.github_repository_radar_flow(
        run_date="2026-04-01",
        refresh_urls=True,
        skip_publish_export=False,
        fail_on_empty=False,
    )

    assert commands == [
        (("python", "-m", "ingestion.app.find_repositories"), str(prefect_flow.PROJECT_ROOT)),
        (
            (
                "python",
                "-m",
                "ingestion.app.fetch_repositories",
                "--run-date",
                "2026-04-01",
            ),
            str(prefect_flow.PROJECT_ROOT),
        ),
        (("dbt", "deps"), str(prefect_flow.DBT_DIR)),
        (("dbt", "run"), str(prefect_flow.DBT_DIR)),
        (("dbt", "test"), str(prefect_flow.DBT_DIR)),
    ]
    assert (tmp_path / "current" / "site_kpis.csv").exists()
    assert result is None
