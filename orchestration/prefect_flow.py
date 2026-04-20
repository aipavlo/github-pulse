import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from prefect import flow, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from publish import export_site_data as export_site_data_module  # noqa: E402

DBT_DIR = PROJECT_ROOT / "dbt"
SITE_DATA_ROOT = PROJECT_ROOT / "evidence" / "sources" / "site_data"


def default_run_date() -> str:
    return date.today().replace(day=1).isoformat()


def run_command(command, cwd):
    print(f"$ {' '.join(command)}")
    subprocess.run(command, cwd=cwd, check=True)


@task(log_prints=True)
def find_repositories():
    run_command(["python", "ingestion/app/find_repositories.py"], cwd=PROJECT_ROOT)


@task(log_prints=True)
def fetch_repositories(run_date):
    run_command(
        ["python", "ingestion/app/fetch_repositories.py", "--run-date", run_date],
        cwd=PROJECT_ROOT,
    )


@task(log_prints=True)
def dbt_deps():
    run_command(["dbt", "deps"], cwd=DBT_DIR)


@task(log_prints=True)
def dbt_run():
    run_command(["dbt", "run"], cwd=DBT_DIR)


@task(log_prints=True)
def dbt_test():
    run_command(["dbt", "test"], cwd=DBT_DIR)


@task(log_prints=True)
def prepare_site_export_dir():
    build_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_root = SITE_DATA_ROOT
    (output_root / "_tmp").mkdir(parents=True, exist_ok=True)
    context = {
        "build_id": build_id,
        "output_root": str(output_root),
        "current_dir": str(output_root / "current"),
        "tmp_export_dir": str(output_root / "_tmp" / build_id),
    }
    print(f"Prepared site export context: {json.dumps(context, ensure_ascii=True)}")
    return context


@task(log_prints=True)
def export_site_data(export_context, run_date, fail_on_empty=False):
    client = export_site_data_module.build_client_from_env()
    export_result = export_site_data_module.export_datasets(
        client=client,
        output_root=Path(export_context["output_root"]),
        build_id=export_context["build_id"],
        run_date=run_date,
        dry_run=True,
        only_dataset=None,
        fail_on_empty=fail_on_empty,
    )
    result = {**export_context, **export_result}
    print(f"Export completed in dry-run mode: {json.dumps(result, ensure_ascii=True)}")
    return result


@task(log_prints=True)
def validate_site_export(export_result):
    output_dir = Path(export_result["output_dir"])
    build_meta_path = output_dir / "build_meta.json"
    manifest_path = output_dir / "manifest.json"

    if not build_meta_path.exists() or not manifest_path.exists():
        raise RuntimeError("Export validation failed: build_meta.json or manifest.json is missing")

    build_meta = json.loads(build_meta_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    if build_meta.get("dbt_test_passed") is not True:
        raise RuntimeError("Export validation failed: dbt_test_passed must be true")

    files = manifest.get("files", [])
    if not files:
        raise RuntimeError("Export validation failed: manifest file list is empty")

    for file_info in files:
        row_count = file_info.get("row_count")
        row_count_text = row_count if row_count is not None else "n/a"
        print(
            f"site export file: {file_info['path']} "
            f"size_bytes={file_info['size_bytes']} row_count={row_count_text}"
        )

    return {
        **export_result,
        "build_meta": build_meta,
        "manifest": manifest,
    }


@task(log_prints=True)
def replace_site_data_atomically(validated_export):
    tmp_export_dir = Path(validated_export["output_dir"])
    current_dir = Path(validated_export["current_dir"])
    tmp_root = Path(validated_export["output_root"]) / "_tmp"

    export_site_data_module.replace_current_dir(tmp_export_dir, current_dir)
    export_site_data_module.clean_tmp_root(tmp_root)

    result = {
        **validated_export,
        "output_dir": str(current_dir),
        "current_dir": str(current_dir),
    }
    print(f"Replaced current site data directory: {current_dir}")
    return result


@flow(name="github-repository-radar")
def github_repository_radar_flow(
    run_date=None,
    refresh_urls=True,
    skip_publish_export=False,
    fail_on_empty=False,
):
    os.environ["DBT_PROFILES_DIR"] = str(DBT_DIR)
    run_date = run_date or default_run_date()

    if refresh_urls:
        find_repositories()

    fetch_repositories(run_date)
    dbt_deps()
    dbt_run()
    dbt_test()

    if skip_publish_export:
        print("Skipping publish export because --skip-publish-export is enabled.")
        return

    export_context = prepare_site_export_dir()
    export_result = export_site_data(export_context, run_date=run_date, fail_on_empty=fail_on_empty)
    validated_export = validate_site_export(export_result)
    replace_site_data_atomically(validated_export)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=None)
    parser.add_argument("--skip-find", action="store_true")
    parser.add_argument("--skip-publish-export", action="store_true")
    parser.add_argument("--fail-on-empty", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    github_repository_radar_flow(
        run_date=args.run_date,
        refresh_urls=not args.skip_find,
        skip_publish_export=args.skip_publish_export,
        fail_on_empty=args.fail_on_empty,
    )
