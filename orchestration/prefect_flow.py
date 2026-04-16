import argparse
import os
import subprocess
from pathlib import Path

from prefect import flow, task


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "dbt"


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
def dbt_run():
    run_command(["dbt", "run"], cwd=DBT_DIR)


@task(log_prints=True)
def dbt_test():
    run_command(["dbt", "test"], cwd=DBT_DIR)


@flow(name="github-repository-radar")
def github_repository_radar_flow(run_date="2026-03-01", refresh_urls=True):
    os.environ["DBT_PROFILES_DIR"] = str(DBT_DIR)

    if refresh_urls:
        find_repositories()

    fetch_repositories(run_date)
    dbt_run()
    dbt_test()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default="2026-03-01")
    parser.add_argument("--skip-find", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    github_repository_radar_flow(
        run_date=args.run_date,
        refresh_urls=not args.skip_find,
    )
