## Project

GitHub Pulse is an end-to-end analytics project for turning raw GitHub repository metadata into a reproducible public dashboard. It uses: 
- Prefect for orchestration, 
- ClickHouse as the warehouse, 
- dbt for tested transformations, 
- Evidence for the dashboard, 
- and GitHub Actions/GitHub Pages for cloud publication.

Live dashboard: https://aipavlo.github.io/github-pulse/

# GitHub Pulse

GitHub Pulse collects public GitHub repository metadata, builds analytics tables in ClickHouse, and publishes a static analytics site with Evidence and GitHub Pages.

The ClickHouse layer is materialized through dbt models: staging, marts, and publish-ready tables are built by dbt directly in ClickHouse, with `ORDER BY` and `PARTITION BY` used where needed to keep storage and reads efficient, while `ReplacingMergeTree` is used in the snapshot staging layer to help deduplicate repository versions and keep the latest records consistent.

This project is designed and evaluated as a batch-orchestrated pipeline with Prefect-driven scheduled ingestion and transformation, not as a streaming pipeline.

ClickHouse storage is optimized for the dashboard query patterns: monthly snapshot tables are partitioned by snapshot month, repository-level tables are ordered around repository and snapshot keys, and publish-facing summary tables are ordered around snapshot date plus the main reporting dimension, which keeps reads aligned with views such as `repo_top`, `topic_summary`, and `repo_trend_monthly`.

The project is meant to help a learner, mentor, or maintainer quickly understand which GitHub repositories in the dataset are worth following, comparing, or revisiting, and to support simple decisions about visibility, activity, and topic/language focus.

In practice, the dashboard answers a few concrete questions:
- which repositories are the most visible and active;
- how stars, forks, and activity change over time;
- which owners, languages, and topics dominate the dataset.

Key use cases: compare repositories by visibility and activity, spot stale vs active projects, and see which languages, owners, and topics are most represented in the snapshot.

Metric definitions: `repo_popularity_score` is a simple score derived from stars, forks, and watchers; `repo_maturity_score` is a simple repository-quality score based on visible project signals; `freshness_bucket` classifies repositories by how recently they were pushed.

## Architecture

Target publication flow:

`Prefect -> ingestion -> dbt run -> dbt test -> export static datasets -> commit datasets branch -> PR checks -> merge to main -> v*.*.* tag -> Evidence build -> GitHub Pages`

Core rules:
- GitHub Actions and GitHub Pages never connect to ClickHouse.
- Prefect never commits, pushes, tags, or otherwise writes to git.
- The site is built only from files committed to the repository.
- Published datasets are replaced atomically instead of accumulating over time.
- Site build output is not committed to git.
- Everything under `evidence/sources/site_data/current/` is public data.
- Local raw snapshots are mounted into a dedicated ClickHouse `user_files_path` outside `/var/lib/clickhouse` to avoid fragile startup issues caused by bind mounts inside the managed data directory.

## Stack

- `Python` for ingestion, export, and orchestration utilities
- `ClickHouse` as the warehouse
- `dbt` for publish-ready models and data tests
- `Prefect` for end-to-end orchestration
- `Evidence` for the static site
- `GitHub Actions + GitHub Pages` for build and deployment

## Quick Start

Prepare the local environment:

```bash
make env
make build
make up
```

The canonical local end-to-end run path is:

```bash
make run-local-e2e
```

If you want a minimal warehouse startup check before the full pipeline, run:

```bash
make smoke-clickhouse
```

Run the main local validation flow:

```bash
make qa-python
make dbt-deps
make dbt-run
make dbt-test
make export-site-data
make check-site
```

If you want the full orchestration run through Prefect:

```bash
make prefect-run
```

`make prefect-run` updates only the local public dataset directory. It does not perform git commit, push, or tag operations. `RUN_DATE` defaults to the first day of the current month.

Text example of a successful flow: `find_repositories -> fetch_repositories -> dbt_deps -> dbt_run -> dbt_test -> prepare_site_export_dir -> export_site_data -> validate_site_export -> replace_site_data_atomically`.

For a controlled monthly backfill, override `RUN_DATE`, for example: `make prefect-run RUN_DATE=2026-03-01` or `make export-site-data RUN_DATE=2026-03-01`. This reruns one logical monthly snapshot at a time instead of changing the default current-month run.

## Site

The Evidence app lives in `evidence/` and is configured as a project site with `basePath=/github-pulse`.

Useful commands:

```bash
make evidence-install
make evidence-dev
make evidence-build
make pages-build-local
```

- `make evidence-dev` starts the local dev server at `http://localhost:3000`
- `make evidence-build` runs a strict local build
- `make pages-build-local` refreshes flat-file sources and produces a Pages-ready artifact in `evidence/build/`

## What Gets Committed

Committed to git:
- ingestion, dbt, orchestration, and site code;
- public datasets and metadata in `evidence/sources/site_data/current/`;
- CI/CD configuration and tests.

Not committed to git:
- `evidence/build/`
- `evidence/.evidence/`
- `evidence/sources/site_data/_tmp/`
- `node_modules`, npm caches, and local build caches
- secrets, tokens, and ClickHouse access details

## Secrets

Local GitHub API access uses `GITHUB_TOKEN` from `.env` or the shell environment. Use the smallest practical scope for a personal access token, keep it local-only, and do not commit it, print it in logs, or export it into `evidence/sources/site_data/current/`. Public datasets and build artifacts must contain repository metadata only and no credentials, database secrets, or private connection details.

## Trust Boundaries

The local ingestion and transformation contour is private and includes the GitHub token, raw snapshot files, Prefect runs, and ClickHouse access. The git publication contour starts only after public datasets are exported into `evidence/sources/site_data/current/` and reviewed as normal repository content. GitHub Pages is a separate public delivery contour that reads committed static artifacts only and never connects back to ClickHouse or the local runtime.

## Deployment

The delivery model has three explicit steps:

1. Data contour: Prefect and dbt prepare data, then export refreshes `evidence/sources/site_data/current/` locally.
2. Git contour: the updated public datasets are committed to a separate branch, reviewed through PR checks, and merged to `main`.
3. Delivery contour: a `v*.*.*` tag, such as `v0.0.1`, triggers GitHub Actions, which reads committed datasets only, runs `npm run sources` and `npm run build:strict`, then deploys `evidence/build/` to GitHub Pages.

Release path: `v*.*.* tag -> deploy Pages`.

This keeps data production, git publication, and Pages deployment separate and auditable.

CI/CD contract: `validate` happens on pull requests and main-branch workflow changes, `release` starts only from a version tag after datasets are already committed, and `deploy` happens only after the tagged Pages artifact is built successfully. If the tagged build fails, there is no Pages deploy, and the safe rollback path is to fix the issue and publish a new version tag.

## Troubleshooting

- Empty datasets: run `make dbt-test`, then retry `make export-site-data`; use the flow with `--fail-on-empty` when needed.
- dbt packages: `make dbt-run` and `make prefect-run` run `dbt deps` first; use `make dbt-deps` to refresh packages directly.
- `npm run sources`: this usually means the files in `evidence/sources/site_data/current/` are missing or invalid.
- `npm run build:strict`: make sure `make evidence-install` and `make check-site` were run first.
- `basePath`: GitHub Pages builds must use `/github-pulse`; this is fixed in `evidence/evidence.config.yaml`.
- Orphan cleanup: temporary export directories are cleaned automatically, and `make clean-site-data-tmp` is available for manual cleanup.

## Validation

Use this as the main local validation command:

```bash
make check
```

It runs Python QA, the dbt layer, static dataset export, and the site build validation flow.
