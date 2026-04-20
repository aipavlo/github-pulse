# GitHub Pulse

GitHub Pulse collects public GitHub repository metadata, builds analytics tables in ClickHouse, and publishes a static analytics site with Evidence and GitHub Pages.

The project is meant to answer a few practical questions in a simple, reproducible way:
- which repositories are the most visible and active;
- how stars, forks, and activity change over time;
- which owners, languages, and topics dominate the dataset.

## Architecture

Target publication flow:

`Prefect -> ingestion -> dbt run -> dbt test -> export static datasets -> commit datasets branch -> PR checks -> merge to main -> tag release -> Evidence build -> GitHub Pages`

Core rules:
- GitHub Actions and GitHub Pages never connect to ClickHouse.
- Prefect never commits, pushes, tags, or otherwise writes to git.
- The site is built only from files committed to the repository.
- Published datasets are replaced atomically instead of accumulating over time.
- Site build output is not committed to git.
- Everything under `evidence/sources/site_data/current/` is public data.

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

## Deployment

The delivery model has three explicit steps:

1. Data contour: Prefect and dbt prepare data, then export refreshes `evidence/sources/site_data/current/` locally.
2. Git contour: the updated public datasets are committed to a separate branch, reviewed through PR checks, and merged to `main`.
3. Delivery contour: a `pages-*` tag triggers GitHub Actions, which reads committed datasets only, runs `npm run sources` and `npm run build:strict`, then deploys `evidence/build/` to GitHub Pages.

This keeps data production, git publication, and Pages deployment separate and auditable.

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
