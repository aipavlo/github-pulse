# GitHub Repository Radar

This project collects public GitHub repository metadata and builds simple analytics tables in ClickHouse. It helps us see which repositories are active, popular, and growing over time.

The stack: Python for ingestion, ClickHouse for storage, dbt for transformations and data tests, Lightdash for BI, and Prefect for an end-to-end flow orchestration.

## Reproducibility

The easiest way to run is with the `Makefile`.

1. Create `.env` from the example and add `GITHUB_TOKEN` if you have one (optional)

```bash
make env
```

2. Build the local images:

```bash
make build
```

3. Start the services:

```bash
make up
```

4. Run the full pipeline with Prefect:

```bash
make prefect-run
```

`RUN_DATE` uses the current month by default.

Before running the flow, validate the Python ingestion code:

```bash
make qa-python
```

For only transformations:

```bash
make dbt-run
make dbt-test
```
