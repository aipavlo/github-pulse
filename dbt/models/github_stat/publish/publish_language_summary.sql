{{ config(
    tags=['publish'],
    engine='MergeTree()',
    order_by='(snapshot_date, language)',
    partition_by='toYYYYMM(snapshot_date)'
) }}

select
    toDate(snapshot_at) as snapshot_date,
    language,
    repo_count,
    total_stars,
    avg_stars,
    median_stars,
    total_forks,
    avg_forks,
    total_watchers,
    total_open_issues,
    archived_repo_count,
    stale_repo_count
from {{ ref('dm_repo_language_summary') }}
order by snapshot_date desc, total_stars desc, repo_count desc, language asc
