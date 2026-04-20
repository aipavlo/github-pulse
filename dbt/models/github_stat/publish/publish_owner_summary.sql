{{ config(
    tags=['publish']
) }}

select
    toDate(snapshot_at) as snapshot_date,
    owner_group,
    repo_count,
    total_stars,
    avg_stars,
    total_forks,
    total_watchers,
    total_open_issues,
    archived_repo_count,
    active_repo_count,
    language_count
from {{ ref('dm_owner_summary') }}
order by snapshot_date desc, total_stars desc, repo_count desc, owner_group asc
