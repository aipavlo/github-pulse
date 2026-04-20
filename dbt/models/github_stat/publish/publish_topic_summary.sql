{{ config(
    tags=['publish']
) }}

select
    toDate(snapshot_at) as snapshot_date,
    topic,
    repo_count,
    repo_uniq_count,
    total_stars,
    avg_stars,
    median_stars,
    total_forks,
    total_watchers,
    archived_repo_count
from {{ ref('dm_repo_topic_summar') }}
order by snapshot_date desc, total_stars desc, repo_count desc, topic asc
