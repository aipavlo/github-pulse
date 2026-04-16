{{ config(
    materialized='table'
) }}

with exploded_topics as (
    select
        meta_full_name,
        meta_fetched_at,
        archived,
        stargazers_count,
        forks_count,
        watchers_count,
        arrayJoin(
            if(length(topics) = 0, ['no_topic'], topics)
        ) as topic
    from {{ ref('stg_repositories_latest') }}
)

select
    topic,
    count() as repo_count,
    uniqExact(meta_full_name) as repo_uniq_count,
    sum(stargazers_count) as total_stars,
    avg(stargazers_count) as avg_stars,
    quantileExact(0.5)(stargazers_count) as median_stars,
    sum(forks_count) as total_forks,
    sum(watchers_count) as total_watchers,
    sum(if(archived = 1, 1, 0)) as archived_repo_count,
    max(meta_fetched_at) as snapshot_at
from exploded_topics
group by topic
order by total_stars desc, repo_count desc
