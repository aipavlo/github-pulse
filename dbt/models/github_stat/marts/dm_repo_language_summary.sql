{{ config(
    materialized='table'
) }}

with base as (
    select
        if(language = '', 'Unknown', language) as language,
        stargazers_count,
        forks_count,
        watchers_count,
        open_issues_count,
        archived,
        pushed_at,
        meta_fetched_at
    from {{ ref('stg_repositories_latest') }}
)

select
    language,
    count() as repo_count,
    sum(stargazers_count) as total_stars,
    avg(stargazers_count) as avg_stars,
    quantileExact(0.5)(stargazers_count) as median_stars,
    sum(forks_count) as total_forks,
    avg(forks_count) as avg_forks,
    sum(watchers_count) as total_watchers,
    sum(open_issues_count) as total_open_issues,
    sum(if(archived = 1, 1, 0)) as archived_repo_count,
    sum(
        if(
            isNull(pushed_at),
            0,
            if(dateDiff('day', toDate(pushed_at), today()) > 90, 1, 0)
        )
    ) as stale_repo_count,
    max(meta_fetched_at) as snapshot_at
from base
group by language
order by total_stars desc, repo_count desc
