{{ config(
    materialized='table'
) }}

with base as (
    select
        if(notEmpty(ifNull(organization_login, '')), organization_login, owner_login) as owner_group,
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
    owner_group,
    count() as repo_count,
    sum(stargazers_count) as total_stars,
    avg(stargazers_count) as avg_stars,
    sum(forks_count) as total_forks,
    sum(watchers_count) as total_watchers,
    sum(open_issues_count) as total_open_issues,
    sum(if(archived = 1, 1, 0)) as archived_repo_count,
    sum(
        if(
            isNull(pushed_at),
            0,
            if(dateDiff('day', toDate(pushed_at), today()) <= 30, 1, 0)
        )
    ) as active_repo_count,
    uniqExact(language) as language_count,
    max(meta_fetched_at) as snapshot_at
from base
group by owner_group
order by total_stars desc, repo_count desc
