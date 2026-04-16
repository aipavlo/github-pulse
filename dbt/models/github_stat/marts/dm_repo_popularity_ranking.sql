{{ config(
    materialized='table'
) }}

with base as (
    select
        meta_full_name,
        meta_owner,
        meta_repo,
        meta_fetched_at,
        if(language = '', 'Unknown', language) as language,
        archived,
        fork,
        pushed_at,
        stargazers_count,
        forks_count,
        watchers_count
    from {{ ref('stg_repositories_latest') }}
),

scored as (
    select
        *,
        round(
            0.6 * log1p(toFloat64(stargazers_count))
          + 0.3 * log1p(toFloat64(forks_count))
          + 0.1 * log1p(toFloat64(watchers_count)),
            4
        ) as repo_popularity_score
    from base
),

ranked as (
    select
        dense_rank() over (
            order by repo_popularity_score desc, stargazers_count desc, meta_full_name asc
        ) as popularity_rank,
        meta_full_name,
        meta_owner,
        meta_repo,
        meta_fetched_at,
        language,
        archived,
        fork,
        stargazers_count,
        forks_count,
        watchers_count,
        if(isNull(pushed_at), null, dateDiff('day', toDate(pushed_at), today())) as days_since_push,
        repo_popularity_score
    from scored
)

select
    popularity_rank,
    meta_full_name,
    meta_owner,
    meta_repo,
    meta_fetched_at,
    language,
    archived,
    fork,
    stargazers_count,
    forks_count,
    watchers_count,
    days_since_push,
    repo_popularity_score
from ranked
