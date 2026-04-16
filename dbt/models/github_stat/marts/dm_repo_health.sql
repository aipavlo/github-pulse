{{ config(
    materialized='table'
) }}

with base as (
    select
        meta_full_name,
        meta_owner,
        meta_repo,
        owner_login,
        organization_login,
        meta_snapshot_month,
        meta_fetched_at,
        if(language = '', 'Unknown', language) as language,
        license_name,
        homepage,
        archived,
        disabled,
        fork,
        has_issues,
        has_discussions,
        has_wiki,
        has_pages,
        allow_forking,
        stargazers_count,
        forks_count,
        watchers_count,
        subscribers_count,
        network_count,
        open_issues_count,
        created_at,
        updated_at,
        pushed_at
    from {{ ref('stg_repositories_latest') }}
),

scored as (
    select
        *,
        if(isNull(created_at), null, dateDiff('day', toDate(created_at), today())) as repo_age_days,
        if(isNull(updated_at), null, dateDiff('day', toDate(updated_at), today())) as days_since_update,
        if(isNull(pushed_at), null, dateDiff('day', toDate(pushed_at), today())) as days_since_push,

        greatest(
            0,
            toInt32(
                if(notEmpty(ifNull(license_name, '')), 1, 0)
              + if(notEmpty(ifNull(homepage, '')), 1, 0)
              + if(has_issues = 1, 1, 0)
              + if(has_discussions = 1, 1, 0)
              + if(has_wiki = 1, 1, 0)
              + if(has_pages = 1, 1, 0)
              + if(allow_forking = 1, 1, 0)
              - if(archived = 1, 2, 0)
              - if(disabled = 1, 2, 0)
            )
        ) as repo_maturity_score,

        round(
            0.6 * log1p(toFloat64(stargazers_count))
          + 0.3 * log1p(toFloat64(forks_count))
          + 0.1 * log1p(toFloat64(watchers_count)),
            4
        ) as repo_popularity_score,

        multiIf(
            isNull(pushed_at), 'unknown',
            dateDiff('day', toDate(pushed_at), today()) <= 30, 'active',
            dateDiff('day', toDate(pushed_at), today()) <= 90, 'warm',
            dateDiff('day', toDate(pushed_at), today()) <= 180, 'stale',
            'cold'
        ) as freshness_bucket
    from base
)

select
    meta_full_name,
    meta_owner,
    meta_repo,
    owner_login,
    organization_login,
    meta_snapshot_month,
    meta_fetched_at,
    language,
    license_name,
    archived,
    disabled,
    fork,
    stargazers_count,
    forks_count,
    watchers_count,
    subscribers_count,
    network_count,
    open_issues_count,
    created_at,
    updated_at,
    pushed_at,
    repo_age_days,
    days_since_update,
    days_since_push,
    repo_maturity_score,
    repo_popularity_score,
    freshness_bucket
from scored
