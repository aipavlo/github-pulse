{{ config(
    tags=['publish']
) }}

with monthly_snapshots as (
    select
        meta_snapshot_month,
        meta_fetched_at,
        meta_full_name,
        meta_owner,
        meta_repo,
        if(language = '', 'Unknown', language) as primary_language,
        stargazers_count,
        forks_count,
        watchers_count,
        open_issues_count,
        row_number() over (
            partition by meta_full_name, meta_snapshot_month
            order by meta_fetched_at desc
        ) as snapshot_rank
    from {{ ref('stg_repositories') }} final
),

latest_monthly_snapshots as (
    select
        meta_snapshot_month,
        meta_fetched_at,
        meta_full_name,
        meta_owner,
        meta_repo,
        primary_language,
        stargazers_count,
        forks_count,
        watchers_count,
        open_issues_count
    from monthly_snapshots
    where snapshot_rank = 1
),

scored_snapshots as (
    select
        meta_snapshot_month as snapshot_month,
        meta_full_name as repo_full_name,
        meta_owner as owner_login,
        meta_repo as repo_name,
        primary_language,
        stargazers_count,
        forks_count,
        watchers_count,
        open_issues_count,
        round(
            0.6 * log1p(toFloat64(stargazers_count))
          + 0.3 * log1p(toFloat64(forks_count))
          + 0.1 * log1p(toFloat64(watchers_count)),
            4
        ) as repo_popularity_score
    from latest_monthly_snapshots
)

select
    snapshot_month,
    repo_full_name,
    owner_login,
    repo_name,
    primary_language,
    stargazers_count,
    forks_count,
    watchers_count,
    open_issues_count,
    repo_popularity_score
from scored_snapshots
order by snapshot_month asc, repo_full_name asc
