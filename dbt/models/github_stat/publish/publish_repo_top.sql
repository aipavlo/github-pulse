{{ config(
    tags=['publish']
) }}

with ranked_repositories as (
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
    from {{ ref('dm_repo_popularity_ranking') }}
),

latest_repository_details as (
    select
        meta_full_name,
        html_url,
        description
    from {{ ref('dm_repo_latest') }}
)

select
    toDate(ranked_repositories.meta_fetched_at) as snapshot_date,
    ranked_repositories.meta_fetched_at as generated_at,
    ranked_repositories.popularity_rank,
    ranked_repositories.meta_full_name as repo_full_name,
    ranked_repositories.meta_owner as owner_login,
    ranked_repositories.meta_repo as repo_name,
    latest_repository_details.html_url as repo_url,
    latest_repository_details.description,
    ranked_repositories.language as primary_language,
    ranked_repositories.stargazers_count,
    ranked_repositories.forks_count,
    ranked_repositories.watchers_count,
    ranked_repositories.days_since_push,
    ranked_repositories.repo_popularity_score,
    ranked_repositories.archived as is_archived,
    ranked_repositories.fork as is_fork
from ranked_repositories
inner join latest_repository_details
    on ranked_repositories.meta_full_name = latest_repository_details.meta_full_name
order by snapshot_date desc, popularity_rank asc, repo_full_name asc
