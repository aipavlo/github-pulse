{{ config(
    tags=['publish']
) }}

with latest_repositories as (
    select
        meta_fetched_at,
        meta_full_name,
        if(notEmpty(ifNull(organization_login, '')), organization_login, owner_login) as owner_group,
        if(language = '', 'Unknown', language) as primary_language,
        stargazers_count,
        forks_count,
        archived,
        pushed_at,
        topics
    from {{ ref('stg_repositories_latest') }}
),

topic_stats as (
    select
        uniqExact(topic) as topic_count
    from (
        select arrayJoin(if(length(topics) = 0, ['no_topic'], topics)) as topic
        from latest_repositories
    )
)

select
    toDate(max(meta_fetched_at)) as snapshot_date,
    max(meta_fetched_at) as generated_at,
    count() as repo_count,
    uniqExact(owner_group) as owner_count,
    uniqExact(primary_language) as language_count,
    any(topic_count) as topic_count,
    sum(stargazers_count) as total_stars,
    sum(forks_count) as total_forks,
    sum(
        if(
            isNull(pushed_at),
            0,
            if(dateDiff('day', toDate(pushed_at), today()) <= 30, 1, 0)
        )
    ) as active_repo_count,
    sum(if(archived = 1, 1, 0)) as archived_repo_count
from latest_repositories
cross join topic_stats
order by snapshot_date desc
