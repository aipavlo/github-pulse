{{ config(
    materialized='view'
) }}

select
    meta_snapshot_month,
    meta_fetched_at,

    meta_entity,
    meta_owner,
    meta_repo,
    meta_full_name,
    meta_run_date,

    id,
    node_id,
    name,
    full_name,
    private,

    html_url,
    description,
    fork,

    created_at,
    updated_at,
    pushed_at,

    homepage,
    size,
    stargazers_count,
    watchers_count,
    forks_count,
    open_issues_count,

    language,
    has_issues,
    has_projects,
    has_downloads,
    has_wiki,
    has_pages,
    has_discussions,

    archived,
    disabled,
    allow_forking,
    is_template,
    visibility,
    default_branch,

    topics,

    network_count,
    subscribers_count,

    owner_login,
    owner_id,
    owner_type,
    owner_html_url,
    owner_site_admin,

    organization_login,
    organization_id,
    organization_type,
    organization_html_url,
    organization_site_admin,

    license_key,
    license_name,
    license_spdx_id,
    license_url

from (
    select
        *,
        row_number() over (
            partition by meta_full_name
            order by meta_fetched_at desc
        ) as rn
    from {{ ref('stg_repositories') }} final
)
where rn = 1