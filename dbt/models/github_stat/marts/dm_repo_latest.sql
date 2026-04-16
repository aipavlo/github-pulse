{{ config(
    materialized='table'
) }}

with latest_repositories as (
    select *
    from {{ ref('stg_repositories_latest') }}
)

select
    meta_full_name,
    meta_snapshot_month,
    meta_fetched_at,
    meta_entity,
    meta_owner,
    meta_repo,
    meta_run_date,

    id,
    node_id,
    name,
    full_name,
    private,
    fork,
    archived,
    disabled,
    visibility,

    html_url,
    homepage,
    description,
    default_branch,

    created_at,
    updated_at,
    pushed_at,

    language,
    topics,
    license_key,
    license_name,
    license_spdx_id,
    license_url,

    size,
    stargazers_count,
    watchers_count,
    forks_count,
    open_issues_count,
    network_count,
    subscribers_count,

    has_issues,
    has_projects,
    has_downloads,
    has_wiki,
    has_pages,
    has_discussions,
    allow_forking,
    is_template,

    owner_login,
    owner_id,
    owner_type,
    owner_html_url,
    owner_site_admin,

    organization_login,
    organization_id,
    organization_type,
    organization_html_url,
    organization_site_admin
from latest_repositories
