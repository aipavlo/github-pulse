{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine="ReplacingMergeTree(meta_fetched_at)",
    order_by="(meta_snapshot_month, meta_owner, meta_repo)",
    partition_by="meta_snapshot_month",
    on_schema_change='append_new_columns'
) }}

with raw_data as (

    select
        toStartOfMonth(assumeNotNull(meta_fetched_at)) as meta_snapshot_month,
        assumeNotNull(meta_fetched_at) as meta_fetched_at,

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

    from {{ source('github_raw', 'raw_normalized') }}
    where meta_entity = 'repository'
      and meta_fetched_at is not null

    {% if is_incremental() %}
      and assumeNotNull(meta_fetched_at) > (
          select coalesce(
              max(meta_fetched_at),
              toDateTime64('1970-01-01 00:00:00', 3)
          )
          from {{ this }}
      )
    {% endif %}

)

select *
from raw_data