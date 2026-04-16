select
    owner_group,
    repo_count,
    active_repo_count
from {{ ref('dm_owner_summary') }}
where active_repo_count > repo_count
