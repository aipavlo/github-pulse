select
    language,
    repo_count,
    archived_repo_count
from {{ ref('dm_repo_language_summary') }}
where archived_repo_count > repo_count
