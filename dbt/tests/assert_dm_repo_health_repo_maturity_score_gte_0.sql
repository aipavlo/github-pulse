select
    meta_full_name,
    repo_maturity_score
from {{ ref('dm_repo_health') }}
where repo_maturity_score < 0
