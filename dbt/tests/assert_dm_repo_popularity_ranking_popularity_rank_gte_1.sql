select
    meta_full_name,
    popularity_rank
from {{ ref('dm_repo_popularity_ranking') }}
where popularity_rank is null
   or popularity_rank < 1
