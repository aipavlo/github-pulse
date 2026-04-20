select
  cast(snapshot_date as date) as snapshot_date,
  cast(generated_at as timestamptz) as generated_at,
  cast(popularity_rank as integer) as popularity_rank,
  cast(repo_full_name as varchar) as repo_full_name,
  cast(owner_login as varchar) as owner_login,
  cast(repo_name as varchar) as repo_name,
  cast(repo_url as varchar) as repo_url,
  nullif(cast(description as varchar), '') as description,
  cast(primary_language as varchar) as primary_language,
  cast(stargazers_count as bigint) as stargazers_count,
  cast(forks_count as bigint) as forks_count,
  cast(watchers_count as bigint) as watchers_count,
  cast(days_since_push as integer) as days_since_push,
  cast(repo_popularity_score as double) as repo_popularity_score,
  cast(is_archived as boolean) as is_archived,
  cast(is_fork as boolean) as is_fork
from read_csv_auto('sources/site_data/current/repo_top.csv', header = true)
order by snapshot_date desc, popularity_rank asc, repo_full_name asc
