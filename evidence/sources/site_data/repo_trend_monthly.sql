select
  cast(snapshot_month as date) as snapshot_month,
  cast(repo_full_name as varchar) as repo_full_name,
  cast(owner_login as varchar) as owner_login,
  cast(repo_name as varchar) as repo_name,
  cast(primary_language as varchar) as primary_language,
  cast(stargazers_count as bigint) as stargazers_count,
  cast(forks_count as bigint) as forks_count,
  cast(watchers_count as bigint) as watchers_count,
  cast(open_issues_count as bigint) as open_issues_count,
  cast(repo_popularity_score as double) as repo_popularity_score
from read_csv_auto('sources/site_data/current/repo_trend_monthly.csv', header = true)
order by snapshot_month asc, repo_full_name asc
