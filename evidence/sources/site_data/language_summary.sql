select
  cast(snapshot_date as date) as snapshot_date,
  cast(language as varchar) as language,
  cast(repo_count as bigint) as repo_count,
  cast(total_stars as bigint) as total_stars,
  cast(avg_stars as double) as avg_stars,
  cast(median_stars as double) as median_stars,
  cast(total_forks as bigint) as total_forks,
  cast(avg_forks as double) as avg_forks,
  cast(total_watchers as bigint) as total_watchers,
  cast(total_open_issues as bigint) as total_open_issues,
  cast(archived_repo_count as bigint) as archived_repo_count,
  cast(stale_repo_count as bigint) as stale_repo_count
from read_csv_auto('sources/site_data/current/language_summary.csv', header = true)
order by snapshot_date desc, total_stars desc, repo_count desc, language asc
