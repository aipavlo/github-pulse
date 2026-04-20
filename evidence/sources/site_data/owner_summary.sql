select
  cast(snapshot_date as date) as snapshot_date,
  cast(owner_group as varchar) as owner_group,
  cast(repo_count as bigint) as repo_count,
  cast(total_stars as bigint) as total_stars,
  cast(avg_stars as double) as avg_stars,
  cast(total_forks as bigint) as total_forks,
  cast(total_watchers as bigint) as total_watchers,
  cast(total_open_issues as bigint) as total_open_issues,
  cast(archived_repo_count as bigint) as archived_repo_count,
  cast(active_repo_count as bigint) as active_repo_count,
  cast(language_count as bigint) as language_count
from read_csv_auto('sources/site_data/current/owner_summary.csv', header = true)
order by snapshot_date desc, total_stars desc, repo_count desc, owner_group asc
