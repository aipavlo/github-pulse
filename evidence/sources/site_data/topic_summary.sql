select
  cast(snapshot_date as date) as snapshot_date,
  cast(topic as varchar) as topic,
  cast(repo_count as bigint) as repo_count,
  cast(repo_uniq_count as bigint) as repo_uniq_count,
  cast(total_stars as bigint) as total_stars,
  cast(avg_stars as double) as avg_stars,
  cast(median_stars as double) as median_stars,
  cast(total_forks as bigint) as total_forks,
  cast(total_watchers as bigint) as total_watchers,
  cast(archived_repo_count as bigint) as archived_repo_count
from read_csv_auto('sources/site_data/current/topic_summary.csv', header = true)
order by snapshot_date desc, total_stars desc, repo_count desc, topic asc
