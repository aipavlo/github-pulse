select
  cast(snapshot_date as date) as snapshot_date,
  cast(generated_at as timestamptz) as generated_at,
  cast(repo_count as bigint) as repo_count,
  cast(owner_count as bigint) as owner_count,
  cast(language_count as bigint) as language_count,
  cast(topic_count as bigint) as topic_count,
  cast(total_stars as bigint) as total_stars,
  cast(total_forks as bigint) as total_forks,
  cast(active_repo_count as bigint) as active_repo_count,
  cast(archived_repo_count as bigint) as archived_repo_count
from read_csv_auto('sources/site_data/current/site_kpis.csv', header = true)
order by snapshot_date desc
