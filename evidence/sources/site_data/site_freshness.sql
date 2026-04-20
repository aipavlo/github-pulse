select
  cast(build_id as varchar) as build_id,
  cast(snapshot_date as date) as snapshot_date,
  cast(source_run_date as date) as source_run_date,
  cast(generated_at as timestamptz) as generated_at,
  cast(dbt_test_passed as boolean) as dbt_test_passed,
  cast(dataset_version as integer) as dataset_version,
  date_diff('hour', cast(generated_at as timestamp), current_timestamp) as hours_since_generated,
  date_diff('day', cast(snapshot_date as date), current_date) as days_since_snapshot,
  case
    when cast(dbt_test_passed as boolean) is true
      and date_diff('hour', cast(generated_at as timestamp), current_timestamp) <= 48
    then true
    else false
  end as is_fresh,
  cast(datasets_dir as varchar) as datasets_dir
from read_json_auto('sources/site_data/current/build_meta.json')
