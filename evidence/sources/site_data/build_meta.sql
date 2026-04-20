select
  cast(build_id as varchar) as build_id,
  cast(generated_at as timestamptz) as generated_at,
  cast(snapshot_date as date) as snapshot_date,
  cast(source_run_date as date) as source_run_date,
  cast(dbt_test_passed as boolean) as dbt_test_passed,
  cast(datasets_dir as varchar) as datasets_dir,
  cast(dataset_version as integer) as dataset_version
from read_json_auto('sources/site_data/current/build_meta.json')
