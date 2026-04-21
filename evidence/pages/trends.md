---
title: Trends
---

{@partial "site_nav.md"}

# Trends

Monthly trend data across the published repository set.

```sql monthly_totals
select
  snapshot_month,
  sum(stargazers_count) as total_stars,
  sum(forks_count) as total_forks,
  sum(watchers_count) as total_watchers,
  sum(open_issues_count) as total_open_issues
from site_data.repo_trend_monthly
group by snapshot_month
order by snapshot_month
```

```sql repo_trends
select
  snapshot_month,
  repo_full_name,
  primary_language,
  stargazers_count,
  forks_count,
  watchers_count,
  open_issues_count,
  repo_popularity_score
from site_data.repo_trend_monthly
order by snapshot_month desc, repo_full_name
limit 100
```

<LineChart
  data={monthly_totals}
  x="snapshot_month"
  y="total_stars"
  title="Stars by Month"
/>

<LineChart
  data={monthly_totals}
  x="snapshot_month"
  y="total_forks"
  title="Forks by Month"
/>

<DataTable data={repo_trends} />
