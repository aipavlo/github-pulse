---
title: GitHub Pulse
---

{@partial "site_nav.md"}

# GitHub Pulse

Static analytics site for public GitHub repository datasets.

```sql kpis
select *
from site_data.site_kpis
limit 1
```

```sql freshness
select *
from site_data.site_freshness
limit 1
```

```sql monthly_totals
select
  snapshot_month,
  sum(stargazers_count) as total_stars,
  sum(forks_count) as total_forks,
  sum(watchers_count) as total_watchers
from site_data.repo_trend_monthly
group by snapshot_month
order by snapshot_month
```

```sql top_repos
select
  popularity_rank,
  repo_full_name,
  primary_language,
  stargazers_count,
  forks_count,
  watchers_count,
  repo_popularity_score
from site_data.repo_top
order by popularity_rank
limit 15
```

## Snapshot

<BigValue data={kpis} value="repo_count" title="Repositories" />
<BigValue data={kpis} value="total_stars" title="Stars" />
<BigValue data={kpis} value="total_forks" title="Forks" />
<BigValue data={freshness} value="days_since_snapshot" title="Days Since Snapshot" />

## Portfolio Momentum

<LineChart
  data={monthly_totals}
  x="snapshot_month"
  y="total_stars"
  title="Stars by Month"
/>

<DataTable data={top_repos} />
