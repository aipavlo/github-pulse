---
title: Use Cases
---

{@partial "site_nav.md"}

# Use Cases

This page groups the published snapshot around a few practical questions instead of only entity slices.

```sql kpis
select *
from site_data.site_kpis
limit 1
```

```sql top_repos
select
  popularity_rank,
  repo_full_name,
  primary_language,
  stargazers_count,
  forks_count,
  watchers_count
from site_data.repo_top
order by popularity_rank
limit 10
```

```sql active_owners
select
  owner_group,
  active_repo_count,
  total_stars
from site_data.owner_summary
order by active_repo_count desc, total_stars desc
limit 10
```

## What should I look at first?

<BigValue data={kpis} value="repo_count" title="Repositories" />
<BigValue data={kpis} value="active_repo_count" title="Active Repositories" />

## Which repositories stand out?

<DataTable data={top_repos} />

## Which owners are most active?

<DataTable data={active_owners} />
